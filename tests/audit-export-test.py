#!/usr/bin/env python3
import copy
import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

spec = importlib.util.spec_from_file_location("audit_export", Path(__file__).resolve().parents[1] / "tools/audit-export.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class ExportTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.output = Path(self.directory.name) / "export.jsonl"
        previous, self.events = "0" * 64, []
        for sequence in range(1, 4):
            event = {"version": 1, "sequence": sequence, "id": "event-" + str(sequence), "actorId": "operator", "status": "OK"}
            current = audit.digest(str(sequence) + "\n" + previous + "\n" + json.dumps(event, ensure_ascii=False, separators=(",", ":")))
            event.update(previousHash=previous, hash=current)
            self.events.append(event)
            previous = current
        self.pages = [self.page(0, self.events[:2], 2), self.page(2, self.events[2:], None)]
        self.requests = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append((self.path, self.headers.get("Authorization")))
                item = owner.pages[min(len(owner.requests) - 1, len(owner.pages) - 1)]
                if item == "REDIRECT":
                    self.send_response(302)
                    self.send_header("Location", "/never-follow-with-token")
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(item).encode())

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = "http://127.0.0.1:" + str(self.server.server_port)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.directory.cleanup()

    def page(self, after, events, next_value):
        return {"storeId": "store-1", "after": after, "through": 3, "next": next_value, "previousHash": events[0]["previousHash"], "throughHash": self.events[-1]["hash"], "anchorSequence": 0, "anchorHash": "0" * 64, "events": copy.deepcopy(events)}

    def run_export(self):
        return audit.export(self.base, self.output, "administrator-private-token-value", 2)

    def saved_window(self, path, after, through, store="store-1", events=None):
        events = self.events if events is None else events
        previous = events[after - 1]["hash"] if after else "0" * 64
        tail = events[through - 1]["hash"] if through else "0" * 64
        records = [{"type": "HEADER", "version": 1, "storeId": store,
                    "anchorSequence": after, "anchorHash": previous, "after": after,
                    "previousHash": previous, "through": through, "throughHash": tail}]
        records.extend({"type": "EVENT", "event": event} for event in events[after:through])
        records.append({"type": "COMPLETE", "events": through - after, "through": through, "hash": tail})
        path.write_text("".join(json.dumps(item) + "\n" for item in records), encoding="utf-8")

    def test_streams_fixed_window_and_publishes_private_completed_file(self):
        self.assertEqual(3, self.run_export())
        records = [json.loads(line) for line in self.output.read_text().splitlines()]
        self.assertEqual(["HEADER", "EVENT", "EVENT", "EVENT", "COMPLETE"], [item["type"] for item in records])
        self.assertEqual(0o600, self.output.stat().st_mode & 0o777)
        self.assertIn("after=2&through=3", self.requests[1][0])
        self.assertNotIn("administrator-private-token-value", self.output.read_text())
        self.assertEqual(3, audit.verify_export(self.output))

    def test_saved_export_rejects_missing_completion_and_appended_records(self):
        self.run_export()
        content = self.output.read_text()
        head = self.output.with_name("head.json")
        self.output.write_text(content[:content.rfind('{"type":"COMPLETE"')])
        with self.assertRaises(ValueError):
            audit.verify_export(self.output, anchor_out=head)
        self.assertFalse(head.exists())
        self.output.write_text(content + '{"type":"EVENT"}\n')
        with self.assertRaises(ValueError):
            audit.verify_export(self.output)

    def test_anchor_record_from_export_is_private_and_append_rechecks_earlier_tail(self):
        self.run_export()
        earlier = self.output.with_name("earlier.jsonl")
        trusted = self.output.with_name("trusted.json")
        updated = self.output.with_name("updated.json")
        self.saved_window(earlier, 0, 1)
        self.assertEqual(1, audit.verify_export(earlier, anchor_out=trusted))
        self.assertEqual({"formatVersion": 1, "storeId": "store-1", "sequence": 1, "hash": self.events[0]["hash"]},
                         json.loads(trusted.read_text()))
        self.assertEqual(0o600, trusted.stat().st_mode & 0o777)
        result = subprocess.run([sys.executable, audit.__file__, "--verify", str(self.output),
                                 "--against-anchor", str(trusted), "--anchor-out", str(updated)],
                                check=True, text=True, capture_output=True)
        self.assertIn("AUDIT_ANCHOR_MATCHED", result.stdout)
        self.assertIn("AUDIT_ANCHOR_WRITTEN", result.stdout)
        self.assertEqual(self.events[-1]["hash"], json.loads(updated.read_text())["hash"])
        with self.assertRaisesRegex(audit.AuditAnchorError, "AUDIT_ANCHOR_OUTPUT_EXISTS"):
            audit.verify_export(self.output, against_anchor=trusted, anchor_out=trusted)
        self.assertEqual(self.events[0]["hash"], json.loads(trusted.read_text())["hash"])

    def test_anchor_checks_window_retention_rollback_and_store_identity(self):
        self.run_export()
        trusted = self.output.with_name("trusted.json")
        earlier = self.output.with_name("earlier.jsonl")
        self.saved_window(earlier, 0, 2)
        audit.verify_export(earlier, anchor_out=trusted)
        for after, through, store, error in ((0, 1, "store-1", "AUDIT_ANCHOR_TAIL_ROLLBACK"),
                                              (3, 3, "store-1", "AUDIT_ANCHOR_OUTSIDE_EXPORT_WINDOW"),
                                              (0, 3, "another-store", "AUDIT_ANCHOR_STORE_CHANGED")):
            with self.subTest(after=after, through=through, store=store):
                self.saved_window(earlier, after, through, store)
                self.assertEqual(through - after, audit.verify_export(earlier))
                with self.assertRaisesRegex(audit.AuditAnchorError, error):
                    audit.verify_export(earlier, against_anchor=trusted)
        self.saved_window(earlier, 2, 3)
        self.assertEqual(1, audit.verify_export(earlier, against_anchor=trusted))

    def test_signed_checkpoint_requires_the_trusted_key_exact_bytes_and_pinned_store(self):
        directory = self.output.parent
        earlier = directory / "earlier.jsonl"
        checkpoint = directory / "checkpoint.json"
        signature = directory / "checkpoint.sig"
        updated = directory / "updated.json"
        private_key = directory / "private.pem"
        public_key = directory / "public.pem"
        wrong_private_key = directory / "wrong-private.pem"
        wrong_public_key = directory / "wrong-public.pem"
        for private, public in ((private_key, public_key), (wrong_private_key, wrong_public_key)):
            subprocess.run(["openssl", "genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256",
                            "-out", str(private)], check=True, capture_output=True)
            subprocess.run(["openssl", "pkey", "-in", str(private), "-pubout", "-out", str(public)],
                           check=True, capture_output=True)

        def sign():
            subprocess.run(["openssl", "dgst", "-sha256", "-sign", str(private_key), "-out", str(signature),
                            str(checkpoint)], check=True, capture_output=True)

        def verify(key=public_key, store="store-1", environment=None):
            return subprocess.run([sys.executable, audit.__file__, "--verify", str(self.output),
                                   "--against-anchor", str(checkpoint), "--anchor-signature", str(signature),
                                   "--anchor-public-key", str(key), "--anchor-store-id", store,
                                   "--anchor-out", str(updated)], text=True, capture_output=True, env=environment)

        def assert_rejected(error, **options):
            result = verify(**options)
            self.assertEqual(1, result.returncode, result)
            self.assertEqual(error, result.stderr.strip())
            self.assertEqual("", result.stdout)
            self.assertFalse(updated.exists())

        self.saved_window(earlier, 0, 1)
        self.saved_window(self.output, 1, 3)
        audit.verify_export(earlier, anchor_out=checkpoint)
        sign()
        accepted = verify()
        self.assertEqual(0, accepted.returncode, accepted)
        self.assertIn("AUDIT_ANCHOR_SIGNATURE_VERIFIED", accepted.stdout)
        self.assertIn("AUDIT_ANCHOR_MATCHED", accepted.stdout)
        self.assertEqual(self.events[-1]["hash"], json.loads(updated.read_text())["hash"])
        updated.unlink()

        assert_rejected("AUDIT_ANCHOR_SIGNATURE_INVALID", key=wrong_public_key)
        assert_rejected("AUDIT_ANCHOR_SIGNATURE_VERIFIER_UNAVAILABLE", environment={"PATH": str(directory)})
        raw = checkpoint.read_bytes()
        for modified in (raw + b" ", raw.replace(b"store-1", b"store-2")):
            checkpoint.write_bytes(modified)
            assert_rejected("AUDIT_ANCHOR_SIGNATURE_INVALID")
        checkpoint.write_bytes(raw)
        signature.write_bytes(b"malformed signature")
        assert_rejected("AUDIT_ANCHOR_SIGNATURE_INVALID")
        signature.unlink()
        assert_rejected("AUDIT_ANCHOR_SIGNATURE_UNREADABLE")

        # The alternate scope uses the same key, sequence and chain hashes, with a valid signature.
        checkpoint.write_bytes(raw.replace(b"store-1", b"store-2"))
        sign()
        self.saved_window(self.output, 1, 3, store="store-2")
        assert_rejected("AUDIT_ANCHOR_STORE_CHANGED")
        self.saved_window(self.output, 1, 3)
        assert_rejected("AUDIT_ANCHOR_STORE_CHANGED", store="store-2")

    def test_signed_checkpoint_options_cannot_be_partially_supplied(self):
        self.saved_window(self.output, 0, 3)
        directory = self.output.parent
        options = ["--against-anchor", str(directory / "earlier.json"),
                   "--anchor-signature", str(directory / "earlier.sig"),
                   "--anchor-public-key", str(directory / "public.pem"),
                   "--anchor-store-id", "store-1"]
        for index in range(0, len(options), 2):
            with self.subTest(missing=options[index]):
                result = subprocess.run([sys.executable, audit.__file__, "--verify", str(self.output),
                                         *options[:index], *options[index + 2:]], text=True, capture_output=True)
                self.assertEqual(1, result.returncode, result)
                self.assertEqual("AUDIT_ANCHOR_SIGNATURE_OPTIONS_REQUIRED", result.stderr.strip())
                self.assertEqual("", result.stdout)

    def test_recomputed_chain_cannot_replace_an_independently_anchored_tail(self):
        self.run_export()
        trusted = self.output.with_name("prior-head.json")
        audit.verify_export(self.output, anchor_out=trusted)
        rewritten = copy.deepcopy(self.events)
        rewritten[0]["actorId"] = "rewritten"
        previous = "0" * 64
        for event in rewritten:
            payload = {key: value for key, value in event.items() if key not in ("previousHash", "hash")}
            encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            current = audit.digest(str(event["sequence"]) + "\n" + previous + "\n" + encoded)
            event.update(previousHash=previous, hash=current)
            previous = current
        self.saved_window(self.output, 0, 3, events=rewritten)
        self.assertEqual(3, audit.verify_export(self.output))
        with self.assertRaisesRegex(audit.AuditAnchorError, "AUDIT_ANCHOR_HASH_MISMATCH"):
            audit.verify_export(self.output, against_anchor=trusted)

    def test_duplicate_and_oversized_anchor_records_refused(self):
        self.run_export()
        trusted = self.output.with_name("trusted.json")
        for content in ('{"storeId":"other","storeId":"store-1"}', " " * 4096 + "{}"):
            trusted.write_text(content, encoding="utf-8")
            with self.assertRaisesRegex(audit.AuditAnchorError, "AUDIT_ANCHOR_INVALID"):
                audit.verify_export(self.output, against_anchor=trusted)

    def test_content_change_does_not_publish_completed_file(self):
        self.pages[1]["events"][0]["actorId"] = "other-actor"
        with self.assertRaises(ValueError):
            self.run_export()
        self.assertFalse(self.output.exists())
        self.assertTrue(self.output.with_name("export.jsonl.partial").exists())

    def test_retention_gap_and_store_replacement_fail(self):
        for field, value in (("after", 3), ("storeId", "replacement")):
            with self.subTest(field=field):
                self.requests.clear()
                self.pages[1] = self.page(2, self.events[2:], None)
                self.pages[1][field] = value
                self.output.with_name("export.jsonl.partial").unlink(missing_ok=True)
                with self.assertRaises(ValueError):
                    self.run_export()
                self.assertFalse(self.output.exists())

    def test_truncated_tail_cannot_claim_complete(self):
        self.pages[1]["events"] = []
        with self.assertRaises(ValueError):
            self.run_export()
        self.assertFalse(self.output.exists())

    def test_does_not_follow_redirects_or_overwrite_previous_export(self):
        self.pages = ["REDIRECT"]
        with self.assertRaises(audit.HTTPError):
            self.run_export()
        self.assertEqual(1, len(self.requests))
        self.output.write_text("existing export")
        with self.assertRaises(ValueError):
            self.run_export()
        self.assertEqual("existing export", self.output.read_text())

    def test_credentials_in_origin_are_rejected_before_any_request(self):
        with self.assertRaises(ValueError):
            audit.export("http://user:password@localhost", self.output, "token")
        self.assertFalse(self.output.exists())
        self.assertEqual([], self.requests)


if __name__ == "__main__":
    unittest.main()
