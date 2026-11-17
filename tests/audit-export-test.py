#!/usr/bin/env python3
import copy
import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
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
        self.output.write_text(content[:content.rfind('{"type":"COMPLETE"')])
        with self.assertRaises(ValueError):
            audit.verify_export(self.output)
        self.output.write_text(content + '{"type":"EVENT"}\n')
        with self.assertRaises(ValueError):
            audit.verify_export(self.output)

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
