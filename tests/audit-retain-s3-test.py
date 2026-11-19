#!/usr/bin/env python3
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
RETENTION = ROOT / "tools" / "audit-retain-s3.py"
spec = importlib.util.spec_from_file_location("audit_retention", RETENTION)
retention = importlib.util.module_from_spec(spec)
spec.loader.exec_module(retention)

AWS_SCRIPT = '''
import base64
import hashlib
import json
import os
from pathlib import Path
import sys

assert os.environ["AWS_IGNORE_CONFIGURED_ENDPOINT_URLS"] == "true"
assert sys.argv[1] == "s3api"
action = sys.argv[2]
options = sys.argv[3:]
directory = Path(__file__).parent
with (directory / "calls.jsonl").open("a") as calls:
    calls.write(json.dumps([action, *options]) + "\\n")

def value(name):
    return options[options.index(name) + 1]

assert value("--bucket") == "rivomesh-worm-fixture"
assert value("--expected-bucket-owner") == "012345678901"
assert value("--region") == "us-east-1"
assert value("--key") == os.environ["FIXTURE_KEY"]
behavior = os.environ["FIXTURE_BEHAVIOR"]
metadata = directory / "metadata.json"
if action == "put-object":
    assert value("--if-none-match") == "*"
    assert value("--object-lock-mode") == "COMPLIANCE"
    assert value("--checksum-algorithm") == "SHA256"
    body = Path(value("--body")).read_bytes()
    checksum = base64.b64encode(hashlib.sha256(body).digest()).decode()
    assert value("--checksum-sha256") == checksum
    if metadata.exists() or behavior == "absent":
        print("cli-private-diagnostic", file=sys.stderr)
        sys.exit(1)
    (directory / "uploaded.json").write_bytes(body)
    record = {"VersionId": "test-version", "ChecksumSHA256": checksum, "ContentLength": len(body),
              "ObjectLockMode": "COMPLIANCE", "ObjectLockRetainUntilDate": value("--object-lock-retain-until-date")}
    metadata.write_text(json.dumps(record))
    if behavior == "lost-response":
        sys.exit(1)
    print(json.dumps({"VersionId": "test-version"}))
elif action == "head-object":
    if not metadata.exists():
        sys.exit(1)
    record = json.loads(metadata.read_text())
    if "--version-id=test-version" in options:
        assert value("--checksum-mode") == "ENABLED"
        if behavior == "wrong-version":
            record["VersionId"] = "other-version"
        elif behavior == "wrong-digest":
            record["ChecksumSHA256"] = "fixture-mismatch"
        elif behavior == "wrong-length":
            record["ContentLength"] += 1
        elif behavior == "wrong-mode":
            record["ObjectLockMode"] = "GOVERNANCE"
        elif behavior == "missing-mode":
            del record["ObjectLockMode"]
        elif behavior == "early-expiry":
            record["ObjectLockRetainUntilDate"] = "2020-01-01T00:00:00Z"
        elif behavior == "missing-deadline":
            del record["ObjectLockRetainUntilDate"]
    print(json.dumps(record))
else:
    raise AssertionError(action)
'''


def export_window(path, through, store_id="scope-1", after=0):
    previous, events = "0" * 64, []
    for sequence in range(1, through + 1):
        event = {"version": 1, "sequence": sequence, "id": "audit-" + str(sequence), "actorId": "event-data-only"}
        payload = json.dumps(event, separators=(",", ":"))
        current = retention.audit.digest(f"{sequence}\n{previous}\n{payload}")
        event.update(previousHash=previous, hash=current)
        events.append({"type": "EVENT", "event": event})
        previous = current
    boundary = events[after - 1]["event"]["hash"] if after else "0" * 64
    records = [{"type": "HEADER", "version": 1, "storeId": store_id, "after": after, "anchorSequence": after,
                "anchorHash": boundary, "previousHash": boundary, "through": through, "throughHash": previous},
               *events[after:], {"type": "COMPLETE", "through": through, "events": through - after, "hash": previous}]
    path.write_text("".join(json.dumps(record) + "\n" for record in records), encoding="utf-8")


class RetentionTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.workspace = Path(self.directory.name)
        self.export = self.workspace / "audit.jsonl"
        self.checkpoint = self.workspace / "head.json"
        self.signature = self.workspace / "head.sig"
        self.private_key = self.workspace / "signing.pem"
        self.public_key = self.workspace / "verifier.pem"
        self.calls = self.workspace / "calls.jsonl"
        self.uploaded = self.workspace / "uploaded.json"
        self.deadline = (datetime.now(timezone.utc) + timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        fake_cli = self.workspace / "aws"
        fake_cli.write_text("#!" + sys.executable + "\n" + AWS_SCRIPT, encoding="utf-8")
        fake_cli.chmod(0o700)
        export_window(self.export, 2)
        retention.audit.verify_export(self.export, anchor_out=self.checkpoint)
        subprocess.run(["openssl", "genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256",
                        "-out", str(self.private_key)], check=True, capture_output=True)
        subprocess.run(["openssl", "pkey", "-in", str(self.private_key), "-pubout", "-out", str(self.public_key)],
                       check=True, capture_output=True)
        self.sign()

    def sign(self):
        subprocess.run(["openssl", "dgst", "-sha256", "-sign", str(self.private_key), "-out", str(self.signature),
                        str(self.checkpoint)], check=True, capture_output=True)

    def run_retention(self, behavior="ok", store_id="scope-1", deadline=None, *,
                      events=False, previous=None, after=0, through=2):
        prefix = "retention/audit"
        key = f"{prefix}/scope-1/"
        key += f"events/{after:020d}-{through:020d}.jsonl" if events else f"{through:020d}.json"
        environment = {"PATH": str(self.workspace) + os.pathsep + os.environ["PATH"],
                       "FIXTURE_BEHAVIOR": behavior, "FIXTURE_KEY": key,
                       "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false"}
        flags = ["--retain-events"] if events else []
        if previous:
            flags.extend(("--previous-checkpoint", str(previous[0]), "--previous-signature", str(previous[1])))
        return subprocess.run(
            [sys.executable, "-B", str(RETENTION), "--export", str(self.export), "--checkpoint", str(self.checkpoint),
             "--signature", str(self.signature), "--public-key", str(self.public_key), "--store-id", store_id,
             "--bucket", "rivomesh-worm-fixture", "--bucket-owner", "012345678901", "--region", "us-east-1",
             "--prefix", prefix, "--retain-until", deadline or self.deadline, *flags],
            env=environment, text=True, capture_output=True, timeout=45,
        )

    def verify_archive(self, path, previous=None):
        flags = [] if previous is None else ["--previous-checkpoint", str(previous[0]),
                                             "--previous-signature", str(previous[1])]
        return subprocess.run(
            [sys.executable, "-B", str(RETENTION), "--verify-event-archive", str(path),
             "--public-key", str(self.public_key), "--store-id", "scope-1", *flags],
            text=True, capture_output=True, timeout=15,
        )

    def assert_rejected(self, result, reason):
        self.assertEqual(1, result.returncode, result)
        self.assertEqual("", result.stdout)
        self.assertEqual(reason, result.stderr.strip())

    def test_exact_version_receipt_and_identical_retry(self):
        result = self.run_retention()
        self.assertEqual(0, result.returncode, result)
        receipt = json.loads(result.stdout)
        self.assertEqual("AUDIT_CHECKPOINT_RETAINED", receipt["status"])
        self.assertEqual(2, receipt["sequence"])
        self.assertEqual("scope-1", receipt["storeId"])
        self.assertEqual("test-version", receipt["versionId"])
        body = self.uploaded.read_bytes()
        self.assertEqual(retention.encode(hashlib.sha256(body).digest()), receipt["checksumSHA256"])
        envelope = json.loads(body)
        self.assertEqual({"formatVersion", "checkpoint", "signature"}, set(envelope))
        self.assertEqual(self.checkpoint.read_bytes(), base64.b64decode(envelope["checkpoint"]))
        self.assertEqual(self.signature.read_bytes(), base64.b64decode(envelope["signature"]))
        retry = self.run_retention()
        self.assertEqual(result.stdout, retry.stdout)
        self.assertEqual("", retry.stderr)
        actions = [json.loads(line)[0] for line in self.calls.read_text().splitlines()]
        self.assertEqual(["put-object", "head-object", "put-object", "head-object", "head-object"], actions)

    def test_committed_upload_with_lost_response_resolves_exact_version(self):
        result = self.run_retention("lost-response")
        self.assertEqual(0, result.returncode, result)
        self.assertEqual("test-version", json.loads(result.stdout)["versionId"])
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertEqual(["put-object", "head-object", "head-object"], [call[0] for call in calls])
        self.assertNotIn("--version-id=test-version", calls[1])
        self.assertIn("--version-id=test-version", calls[2])

    def test_valid_different_pair_at_same_sequence_is_not_acknowledged(self):
        result = self.run_retention()
        self.assertEqual(0, result.returncode, result)
        original = self.uploaded.read_bytes()
        self.checkpoint.write_bytes(self.checkpoint.read_bytes() + b" ")
        self.sign()
        self.assert_rejected(self.run_retention(), "AUDIT_RETENTION_CONTENT_UNCONFIRMED")
        self.assertEqual(original, self.uploaded.read_bytes())

    def test_retention_requires_confirmed_remote_version_content_and_compliance(self):
        for behavior, error in (("absent", "VERSION"), ("wrong-version", "VERSION"),
                                ("wrong-digest", "CONTENT"), ("wrong-length", "CONTENT"),
                                ("wrong-mode", "LOCK"), ("missing-mode", "LOCK"),
                                ("early-expiry", "LOCK"), ("missing-deadline", "LOCK")):
            with self.subTest(behavior=behavior):
                self.assert_rejected(self.run_retention(behavior), f"AUDIT_RETENTION_{error}_UNCONFIRMED")
                metadata = self.workspace / "metadata.json"
                if metadata.exists():
                    metadata.unlink()

    def test_invalid_inputs_fail_before_contacting_s3(self):
        self.assert_rejected(self.run_retention(store_id="scope-2"), "AUDIT_ANCHOR_STORE_CHANGED")
        self.assert_rejected(self.run_retention(deadline="2020-01-01T00:00:00Z"), "AUDIT_RETENTION_DEADLINE_INVALID")
        export_window(self.export, 3)
        self.assert_rejected(self.run_retention(), "AUDIT_RETENTION_CHECKPOINT_NOT_EXPORT_HEAD")
        self.export.write_text("[1, 2]\n", encoding="utf-8")
        self.assert_rejected(self.run_retention(), "AUDIT_RETENTION_INPUT_UNREADABLE_OR_INVALID")
        export_window(self.export, 2)
        self.signature.write_bytes(b"invalid-signature")
        self.assert_rejected(self.run_retention(), "AUDIT_ANCHOR_SIGNATURE_INVALID")
        self.assertFalse(self.calls.exists())

    def test_complete_event_archive_versions_and_offline_readback(self):
        first = self.run_retention("lost-response", events=True)
        self.assertEqual(0, first.returncode, first)
        receipt = json.loads(first.stdout)
        self.assertEqual("AUDIT_EVENT_ARCHIVE_RETAINED", receipt["status"])
        self.assertEqual(0, receipt["fromExclusive"])
        self.assertEqual(2, receipt["eventCount"])
        archive = self.uploaded.read_bytes()
        raw_header, raw_export = archive.split(b"\n", 1)
        envelope = json.loads(raw_header)
        self.assertEqual("RIVOMESH_SIGNED_AUDIT", envelope["type"])
        self.assertEqual(self.checkpoint.read_bytes(), base64.b64decode(envelope["checkpoint"], validate=True))
        self.assertEqual(self.signature.read_bytes(), base64.b64decode(envelope["signature"], validate=True))
        self.assertEqual(self.export.read_bytes(), raw_export)
        verified = self.verify_archive(self.uploaded)
        self.assertEqual(0, verified.returncode, verified)
        self.assertEqual("AUDIT_EVENT_ARCHIVE_VERIFIED", json.loads(verified.stdout)["status"])
        self.assertEqual(receipt["checksumSHA256"], json.loads(verified.stdout)["checksumSHA256"])
        retry = self.run_retention(events=True)
        self.assertEqual(first.stdout, retry.stdout)
        self.export.write_bytes(self.export.read_bytes().replace(b"\n", b"\r\n"))
        self.assert_rejected(self.run_retention(events=True), "AUDIT_RETENTION_CONTENT_UNCONFIRMED")
        self.assertEqual(archive, self.uploaded.read_bytes())

    def test_rolling_event_archive_needs_previous_signed_checkpoint(self):
        previous = self.checkpoint, self.signature
        self.checkpoint, self.signature = self.workspace / "next.json", self.workspace / "next.sig"
        export_window(self.export, 4, after=2)
        retention.audit.verify_export(self.export, anchor_out=self.checkpoint)
        self.sign()

        self.assert_rejected(self.run_retention(events=True), "AUDIT_RETENTION_PREVIOUS_CHECKPOINT_REQUIRED")
        self.assertFalse(self.calls.exists())
        result = self.run_retention(events=True, previous=previous, after=2, through=4)
        self.assertEqual(0, result.returncode, result)
        receipt = json.loads(result.stdout)
        self.assertEqual(4, receipt["sequence"])
        self.assertEqual(2, receipt["fromExclusive"])
        self.assertEqual(2, receipt["eventCount"])
        self.assertEqual(0, self.verify_archive(self.uploaded, previous).returncode)
        self.assert_rejected(self.verify_archive(self.uploaded), "AUDIT_RETENTION_PREVIOUS_CHECKPOINT_REQUIRED")

        export_window(self.export, 5, after=3)
        self.checkpoint.unlink()
        retention.audit.verify_export(self.export, anchor_out=self.checkpoint)
        self.sign()
        self.calls.unlink()
        self.assert_rejected(self.run_retention(events=True, previous=previous, after=3, through=5),
                             "AUDIT_ANCHOR_OUTSIDE_EXPORT_WINDOW")
        self.assertFalse(self.calls.exists())

    def test_tampered_event_fails_offline_and_before_storage(self):
        result = self.run_retention(events=True)
        self.assertEqual(0, result.returncode, result)
        self.uploaded.write_bytes(self.uploaded.read_bytes().replace(b"event-data-only", b"modified-event"))
        self.assert_rejected(self.verify_archive(self.uploaded), "AUDIT_RETENTION_INPUT_UNREADABLE_OR_INVALID")
        self.export.write_bytes(self.export.read_bytes().replace(b"event-data-only", b"modified-event"))
        self.calls.unlink()
        self.assert_rejected(self.run_retention(events=True), "AUDIT_RETENTION_INPUT_UNREADABLE_OR_INVALID")
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
