#!/usr/bin/env python3
"""Confirm external Object Lock retention of a signed Rivomesh audit checkpoint."""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


exporter = importlib.util.spec_from_file_location("audit_export", Path(__file__).with_name("audit-export.py"))
audit = importlib.util.module_from_spec(exporter)
exporter.loader.exec_module(audit)


class RetentionError(ValueError):
    pass


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Invalid timestamp")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Missing timezone")
    return result


def read_artifact(path, limit):
    with path.open("rb") as stream:
        content = stream.read(limit + 1)
    if not 0 < len(content) <= limit:
        raise RetentionError("AUDIT_RETENTION_ARTIFACT_INVALID")
    return content


def encode(data):
    return base64.b64encode(data).decode("ascii")


def s3(action, destination, *options):
    # Only the enrolled AWS destination is supported. Ignore endpoint overrides in shared CLI configuration.
    environment = dict(os.environ, AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true")
    try:
        result = subprocess.run(
            ["aws", "s3api", action, *destination, *options, "--output", "json", "--no-cli-pager",
             "--no-cli-auto-prompt", "--cli-connect-timeout", "5", "--cli-read-timeout", "10"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env=environment, timeout=30,
        )
    except OSError as error:
        raise RetentionError("AUDIT_RETENTION_AWS_CLI_UNAVAILABLE") from error
    except subprocess.TimeoutExpired:
        return {}
    if result.returncode != 0:
        return {}
    try:
        response = json.loads(result.stdout, object_pairs_hook=audit.strict_pairs)
    except ValueError:
        return {}
    return response if isinstance(response, dict) else {}


def version_id(response):
    value = response.get("VersionId")
    if isinstance(value, str) and 0 < len(value) <= 1024 and value != "null" and value.isprintable():
        return value
    return None


def retain(args):
    if (not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", args.bucket)
            or not re.fullmatch(r"[0-9]{12}", args.bucket_owner)
            or not re.fullmatch(r"[a-z]{2}(?:-[a-z0-9]+)+-[0-9]+", args.region)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*(?:/[A-Za-z0-9][A-Za-z0-9._-]*)*", args.prefix)
            or len(args.prefix) > 512):
        raise RetentionError("AUDIT_RETENTION_DESTINATION_INVALID")
    try:
        requested = datetime.strptime(args.retain_until, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise RetentionError("AUDIT_RETENTION_DEADLINE_INVALID") from error
    if requested <= datetime.now(timezone.utc):
        raise RetentionError("AUDIT_RETENTION_DEADLINE_INVALID")

    with tempfile.TemporaryDirectory() as directory:
        workspace = Path(directory)
        checkpoint, signature, verified_head = (workspace / name for name in ("checkpoint.json", "checkpoint.sig", "head.json"))
        checkpoint_bytes = read_artifact(args.checkpoint, 4096)
        signature_bytes = read_artifact(args.signature, 16384)
        checkpoint.write_bytes(checkpoint_bytes)
        signature.write_bytes(signature_bytes)
        audit.verify_export(args.export, checkpoint, verified_head, anchor_signature=signature,
                            anchor_public_key=args.public_key, anchor_store_id=args.store_id)
        head = audit.load_anchor(verified_head)
        if audit.load_anchor(checkpoint) != head:
            raise RetentionError("AUDIT_RETENTION_CHECKPOINT_NOT_EXPORT_HEAD")

        key = f"{args.prefix}/{head['storeId']}/{head['sequence']:020d}.json"
        destination = ("--bucket", args.bucket, "--key", key, "--expected-bucket-owner", args.bucket_owner,
                       "--region", args.region)
        envelope = {"formatVersion": 1, "checkpoint": encode(checkpoint_bytes), "signature": encode(signature_bytes)}
        body = (json.dumps(envelope, separators=(",", ":")) + "\n").encode("ascii")
        checksum = encode(hashlib.sha256(body).digest())
        body_path = workspace / "retained-checkpoint.json"
        body_path.write_bytes(body)
        put = s3("put-object", destination, "--body", str(body_path), "--content-type", "application/json",
                 "--if-none-match", "*", "--checksum-algorithm", "SHA256", "--checksum-sha256", checksum,
                 "--object-lock-mode", "COMPLIANCE", "--object-lock-retain-until-date", args.retain_until)
        version = version_id(put)
        if version is None:
            # A response can be lost after S3 commits. Resolve the current version, then verify that exact version.
            version = version_id(s3("head-object", destination))
        if version is None:
            raise RetentionError("AUDIT_RETENTION_VERSION_UNCONFIRMED")
        retained = s3("head-object", destination, "--version-id=" + version, "--checksum-mode", "ENABLED")
        if version_id(retained) != version:
            raise RetentionError("AUDIT_RETENTION_VERSION_UNCONFIRMED")
        if retained.get("ChecksumSHA256") != checksum or retained.get("ContentLength") != len(body):
            raise RetentionError("AUDIT_RETENTION_CONTENT_UNCONFIRMED")
        try:
            deadline = timestamp(retained.get("ObjectLockRetainUntilDate"))
        except ValueError as error:
            raise RetentionError("AUDIT_RETENTION_LOCK_UNCONFIRMED") from error
        if retained.get("ObjectLockMode") != "COMPLIANCE" or deadline < requested or deadline <= datetime.now(timezone.utc):
            raise RetentionError("AUDIT_RETENTION_LOCK_UNCONFIRMED")
    return {"status": "AUDIT_CHECKPOINT_RETAINED", "storeId": head["storeId"], "sequence": head["sequence"],
            "checkpointHash": head["hash"], "bucket": args.bucket, "key": key, "versionId": version,
            "checksumSHA256": checksum, "retainUntil": deadline.isoformat()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("export", "checkpoint", "signature", "public-key"):
        parser.add_argument("--" + option, type=Path, required=True)
    for option in ("store-id", "bucket", "bucket-owner", "region", "prefix", "retain-until"):
        parser.add_argument("--" + option, required=True)
    try:
        receipt = retain(parser.parse_args())
    except (RetentionError, audit.AuditAnchorError) as error:
        print(str(error), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError, AttributeError, RecursionError):
        print("AUDIT_RETENTION_INPUT_UNREADABLE_OR_INVALID", file=sys.stderr)
        return 1
    print(json.dumps(receipt, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
