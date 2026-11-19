#!/usr/bin/env python3
"""Retain signed Rivomesh audit checkpoints or full event windows under S3 Object Lock."""
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


CHUNK_BYTES = 1024 * 1024
MAX_SINGLE_PUT_BYTES = 5_000_000_000
ARCHIVE_HEADER_BYTES = 32 * 1024


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


def s3(action, destination, *options, timeout=30):
    # Only the enrolled AWS destination is supported. Ignore endpoint overrides in shared CLI configuration.
    environment = dict(os.environ, AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true")
    try:
        result = subprocess.run(
            ["aws", "s3api", action, *destination, *options, "--output", "json", "--no-cli-pager",
             "--no-cli-auto-prompt", "--cli-connect-timeout", "5", "--cli-read-timeout", "10"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env=environment, timeout=timeout,
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


def confirmed_version(args, requested, key, body_path, checksum, size, mime, timeout=30):
    destination = ("--bucket", args.bucket, "--key", key, "--expected-bucket-owner", args.bucket_owner,
                   "--region", args.region)
    put = s3("put-object", destination, "--body", str(body_path), "--content-type", mime,
             "--if-none-match", "*", "--checksum-algorithm", "SHA256", "--checksum-sha256", checksum,
             "--object-lock-mode", "COMPLIANCE", "--object-lock-retain-until-date", args.retain_until,
             timeout=timeout)
    version = version_id(put)
    if version is None:
        # Resolve a lost response or a conditional retry against the exact stored version.
        version = version_id(s3("head-object", destination))
    if version is None:
        raise RetentionError("AUDIT_RETENTION_VERSION_UNCONFIRMED")
    retained = s3("head-object", destination, "--version-id=" + version, "--checksum-mode", "ENABLED")
    if version_id(retained) != version:
        raise RetentionError("AUDIT_RETENTION_VERSION_UNCONFIRMED")
    if retained.get("ChecksumSHA256") != checksum or retained.get("ContentLength") != size:
        raise RetentionError("AUDIT_RETENTION_CONTENT_UNCONFIRMED")
    try:
        deadline = timestamp(retained.get("ObjectLockRetainUntilDate"))
    except ValueError as error:
        raise RetentionError("AUDIT_RETENTION_LOCK_UNCONFIRMED") from error
    if retained.get("ObjectLockMode") != "COMPLIANCE" or deadline < requested or deadline <= datetime.now(timezone.utc):
        raise RetentionError("AUDIT_RETENTION_LOCK_UNCONFIRMED")
    return {"bucket": args.bucket, "key": key, "versionId": version,
            "checksumSHA256": checksum, "retainUntil": deadline.isoformat()}


def copy_archive(source_path, destination, header=b""):
    size, checksum = len(header), hashlib.sha256(header)
    with source_path.open("rb") as source, destination.open("wb") as target:
        target.write(header)
        for chunk in iter(lambda: source.read(CHUNK_BYTES), b""):
            size += len(chunk)
            if size > MAX_SINGLE_PUT_BYTES:
                raise RetentionError("AUDIT_RETENTION_ARCHIVE_TOO_LARGE")
            checksum.update(chunk)
            target.write(chunk)
    return encode(checksum.digest()), size


def verified_window(args, archive, workspace):
    offset, header = read_event_archive(archive, workspace)
    checkpoint, signature, verified_head = (workspace / name for name in ("checkpoint.json", "checkpoint.sig", "head.json"))
    count = audit.verify_export(archive, checkpoint, verified_head, anchor_signature=signature,
                                anchor_public_key=args.public_key, anchor_store_id=args.store_id, offset=offset)
    head = audit.load_anchor(verified_head)
    if audit.load_anchor(checkpoint) != head:
        raise RetentionError("AUDIT_RETENTION_CHECKPOINT_NOT_EXPORT_HEAD")

    if args.previous_checkpoint is not None:
        audit.verify_export(archive, args.previous_checkpoint, anchor_signature=args.previous_signature,
                            anchor_public_key=args.public_key, anchor_store_id=args.store_id, offset=offset)
    elif header["after"] != 0 or header["previousHash"] != "0" * 64:
        raise RetentionError("AUDIT_RETENTION_PREVIOUS_CHECKPOINT_REQUIRED")
    return {"storeId": head["storeId"], "sequence": head["sequence"], "checkpointHash": head["hash"],
            "fromExclusive": header["after"], "previousHash": header["previousHash"], "eventCount": count}


def read_event_archive(archive, workspace):
    with archive.open("rb") as stream:
        if os.fstat(stream.fileno()).st_size > MAX_SINGLE_PUT_BYTES:
            raise RetentionError("AUDIT_RETENTION_ARCHIVE_TOO_LARGE")
        raw = stream.readline(ARCHIVE_HEADER_BYTES + 1)
        if len(raw) > ARCHIVE_HEADER_BYTES or not raw.endswith(b"\n"):
            raise RetentionError("AUDIT_RETENTION_ARCHIVE_HEADER_INVALID")
        envelope = json.loads(raw.decode("ascii"), object_pairs_hook=audit.strict_pairs)
        if (not isinstance(envelope, dict) or set(envelope) != {"type", "formatVersion", "checkpoint", "signature"}
                or envelope["type"] != "RIVOMESH_SIGNED_AUDIT"
                or type(envelope["formatVersion"]) is not int or envelope["formatVersion"] != 1):
            raise RetentionError("AUDIT_RETENTION_ARCHIVE_HEADER_INVALID")
        for field, name, limit in (("checkpoint", "checkpoint.json", 4096), ("signature", "checkpoint.sig", 16384)):
            content = base64.b64decode(envelope[field], validate=True)
            if not 0 < len(content) <= limit:
                raise RetentionError("AUDIT_RETENTION_ARCHIVE_HEADER_INVALID")
            (workspace / name).write_bytes(content)
        window_header = stream.readline(CHUNK_BYTES + 1)
        if len(window_header) > CHUNK_BYTES:
            raise RetentionError("AUDIT_RETENTION_ARCHIVE_HEADER_INVALID")
    return len(raw), json.loads(window_header, object_pairs_hook=audit.strict_pairs)


def verify_archive(args):
    with tempfile.TemporaryDirectory() as directory:
        workspace = Path(directory)
        archive = workspace / "retained-events.jsonl"
        checksum, _ = copy_archive(args.verify_event_archive, archive)
        window = verified_window(args, archive, workspace)
    return {"status": "AUDIT_EVENT_ARCHIVE_VERIFIED", **window, "checksumSHA256": checksum}


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
        checkpoint_bytes = read_artifact(args.checkpoint, 4096)
        signature_bytes = read_artifact(args.signature, 16384)
        envelope = {"formatVersion": 1, "checkpoint": encode(checkpoint_bytes), "signature": encode(signature_bytes)}
        if args.retain_events:
            archive = workspace / "retained-events.jsonl"
            header = json.dumps({"type": "RIVOMESH_SIGNED_AUDIT", **envelope}, separators=(",", ":")) + "\n"
            checksum, size = copy_archive(args.export, archive, header.encode("ascii"))
            window = verified_window(args, archive, workspace)
            key = f"{args.prefix}/{window['storeId']}/events/{window['fromExclusive']:020d}-{window['sequence']:020d}.jsonl"
            retained = confirmed_version(args, requested, key, archive, checksum, size, "application/x-ndjson", timeout=600)
            return {"status": "AUDIT_EVENT_ARCHIVE_RETAINED", **window, **retained}

        checkpoint, signature, verified_head = (workspace / name for name in ("checkpoint.json", "checkpoint.sig", "head.json"))
        checkpoint.write_bytes(checkpoint_bytes)
        signature.write_bytes(signature_bytes)
        audit.verify_export(args.export, checkpoint, verified_head, anchor_signature=signature,
                            anchor_public_key=args.public_key, anchor_store_id=args.store_id)
        head = audit.load_anchor(verified_head)
        if audit.load_anchor(checkpoint) != head:
            raise RetentionError("AUDIT_RETENTION_CHECKPOINT_NOT_EXPORT_HEAD")

        key = f"{args.prefix}/{head['storeId']}/{head['sequence']:020d}.json"
        body = (json.dumps(envelope, separators=(",", ":")) + "\n").encode("ascii")
        checksum = encode(hashlib.sha256(body).digest())
        body_path = workspace / "retained-checkpoint.json"
        body_path.write_bytes(body)
        retained = confirmed_version(args, requested, key, body_path, checksum, len(body), "application/json")
    return {"status": "AUDIT_CHECKPOINT_RETAINED", "storeId": head["storeId"], "sequence": head["sequence"],
            "checkpointHash": head["hash"], **retained}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--retain-events", action="store_true", help="Retain the complete signed JSONL event window")
    mode.add_argument("--verify-event-archive", type=Path, help="Verify a downloaded event archive without using S3")
    parser.add_argument("--public-key", type=Path, required=True)
    parser.add_argument("--store-id", required=True)
    parser.add_argument("--previous-checkpoint", type=Path, help="Independently retained preceding signed audit head")
    parser.add_argument("--previous-signature", type=Path)
    paths = ("export", "checkpoint", "signature")
    destination = ("bucket", "bucket-owner", "region", "prefix", "retain-until")
    for option in paths:
        parser.add_argument("--" + option, type=Path)
    for option in destination:
        parser.add_argument("--" + option)
    args = parser.parse_args()
    previous = (args.previous_checkpoint, args.previous_signature)
    if any(previous) and (not all(previous) or not (args.retain_events or args.verify_event_archive is not None)):
        parser.error("Event continuity requires both --previous-checkpoint and --previous-signature")
    storage_args = [getattr(args, option.replace("-", "_")) for option in (*paths, *destination)]
    if args.verify_event_archive is not None:
        if any(value is not None for value in storage_args):
            parser.error("Offline verification uses the archive and independent verification inputs")
    elif None in storage_args:
        parser.error("Retention requires --" + ", --".join((*paths, *destination)))
    try:
        receipt = verify_archive(args) if args.verify_event_archive is not None else retain(args)
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
