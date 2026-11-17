#!/usr/bin/env python3
"""Stream one fixed retained audit window to a private JSONL file; verify each hash before publishing it."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def strict_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON property")
        result[key] = value
    return result


def verify_page(page, after=None, through=None, previous=None):
    if after is not None and page["after"] != after:
        raise ValueError("Retention moved: export is incomplete")
    if through is not None and page["through"] != through:
        raise ValueError("Audit window changed")
    if previous is not None and page["previousHash"] != previous:
        raise ValueError("Audit page boundary changed")
    sequence, current = page["after"], page["previousHash"]
    for event in page["events"]:
        payload = {key: value for key, value in event.items() if key not in ("previousHash", "hash")}
        sequence += 1
        if event["sequence"] != sequence or event["previousHash"] != current:
            raise ValueError("Audit sequence gap or previous hash mismatch")
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        current = digest(str(sequence) + "\n" + current + "\n" + encoded)
        if current != event["hash"]:
            raise ValueError("Audit event content hash mismatch")
    if page["next"] is None:
        if sequence != page["through"] or current != page["throughHash"]:
            raise ValueError("Audit window tail mismatch")
    elif page["next"] != sequence or sequence >= page["through"]:
        raise ValueError("Invalid continuation")
    return sequence, current


def verify_export(path):
    """Verify a saved JSONL export with bounded memory, including its completion marker."""
    with Path(path).open(encoding="utf-8") as stream:
        def record():
            line = stream.readline(1024 * 1024 + 1)
            if len(line) > 1024 * 1024:
                raise ValueError("Export record exceeds limit")
            return json.loads(line, object_pairs_hook=strict_pairs) if line else None
        header = record()
        if not header or header.get("type") != "HEADER" or header.get("version") != 1:
            raise ValueError("Missing export header")
        sequence, previous, count = header["after"], header["previousHash"], 0
        if sequence == header["anchorSequence"] and previous != header["anchorHash"]:
            raise ValueError("Export retention anchor mismatch")
        while True:
            item = record()
            if not item:
                raise ValueError("Incomplete export")
            if item.get("type") == "EVENT":
                event = item["event"]
                sequence, previous = verify_page({"after": sequence, "through": sequence + 1, "previousHash": previous, "throughHash": event["hash"], "next": None, "events": [event]})
                count += 1
            elif item.get("type") == "COMPLETE":
                if sequence != header["through"] or previous != header["throughHash"] or item.get("through") != sequence or item.get("events") != count or item.get("hash") != previous or record() is not None:
                    raise ValueError("Export completion mismatch")
                return count
            else:
                raise ValueError("Unknown export record")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def export(base, output, token, page_size=1000):
    uri = urlsplit(base)
    if uri.scheme not in ("http", "https") or not uri.hostname or uri.username or uri.password or uri.query or uri.fragment or uri.path not in ("", "/"):
        raise ValueError("Use an HTTP(S) console origin without credentials, query or path")
    if not token or len(token) > 4096 or any(ord(char) < 33 or ord(char) > 126 for char in token):
        raise ValueError("Set RIVOMESH_TOKEN to the administrator token")
    output = Path(output)
    if output.exists():
        raise ValueError("Output already exists")
    partial = output.with_name(output.name + ".partial")
    descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    opener = build_opener(NoRedirect())
    after, through, previous, expected_through_hash, count, store_id = None, None, None, None, 0, None
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
        def write(item):
            stream.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
        while True:
            query = {"limit": page_size}
            if after is not None:
                query.update(after=after, through=through)
            request = Request(base.rstrip("/") + "/api/audit/export?" + urlencode(query), headers={"Authorization": "Bearer " + token})
            try:
                response = opener.open(request, timeout=30)
            except HTTPError as error:
                error.close()
                raise
            with response:
                raw = response.read(16 * 1024 * 1024 + 1)
                if len(raw) > 16 * 1024 * 1024:
                    raise ValueError("Audit page exceeds limit")
                page = json.loads(raw.decode("utf-8"), object_pairs_hook=strict_pairs)
            sequence, chain_hash = verify_page(page, after, through, previous)
            if through is None:
                through, expected_through_hash, store_id = page["through"], page["throughHash"], page["storeId"]
                write({"type": "HEADER", "version": 1, "storeId": store_id, "anchorSequence": page["anchorSequence"], "anchorHash": page["anchorHash"], "after": page["after"], "previousHash": page["previousHash"], "through": through, "throughHash": expected_through_hash})
            elif page["throughHash"] != expected_through_hash or page["storeId"] != store_id:
                raise ValueError("Audit tail anchor changed")
            for event in page["events"]:
                write({"type": "EVENT", "event": event})
                count += 1
            stream.flush()
            os.fsync(stream.fileno())
            if page["next"] is None:
                if chain_hash != expected_through_hash:
                    raise ValueError("Incomplete audit export")
                write({"type": "COMPLETE", "events": count, "through": through, "hash": chain_hash})
                stream.flush()
                os.fsync(stream.fileno())
                break
            after, previous = sequence, chain_hash
    # No replacement of an existing export, including one created during the download.
    os.link(partial, output)
    partial.unlink()
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="Console origin; never put the token in the URL")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--verify-page", type=Path, help="Verify a console page JSON export without authentication")
    parser.add_argument("--verify", type=Path, help="Verify a saved complete JSONL export without authentication")
    args = parser.parse_args()
    try:
        if args.verify:
            print("AUDIT_EXPORT_VERIFIED: " + str(verify_export(args.verify)) + " events")
        elif args.verify_page:
            page = json.loads(args.verify_page.read_text(encoding="utf-8"), object_pairs_hook=strict_pairs)
            verify_page(page)
            print("AUDIT_PAGE_VERIFIED: " + str(len(page["events"])) + " events; continuation=" + str(page["next"]))
        else:
            if not args.url or not args.output:
                parser.error("--url and --output are required")
            count = export(args.url, args.output, os.environ.get("RIVOMESH_TOKEN"))
            print("AUDIT_EXPORT_COMPLETE: " + str(count) + " events")
    except Exception as error:
        # HTTPError text can contain a URL supplied by the caller. Do not print exception values or bodies.
        print("AUDIT_EXPORT_FAILED: " + ("HTTP_" + str(error.code) if isinstance(error, HTTPError) else type(error).__name__) + "; any .partial file is not a complete export", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
