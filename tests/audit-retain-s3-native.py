#!/usr/bin/env python3
"""Test Object Lock with a fresh loopback MinIO server and AWS CLI v2."""
import argparse
import base64
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.request import ProxyHandler, build_opener


spec = importlib.util.spec_from_file_location("audit_retention_test", Path(__file__).with_name("audit-retain-s3-test.py"))
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def fixture(workspace, minio, cli):
    port = free_port()
    console_port = free_port()
    while console_port == port:
        console_port = free_port()
    endpoint = f"http://127.0.0.1:{port}"
    access, secret = "rivomesh-fixture", "rivomesh-fixture-secret"
    minio_env = {"PATH": os.environ["PATH"], "MINIO_ROOT_USER": access, "MINIO_ROOT_PASSWORD": secret,
                 "MINIO_BROWSER": "off", "MINIO_UPDATE": "off"}
    aws_config = workspace / "aws-config"
    aws_config.write_text("[default]\nregion = us-east-1\ns3 =\n    addressing_style = path\n", encoding="utf-8")
    aws_env = {"PATH": str(workspace) + os.pathsep + os.environ["PATH"], "AWS_ACCESS_KEY_ID": access,
               "AWS_SECRET_ACCESS_KEY": secret, "AWS_REGION": "us-east-1", "AWS_MAX_ATTEMPTS": "1",
               "AWS_EC2_METADATA_DISABLED": "true", "AWS_CONFIG_FILE": str(aws_config),
               "AWS_SHARED_CREDENTIALS_FILE": str(workspace / "empty-credentials"),
               "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "true"}
    shim = workspace / "aws"
    shim.write_text("#!" + sys.executable + "\nimport os, sys\n"
                    + f"os.execv({str(cli)!r}, [{str(cli)!r}, '--endpoint-url', {endpoint!r}, *sys.argv[1:]])\n",
                    encoding="utf-8")
    shim.chmod(0o700)

    def aws(action, *options, expected_error=None):
        response = subprocess.run([str(shim), "s3api", action, "--no-cli-pager", "--output", "json", *options],
                                  env=aws_env, capture_output=True, text=True, timeout=15)
        if expected_error:
            assert response.returncode and expected_error in response.stderr, (action, response.stderr)
            return None
        assert response.returncode == 0, (action, response.stderr)
        return json.loads(response.stdout) if response.stdout else {}

    with (workspace / "server.log").open("w", encoding="utf-8") as log:
        server = subprocess.Popen([str(minio), "server", "--address", f"127.0.0.1:{port}",
                                   "--console-address", f"127.0.0.1:{console_port}", str(workspace / "data")],
                                  cwd=workspace, env=minio_env, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        try:
            web = build_opener(ProxyHandler({}))
            timeout = time.monotonic() + 20
            while True:
                try:
                    with web.open(endpoint + "/minio/health/ready", timeout=1):
                        break
                except URLError:
                    assert server.poll() is None and time.monotonic() < timeout, (workspace / "server.log").read_text()
                    time.sleep(0.2)

            bucket = "rivomesh-object-lock-fixture"
            aws("create-bucket", "--bucket", bucket, "--object-lock-enabled-for-bucket")
            export, checkpoint, signature, private, public = (workspace / name for name in
                ("audit.jsonl", "head.json", "head.sig", "signer.pem", "trusted.pem"))
            fixtures.export_window(export, 2)
            fixtures.retention.audit.verify_export(export, anchor_out=checkpoint)
            for command in (("genpkey", "-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256", "-out", str(private)),
                            ("pkey", "-in", str(private), "-pubout", "-out", str(public)),
                            ("dgst", "-sha256", "-sign", str(private), "-out", str(signature), str(checkpoint))):
                subprocess.run(["openssl", *command], check=True, capture_output=True, timeout=10)
            deadline = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
            command = [sys.executable, "-B", str(fixtures.RETENTION), "--export", str(export), "--checkpoint", str(checkpoint),
                       "--signature", str(signature), "--public-key", str(public), "--store-id", "scope-1",
                       "--bucket", bucket, "--bucket-owner", "012345678901", "--region", "us-east-1",
                       "--prefix", "rivomesh/audit", "--retain-until", deadline]
            published = subprocess.run(command, env=aws_env, capture_output=True, text=True, timeout=30)
            assert published.returncode == 0, published.stderr
            receipt = json.loads(published.stdout)
            assert receipt["status"] == "AUDIT_CHECKPOINT_RETAINED"
            print("EXACT_VERSION_COMPLIANCE_AND_SHA256_VERIFIED", flush=True)

            retry = subprocess.run(command, env=aws_env, capture_output=True, text=True, timeout=30)
            assert retry.returncode == 0 and retry.stdout == published.stdout, retry.stderr
            print("IDENTICAL_RETRY_RETURNS_SAME_VERSION", flush=True)
            target = ("--bucket", bucket, "--key", receipt["key"])
            versioned = (*target, "--version-id", receipt["versionId"])
            alternative = workspace / "alternative.json"
            alternative.write_text("{}", encoding="utf-8")
            aws("put-object", *target, "--body", str(alternative), "--if-none-match", "*", expected_error="PreconditionFailed")
            print("CONDITIONAL_REPLACEMENT_REFUSED", flush=True)
            aws("delete-object", *versioned, expected_error="WORM protected")
            print("COMPLIANCE_VERSION_DELETE_REFUSED_FOR_STORAGE_ADMIN", flush=True)
            shorter = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
            aws("put-object-retention", *versioned, "--retention", f"Mode=COMPLIANCE,RetainUntilDate={shorter}",
                expected_error="WORM protected")
            print("COMPLIANCE_DEADLINE_SHORTENING_REFUSED", flush=True)

            control = ("--bucket", bucket, "--key", "unlocked-control")
            unlocked = aws("put-object", *control, "--body", str(alternative))
            aws("delete-object", *control, "--version-id", unlocked["VersionId"])
            aws("head-object", *control, "--version-id", unlocked["VersionId"], expected_error="404")
            print("UNLOCKED_CONTROL_VERSION_DELETE_SUCCEEDED", flush=True)

            downloaded = workspace / "retained.json"
            aws("get-object", *versioned, str(downloaded))
            envelope = json.loads(downloaded.read_bytes())
            assert base64.b64decode(envelope["checkpoint"], validate=True) == checkpoint.read_bytes()
            assert base64.b64decode(envelope["signature"], validate=True) == signature.read_bytes()
            print("VERSION_READBACK_HAS_EXACT_SIGNED_ARTIFACTS", flush=True)
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minio", type=Path, required=True)
    parser.add_argument("--aws", type=Path, required=True)
    arguments = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="rivomesh-s3-object-lock-") as directory:
        fixture(Path(directory), arguments.minio.resolve(strict=True), arguments.aws.resolve(strict=True))
