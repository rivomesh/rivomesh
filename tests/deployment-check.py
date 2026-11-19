#!/usr/bin/env python3
"""Offline Helm/native parity checks; optional smoke test against a locally built Docker image."""
import argparse
import json
import pathlib
import secrets
import subprocess
import time
import tempfile
import urllib.error
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()


def manifests():
    chart = "deploy/helm/rivomesh"
    command("helm", "lint", chart)
    rendered = command("helm", "template", "rivomesh", chart, "--namespace", "rivomesh")
    assert "secureCookie: true" in rendered and "idleSeconds: 1800" in rendered
    assert "secureCookie: false" in command("helm", "template", "rivomesh", chart, "--set", "sessions.secureCookie=false")
    assert "publicOrigin: \"https://sync.example.com\"" in command("helm", "template", "rivomesh", chart, "--set", "sessions.publicOrigin=https://sync.example.com")
    assert "value: -Xmx512m" in rendered
    assert "value: -Xmx1024m" in command("helm","template","rivomesh",chart,"--set","controllerHeapMiB=1024")
    assert "auditMaxEvents: 1000000" in rendered
    assert "auditMaxEvents: 2000" in command("helm", "template", "rivomesh", chart, "--set", "platform.auditMaxEvents=2000")
    assert rendered == (ROOT / "deploy/kubernetes/rivomesh.yaml").read_text().strip(), "Native YAML drifted"
    existing = command("helm", "template", "rivomesh", chart, "--set", "persistence.existingClaim=existing-data")
    assert "kind: PersistentVolumeClaim" not in existing and "claimName: existing-data" in existing
    for invalid in ("controllerHeapMiB=64", "controllerHeapMiB=65537", "platform.maxRunningTasks=3,platform.maxTasks=2", "auth.existingSecret=", "platform.auditMaxEvents=999", "platform.auditMaxEvents=10000001", "sessions.idleSeconds=29", "sessions.absoluteSeconds=30", "sessions.maximum=10001"):
        result = subprocess.run(["helm", "template", "rivomesh", chart, "--set", invalid], cwd=ROOT, capture_output=True)
        assert result.returncode != 0, invalid
    roles_values = chart + "/values-rbac.yaml"
    roles = command("helm", "template", "rivomesh", chart, "--namespace", "rivomesh", "-f", roles_values)
    assert roles == (ROOT / "deploy/kubernetes/rivomesh-rbac.yaml").read_text().strip(), "Native RBAC YAML drifted"
    assert "authToken:" not in roles and "name: RIVOMESH_TOKEN\n" not in roles
    assert roles.count("secretKeyRef:") == 3
    for index, role in enumerate(("ADMIN", "OPERATOR", "VIEWER")):
        assert f'role: "{role}"' in roles
        assert "${RIVOMESH_USER_TOKEN_" + str(index) + "}" in roles
        assert "name: RIVOMESH_USER_TOKEN_" + str(index) in roles
    invalid_roles = (
        "auth.legacyTokenEnabled=false,auth.users=[]",
        "auth.users[0].role=VIEWER",
        "auth.users[1].id=migration-admin",
        "auth.users[1].secretRef.key=admin-token",
        "auth.users[0].role=SUPERADMIN",
        "auth.users[0].token=literal-secret-forbidden",
    )
    for invalid in invalid_roles:
        result = subprocess.run(["helm", "template", "rivomesh", chart, "-f", roles_values, "--set", invalid], cwd=ROOT, capture_output=True)
        assert result.returncode != 0, "Invalid auth accepted: " + invalid
    scopes_values = chart + "/values-task-scopes.yaml"
    scopes = command("helm", "template", "rivomesh", chart, "--namespace", "rivomesh", "-f", scopes_values)
    assert scopes == (ROOT / "deploy/kubernetes/rivomesh-task-scopes.yaml").read_text().strip(), "Native task-scope YAML drifted"
    assert 'taskIds: ["sales-migration","orders-migration"]' in scopes
    assert 'taskIds: ["sales-migration"]' in scopes
    for invalid in ("auth.users[0].taskIds[0]=sales-migration", "auth.users[1].taskIds[0]=*", "auth.users[1].taskIds[1]=sales-migration"):
        result = subprocess.run(["helm", "template", "rivomesh", chart, "-f", scopes_values, "--set", invalid], cwd=ROOT, capture_output=True)
        assert result.returncode != 0, "Invalid scope accepted: " + invalid
    with tempfile.TemporaryDirectory(prefix="rivomesh-empty-grants-") as temporary:
        profile = pathlib.Path(temporary) / "values.json"
        profile.write_text(json.dumps({"auth": {"legacyTokenEnabled": False, "users": [
            {"id": "test-admin", "role": "ADMIN", "secretRef": {"name": "test-accounts", "key": "admin-token"}},
            {"id": "empty-operator", "role": "OPERATOR", "taskIds": [], "secretRef": {"name": "test-accounts", "key": "operator-token"}}
        ]}}))
        empty = command("helm", "template", "rivomesh", chart, "-f", str(profile))
        assert "taskIds: []" in empty
    managed = command("helm", "template", "rivomesh", chart, "--namespace", "rivomesh", "-f", chart + "/values-managed.yaml")
    assert managed == (ROOT / "deploy/kubernetes/rivomesh-managed.yaml").read_text().strip(), "Native managed-access YAML drifted"
    assert "accessMode: MANAGED" in managed and "authToken:" not in managed and "secretKeyRef:" not in managed
    invalid = subprocess.run(["helm", "template", "rivomesh", chart, "--set", "auth.mode=UNKNOWN"], cwd=ROOT, capture_output=True)
    assert invalid.returncode != 0
    for node in ('a', 'b'):
        registry = command("helm", "template", "rivomesh-node-" + node, chart, "--namespace", "rivomesh", "-f", chart + "/values-cluster.yaml")
        assert registry == (ROOT / ("deploy/kubernetes/rivomesh-node-" + node + ".yaml")).read_text().strip()
        assert "nodeId: ${RIVOMESH_NODE_ID}" in registry and "RIVOMESH_CONTROL_PASSWORD" in registry
        assert 'value: "rivomesh-node-' + node + '"' in registry
        assert "replicas: 1" in registry  # Separate releases/PVCs, never shared local controller state.
    for invalid in ('cluster.enabled=true', 'cluster.enabled=true,cluster.url=jdbc:postgresql://db/control', 'cluster.heartbeatMillis=99', 'cluster.enabled=true,cluster.url=jdbc:postgresql://db/control,cluster.existingSecret=control,cluster.leaseMillis=1000'):
        result = subprocess.run(["helm", "template", "rivomesh", chart, "--set", invalid], cwd=ROOT, capture_output=True)
        assert result.returncode != 0, invalid
    for node in ('a', 'b'):
        shared = command("helm", "template", "rivomesh-shared-" + node, chart, "--namespace", "rivomesh", "-f", chart + "/values-shared-access.yaml")
        assert shared == (ROOT / ("deploy/kubernetes/rivomesh-shared-" + node + ".yaml")).read_text().strip()
        assert "sharedAccess: true" in shared
    for node in ('a','b'):
        global_control=command('helm','template','rivomesh-global-'+node,chart,'--namespace','rivomesh','-f',chart+'/values-global-control.yaml')
        assert global_control==(ROOT/('deploy/kubernetes/rivomesh-global-'+node+'.yaml')).read_text().strip()
        assert 'sharedTasks: true' in global_control and 'taskKey: ${RIVOMESH_TASK_KEY}' in global_control
        assert 'name: RIVOMESH_TASK_KEY' in global_control and 'key: "task-key"' in global_control
        assert 'maximumTasks: 10000' in global_control and 'maximumCommands: 1000' in global_control
    for invalid in ('cluster.sharedTasks=true','cluster.sharedAccess=false','cluster.taskKeyKey=','cluster.maximumTasks=0','cluster.maximumTasks=200001','cluster.maximumCommands=0','cluster.maximumCommands=100001','cluster.taskKey=plaintext'):
        arguments=['helm','template','rivomesh',chart]
        if invalid!='cluster.sharedTasks=true':arguments+=['-f',chart+'/values-global-control.yaml']
        result=subprocess.run(arguments+['--set',invalid],cwd=ROOT,capture_output=True)
        assert result.returncode!=0,invalid
    for role,names in (('controller',('rivomesh-controller-a','rivomesh-controller-b')),('agent',('rivomesh-agent-a',))):
        for name in names:
            separated=command('helm','template',name,chart,'--namespace','rivomesh','-f',chart+'/values-'+role+'.yaml')
            assert separated==(ROOT/('deploy/kubernetes/'+name+'.yaml')).read_text().strip()
            assert 'separateExecution: true' in separated and 'role: "'+role.upper()+'"' in separated
            assert 'sharedTasks: true' in separated and 'replicas: 1' in separated
    for invalid in ('cluster.role=AGENT','cluster.separateExecution=true','cluster.role=UNKNOWN'):
        assert subprocess.run(['helm','template','rivomesh',chart,'--set',invalid],cwd=ROOT,capture_output=True).returncode!=0,invalid
    for invalid in ('cluster.sharedTasks=false','cluster.separateExecution=false','cluster.sharedAccess=false'):
        assert subprocess.run(['helm','template','rivomesh',chart,'-f',chart+'/values-agent.yaml','--set',invalid],cwd=ROOT,capture_output=True).returncode!=0,invalid
    for node in ('a', 'b'):
        custody = command('helm', 'template', 'rivomesh-agent-' + node, chart, '-f', chart + '/values-agent.yaml',
                          '--set', 'cluster.handoffEvidenceClaim=handoff-shared',
                          '--set', 'persistence.existingClaim=agent-' + node + '-private')
        assert 'handoffEvidenceRoot: "/app/handoff-evidence"' in custody
        assert 'mountPath: /app/handoff-evidence' in custody and 'claimName: "handoff-shared"' in custody
        assert 'claimName: agent-' + node + '-private' in custody and 'runAsUser: 10001' in custody
        assert 'fsGroup:' not in custody and 'kind: PersistentVolumeClaim' not in custody
    for role, overrides in (
        ('controller', 'cluster.handoffEvidenceClaim=handoff-shared,persistence.existingClaim=controller-private'),
        ('agent', 'cluster.handoffEvidenceClaim=handoff-shared'),
        ('agent', 'cluster.handoffEvidenceClaim=handoff-shared,persistence.existingClaim=handoff-shared'),
        ('agent', 'cluster.handoffEvidenceClaim=/invalid/path,persistence.existingClaim=agent-private'),
    ):
        arguments = ['helm', 'template', 'rivomesh', chart, '-f', chart+'/values-'+role+'.yaml', '--set', overrides]
        assert subprocess.run(arguments, cwd=ROOT, capture_output=True).returncode != 0, overrides
    print('PASS optional shared custody claim on both Agents with independent private state and owner-only volume permissions')
    evidence = 'platform.kafkaPhysicalOwnershipEvidence=true'
    for profile in ('agent', 'global-control'):
        rendered = command('helm', 'template', 'rivomesh-kafka', chart, '-f',
                           chart + '/values-' + profile + '.yaml', '--set', evidence)
        assert '\n    kafkaPhysicalOwnershipEvidence: true\n' in rendered
    for flags in ((), ('-f', chart + '/values-controller.yaml')):
        result = subprocess.run(['helm', 'template', 'rivomesh-kafka', chart, *flags, '--set', evidence],
                                cwd=ROOT, capture_output=True)
        assert result.returncode != 0
        assert b'requires a shared-task member that runs Workers' in result.stderr
    print('PASS optional Kafka ownership evidence on executing shared-task members')
    oidc = (
        'oidc.enabled=true,sessions.publicOrigin=https://sync.example.test,'
        'oidc.issuer=https://identity.example.test/tenant,'
        'oidc.authorizationEndpoint=https://identity.example.test/tenant/authorize,'
        'oidc.tokenEndpoint=https://identity.example.test/tenant/token,'
        'oidc.clientId=fixture-client,oidc.keyId=fixture-key,oidc.rsaPublicKey=fixture-public-key,'
        'oidc.existingSecret=fixture-oidc'
    )
    rendered = command('helm', 'template', 'rivomesh-oidc', chart, '-f', chart + '/values-controller.yaml',
                       '--set', oidc)
    assert 'oidc:' in rendered and 'clientSecret: "${RIVOMESH_OIDC_CLIENT_SECRET}"' in rendered
    assert 'name: RIVOMESH_OIDC_CLIENT_SECRET' in rendered and 'name: "fixture-oidc"' in rendered
    assert 'key: "client-secret"' in rendered
    for role, invalid in (('agent', ''), ('controller', ',sessions.secureCookie=false'),
                          ('controller', ',sessions.publicOrigin=http://sync.example.test')):
        flags = ['-f', chart + '/values-' + role + '.yaml', '--set', oidc + invalid]
        result = subprocess.run(['helm', 'template', 'rivomesh-oidc', chart, *flags],
                                cwd=ROOT, capture_output=True)
        assert result.returncode != 0 and b'oidc requires a shared-task Controller' in result.stderr
    print('PASS OIDC Controller Helm configuration uses a Secret reference and requires HTTPS session settings')
    reviewed=command('helm','template','rivomesh-review',chart,'-f',chart+'/values-controller.yaml','-f',chart+'/values-recovery-review.yaml')
    assert 'recoveryReview: true' in reviewed and 'sharedTasks: true' in reviewed
    committed=command('helm','template','rivomesh-commit',chart,'-f',chart+'/values-controller.yaml','-f',chart+'/values-recovery-commit.yaml')
    assert 'recoveryCommit: true' in committed and 'recoveryReview: true' in committed
    assert subprocess.run(['helm','template','rivomesh',chart,'-f',chart+'/values-controller.yaml','-f',chart+'/values-recovery-commit.yaml','--set','cluster.recoveryReview=false'],cwd=ROOT,capture_output=True).returncode!=0
    for role,names in (('controller',('rivomesh-skip-controller-a','rivomesh-skip-controller-b')),('agent',('rivomesh-skip-agent-a',))):
        for name in names:
            skipped=command('helm','template',name,chart,'--namespace','rivomesh','-f',chart+'/values-'+role+'.yaml','-f',chart+'/values-recovery-skip.yaml')
            assert skipped==(ROOT/('deploy/kubernetes/'+name+'.yaml')).read_text().strip()
            assert all(flag+': true' in skipped for flag in ('recoveryReview','recoveryCommit','recoverySkip'))
    for invalid in ('cluster.recoveryCommit=false','cluster.recoveryReview=false','cluster.sharedTasks=false'):
        assert subprocess.run(['helm','template','rivomesh',chart,'-f',chart+'/values-controller.yaml','-f',chart+'/values-recovery-skip.yaml','--set',invalid],cwd=ROOT,capture_output=True).returncode!=0,invalid
    for role,names in (('controller',('rivomesh-review-controller-a','rivomesh-review-controller-b')),('agent',('rivomesh-review-agent-a',))):
        for name in names:
            profile=command('helm','template',name,chart,'--namespace','rivomesh','-f',chart+'/values-'+role+'.yaml','-f',chart+'/values-recovery-review.yaml')
            assert profile==(ROOT/('deploy/kubernetes/'+name+'.yaml')).read_text().strip()
            assert 'recoveryReview: true' in profile
    for invalid in ('cluster.sharedTasks=false','cluster.sharedAccess=false','cluster.enabled=false'):
        assert subprocess.run(['helm','template','rivomesh',chart,'-f',chart+'/values-controller.yaml','-f',chart+'/values-recovery-review.yaml','--set',invalid],cwd=ROOT,capture_output=True).returncode!=0,invalid
    print('PASS opt-in protocol5 review overlay and required global/shared authority guards; live upgrade acceptance remains separate')
    print('PASS separate controller/Agent Helm/native parity, explicit protocol/role guards and independent PVC profiles; live K8s acceptance remains separate')
    print('PASS opt-in global-control two-release/native parity, protected task-key Secret reference, budgets and invalid global/key settings; live K8s acceptance remains separate')
    print("PASS shared identity/session/audit opt-in profile and two native manifests; task routing remains separate")
    print("PASS shared-registry two-release/native parity, stable node IDs/Secret references and invalid store/lease rejection; global task routing remains local")
    print("PASS managed-access persisted-directory profile/default compatibility; live PVC startup acceptance remains separate")
    print("PASS exact task grants/default+scoped native parity, empty grants and invalid wildcard/duplicate/scoped-admin rejection")
    print("PASS Helm lint/default+RBAC native parity, Secret references, existing PVC, invalid limits/roles/no-admin/duplicate identities/credentials/literal-token rejection")


def docker(image):
    name = "rivomesh-console-smoke-" + uuid.uuid4().hex[:10]
    volume = name + "-data"
    token = secrets.token_hex(32)
    created = False
    command("docker", "volume", "create", volume)
    try:
        command("docker", "run", "-d", "--name", name, "--mount", f"type=volume,src={volume},dst=/app/data",
                "--env", "RIVOMESH_TOKEN=" + token, "--env", "JAVA_TOOL_OPTIONS=-Xmx128m",
                "--publish", "127.0.0.1::8080", image)
        created = True

        def base():
            return "http://" + command("docker", "port", name, "8080/tcp").splitlines()[0]

        def request(path, body=None, authorized=True):
            headers = {"Authorization": "Bearer " + token} if authorized else {}
            if body is not None:
                headers.update({"Content-Type": "application/json", "X-Request-ID": "deployment-create-task"})
            req = urllib.request.Request(base() + path, data=None if body is None else json.dumps(body).encode(), headers=headers)
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status, response.read().decode()

        def ready():
            for _ in range(40):
                try:
                    if request("/healthz", authorized=False)[0] == 200:
                        return
                except (OSError, urllib.error.URLError):
                    pass
                time.sleep(0.25)
            raise AssertionError("Console did not become healthy")

        ready()
        assert command("docker", "exec", name, "id", "-u") == "10001"
        command("docker", "exec", name, "java", "-cp", "/app/rivomesh.jar", "io.github.rivomesh.platform.Healthcheck")
        try:
            request("/api/tasks", authorized=False)
            raise AssertionError("API accepted unauthenticated access")
        except urllib.error.HTTPError as error:
            assert error.code == 401
        assert "Rivomesh" in request("/", authorized=False)[1]
        assert request("/app.js", authorized=False)[1] == (ROOT / "sync-app/src/main/resources/console/app.js").read_text(), "Image has stale UI resources"
        config = {"jobId": "smoke", "source": {"database": "unused", "username": "unused", "password": "smoke-source-private"},
                  "target": {"url": "jdbc:postgresql://127.0.0.1:1/unused?connectTimeout=1&socketTimeout=1", "schema": "smoke",
                             "username": "unused", "password": "smoke-target-private"}}
        for _ in range(2):
            status, body = request("/api/tasks", {"config": config})
            assert status == 201 and json.loads(body)["desired"] == "STOPPED"
            assert "smoke-source-private" not in body and "smoke-target-private" not in body
        assert command("docker", "exec", name, "stat", "-c", "%a", "/app/data/tasks/smoke.yml") == "600"
        command("docker", "restart", "--timeout", "90", name)
        ready()
        tasks = json.loads(request("/api/tasks")[1])["tasks"]
        assert len(tasks) == 1 and tasks[0]["desired"] == "STOPPED"
        print("PASS nonroot Docker/health/auth/UI/config permissions/idempotency/volume restart")
    finally:
        if created:
            command("docker", "rm", "-f", name)
        command("docker", "volume", "rm", volume)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", help="Also test a locally built image; never publishes it")
    args = parser.parse_args()
    manifests()
    if args.image:
        docker(args.image)
