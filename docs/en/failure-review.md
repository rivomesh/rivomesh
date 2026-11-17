# Managed transaction evidence review

[中文](../zh-CN/failure-review.md) · [Diagnostics](failed-transactions.md) · [Files/quotas](failure-evidence.md)

This workflow passes the [documented bounded profile](../verification/2026-11-17-managed-failure-review/README.md). Review only retains/reads historical evidence. It does not skip,compensate,change GTIDs or automatically resume.

## Enable and upgrade

Standalone administration supports administrator review. Shared global control explicitly enables it on every controller/Agent:

```yaml
cluster:
  sharedAccess: true
  sharedTasks: true
  recoveryReview: true
```

This selects control protocol5. Defaultoff preserves protocols3/4 and existing workflows. Stop old members normally,confirm registered STOPPED lifecycle,then enable/start all members. Expired leases,manual registry edits or lost Pods do not establish normal stop. Old modes cannot rejoin protocol5;automatic downgrade is not provided.

Preserve original node IDs,PVCs/private directories and keys. New example release names are for fresh deployments;do not replace an existing Agent with a demo name/empty directory during upgrade.

| Deployment | Entry |
|---|---|
| Linux |[controller](../../config/platform-controller-review.example.yml),[Agent](../../config/platform-agent-review.example.yml),independent original directories and existing systemd procedure|
| Compose |[separated deployment](../../deploy/docker-compose-execution.yml) plus [review overlay](../../deploy/docker-compose-review.yml),preserving all three state volumes|
| Helm |controller/Agent values plus [review values](../../deploy/helm/rivomesh/values-recovery-review.yaml),same control store/task-key Secret|
| Native Kubernetes |[controllerA](../../deploy/kubernetes/rivomesh-review-controller-a.yaml),[controllerB](../../deploy/kubernetes/rivomesh-review-controller-b.yaml),[Agent](../../deploy/kubernetes/rivomesh-review-agent-a.yaml);adapt Secrets,storage and addresses|

Production browser sessions retain existing HTTPS/Origin requirements. Agents expose probes only;controllers route to the original Agent. These examples alone do not accept live Kubernetes/CSI or mixed-binary rolling upgrades.

## Start from a paused destination

1. Sign in as ADMIN and check the paused destination's phase/error generation. Preparation/snapshot faults use their own recovery paths rather than failed incremental review.
2. Select “Retain transaction evidence.” The operation persists its review ID before capturing the whole head transaction/historical schemas.202means accepted only. After success,select “Review transaction evidence” in operation history.
3. Inspect original task revision,configuration fingerprint,owner/directory,source identity,GTID/log position,transaction/event range,historical tables,image availability and hashes. ACK/epoch are historical observations,not current destination commit proof.
4. Explicitly show raw or normalized events. Historical column names/types accompany before/after images. SQL NULL,JSON text`null`,Not provided and Not applicable remain distinct. Expand raw details for internal row identities.
5. Page through complete events bound to the same evidence hash. Oversized events refuse rather than truncate. Missing retained normalization disables that view.

OPERATOR/VIEWER cannot create/read evidence,including direct URLs. Raw values do not enter ordinary task status,operation results or audit bodies. Logout clears/closes review and stale responses cannot reopen old-session data.

Both entries use the same durable command/original-owner query. Repeating the same request ID retains one review ID. Original-directory Agent recovery preserves evidence,including after journal GC. Unavailable original Agents/control stores refuse rather than returning invented local data.

`configurationRevision`in the summary is the create action's original task revision. The configuration fingerprint is separate;this field is not a configuration-history restore reference.

## API mapping

Create through the existing action route with`X-Request-ID`:

```json
{"action":"FAILURE_REVIEW","expectedRevision":"current-task-revision","subscriptionId":"pg","errorCount":1}
```

Poll task operations for completion/`failureEvidenceId`. Retry the original request parameters/ID.

- `GET /api/tasks/{task}/failure-evidence/{id}`:administrator summary.
- `POST /api/tasks/{task}/failure-evidence/{id}/events`:administrator pages with`{"view":"RAW","from":0,"limit":20}`or`NORMALIZED`.
- Cookie POSTs retain CSRF/Origin checks;Bearer APIs remain compatible. Stale generation/revision,authorization changes,corruption and limits refuse explicitly.

## Native checkpoint comparison and proven-commit confirmation

New review operations attempt a read-only native checkpoint comparison for transactional PostgreSQL/MySQL destinations. The operation retains the observation; the evidence dialog displays it:

- **Checkpoint not advanced**: native progress equals journal ACK. This does not prove business data is absent.
- **Committed but unacknowledged**: native progress is exactly ACK+1 and transaction ID/projected hash both match. Reconcile acknowledgment before any skip or compensation.
- **Unavailable/refused**: display a safe problem code. Missing normalization, changed epoch/error generation, collected history, unavailable/unsupported destinations must not imply a commit verdict. Retained evidence remains readable.

This is a historical observation made while retaining evidence. Opening old evidence does not query the destination; retain new evidence to compare again. Inspection neither activates targets nor advances ACK. A lagging target epoch is explicitly reported as requiring fencing before a later mutation. Older members returning no observation are shown as unavailable. Eligible administrators can use [proven-commit confirmation](commit-acknowledgement.md);skip and compensation remain unimplemented.

Follow [RC-05 design](../sdd/001-control-recovery-compatibility/design-checkpoint-recovery.en.md). The [bounded read-only profile](../verification/2026-11-17-checkpoint-inspection/README.md) passed. The [managed confirmation bounded profile](../verification/2026-11-17-managed-commit/README.md) also passed;skip and compensation remain separate,and RC-02 historical scope is unchanged.

## FAQ and limits

**Does saving resume replication?** No. Business data,ACK,GTID and pause intent remain unchanged. Fix the cause and explicitly resume separately.

**Can review edit or skip?** No. Proven commits can be acknowledged through the separate confirmation workflow. Editing/skipping,native SKIP/compensation receipts and gap tracking remain open.

**Why is creation unavailable to an administrator?** Shared review may be disabled. Old profiles do not accept new commands;follow normal stop/upgrade steps for every member.

**Why no transaction or a changed generation?** The target may have resumed,changed fault,lack a complete incremental head or lack historical schemas. Recheck state;do not advance positions manually.

**Why refuse a replaced directory/key?** Evidence binds the original directory and HMAC key. Restore records,key,authority and directory identity together rather than substituting empty storage/new keys.

**Are files encrypted or automatically cleaned?** Files use private permissions/HMAC integrity;business content is not encrypted by this store. Protect backups,volumes and accounts appropriately. Automatic cleanup,key rotation and an evidence directory management UI remain open;full quotas preserve old records and refuse additions.
