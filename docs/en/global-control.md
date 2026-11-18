# Global task control: activation, recovery and limits

[中文](../zh-CN/global-control.md) · [SDD design](../sdd/001-control-recovery-compatibility/design-global-control.en.md)

2026-11-17: an optional fixed-owner global profile now wires public HTTP, UI, shared configuration, durable commands and remote reads. Local control remains the default. CP-02/03 remain PARTIAL: legacy task import, complete business workflows, capacity and fault matrices are not fully accepted. Automatic owner relocation is separate.

## Activation

Members share the control database, cluster.id, namespace, sharedAccess=true, sharedTasks=true and taskKey. Each has a distinct stable nodeId and private persistent task/data directories. taskKey is canonical Base64 text encoding 32 random bytes, supplied through protected environment configuration. Keep it out of task YAML. Never share controller.json. First activation requires empty task directories; existing registered tasks or YAML fail with GLOBAL_TASK_IMPORT_REQUIRED. There is no automatic legacy import tool yet.

The[platform example](../../config/platform-global.example.yml)supports Linux. Follow the[first-release baseline](first-release.md);node modes,identities and keys must agree. Mode mismatches refuse without automatic migration or local fallback.

```sh
java -jar rivomesh-app-0.1.0-SNAPSHOT.jar serve config/platform-global.example.yml
```

Supply database credentials, token, key and node ID through the deployment environment. The example enables Secure cookies: expose HTTPS and configure the correct publicOrigin at the proxy.

## Helm and native Kubernetes

The [global Helm values](../../deploy/helm/rivomesh/values-global-control.yaml) reference existing Secrets: token in rivomesh-auth, and username/password/task-key in rivomesh-control-store. task-key contains the same Base64 key text on all members. Use separate releases, private PVCs and stable node IDs:

```sh
helm upgrade --install rivomesh-global-a deploy/helm/rivomesh --namespace rivomesh -f deploy/helm/rivomesh/values-global-control.yaml
helm upgrade --install rivomesh-global-b deploy/helm/rivomesh --namespace rivomesh -f deploy/helm/rivomesh/values-global-control.yaml
```

Alternatively adapt and apply the native manifests [A](../../deploy/kubernetes/rivomesh-global-a.yaml) and [B](../../deploy/kubernetes/rivomesh-global-b.yaml) with kubectl apply -f. Independent controllers must not share a private directory. Static Helm/native parity and invalid-configuration checks exist; live Kubernetes/CSI, node-fault and rolling-upgrade acceptance remain open. Docker uses the same independent persistent-directory and key requirements. Acceptance fixtures leave no persistent preview.

## UI and configuration

The session field taskControlScope=GLOBAL identifies this profile. Any entry lists authorized tasks with cursor pagination, owner/node state, global revision, Worker state, desired/active configuration and command results. Creation and import allow a specific execution node or automatic assignment. Import creates a new task. Unavailable owner execution is UNKNOWN, not stopped.

Create/edit/restore/actions return 202 for durable admission only. Configuration history separates desired/active; the active pointer advances after successful owner application. Failed proposals preserve the old active configuration. Masked secret references work across entries. Connection previews, raw differences and repair-plan reads use expiring owner queries and require the owner to be online.

Global maximumTasks/maximumCommands differ from per-node maxTasks/maxRunningTasks. Defaults are 10000 global tasks and 1000 active commands; configuration ranges are resource guards, not hardware throughput promises. Queries are limited to 512KiB requests, 1MiB responses, 1–60 second lifetime, bounded readers and retained capacity.

Filter the shared task list by exact task ID or execution node ID. Both filters also work together. `GET /api/tasks?ownerNodeId=agent-a&taskId=orders&limit=20` applies account grants and the requested filters before computing the total or next page. Keep the same filters when using `after`. Within a task select a destination to isolate its state, SQL error and pending journal transactions. The count is the difference between captured and safely covered sequence at the observation time. Stale observations hide this count. Consumer completion requires its own measurement. [Evidence](../verification/README.md#global-task-filters-and-destination-backlog)

In **Diagnostics**, administrators can switch from **Source capture** to an individual destination. The scoped endpoint is `GET /api/tasks/orders/diagnostics?subscriptionId=warehouse&limit=20`. It returns recent destination pauses, retries and cleared retries with observation timestamps. Capture and destination events have separate rotating files in the execution Agent's private log directory. Destination files share two 64 KiB segments per task. Events can expire during heavy fault activity. Keep the same destination when using `after`. Error text, SQL, row values and credentials are excluded. A cleared retry records the Worker’s recovery observation. Check the destination's acknowledged sequence and pending count for delivery progress.

Each Agent shares a budget of eight diagnostics pages per second across its Controller entries, with an initial burst of 16 pages. On HTTP 429 with `DIAGNOSTICS_RATE_LIMITED`, use the `Retry-After` header and retry the original cursor. Each page carries at most 25 events. Other task status reads and control commands have their own request paths.

For PostgreSQL sources and transactional SQL destinations, the task status field `lastDeliveryTiming` shows the last verifiable sample for each destination. It compares the source commit timestamp on the logical replication wire with the time the Worker observed the destination return from commit. It includes transport time and assumes the source and Worker clocks agree. Synchronize those clocks. If the Worker sees the destination complete before the source timestamp, the API reports `CLOCK_SKEW` without a duration. Both timestamps are UTC. Delay in whole milliseconds and the journal sequence are decimal strings. [PostgreSQL logical replication message formats](https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html)

The sample belongs to a specific confirmed transaction. During a pause, retry or backlog, the console labels it historical. Before the first sample or when a newer acknowledgment lacks a direct observation, it reports `UNKNOWN`. Empty projections, snapshots, barriers, skips and reconciliation after an uncertain commit do not produce a sample. Run status, pending count and contiguous delivery progress describe the current task. The latest sample survives Worker restarts and collection of its original log entry.

The same measurement is available for direct MySQL 8.4 writes when `source.expectedVersion` is set to `"8.4"` or an exact 8.4 patch release. Use a local or external journal without writer assignment and a transactional SQL destination. The sample uses the standard GTID event's `original_commit_timestamp` in microseconds, verified against the connected writer's identity and version. The source must have originated the write and report the same original and immediate timestamps. Relayed transactions, older binlogs, missing timestamp metadata, Group Replication and writer-assignment journals report `UNKNOWN`. Group Replication records its original timestamp when the transaction is ready for certification. [MySQL replication delay timestamps](https://dev.mysql.com/doc/refman/8.4/en/replication-delayed.html) · [GTID event format](https://dev.mysql.com/doc/dev/mysql-server/8.4.13/classmysql_1_1binlog_1_1event_1_1Gtid__event.html)

### Automatic assignment for new tasks

In the create wizard or YAML import, select **Assign an Agent automatically**. The control store chooses an authorized, live Agent with task capacity. It compares assigned tasks as a share of each Agent's `maxTasks` and reserves the chosen slot in the same transaction that persists the task. Use `placement: "AUTO"` with `POST /api/tasks`, the configuration and an `X-Request-ID`. To choose a specific Agent, send `ownerNodeId`.

The saved task lists its actual owner. A repeat with the same payload and request identity returns that original assignment even when other tasks have filled the cluster. `AGENT_PLACEMENT_UNAVAILABLE` means no permitted healthy Agent has a free task slot. Provision required connector drivers, secret references and network access on eligible Agents. Automatic transfer of a running task is covered by the Agent handoff requirements.

### Assess transfer to another Agent

In the task details, select **Assess Agent handoff**. Choose a candidate from the node directory and record the assessment. The dialog shows the candidate's registry status and the recorded blockers. Select **Refresh nodes and assessment** to read current information from either entry. If a submission outcome is unknown, **Retry the same assessment** preserves its request ID and inputs. If another entry saved a newer assessment, review that receipt before recording a replacement.

An administrator can record a handoff assessment from either Controller. Send `POST /api/tasks/{id}/handoff` with an `X-Request-ID` and a JSON body containing `candidateNodeId` and the task's `expectedRevision`. `GET /api/tasks/{id}/handoff` reads the current assessment. To refresh it, send a new request ID and set `expectedAssessment` to the current assessment ID. Retrying the current request with its original inputs returns the same assessment. Both Agents and the task must be in the caller's scope.

The assessment binds the task revision and active configuration, the two Agent registrations, candidate task capacity and the original Agent's observed capture position. It checks for outstanding commands and a complete, acknowledged cut on a supported MySQL source with a PostgreSQL external journal and PostgreSQL or MySQL targets. A change in the observed cut, pending work, Agent registration or available capacity marks the assessment `STALE`. Its checkpoint is represented by a digest. The response excludes source positions, credentials and private recovery files.

`EVIDENCE_REQUIRED` means these control-plane checks passed. The candidate must still prove connectivity and access to the original recovery reports and receipts. `BLOCKED` identifies earlier prerequisites such as an active Worker or exhausted task capacity. Assessment leaves the task owner, Worker, capacity reservations and journal unchanged. The response carries `executionAuthorized: false`.

Send `POST /api/tasks/{id}/handoff/probe` with `expectedAssessment` to ask the currently enrolled candidate to verify the original journal generation, MySQL GTID history and native PostgreSQL/MySQL target receipts. The request uses the candidate's own connection settings and secret resolution. The two read-only queries expire within 30 seconds and verify the task revision and both Agent enrollments again on completion. The latest assessment retains the sanitized result. A stale or replaced assessment requires a refresh before probing again.

The original Agent reports presence of its private validation, source and failure evidence directories and its retained operation receipt count. This inventory leaves private custody with the original Agent and reports `ORIGINAL_PRIVATE_EVIDENCE_CUSTODY_UNPROVEN`. Successful candidate verification clears the connectivity blocker while fresh. The result still carries `executionAuthorized: false`.

To verify custody of completed validation reports, mount the same private storage on both Agents and set `cluster.handoffEvidenceRoot` to the existing absolute path on each Agent. Use owner-only POSIX permissions and an execution identity that can access the mount. The original Agent needs write access and the candidate needs read access. The mount must support file locks and atomic same-directory rename. Earlier tasks and installations continue to use their original report directories.

For Helm, set `cluster.handoffEvidenceClaim` to the same existing ReadWriteMany PVC in both Agent releases. Set each release's `persistence.existingClaim` to its own initialized private state PVC. The chart mounts the shared claim at `/app/handoff-evidence` and passes that path to Rivomesh. Prepare both volumes for the Agent's UID and GID `10001`. The shared claim root requires mode `0700`. This profile omits Pod `fsGroup` so Kubernetes preserves the filesystem permissions. [Kubernetes volume permissions](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#configure-volume-permission-and-ownership-change-policy-for-pods)

For native Kubernetes YAML, add the existing shared PVC to each Agent Deployment and mount it at `/app/handoff-evidence`. Keep each Agent's own state PVC mounted at `/app/data`. Omit `spec.template.spec.securityContext.fsGroup` for this profile and set `cluster.handoffEvidenceRoot: /app/handoff-evidence` in each Agent ConfigMap. Roll out the updated Agents after preparing the two volumes.

During a probe, the original Agent locks each completed report and exports the exact `rows.mv.db` and `summary.json` bytes. Each attempt handles up to 32 reports within a shared 64 MiB payload budget for reports and historical failure evidence. The scoped archive replaces the previous export for that original task only after a complete write. The candidate verifies the archive digest, assessment, task revision and both Agent directory identities. `probe.sharedReportCustody` reports the result, file size and digest. The protected mount holds the report data. MySQL binlog evidence and repair plans remain under the original Agent’s private state. The probe preserves task ownership and returns `executionAuthorized: false`.

The same probe can verify closed operation receipts through `probe.sharedReceiptCustody`. The original Agent matches the signed global command history against its private request signatures, actors and outcomes. It archives only these closed records and the candidate checks their exact bytes in the scoped archive. This first profile covers up to 64 global commands for the selected task. A missing, pruned, mismatched or unresolved local receipt keeps custody unproven. The response contains counts and digests. Receipt import and transfer authorization require separate verification.

`probe.sharedFailureEvidenceCustody` reports custody of up to 64 historical failure records. Under the original store lock, the original Agent verifies the authority scope and the HMAC and content digest of every retained record. The archive contains their exact authority and record bytes. The original Agent keeps the signing key. The candidate verifies the archive digest received over the authenticated owner route, the assessment scope, and the number and shape of archived entries. Missing authority, a changed record, or a busy store prevents the original Agent from publishing that export. The response exposes counts, size and digest. Using a preserved record for recovery requires the original authorization and key.

When both Agents verify the exact stopped cut and the original closed receipts, the assessment returns `preparationEvidenceSha256`. If a validation report directory exists, it also requires verified completed-report custody. An administrator can submit `POST /api/tasks/{task}/handoff/preparation` with an `X-Request-ID` and the `expectedAssessment`, `expectedRevision`, and `expectedEvidenceSha256` fields. `GET` on the same route returns the latest preparation, including its expiration and the original and candidate Agent identities.

A preparation expires after 15 minutes on the control database clock. Until expiration or release, new commands and configuration edits for the task are blocked. The candidate reserves one task slot and one Worker slot. The first profile counts every task already assigned to the candidate as a potential Worker, including stopped tasks. Explicit and automatic task placement account for the reservation. A current administrator can immediately release it with `POST /api/tasks/{task}/handoff/preparation/release`, an `X-Request-ID`, and `expectedPreparation`, then manage the original task again. Replaying the same accepted request with the same actor and input returns its recorded preparation even after release or expiration. This preparation keeps the original task owner and writer generation. Transfer execution requires verified source and failure evidence and separate journal and control-store fencing.

For a new fixed-owner MySQL task with a PostgreSQL external journal and MySQL or PostgreSQL targets, set `nodeId` to the registered original Agent ID, set `journal.managedWriter: true`, and set `journal.writerAssignment` to a new UUID. Run `java -jar rivomesh.jar init-writer task.yml` with the journal credentials before creating the task on that Agent. Automatic placement and ordinary configuration changes to this writer binding are refused. Existing unassigned journals require their own migration. The standalone `transfer-writer` command refuses this journal even when the candidate configuration omits `managedWriter`.

A verified preparation records the original assignment and a private successor assignment for this request. The response reports digests through `writerActivation`. When both stores use the same PostgreSQL JDBC URL and the control database role can lock the journal rows, `checkedInControlTransaction` confirms that the control transaction checked the same expired or released journal lease, capture cut, and acknowledged subscription positions. The task and writer placement stay with the original Agent during this check.

In the console, open **Assess Agent handoff**. After verifying the current evidence, choose **Reserve task and Worker slots** and confirm. You can view the same receipt and expiration from either Controller. Select **Release reservation** before sending a new command to the original task.

## Admission and recovery

PENDING is durable queueing. DISPATCHED saves the exact original local request/revision before invocation. ACCEPTED is local durable admission. SUCCEEDED/FAILED come from actual operation receipts. UNKNOWN requires reconciliation, not a success claim or reissue under a new revision. Startup restores receipt protection and validates local configuration against shared authority before scheduling.

The latest startup fix scans all outstanding command pages, including undispatched STOP/PAUSE. Corresponding Workers cannot restore old RUNNING intent before local durable admission; older local operations may finish and stop commands receive priority retries. Rejection after authorization changes releases the block; uncertainty retains it. Focused tests and the application build pass; native business acceptance remains candidate-specific and does not retroactively change earlier evidence.

TASK_BUSY/OPERATION_PENDING retain the original request. Outstanding receipts bypass ordinary 500-entry pruning. Cancellation has a parent and reserved capacity: cancel undispatched work atomically, reconcile uncertain delivery first, and perform actual cancellation after local admission. Success does not imply all targets rolled back.

Offline STOP waits for the original owner. Restore its private directory/volume and keys; an empty directory cannot impersonate it. Controller shutdown still stops its Workers. Independent execution, relocation, drain, scheduling and stale-Worker fencing remain CP-04/07. Source MySQL primary promotion is a separate fault scenario from controller-owner takeover.

## Operations FAQ

**How do I distinguish accepted from executed?** Inspect deliveryStatus, status, code and actual Worker/target checkpoints for the same request ID. HTTP 202 is not business commit evidence.

**Does refresh or another entry preserve login?** Shared sessions restore identity subject to cookie domain, Secure, SameSite, expiry and revocation. Different domains require correct proxy/session configuration.

**Can A stop an offline owner?** A durably queues STOP. It stays pending/unknown until the original owner supplies an actual receipt. Heartbeat expiry does not prove process termination.

**Can I edit owner fields, delete root tables or use an empty directory?** No. Authenticated metadata, counts, private-directory identity, receipts and configuration projections reject missing/substituted authority.

**Does control-store failure fall back to local accounts?** No. Management refuses control without shared authority. Existing data-process behavior follows its journal/lease/target protocol; management availability does not establish complete data-plane fencing.

**What belongs in backups?** Coordinate the control database, every private directory, logs/reports/local journal, external journal if selected, and original keys. Restoring just a cursor or old control snapshot is insufficient; automated coordination and old-backup fault matrices remain open.

**What is already evidenced?** Earlier candidates cover real PostgreSQL/MySQL control stores, two independent JVMs, HTTPS/shared login, A creating B-owned tasks and offline commands. Configuration fixtures also invoke real TaskService with fixture Workers. The startup fix and real cross-node business CDC are under additional acceptance. These do not establish all cluster, adapter or hardware profiles.

**Why is no persistent preview running?** The user requested temporary fixtures only. They remove their own containers/volumes and leave unrelated resources untouched.
