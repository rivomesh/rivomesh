# Failed-transaction diagnostics and manual recovery

[中文](../zh-CN/failed-transactions.md) · [SDD contract](../sdd/001-control-recovery-compatibility/design-failed-transactions.en.md)

The UI supports destination phase summaries,original incremental retry,snapshot-part recovery and [administrator transaction review](failure-review.md). Bounded inspection/[durable evidence](failure-evidence.md) now back explicit raw/normalized event pages. Administrators can review the [transaction skip](transaction-skip.md), [before-image compensation](transaction-compensation.md), [typed correction](manual-correction.md) and [common-cut repair](managed-repair.md) workflows.

## UI workflow

1. Open the paused destination in task details. Record destination ID,error generation,ACK,error code,phase and failed sequence when available.
2. For activation faults,check connectivity,credentials,permissions and metadata. For apply faults,check types,constraints and destination data. For projection faults,check mappings and historical schemas. For reconciliation faults,verify native receipts and journal ACKs before changing anything;do not manually advance positions.
3. Fix the cause and select “Resume this target.” The request binds its error generation,retrying the whole original incremental transaction or reconciling/resuming snapshot parts. Stale generations refuse. Pipeline resume and destination resume are distinct.
4. Check catch-up and run appropriate reconciliation. Accepted commands,live processes or matching ACKs alone do not prove business takeover readiness.

Activation can fail before a transaction is identified,so no invented failed sequence is shown. Legacy pauses without trusted phase records remain Unknown. Corrupt diagnostics are visibly unavailable and raw inspection refuses. Equal SQLStates do not make activation and apply faults equivalent.

Existing global task queries carry summaries through different management entries. Summaries contain only phase,sequence and failure epoch;no event IDs,row values,SQL or credentials. Existing task-view grants apply. Ordinary status does not gain raw-value access. Initial preparation/snapshot have dedicated diagnostics;legacy records may remain Unknown. Complete source connection/read/decode/DDL phases remain open. See [native snapshot evidence](../verification/README.md).

[Accepted scope and evidence](../verification/README.md) cover internal inspection,two-entry real SQL fault/restart and five-JRE foundation contracts;they do not accept the complete manual recovery workflow.

## Internal inspection limits

`Journal.inspectFailure(subscription, expectedErrorGeneration, maximumBytes)` reads only a paused subscriber's next whole transaction. It binds stream generation,current epoch,configuration fingerprint,error generation and ACK and verifies raw/existing normalized hashes. It never connects to or activates business destinations,creates normalization,changes keyless identities or advances ACK. Missing complete boundaries refuse rather than being guessed.

Raw and normalized payloads share a maximum4MiB budget;diagnostics have a separate16KiB limit. Stored-length checks and bounded character streaming enforce limits,with UTF-8 byte verification. Unconsumed normalization reservations may conservatively refuse a payload early. Oversize refuses the whole review rather than approving a truncated transaction. Streaming large-transaction review remains open.

This low-level inspection is not an execution plan;managed access adds [durable creation,authorization and audit](failure-review.md). Its returned object alone is not durable evidence or approval. Paused ACKs pin history;resume can allow collection. UI review reads retained records. For a proven target commit with a missing journal acknowledgment, use the [commit acknowledgment workflow](commit-acknowledgement.md). Explicit [transaction skip](transaction-skip.md) records a gap for the selected target. Preparation/snapshot faults explicitly refuse inspection as failed incremental transactions.

## FAQ

**Can I skip an error like a MySQL replica?** Use [controlled transaction skip](transaction-skip.md) for a full transaction at a supported destination. It records the native receipt and a durable gap. Editing ACKs or GTIDs or deleting logs breaks the history needed for recovery.

**What if the destination committed but the response was lost?** Automatic delivery verifies destination checkpoints,transaction IDs and projected-content hashes before repairing journal ACKs. Unknown outcomes stop/retry rather than authorizing skip. Manual compensation must reconcile first too.

**Can a source update fix an old transaction?** It creates a new transaction;persisted old events remain unchanged. Fix the destination condition preventing old replay. Use [compensation preview](transaction-compensation.md) to compare historical images with the current destination.

**Does restart clear an error?** No. Pause,error generation and phase diagnostics persist. Explicit resume remains necessary. Healthy destinations may continue while the paused subscriber retains history and consumes quota.

**Can Unknown be treated as an apply error?** No. Legacy records,corrupt diagnostics and capture/decode faults do not establish a known skippable transaction. Inspect source history,retention and historical configuration first.

**What if required binlogs were purged or cannot be decoded?** Destination resume cannot recreate source history. Restore logs,use a qualified source retaining required history or prepare a new baseline/rebuild. Capture-side range exclusion remains open under RC-04.

**Can I patch destination rows directly?** Synchronization assumes exclusive destination writes. Unplanned edits can affect constraints,keyless identities and replay. To edit approved rows while the task is paused, use [typed correction](manual-correction.md).
