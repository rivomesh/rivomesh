# CP-04 managed Agent handoff

The control store owns task placement and the command queue. The data journal owns capture and delivery positions and the current write lease. A replacement Agent needs authority in both stores and access to the original protected recovery data. Reopening the same Agent directory after a Pod move remains a separate deployment workflow.

## Admission

Automatic assignment for a new task reserves capacity once under the control authority lock. Relocation also needs a durable plan bound to the original and candidate Agent, the configuration version, the journal fingerprint and generation, and the task request identity.

The candidate checks the same transactional journal, source history, configured destinations and required secrets. It verifies the exact existing reports, evidence references and operation receipts needed to continue recovery. Stage candidate configuration outside the active task directory until the transfer commits. Confirm task-slot and Worker capacity on the candidate. A task using a private LOCAL journal can move with its volume and original identity. Moving to a different private directory requires a verified state transfer.

## Planned handoff

1. Persist the request and expected authority revision. Block further writes to task configuration and record any outstanding commands.
2. Drain the original Agent. Record the final capture position, destination receipts, journal epoch and desired state. Resolve operations with uncertain outcomes before granting a new writer.
3. Activate the candidate's assignment generation in the transactional journal when the old lease is no longer live. The journal checks that assignment when a Worker claims or renews its lease. Old Workers cannot reacquire it with an earlier assignment. The target protocol must fence stale in-flight writes.
4. Commit the candidate binding in the control store. Start from the saved state only after checking both bindings again. Preserve pause and stop intents. Publish the resulting owner and lease through every management entry.

The two stores commit separately. Persist each phase and its evidence before the next phase. A retry resolves the same transfer identity and finishes the pending phase. Treat a journal change without a matching control-store commit as a fenced pending transfer. A rollback then needs an explicit newer assignment and verified state.

## Failure tests

- Stop an Agent during snapshot and after a committed update. Reopen the existing report and original snapshot on the eligible candidate.
- Lose the control-store response after activating the new journal assignment. Retry the same transfer without issuing a new generation.
- Freeze an old Worker, allow fencing and then unfreeze it. Its writes and renewed claims must be refused.
- Refuse a candidate with missing logs, wrong journal generation, lost evidence, incompatible targets or exhausted capacity. Preserve the latest recorded position.
- Check authorization from both entries and verify that cross-owner operation history follows the administrator's task and node grants.

Initial assignment is validated independently of relocation. Apply the shared-journal handoff first to target profiles with native verified epoch fencing.

## Standalone journal admission

The [standalone pipeline workflow](../../en/pipeline-v3.md#standalone-writer-transfer-with-a-postgresql-journal) exercises the journal-side placement change through `init-writer` and `transfer-writer`. It commits a placement receipt and a higher epoch in one PostgreSQL transaction after checking one fully acknowledged cut. The replacement then raises each transactional target's epoch before applying data. If the former Worker thaws between the two steps, the completed snapshot and acknowledged stream tail leave it no outstanding business transaction, and the journal rejects its next mutation or claim.

The managed handoff still needs durable control phases and verified access to original operation receipts and recovery documents. Bounded diagnostic rotations are operator observations. Plan their continued visibility through the original Agent or an archive, independently of whether the replacement can resume the stream. The first standalone path requires an expired or released lease, a complete snapshot, fully caught-up targets and intact journal and target metadata. Interrupted snapshots, unreachable targets, open operations, and recovery across stores require separate acceptance.

[中文](design-agent-handoff.md) · [Existing lifecycle](design-execution-lifecycle.en.md)
