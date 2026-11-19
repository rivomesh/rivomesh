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

## Installed evidence and parked journal

The private same-store handoff binds one signed request before the placement transaction. A different Controller can replay the original request after a crash. The candidate confirms the stopped configuration and independently installs the privately staged original reports and closed receipts. The `EVIDENCE_ADOPTED` receipt gates authorized read-only access after restart.

The same transaction parks the journal and advances its managed format to the corresponding 15–18 variant. This preserves enabled target-gap and source-exclusion features. Earlier binaries reject the new format during identity validation. The current runtime refuses both ordinary Worker claims and generic resume for the parked owner. Explicit activation needs fresh candidate evidence and target-native epoch fences before issuing one admission for the exact assignment.

### Target fencing before Worker admission

A separate administrator request binds its identity and current authorization revision to the signed adopted preparation. The candidate verifies the same source position and snapshot set. It compares each destination’s recent checkpoint and writing epoch against the signed original evidence, then raises the destination epoch under its local transaction lock. Retry can accept a destination already at the parked epoch only when the original receipt and snapshots still match the signed cut. The published fence receipt covers every configured SQL target. This bounded operation leaves the journal parked for the independent Worker admission step.

The native target verification uses a 20-second deadline on the shared control transaction. A stalled verification leaves the approval pending and can temporarily delay concurrent management changes. The current evidence covers the bounded handoff profile.

### Worker admission after fencing

A separate administrator request binds the committed configuration, adopted evidence and every signed native SQL fence. The current candidate lease authorizes the final source and destination recheck. The shared PostgreSQL transaction advances the parked journal to its admitted format 19–22 and persists the signed `WORKER_ADMITTED` receipt. This preserves the source position and recovery format features. Older builds refuse the admitted format.

The candidate’s local registry records the matching request ID and the RUNNING intent atomically. The candidate then records `WORKER_PROJECTED` in the control database. When interrupted after shared admission, the candidate can resume local projection from the shared receipt. At startup the candidate checks any local start marker against the shared assignment, preparation and native journal format before scheduling. A replay of the installed intent preserves later STOP/PAUSE operations. The exclusive journal lease remains the final writer guard.

Exercise partial destination fencing, a moved native checkpoint before Worker admission, candidate process loss, ordinary lifecycle commands and source CDC after ownership transfer. History readers keep original and candidate grants and verify the adopted files against their signed manifest. The public ownership confirmation runs the same signed bind and private readiness checks through either Controller. Automatic relocation and partition acceptance remain separate work.

### Automatic continuation after a Controller interruption

When an administrator has bound the original execution request for a stopped, drained task and the candidate has durably acknowledged signed readiness, the candidate can finish the ownership transaction after the accepting Controller exits. It opens the sealed configuration and evidence again and checks the source position, SQL destination receipts and journal lease. The shared PostgreSQL control transaction then confirms the same task revision, current administrator authorization, both Agent bindings and the signed cut. Ownership moves at most once for that request and the transaction parks the journal. The candidate remains STOPPED and can install the read-only evidence through the existing flow.

The original Agent's control binding must remain current and its journal lease must have expired or been released. Source and destination checks use the existing exclusive-writer profile. Raw capture history requires explicit Controller commit. Changed authorization, leases, custody or native cut leave ownership where it is and the bounded pending receipt can retry until it expires. Interruption before queuing candidate readiness, promotion of an offline original Agent, independent control and journal stores, and automatic write activation require separate flows.

### Offline continuation from a durably frozen cut

The existing assessment and preparation authenticate a reviewed stop, the bound Agent directory and incarnation, and the matching candidate custody probe. Ordinary observations become unavailable when the original control lease expires. A `PREPARED` reservation can expire without retiring the original journal assignment. The administrative `GET /api/tasks/{task}/handoff` response adds `offlinePreflight` when the recorded original still owns the task and its control state is stale, stopped or unknown. `authorityMatchesReview`, `historicalDrainedCut` and `historicalReportsAndReceipts` describe the retained review. The result is `BLOCKED` and names the missing durable freeze or complete independently retained archive. The handoff dialog displays these checks in the assessment receipt. Missing or invalid historical archive fields are reported as missing.

The proposed recovery path applies to an already drained MySQL task that uses a single PostgreSQL control and journal database with the existing transactional SQL destination profile. The original and the selected candidate use different private directories. The original must still hold its authenticated control lease when the task is frozen. The authority for later recovery is the committed freeze and its exact cut. An elapsed lease alone cannot supply either.

#### Proposed durable record

Add `rmc_task_handoff_recovery_seal` keyed by `(cluster_id, task_id, seal_id)` with a unique per-task ordinal and a control-root-guarded current seal reference. The row stores the phase `FROZEN`, `REVOKED` or `CONSUMED`, the original stable request ID and the authenticated envelope. Bind the payload to the control realm, task revision and active configuration version and digest, original and selected candidate node/directory/incarnation/epoch, approving administrator and authorization revision, database issue time, acceptance deadline and ordinal. Terminal receipts record the closing request and exact accepted result. Reusing one request ID with different input conflicts. Expiry blocks consumption and keeps the journal frozen.

The envelope authenticates the native journal placement and generation, frozen epoch, captured and normalized sequence, source-position digest, completed table and destination snapshot acknowledgements, and the exact projected receipt from each destination. Every destination must already acknowledge the same retained sequence. The sealed inventory enumerates validation reports, closed operation receipts, signed failure records and raw capture files with their verified receipts. Every category has an explicit presence or absence proof from the original Agent. Present artifacts bind their exact lengths, hashes and protected archive versions. The archive and its verification material must survive loss of the original private directory. The existing candidate-bound shared archive can supply scoped bytes when these checks pass. A historical descriptor provides neither a complete inventory nor proof of current independent availability.

#### Proposed operations

1. `Seal` starts from the existing stopped review and reservation. Export, authenticate and stage the inventory while both Agent leases are current. Resolve pending commands and ambiguous operation outcomes. In the shared PostgreSQL transaction, compare the authority revision and exact journal cut, retire the previous journal lease, enter a versioned frozen state for the original assignment and persist the seal. Older runtimes and ordinary claim, renew, resume and direct Worker entry must refuse that state. The freeze stops configuration changes and recovery mutations that could invalidate the sealed cut.
2. `Review` verifies the current actor's task and both node grants, live candidate binding and capacity, complete independent archive and sealed STOPPED configuration. Require native proof of the same current generation, source history, schema and complete snapshots, journal receipts and SQL target checkpoints. Missing or changed evidence produces a typed refusal. Keep the original binding intact.
3. `Revoke` requires a separately authorized request and current freeze revision. If the original can safely resume, require its authenticated lease and private state, recheck the cut and restore execution only through a newer generation. Recovery after a missing archive remains blocked while the original is absent.
4. `Commit owner`, a future separately authorized operation, additionally requires the original control lease to be absent or expired on the database clock. Recheck the seal, native cut, current authorization and exact candidate admission. In the same PostgreSQL transaction consume the seal, retire the original binding for this task, park the candidate journal assignment at a newer epoch and update the task authority. The candidate imports evidence and remains STOPPED. Existing target-native fences and explicit Worker admission govern activation. A returned original Agent remains retired for this task.

Acceptance must exercise original loss before and after sealing, copy or rollback of an archive version, missing categories or verification keys, changed source or target receipts, authorization revocation, capacity changes, replay after a lost commit response and return of the original Agent. Verify the two Controller views, untouched source position and destination histories, and refusal of old writers. Loss during active capture, incomplete snapshots, pending acknowledgements, independent control and journal databases and automatic write activation require separate protocols.

[中文](design-agent-handoff.md) · [Existing lifecycle](design-execution-lifecycle.en.md)
