# Gap verification: common cut and atomic records

[中文](design-gap-verification.md) · [Tasks](tasks.en.md) · RC-08 PARTIAL

Internal models and execution components exist;management API/UI integration is pending. Representative H2/PG/MySQL internal contracts pass;real-CDC acceptance and the operator workflow must pass before RC-08 closes.

## Reviewed scope

A plan binds one destination,configuration fingerprint,journal generation,source identity,common cut,sealed report and source roots. It covers at most100 existing gaps and1000 affected tables. Each gap requires its complete transaction plan and native execution receipt. Historical schema versions must match the common cut. Different source,destination,generation,schema or cut,and incomplete/differing reports are refused. Plans contain metadata,not business row values.

A historical MATCH only permits preparing a plan. Execution freshly scans every affected destination table and uses bounded disk sorting to compare row counts and content roots against sealed source data. Counts alone or only the skipped transaction's rows are insufficient. Differences,timeouts,moved native checkpoints or source pause fences prevent a successful record.

## Concurrency and persistence

The source job row stays locked throughout original-gap inspection,fresh destination scans and source-record commit. Resume,Worker ownership changes and service repair mutations must take the same fence. Native target epochs/common-cut checks also fence stale Workers. Tables need not fit in memory.

Destination business data must be exclusively written by the sync service. Direct application/administrator writes are outside this fence. Future manual-data correction must acquire the same fence. Verification compares the sealed historical source cut,not the source's current live state.

The source journal adds `rm3_gap_verification` and stores a verification ID/hash on `rm3_gap`. One source transaction writes the receipt and all selected gap links;any failure rolls everything back. Original SKIP plans,receipts,gaps and reports remain unchanged. Verification does not issue business DML,advance ACK or resume execution.

`VERIFIED_AT_COMMON_CUT` records the reviewed plan,cut and observation time. Identical request/plan replay returns the immutable receipt,including after a later resume;it reports historical success without scanning again. Changed reason/scope with the same request is refused. Another request cannot overwrite an already-verified gap. Concurrent duplicates return one receipt. Missing/changed receipts or incomplete links refuse instead of recreating empty history.

The first release creates metadata directly,without ALTER/migration support for old development candidates. Assess persistent state/data before resetting a test environment;copying only a cursor is invalid. See the [first-release baseline](../../en/first-release.md).

## Remaining work

Administrator review/approval,durable cross-node commands/idempotent results,paged gap states and the console workflow remain pending. Public status must distinguish SKIP history,unresolved gaps and equality verified at a particular cut. Existing `hasGaps` still indicates SKIP history;it establishes neither failed verification nor permanent equality. Internal scope currently covers H2/PG/MySQL transaction models,not every SQL,analytical or messaging destination.
