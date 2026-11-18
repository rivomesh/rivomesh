# Managed data validation

Use the validation workflow to select SQL destinations, set time and storage budgets, run or cancel background checks, retry destination comparisons against a sealed source, and review per-table results. To correct differences reported at a common cut, use [managed repair](managed-repair.md). To review a stalled MySQL source transaction, see [capture recovery](capture-recovery.md).

## Use the console

1. Complete V3 snapshots, run the local Worker and verify shared RUNNING state. Selected destinations must be able to reach the common cut; unselected destinations can be paused/offline.
2. As ADMIN, select transactional SQL destinations under Data validation. Messaging/analytical destinations are excluded. First native row profiles cover MySQL/PostgreSQL; other SQL product versions/modes require separate acceptance.
3. Set source-gate, source-read and per-target-table deadlines plus logical/per-row budgets. Gate maximum is120seconds, reads24hours, rows32MiB. Total logical budget must be at least four times the row budget and includes source, destination and keyless identity records. Physical storage is additional.
4. Start comparison. After checking the current configuration revision, scope, report ID and budgets are persisted before execution in a separate background queue. Establishing the cut briefly blocks MySQL instance writes; business writes can continue after gate release while replication retains its durable pause.
5. Follow operation history, then open the report. Results show source/target counts and missing/extra/changed rows per table,50table results per page. A historical result does not establish current equality.
6. Inspect results/shared state and explicitly resume using the current error version. Validation never clears/repairs business tables, rebuilds UNIQUE or transfers write ownership.

## Cancel and recover

Cancel is a durable request, not resume. Owned read connections/locks are aborted and complete sealed source data is retained. The response does not guarantee release at an exact millisecond:driver/connect/network timeouts also matter. Observe task state and database sessions.

Cancellation before establishing a barrier can leave shared RUNNING state; the controller stops its local Worker and displays actual shared state. Once a barrier is established, cancellation never bypasses PAUSED.

A sealed source allows Retry target comparison after loading the report. Original configuration/positions/epoch/physical target checkpoints and source integrity are checked; partial destination scans are discarded and reread, even with the source offline. The report's staging budget is retained; retries default to ten minutes per target table. Existing-report quota expansion and richer retry planning remain pending.

An incomplete source MVCC view cannot survive process loss. Keep the failed report, inspect/explicitly resume as appropriate, and establish a new cut. Never mix source views or rewrite report hashes/GTIDs. Controller restart can reuse a report ID when complete source data survived but the operation completion did not; without a complete source view/local Worker, it fails explicitly rather than resuming the shared pipeline. One validation runs at a time; ordinary controls use a separate pool.

## Access, audit and storage

ADMIN starts/cancels/retries; OPERATOR/VIEWER read summaries only for granted tasks. Cross-task access is hidden. Mutations require current revision and request ID; replay does not create another job. Previously accepted asynchronous work can continue after authorization changes; subsequent requests use current grants.

Audit records actor, task, report ID, request hash and acceptance/completion/cancellation/read events. Corresponding audit writes are required before the HTTP response. Raw business values are excluded from summaries/history.

Reports live under `platform.dataDirectory/validations/<taskId>/<reportId>`. Put the private H2 files on a protected persistent volume and include them in coordinated backups. The console keeps approximately 40 recent operations per task. Reports can still be opened by ID or browsed in the historical directory. A restarted Agent must have access to the original report directory.

## API

POST `/api/tasks/<taskId>/actions` with `X-Request-ID` and current `expectedRevision`:

```json
{"action":"VALIDATE","expectedRevision":"<revision>","validation":{"targets":["postgres"],"gateMillis":30000,"sourceMillis":3600000,"targetMillis":600000,"limits":{"maximumBytes":1073741824,"maximumRows":1000000000,"maximumRowBytes":4194304}}}
```

Retry uses `VALIDATE_RETRY` and `validation:{"reportId":"<UUID>","targetMillis":600000}`. Cancel uses `VALIDATE_CANCEL` without validation input. GET `/api/tasks/<taskId>/validations/<reportId>` reads a summary. `VALIDATION_REPORT_NOT_CREATED` means inspect the operation failure; `VALIDATION_REPORT_BUSY` means retry after the active scan.

[Core contract and FAQ](reconciliation-workflow.md) / [中文](../zh-CN/managed-validation.md)


## Job recovery paths

```mermaid
stateDiagram-v2
    [*] --> Accepted
    Accepted --> SourceScan: establish common cut
    SourceScan --> Sealed: complete source scan
    SourceScan --> NewViewRequired: lost MVCC / interruption
    Sealed --> TargetScan
    TargetScan --> Sealed: cancel / interrupted partial scan
    TargetScan --> Complete: match or differences
    Sealed --> Rejected: configuration or checkpoint changed
    Complete --> Historical: pipeline resumes
    NewViewRequired --> [*]: inspect and start a new validation
    Rejected --> [*]
    Historical --> [*]
```


First native managed acceptance passed LOCAL/H2 and external PostgreSQL/MySQL/openGauss journals with MySQL8.4/PostgreSQL17 destinations, including cancellation during an actual relation-lock wait, sealed-source retry and report/audit persistence across controller restart. See [evidence and failure corrections](../verification/README.md).


## Administrator difference details

For a completed report, ADMIN can open differences per target/table, page by identity hash, then explicitly reveal raw values. Stored source column names/types accompany expected/actual cells. An absent row is distinct from a present SQL NULL cell. Keyless source/target occurrence IDs are displayed without generating replacements.

POST `/api/tasks/<taskId>/validations/<reportId>/differences` with `{"target":"postgres","tableId":"<table ID>","after":"","limit":25}` returns items/next; maximum limit is100. POST `.../values` with the target/table and identityHash returns columns and expected/actual cells. Null for an entire side means absent row; cell value:null means SQL NULL. OPERATOR/VIEWER remain limited to summaries.

Before releasing values, the server checks difference hashes, raw-row checksums and row identity and requires report/scope-linked audit. Business values never enter audit/controller operations. The API checks total UTF-8 raw JSON lengths before fetching CLOBs and refuses more than1MiB with VALIDATION_DETAIL_TOO_LARGE; it never truncates or substitutes an empty row. Large-row chunking/streaming export remains pending; complete private report data is retained. Values remain in page memory and are cleared on logout. These are historical cut values, not live queries or repair.

Repair is separate from read-only validation. The new [managed repair workflow](managed-repair.md) requires plan review and explicit confirmation;validation itself never changes destination data.

### Historical report directory

In **Data validation**, choose **Browse historical reports** to open reports retained in the task owner's data directory. The list shows report IDs and creation times, newest first. It remains available after the original operation leaves recent history. Open a report to read its summary. Detailed differences and row values still use the task's administrator grants. The report remains readable after the Agent reopens its original directory.

Clients can page metadata using `GET /api/tasks/{id}/validations?limit=20` and pass `nextCursor` as `after`. The task owner removes abandoned summary staging files when it next acquires the report lock. Include retained reports and review evidence in the task's protected backup policy.

The catalog uses each report's identifier and creation time. It does not open the report's H2 database or read row values to build the list. Opening a report that is currently in use returns `VALIDATION_REPORT_BUSY`. Wait for the operation to finish and open it again. Only administrators can explicitly reveal raw values after selecting a difference; use **Previous** and **Next** to page through identities for the selected destination and table.

There is no automatic expiration of report or recovery evidence. Budget for the retained files in addition to the current job's quota. Keep a report while a retry, repair plan, gap verification, or audit relies on it. Removal from recent operation history does not release any report storage.
