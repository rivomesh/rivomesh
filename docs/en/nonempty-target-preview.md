# Existing PostgreSQL target preview

Use `preview-adoption` to assess existing MySQL data copied to PostgreSQL before registering or starting a pipeline. The preview compares mapped tables with source primary keys. It reports how many rows are missing, extra, changed or equal. Checks use the same canonical types as replication, including JSON values, timestamps and decimals.

```sh
java -jar rivomesh.jar preview-adoption config/task.yml postgres
```

The second argument names a V3 target ID. The target must be a PostgreSQL primary. Grant the target account schema usage and SELECT privileges on the mapped tables and any existing Rivomesh ownership and job tables. Use the MySQL read and lock privileges documented for [common-cut validation](reconciliation-workflow.md). The preview reads PostgreSQL in a READ ONLY transaction and does not open a journal or write ownership, business rows or checkpoints.

Schedule the preview during a maintenance window. It holds the MySQL write gate for up to 15 seconds while it establishes the source snapshot and pins the target tables and PostgreSQL REPEATABLE READ snapshot. It releases the write gate before scanning rows. Read locks can delay DDL until the views close. The preview requires the original MySQL lock and snapshot sessions. A disconnect or driver reconnection invalidates the comparison. The report describes the two views at the recorded boundary. New commits after that boundary can change the result. [PostgreSQL transaction isolation](https://www.postgresql.org/docs/17/sql-set-transaction.html)

- `MATCH_REQUIRES_VERIFIED_BASELINE` means the examined rows compared equal. Keep the report for planning and verify the original data baseline, retained source logs and destination ownership before any adoption.
- `DIFFERENCES_FOUND` means at least one comparison found rows to add, remove or reconcile. Stable primary keys determine the corresponding rows. Choose a separate reconciliation or staging plan.
- `BLOCKED` means at least one mapped table cannot be compared safely. Review each table's blocker and any comparisons that succeeded. Existing owners or checkpoints, missing tables, keyless sources, structural mismatches, inheritance, partitions, row security, generation, triggers and extra constraints can block this profile.

The preview covers at most 100 discovered source tables, 100,000 rows per side of each compared table, 4 MiB per decoded or canonical row and 64 MiB of accounted retained comparison data per table. Read deadlines are 120 seconds for each view. The limits apply to decoded data in the verifier, while JDBC fetches and JVM overhead require additional memory. Limit, permission and view errors terminate the command without a comparison result. The JSON result contains table names and totals, and uses a SHA-256 digest to identify the observed source position.

## Review downgraded UNIQUE indexes before handover

For an already managed PostgreSQL target, establish and complete a [common-cut report](reconciliation-workflow.md) for the destination, and keep the pipeline paused. Suspend business writes and DDL on the target, then run the review with the same task configuration and report ID.

```sh
java -jar rivomesh.jar review-unique config/task.yml /private/reports <reportId> postgres
```

The review verifies the report scope, durable journal cut, target ownership, completed snapshots and physical checkpoint. It acquires the journal and target checkpoint locks, queries PostgreSQL, then rolls back the lock transactions. Business rows, indexes and checkpoints keep their current values. Scans cover up to 100 mapped tables and 128 relevant indexes under a 120-second deadline. Use the synchronization account, which can lock the current checkpoint rows.

For a downgraded simple business UNIQUE, the review counts conflicting key groups and rows using PostgreSQL's target column collations and default `NULLS DISTINCT` behavior. A multicolumn key with at least one NULL does not contribute to these counts. The response lists key column names and counts. Different collation rules can produce conflicts even when the original MySQL and PostgreSQL row contents compared equal. [PostgreSQL unique indexes](https://www.postgresql.org/docs/17/indexes-unique.html) · [ICU collations](https://www.postgresql.org/docs/17/collation.html)

- `DUPLICATES_FOUND` identifies conflicts to reconcile before restoring the constraint.
- `REQUIRES_RECONCILIATION` points to recorded exclusions, differences in the historical comparison or a special index that needs its own plan.
- `NO_DUPLICATES_OBSERVED` describes this read view. Review the final cut, SQL definition and expected business semantics before approving any constraint or write handover. Resuming or moving the checkpoint invalidates this report for a new review.
- `NO_DOWNGRADED_UNIQUE` indicates the selected scope has no relevant candidate.

This profile checks mapped plain PostgreSQL tables and default B-tree key semantics. GSI uniqueness, expressions, prefix lengths, nondefault operator classes and alternate null policies need separate review. Constraint creation, write handover and failback remain operator actions under the migration plan.

### FAQ

**Does equality let me start a new pipeline on the existing tables?** A new run still requires compatible empty tables. The preview neither establishes a usable replication position nor grants ownership. For a nonempty target, plan a verified baseline and write handoff, or synchronize into separate empty schemas and reconcile before switching.

**Does it examine other tables in the destination schemas?** The preview examines same-named tables selected by the configured source-to-target schema mappings. Inventory any extra schemas or tables as part of the migration.

**Can this repair existing data or restore a downgraded UNIQUE constraint?** Use [common-cut repair](common-cut-repair.md) for already managed targets and its own validation. Constraint restoration, final write handoff and failback need a product-specific plan.

[中文](../zh-CN/nonempty-target-preview.md)
