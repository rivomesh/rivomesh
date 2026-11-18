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

## Plan adoption at a frozen source boundary

Use `plan-adoption` after `preview-adoption` finds matching data. This profile covers one nonempty InnoDB table in one directly connected MySQL 8.4 database and one UTF-8 PostgreSQL 17 primary. Configure one target and an external PostgreSQL journal with the same JDBC URL as the target, separate absent metadata schemas, and no existing writer assignment. Hold all application writers and pipeline workers stopped while reviewing the plan. [PostgreSQL transaction isolation](https://www.postgresql.org/docs/17/transaction-iso.html)

The source operator must enable and persist `read_only` and `super_read_only` and keep `persisted_globals_load` enabled. The MySQL account must see all tables and related schema metadata and have the grants for the global read lock and replication status checks. The planner refuses inbound replication and group membership. It takes a bounded lock on the original MySQL session and verifies the original physical server and exact binlog boundary before and after comparison. [MySQL FLUSH statement](https://dev.mysql.com/doc/refman/8.4/en/flush.html)

The source has a single signed `BIGINT` primary key and no secondary indexes, generated values, defaults other than `NULL`, triggers or foreign keys. Other columns may be signed integers, JSON or `utf8mb4_0900_bin` VARCHAR up to 4,000 characters. The target must have matching column order and nullability, mapped to `bigint`, `jsonb` and equal-width `varchar` with explicit `pg_catalog."C"` collation. The target has a single index on its nondeferrable `bigint` B-tree primary key. The target role must have ownership authority, schema and database creation privileges and permission to read `pg_control_system()`. Existing Rivomesh ownership, journal state, sink metadata, enabled event triggers and target subscription membership stop the plan.

```sh
java -jar rivomesh.jar plan-adoption config/task.yml postgres
java -jar rivomesh.jar plan-adoption config/task.yml postgres --expect-sha256 <reviewed-digest>
```

`ADOPTION_PLAN_OBSERVED` binds the exact source position, matching row count and digest, configuration and native target identities to the plan SHA-256 digest. The digest stays stable while those inputs match. Limits are 10,000 rows, 1 MiB per row, 16 MiB of retained comparison data and 30 seconds per read guard. JDBC and JVM overhead require additional memory. Keep the result in the operator's private records. PostgreSQL uses a read-only snapshot, so this result is evidence from that snapshot. Starting from that baseline requires a separate operation that rechecks the plan while acquiring target ownership and creating the journal and snapshot checkpoints in a single transaction.

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

## Observe a frozen restoration plan

The bounded restoration planner covers one downgraded business UNIQUE on a keyed InnoDB table. It requires a direct standalone MySQL 8.4 endpoint with `source.topology: AUTO`, no configured candidate nodes, and a UTF-8 PostgreSQL 17 primary. Stop application writers on both sides, establish a matching common-cut report and keep Rivomesh paused. Then have the source database operator place the same MySQL instance in `read_only` and `super_read_only` mode. The source must stay on the report's exact server UUID, GTID, binlog file and offset. MySQL can still admit replication writes in read-only mode, so any configured inbound channel or Group Replication membership is rejected. [MySQL server variables](https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html)

Use the table ID and original index name shown by `review-unique`.

```sh
java -jar rivomesh.jar plan-unique-restore config/task.yml /private/reports <reportId> postgres <tableId> <sourceIndex>
```

The planner verifies a sealed matching report and rejects recorded source or target exclusions. It obtains a bounded MySQL global read lock on the already frozen server, checks the original session and rehashes source rows. It holds the paused journal fence and a PostgreSQL table and checkpoint lock while reading target rows, confirming ownership, checking uniqueness and observing the original ordinary index. The supported scope is 10,000 rows, 1 MiB per canonical row, a 16 MiB private scratch budget and a 30-second fence. The source account needs the common-cut read, lock and replication status privileges. The target account also needs permission to execute `pg_catalog.pg_control_system()` for physical identity. [PostgreSQL control data functions](https://www.postgresql.org/docs/17/functions-info.html)

Supported key columns are signed integers mapped to `bigint`, or ordinary MySQL `utf8mb4_0900_bin` VARCHAR mapped to PostgreSQL `varchar` or `text` with explicit `pg_catalog."C"` collation. The planner checks the existing ordinary B-tree index's columns, order, collation and default operator classes. Neither GSI nor expression or generated key columns are covered. Case variants, trailing spaces and NULL handling follow the stated profile. [MySQL binary collations](https://dev.mysql.com/doc/refman/8.4/en/charset-binary-collations.html) · [PostgreSQL unique indexes](https://www.postgresql.org/docs/17/indexes-unique.html)

`RESTORE_PLAN_OBSERVED` includes a stable SHA-256 digest of the plan, including report seal, exact positions, current table content, journal namespace and target physical identity and timeline. Keep the output as a review artifact. To compare the reviewed digest, rerun the command with `--expect-sha256 <digest>`. It verifies the live state again and refuses a changed plan, including an index rebuild. A change in source GTID, even outside the table, rejects this cut. Fresh row hashes also detect writes excluded from the MySQL binlog and external target edits. The observation expires when the freeze, target state or cut changes.

## Apply an approved PostgreSQL UNIQUE restoration

This CLI profile requires one destination and a PostgreSQL external journal configured with exactly the same JDBC URL as the destination. The operator runs with the PostgreSQL privileges to lock the journal, target metadata, business table and event trigger catalog, and to add a constraint and write a receipt. Enabled target event triggers stop execution. The operator keeps exclusive control of both databases during the maintenance window, including startup configuration, replication, privilege changes and application writers.

The direct, standalone MySQL source must have both `read_only` and `super_read_only` active and persisted, with `persisted_globals_load` enabled on the original instance. Native persistence supports a restart on the same host and data directory using the same startup options. Rivomesh checks both persisted flags, original source and binlog cut, and the pinned session immediately before committing PostgreSQL. Keep the write fence in place through receipt resolution and the separately managed business handover. [MySQL persisted system variables](https://dev.mysql.com/doc/refman/8.4/en/persisted-system-variables.html)

Review a version-2 plan and use its exact digest, a stable request UUID and the local operator's audit identifier.

```sh
java -jar rivomesh.jar restore-unique config/task.yml /private/reports <reportId> postgres <tableId> <sourceIndex> <digest> <requestUUID> <actor> --acknowledge-exclusive-freeze
```

The command recalculates the plan under the source freeze and a single PostgreSQL transaction that holds the journal cut, subscription, target checkpoint, snapshot, owner and business table locks. It adds one validated, nondeferrable `UNIQUE NULLS DISTINCT`, retains the ordinary index and stores the approved digest, actor, request ID and native catalog evidence in the same transaction. PostgreSQL must have `fsync` enabled, and the transaction uses synchronous commit. Business rows, pipeline state and write authority keep their existing values. [PostgreSQL ALTER TABLE](https://www.postgresql.org/docs/17/sql-altertable.html)

`UNIQUE_RESTORED` indicates an acknowledged PostgreSQL commit or a verified native receipt for the identical request. `UNIQUE_RESTORE_COMMIT_UNKNOWN` indicates that the driver could not confirm the commit. Keep the freeze and report, and retry the identical request to resolve it. A request with a different actor, scope or digest conflicts with the stored approval. A receipt whose native constraint has changed also requires operator review. Application write handover and failback remain separate operations.

### FAQ

**Does equality let me start a new pipeline on the existing tables?** A new run still requires compatible empty tables. The preview neither establishes a usable replication position nor grants ownership. For a nonempty target, plan a verified baseline and write handoff, or synchronize into separate empty schemas and reconcile before switching.

**Does it examine other tables in the destination schemas?** The preview examines same-named tables selected by the configured source-to-target schema mappings. Inventory any extra schemas or tables as part of the migration.

**Can this repair existing data or restore a downgraded UNIQUE constraint?** Use [common-cut repair](common-cut-repair.md) for already managed targets and its own validation. Constraint restoration, final write handoff and failback need a product-specific plan.

[中文](../zh-CN/nonempty-target-preview.md)
