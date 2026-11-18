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

### FAQ

**Does equality let me start a new pipeline on the existing tables?** A new run still requires compatible empty tables. The preview neither establishes a usable replication position nor grants ownership. For a nonempty target, plan a verified baseline and write handoff, or synchronize into separate empty schemas and reconcile before switching.

**Does it examine other tables in the destination schemas?** The preview examines same-named tables selected by the configured source-to-target schema mappings. Inventory any extra schemas or tables as part of the migration.

**Can this repair existing data or restore a downgraded UNIQUE constraint?** The preview is read-only. [Common-cut repair](common-cut-repair.md) covers already managed targets and requires its own validation. Constraint restoration, final write handoff and failback need a product-specific plan.

[中文](../zh-CN/nonempty-target-preview.md)
