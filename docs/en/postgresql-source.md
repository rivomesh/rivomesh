# PostgreSQL source

[中文](../zh-CN/postgresql-source.md)

This experimental profile reads committed transactions from one writable PostgreSQL 17 or 18 primary using built-in `pgoutput`. Configure one source database per task. In this profile, `databases` lists source schemas. Declare destination mappings with `targets[].mappings`.

## Configure and run

1. Set `wal_level=logical` and reserve slot capacity for the durable stream and an exported snapshot. Give the source role `LOGIN`, `REPLICATION` and `SELECT` on the published tables.
2. Create a publication that explicitly lists the synchronized tables and includes INSERT, UPDATE and DELETE. Set `REPLICA IDENTITY FULL` on each table to retain full before images, including for keyless rows. Keep the publication scope stable and pause for review before schema changes.
3. Use the [example configuration](../../config/postgresql-source-v3.yml) for the source, publication and destination. A PostgreSQL destination connection must be able to read `pg_control_system()` to verify physical identity. Configure distinct source and destination databases.
4. Run `check <config file>` to verify the connection and publication, then use `run <config file>`. In the management console, choose **V3 → PostgreSQL** in New task or import the V3 configuration. Enter one database, the source schemas, the publication and slot prefix. The wizard offers PostgreSQL and MySQL destinations. Save, run **Check** on the assigned Agent, then use **Start**.

PostgreSQL supplies a consistent position when creating a logical replication slot. Rivomesh imports an exported MVCC snapshot for each table and captures concurrent transactions through the original LSN stream. It reports the flushed LSN only after journaling the complete transaction. Each task uses a slot bound to its journal generation. [PostgreSQL replication protocol](https://www.postgresql.org/docs/17/protocol-replication.html) · [pgJDBC replication API](https://jdbc.postgresql.org/documentation/server-prepare/)

The PostgreSQL source uses pgJDBC 42.7.14 with automatic flush advancement disabled. Ordinary keepalives and WAL outside the publication leave the durable journal position unchanged. Monitor retained WAL during extended intervals without published transactions. [pgJDBC download](https://jdbc.postgresql.org/download/) · [Replication stream builder](https://github.com/pgjdbc/pgjdbc/blob/REL42.7.14/pgjdbc/src/main/java/org/postgresql/replication/fluent/ChainedCommonStreamBuilder.java)

## Profile and recovery

- The initial type profile includes signed integers, booleans, bounded numeric values, text, JSON/JSONB and BYTEA. Keyed and keyless tables use full row identity.
- PostgreSQL UUID values are copied as lowercase canonical text in `VARCHAR(36)`. PostgreSQL 17.12 to PostgreSQL 17.12 verified nullable UUID values and primary-key changes through CDC. Reproduce with `--uuid` in the native fixture. [PostgreSQL UUID representation](https://www.postgresql.org/docs/17/datatype-uuid.html)
- PostgreSQL 18 stored generated columns can be materialized as ordinary destination columns when the publication includes them. Use `ALTER PUBLICATION rivomesh_capture SET (publish_generated_columns=stored)` and retain `REPLICA IDENTITY FULL`. PostgreSQL 17 generated columns, PostgreSQL 18 virtual generated columns, publication filters, partition routing, TRUNCATE and automatic DDL migration require separate profiles. Preflight or capture reports unsupported relation changes and types.
- Restart uses the existing LSN and snapshot when the original primary, timeline, slot and journal remain available. Resume a failed target subscription after addressing its SQL error.
- With `POSTGRESQL_REQUIRED_WAL_UNAVAILABLE`, retain the old journal and record the last captured LSN. Evaluate whether the original slot and required WAL can be recovered. If needed, rebuild through a new pipeline and a reviewed destination scope.
- SO-06 continues to track common-cut validation and additional target, promotion and driver profiles.

Reproduce the native checks with `python3 tests/postgres-source-run.py --artifact <release jar> --output target/pg-source-proof`. The fixture simulates a lost PostgreSQL slot in disposable containers.

Use Temurin 21 for current builds and fixtures. Earlier cross-runtime results are archived with their exact artifact. [Verification record](../verification/README.md#postgresql-source-runtime-matrix)

PostgreSQL 18.7 with PostgreSQL 17.12 as the destination passed eight Temurin 21 checks. The fixture covers stored generated values on keyed and keyless updates, materialization at the destination and omission from the publication. Add `--source-image postgres:18 --generated` to reproduce. [Generated column publication](https://www.postgresql.org/docs/18/logical-replication-gencols.html)

## Synchronized standby promotion

For a new task, keep one stable PostgreSQL writer endpoint in `source.postgresql.url` and set `source.postgresql.failoverSlot: true`. Equip the primary with a physical standby slot listed in `synchronized_standby_slots`. Configure the standby with that `primary_slot_name`, `hot_standby_feedback=on`, `sync_replication_slots=on` and a valid database name in `primary_conninfo`. Verify that the logical slot on the standby is persistent, synchronized and has received the required WAL before routing a failover. [PostgreSQL slot synchronization](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION) · [Failover checklist](https://www.postgresql.org/docs/17/logical-replication-failover.html)

The database HA service fences the previous primary, promotes the standby and routes the writer endpoint. Rivomesh reconnects through that endpoint. The promoted server must have the original system ID, an actual timeline history covering the durable LSN and snapshot coordinates, and the original failover slot. The slot must retain the required WAL and its confirmed LSN must not lead the journal. Retain the journal and old WAL if any of these checks fails. Configure the next physical standby before attempting another promotion.

PostgreSQL 17.12 on Temurin 21 passed actual primary SIGKILL, native promotion, continued delivery with unchanged snapshot IDs, and refusal of a route back to the original timeline. The fixture checks the active timeline via `IDENTIFY_SYSTEM`. Select `--source-image postgres:17.12 --failover` and append `--reject-old-primary` to run the divergent-route case. Load balancer products, cascading topologies and further version combinations have separate acceptance. [Streaming replication protocol](https://www.postgresql.org/docs/17/protocol-replication.html)
