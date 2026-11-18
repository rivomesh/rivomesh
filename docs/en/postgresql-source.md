# PostgreSQL 17 source

[中文](../zh-CN/postgresql-source.md)

This experimental profile reads committed transactions from one writable PostgreSQL 17 primary using built-in `pgoutput`. Configure one source database per task. In this profile, `databases` lists source schemas. Declare destination mappings with `targets[].mappings`.

## Configure and run

1. Set `wal_level=logical` and reserve slot capacity for the durable stream and an exported snapshot. Give the source role `LOGIN`, `REPLICATION` and `SELECT` on the published tables.
2. Create a publication that explicitly lists the synchronized tables and includes INSERT, UPDATE and DELETE. Set `REPLICA IDENTITY FULL` on each table to retain full before images, including for keyless rows. Keep the publication scope stable and pause for review before schema changes.
3. Use the [example configuration](../../config/postgresql-source-v3.yml) for the source, publication and destination. A PostgreSQL destination connection must be able to read `pg_control_system()` to verify physical identity. Configure distinct source and destination databases.
4. Run `check <config file>` to verify the connection and publication, then use `run <config file>`. The management console can import the V3 configuration.

PostgreSQL supplies a consistent position when creating a logical replication slot. Rivomesh imports an exported MVCC snapshot for each table and captures concurrent transactions through the original LSN stream. It reports the flushed LSN only after journaling the complete transaction. Each task uses a slot bound to its journal generation. [PostgreSQL replication protocol](https://www.postgresql.org/docs/17/protocol-replication.html) · [pgJDBC replication API](https://jdbc.postgresql.org/documentation/server-prepare/)

## Profile and recovery

- The initial type profile includes signed integers, booleans, bounded numeric values, text, JSON/JSONB and BYTEA. Keyed and keyless tables use full row identity.
- Generated columns, publication filters, partition routing, TRUNCATE and automatic DDL migration require further profiles. Preflight or capture reports unsupported relation changes and types.
- Restart uses the existing LSN and snapshot when the original primary, timeline, slot and journal remain available. Resume a failed target subscription after addressing its SQL error.
- With `POSTGRESQL_REQUIRED_WAL_UNAVAILABLE`, retain the old journal and record the last captured LSN. Evaluate whether the original slot and required WAL can be recovered. If needed, rebuild through a new pipeline and a reviewed destination scope.
- SO-06 still tracks cross-timeline promotion, synchronized slots, common-cut validation and the broader driver matrix.

Reproduce the native checks with `python3 tests/postgres-source-run.py --artifact <release jar> --output target/pg-source-proof`. The fixture simulates a lost PostgreSQL slot in disposable containers.
