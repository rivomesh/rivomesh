# Oracle source, archived capture and preflight

[中文](../zh-CN/oracle-source.md)

The Oracle 26ai profile verifies one writable CDB/PDB, archive mode, force and minimal supplemental logging, source identity and Flashback read access. Archived capture and durable SCN recovery to PostgreSQL were revalidated on ARM64 with Oracle Free 23.26.4.0.0, Temurin 21 and Oracle JDBC 23.26.4.0.0. SO-07 tracks the remaining versions, types, RAC and failover.

1. Provide an Oracle JDBC JAR compatible with your Java runtime alongside the Rivomesh JAR. [Oracle JDBC compatibility](https://docs.oracle.com/en/database/oracle/oracle-database/26/jjdbc/JDBC-getting-started.html)
2. Give the CDB-root account read access to `V_$DATABASE`, `V_$INSTANCE` and the selected PDB in `V_$PDBS`. Configure its `CONTAINER_DATA` visibility for that PDB. Give the PDB account read access to its `V_$PDBS` identity and `SELECT` and `FLASHBACK` on the declared tables. [Oracle container data privileges](https://docs.oracle.com/en/database/oracle/oracle-database/26/dbseg/managing-commonly-and-locally-granted-privileges.html)
3. Configure the two services and uppercase `OWNER.TABLE` names with the [probe example](../../config/oracle-source-probe.yml).

```sh
java -cp 'rivomesh.jar:ojdbc8.jar' io.github.rivomesh.Main probe-oracle config/oracle-source-probe.yml
```

The report includes DBID, the resetlogs SCN, PDB GUID and a common snapshot SCN. It checks a Flashback read in each table at that SCN and reports primary-key presence. For `ORACLE_WRITABLE_PDB_REQUIRED`, verify the PDB is open read/write and the root account can see that PDB. For `ORACLE_PDB_AUTHORITY_MISMATCH`, compare the supplied service and PDB identity.

The [native fixture](../../tests/oracle-source-probe.py) verifies keyed and keyless tables, a historical update/delete and rejection of the root service supplied as the PDB.

Add `--logminer` for the native archive verification. One commit XID contains an update and keyless delete with redo and undo SQL, and the committed-only result excludes a rolled-back insert. [Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/26/sutil/oracle-logminer-utility.html)

Add `archiveWindow.startScn` and `archiveWindow.endScnExclusive` as decimal strings to validate a selected archived interval. The root account needs read access to `V_$THREAD` and `V_$ARCHIVED_LOG`. The check covers the current resetlogs incarnation and one redo thread. `CATALOG_CONTIGUOUS` reports uninterrupted catalog coverage. `ORACLE_ARCHIVE_LOG_GAP` indicates that the archive catalog does not cover the entire interval. Retain the original interval and verify physical files with LogMiner before a recovery.

Set `archiveWindow.decodeCommitted: true` to read the registered archive files with LogMiner. The CDB-root account additionally needs `LOGMINING`, `EXECUTE` on `SYS.DBMS_LOGMNR` and access to `SYS.V_$LOGMNR_CONTENTS`. The report includes the committed transaction count, operation counts and SHA-256 of typed changes. The probe reads the generated redo and undo statements as data. It accepts complete images for supported `NUMBER(p,s)`, `VARCHAR2` and `DATE` columns, and refuses crossing transactions, split redo statements, missing supplemental images and unsupported operations or types. Enable appropriate supplemental logging on each table before recording the interval.

The decoded output includes the last typed commit coordinate with commit SCN and XID. Replay groups complete transactions with per-transaction event ordinals. It refuses an ambiguous sequence if different XIDs share a commit SCN. The native fixture also rotates an initially unarchived suffix and verifies a second distinct commit coordinate.

## V3 Oracle to PostgreSQL

Use [the V3 example](../../config/oracle-source-v3.yml) with `source.type: ORACLE`. The first experimental profile uses Oracle 26ai Free 23.26.4.0.0 on ARM64 and PostgreSQL 17.12. Declare uppercase source owners and tables, owner-to-destination schemas and one writable PDB. The root account additionally needs read access to `V_$TRANSACTION` and `V_$PDB_INCARNATION`, plus the archive and LogMiner permissions above. Use all-column supplemental logging for each selected table. The PDB account reads the selected tables, their metadata and the common Flashback SCN.

```sh
java -cp 'rivomesh.jar:ojdbc8.jar' io.github.rivomesh.Main check config/oracle-source-v3.yml
java -cp 'rivomesh.jar:ojdbc8.jar' io.github.rivomesh.Main run config/oracle-source-v3.yml
```

For the UI, launch the runtime that owns the Worker (`serve` in combined mode or `agent` in a separated cluster) with the same classpath. In New task choose **V3 → Oracle** or import the V3 task. Enter the CDB root and PDB services separately, the PDB name and uppercase `OWNER.TABLE` selections. Map each source owner to a PostgreSQL schema and run **Check** on the assigned Agent before starting. The supervised Worker inherits the separately supplied Oracle JDBC driver from the owner’s classpath. The profile supports bounded `NUMBER(p,s)`, `VARCHAR2` and `DATE` in keyed and keyless rows. The snapshot uses one SCN per capture start. An interrupted unfinished snapshot can restart from a new Flashback view while completed snapshots remain reusable.

Capture polls closed archive segments. The durable coordinate binds CDB DBID and resetlogs, PDB GUID, UID and incarnation, captured SCN and the retained archive floor. A transaction already open at the snapshot can hold that floor back. When its commit lands in a closed archive, the complete transaction enters the journal. The floor advances only after coverage passes the SCN of a later active-transaction sample. Retain archives starting from the reported floor. Verify archive rotation and retention under the real workload.

A missing archive, unsupported record, ambiguous ordering or unreviewed DDL stops capture with the previous durable position. Use the capture review and preserve the source and journal. Source promotion, RAC, online-redo capture, native binary JSON, LOBs, other generated expressions, partial rollback and further target profiles require separate acceptance. LogMiner omits `SQL_UNDO` for SecureFiles LOBs, so preflight refuses LOB columns in this full before-and-after image profile. [Oracle LogMiner data types and SecureFiles](https://docs.oracle.com/en/database/oracle/oracle-database/26/sutil/oracle-logminer-utility.html) · [Oracle active transactions](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/V-TRANSACTION.html) · [PDB incarnation](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/V-PDB_INCARNATION.html)

### Textual JSON columns

An Oracle `VARCHAR2` column declared with up to 4,000 bytes maps to PostgreSQL `jsonb` when an enabled, validated, nondeferrable `CHECK` tests that single column for strict JSON and unique object keys. The check can be declared at column or table level. Other `VARCHAR2` columns retain their string mapping. [Oracle SQL/JSON constraints](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/is-json-condition.html)

```sql
CREATE TABLE RIVO_APP.DOCUMENTS (
    ID NUMBER(12) PRIMARY KEY,
    PAYLOAD VARCHAR2(1000 BYTE)
        CHECK (PAYLOAD IS JSON (STRICT WITH UNIQUE KEYS))
);
ALTER TABLE RIVO_APP.DOCUMENTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

Keep both modifiers inside the same parentheses. Oracle Free 23.26.4 returns `ORA-02252` for the split form `IS JSON (STRICT) WITH UNIQUE KEYS` in a `CHECK`.

Both Flashback and archived capture verify strict syntax, one complete JSON value and unique object keys. SQL `NULL` remains distinct from JSON `null`. PostgreSQL `jsonb` preserves the value and may change whitespace, member order and numeric formatting. Preflight returns `ORACLE_TEXT_JSON_PRIMARY_KEY_UNSUPPORTED` for a textual JSON primary key because distinct source strings can become equivalent target keys. Values outside either database's JSON representation use its usual error and recovery flow. [PostgreSQL JSON types](https://www.postgresql.org/docs/17/datatype-json.html)

Adding or removing this constraint on a synchronized column changes its target type mapping. Review the destination schema and existing data before resuming capture through the DDL recovery workflow. Oracle returns `ORA-40664` if the column already has an `IS JSON` constraint. Replace the existing constraint during the reviewed schema migration. [Oracle ORA-40664](https://docs.oracle.com/en/error-help/db/ora-40664/)

## Oracle DATE in PostgreSQL

Oracle `DATE` maps to PostgreSQL `timestamp(6) without time zone` and preserves the date, hours, minutes, seconds and SQL `NULL`. Capture sets the Gregorian calendar and American date language on its own Oracle sessions. PostgreSQL reads and writes use `LocalDateTime` so the Worker time zone does not move a valid Oracle wall-clock time during a daylight-saving transition. An existing destination column must also use `timestamp without time zone`. [Oracle DATE storage](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html) · [Oracle format models](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Format-Models.html) · [PostgreSQL JDBC date and time types](https://jdbc.postgresql.org/documentation/query/)

The profile accepts dates from 1582-10-15 through 9999-12-31. A value before that range, including BCE, raises `ORACLE_DATE_VALUE_OUT_OF_RANGE` when its era and format can be decoded. Unexpected LogMiner date masks raise `ORACLE_DATE_FORMAT_UNSUPPORTED`. Inspect the source values, the affected table and the durable position before retrying an interrupted run. Oracle's `TIMESTAMP` variants require a separate profile.

Run `tests/oracle-source-probe.py --logminer --pipeline` with the frozen JAR and Oracle JDBC arguments to reproduce the temporary real databases and actual SIGKILL/restart. Add `--text-json` for strict textual JSON or `--date-values` for Oracle DATE, covering Flashback, archived CDC and recovery for both keyed and keyless tables. [Evidence](../verification/README.md#oracle-archived-cdc-and-restart)

## Integer virtual columns

For Oracle 26ai, select **Sync supported integer virtual columns** in the Oracle source form or set `source.oracle.allowVirtualNumericColumns: true` in the V3 YAML. Check the task before starting.

The profile accepts a virtual `NUMBER(p,0)` column whose dictionary expression multiplies one physical `NUMBER(p,0)` column by an integer from 1 to 999. Each column needs an explicit precision of at most 18, and the output precision must hold the complete input range. For example, `AMOUNT NUMBER(12)` and `DERIVED NUMBER(16,0) GENERATED ALWAYS AS (AMOUNT * 2) VIRTUAL` fit this profile. Virtual primary keys, function expressions and decimal operands require a separate profile. [Oracle column expressions](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Column-Expressions.html)

The Worker reads the original Flashback value and verifies it against the same arithmetic used on archived physical operands. PostgreSQL receives a regular numeric column containing that value. Redo and undo generate separate before and after images, so repeated writes within one transaction and keyless duplicate rows keep their original values and multiplicity. SQL NULL propagates into the virtual result.

Enable the setting when creating the journal. It changes the source fingerprint. An existing task needs a separately reviewed schema and checkpoint migration before it can adopt this profile.

Run `tests/oracle-source-probe.py --logminer --pipeline --virtual-columns --startup-timeout 480` with the frozen application and Oracle JDBC arguments. The fixture checks old and new outputs, keyless duplicates, restart from the same snapshot IDs and unsupported expression refusal. Use `--virtual-evidence` for the separate dictionary, Flashback and archived operand inspection. [Verification evidence](../verification/README.md#oracle-virtual-column-feasibility)

## Oracle virtual column feasibility

Oracle Free 26ai `23.26.4.0.0` exposed the virtual `NUMBER` definition `"AMOUNT"*2` without a physical segment column. A native Flashback read returned the original generated values and duplicate multiplicity at one SCN. The archived redo and undo contained the physical operands for two successive updates of the same row, a SQL NULL insert and a keyless delete. They omitted the generated value. The independent `COLUMN_PRESENT` probe returned `ORA-00904` for the qualified operand, so its presence flags are unavailable. That feasibility run established which operand images are available in native archives.

Use `tests/oracle-source-probe.py --logminer --virtual-evidence --startup-timeout 480` with the frozen application and Oracle JDBC arguments. This profile inspects a disposable native database. [Evidence](../verification/README.md#oracle-virtual-column-feasibility)
