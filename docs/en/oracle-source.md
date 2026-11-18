# Oracle source preflight

[中文](../zh-CN/oracle-source.md)

The first Oracle 26ai profile verifies one writable CDB/PDB, archive mode, force/minimal supplemental logging, source identity and Flashback read access. The native test used Oracle Free 23.26.4.0.0 on ARM64 with Java 8 and Oracle JDBC 23.26.4.0.0. SO-07 tracks continuous capture, durable checkpoints, destination delivery, RAC and failover.

1. Provide an Oracle JDBC JAR compatible with your Java runtime alongside the Rivomesh JAR. [Oracle JDBC compatibility](https://docs.oracle.com/en/database/oracle/oracle-database/26/jjdbc/JDBC-getting-started.html)
2. Give the CDB-root account read access to `V_$DATABASE`, `V_$INSTANCE` and the selected PDB in `V_$PDBS`. Configure its `CONTAINER_DATA` visibility for that PDB. Give the PDB account read access to its `V_$PDBS` identity and `SELECT` and `FLASHBACK` on the declared tables. [Oracle container data privileges](https://docs.oracle.com/en/database/oracle/oracle-database/26/dbseg/managing-commonly-and-locally-granted-privileges.html)
3. Configure the two services and uppercase `OWNER.TABLE` names with the [probe example](../../config/oracle-source-probe.yml).

```sh
java -cp 'rivomesh.jar:ojdbc8.jar' io.github.rivomesh.Main probe-oracle config/oracle-source-probe.yml
```

The report includes DBID, the resetlogs SCN, PDB GUID and a common snapshot SCN. It checks a Flashback read in each table at that SCN and reports primary-key presence. For `ORACLE_WRITABLE_PDB_REQUIRED`, verify the PDB is open read/write and the root account can see that PDB. For `ORACLE_PDB_AUTHORITY_MISMATCH`, compare the supplied service and PDB identity.

The [native fixture](../../tests/oracle-source-probe.py) verifies keyed and keyless tables, a historical update/delete and rejection of the root service supplied as the PDB.

Add `--logminer` for the native archive verification. One commit XID contains an update and keyless delete with redo and undo SQL, and the committed-only result excludes a rolled-back insert. The streaming adapter still needs durable checkpoints and normalized transaction delivery. [Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/26/sutil/oracle-logminer-utility.html)

Add `archiveWindow.startScn` and `archiveWindow.endScnExclusive` as decimal strings to validate a selected archived interval. The root account needs read access to `V_$THREAD` and `V_$ARCHIVED_LOG`. The check covers the current resetlogs incarnation and one redo thread. `CATALOG_CONTIGUOUS` reports uninterrupted catalog coverage. `ORACLE_ARCHIVE_LOG_GAP` indicates that the archive catalog does not cover the entire interval. Retain the original interval and verify physical files with LogMiner before a recovery.

Set `archiveWindow.decodeCommitted: true` to read the registered archive files with LogMiner. The CDB-root account additionally needs `LOGMINING`, `EXECUTE` on `SYS.DBMS_LOGMNR` and access to `SYS.V_$LOGMNR_CONTENTS`. The report includes the committed transaction count, operation counts and SHA-256 of typed changes. The probe reads the generated redo and undo statements as data. It accepts complete images for supported `NUMBER(p,s)` and `VARCHAR2` columns, and refuses crossing transactions, split redo statements, missing supplemental images and unsupported operations or types. Enable appropriate supplemental logging on each table before recording the interval.

The decoded output includes the last typed commit coordinate with commit SCN and XID. Replay groups complete transactions with per-transaction event ordinals. It refuses an ambiguous sequence if different XIDs share a commit SCN. The native fixture also rotates an initially unarchived suffix and verifies a second distinct commit coordinate.
