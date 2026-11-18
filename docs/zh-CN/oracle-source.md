# Oracle 源预检

[English](../en/oracle-source.md)

首批 Oracle 26ai 档案检查单一可写 CDB/PDB、归档模式、强制日志和最小补充日志、源身份及 Flashback 读取权限。ARM64 上已实测 Oracle Free 23.26.4.0.0、Java 8 和 Oracle JDBC 23.26.4.0.0。LogMiner 采集、事务解码、断点投递、RAC 与切主继续由 SO-07 跟踪。

1. 在 Rivomesh JAR 旁提供与运行时匹配的 Oracle JDBC JAR。[Oracle JDBC 兼容矩阵](https://docs.oracle.com/en/database/oracle/oracle-database/26/jjdbc/JDBC-getting-started.html)
2. 为 CDB 根账号授予 `V_$DATABASE`、`V_$INSTANCE`、`V_$PDBS` 的读取权限，按所选 PDB 配置 `CONTAINER_DATA`。PDB 账号需要读取 `V_$PDBS` 中的自身身份，以及所选表的 `SELECT`、`FLASHBACK` 权限。[Oracle 容器数据权限](https://docs.oracle.com/en/database/oracle/oracle-database/26/dbseg/managing-commonly-and-locally-granted-privileges.html)
3. 参照[配置示例](../../config/oracle-source-probe.yml)配置两个服务以及大写 `OWNER.TABLE` 表名。

```sh
java -cp 'rivomesh.jar:ojdbc8.jar' io.github.rivomesh.Main probe-oracle config/oracle-source-probe.yml
```

报告包含 DBID、resetlogs SCN、PDB GUID 和共同快照 SCN。每张表都在这个 SCN 做 Flashback 读取，并显示是否有主键。遇到 `ORACLE_WRITABLE_PDB_REQUIRED` 时，检查 PDB 是否处于读写状态，以及 CDB 根账号是否可见该 PDB。遇到 `ORACLE_PDB_AUTHORITY_MISMATCH` 时，核对配置的服务和 PDB 身份。

[实库夹具](../../tests/oracle-source-probe.py)验证有主键和无主键表、更新和删除后的历史值，以及将根服务配置为 PDB 时的拒绝行为。
