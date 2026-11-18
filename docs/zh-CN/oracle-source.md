# Oracle 源预检

[English](../en/oracle-source.md)

首批 Oracle 26ai 档案检查单一可写 CDB/PDB、归档模式、强制日志和最小补充日志、源身份及 Flashback 读取权限。ARM64 上已实测 Oracle Free 23.26.4.0.0、Java 8 和 Oracle JDBC 23.26.4.0.0。持续采集、持久断点、目标投递、RAC 与切主由 SO-07 跟踪。

1. 在 Rivomesh JAR 旁提供与运行时匹配的 Oracle JDBC JAR。[Oracle JDBC 兼容矩阵](https://docs.oracle.com/en/database/oracle/oracle-database/26/jjdbc/JDBC-getting-started.html)
2. 为 CDB 根账号授予 `V_$DATABASE`、`V_$INSTANCE`、`V_$PDBS` 的读取权限，按所选 PDB 配置 `CONTAINER_DATA`。PDB 账号需要读取 `V_$PDBS` 中的自身身份，以及所选表的 `SELECT`、`FLASHBACK` 权限。[Oracle 容器数据权限](https://docs.oracle.com/en/database/oracle/oracle-database/26/dbseg/managing-commonly-and-locally-granted-privileges.html)
3. 参照[配置示例](../../config/oracle-source-probe.yml)配置两个服务以及大写 `OWNER.TABLE` 表名。

```sh
java -cp 'rivomesh.jar:ojdbc8.jar' io.github.rivomesh.Main probe-oracle config/oracle-source-probe.yml
```

报告包含 DBID、resetlogs SCN、PDB GUID 和共同快照 SCN。每张表都在这个 SCN 做 Flashback 读取，并显示是否有主键。遇到 `ORACLE_WRITABLE_PDB_REQUIRED` 时，检查 PDB 是否处于读写状态，以及 CDB 根账号是否可见该 PDB。遇到 `ORACLE_PDB_AUTHORITY_MISMATCH` 时，核对配置的服务和 PDB 身份。

[实库夹具](../../tests/oracle-source-probe.py)验证有主键和无主键表、更新和删除后的历史值，以及将根服务配置为 PDB 时的拒绝行为。

追加 `--logminer` 运行原生归档验证。在同一提交 XID 看到更新前后 SQL 和无主键删除的撤销 SQL，已回滚的插入未出现在已提交集合。证据来自 LogMiner 查询。增量适配器仍需持久化断点并交付规范化事务。

设置 `archiveWindow.startScn` 和 `archiveWindow.endScnExclusive` 两个十进制字符串可检查指定归档区间。CDB 根账号需要读取 `V_$THREAD` 和 `V_$ARCHIVED_LOG`。检查绑定当前 resetlogs 世代和一个 redo 线程。`CATALOG_CONTIGUOUS` 表示目录里的归档段覆盖完整区间，`ORACLE_ARCHIVE_LOG_GAP` 表示至少一段缺失。恢复前保留原始区间，并用 LogMiner 验证物理日志。

设置 `archiveWindow.decodeCommitted: true` 后，预检用 LogMiner 读取已登记的归档文件。CDB 根账号额外需要 `LOGMINING`、`SYS.DBMS_LOGMNR` 执行权限和 `SYS.V_$LOGMNR_CONTENTS` 读取权限。结果包含已提交事务数、操作计数和有类型变更的 SHA-256。预检将 redo 和 undo 语句作为数据解析。目前接受支持范围内 `NUMBER(p,s)` 和 `VARCHAR2` 列的完整前后镜像，遇到跨区间事务、拆分语句、镜像缺失或未支持的操作及类型会拒绝。采集该区间之前，先为所选表配置相应的补充日志。

解码输出包含最后一笔事务的有类型提交坐标，包括提交 SCN 和 XID。解析器按完整事务分组，并在事务内编号事件。不同 XID 共用提交 SCN、顺序无法证明时会拒绝。实库还验证归档轮换后第二笔事务有独立提交坐标。
