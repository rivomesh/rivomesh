# Oracle 源、归档采集与预检

[English](../en/oracle-source.md)

Oracle 26ai 档案检查单一可写 CDB/PDB、归档模式、强制日志和最小补充日志、源身份及 Flashback 读取权限。归档采集到 PostgreSQL 和持久 SCN 恢复已在 ARM64 上使用 Oracle Free 23.26.4.0.0、Temurin 21 和 Oracle JDBC 23.26.4.0.0 复验。更多版本、类型、RAC 和切主继续由 SO-07 跟踪。

1. 在 Rivomesh JAR 旁提供与运行时匹配的 Oracle JDBC JAR。[Oracle JDBC 兼容矩阵](https://docs.oracle.com/en/database/oracle/oracle-database/26/jjdbc/JDBC-getting-started.html)
2. 为 CDB 根账号授予 `V_$DATABASE`、`V_$INSTANCE`、`V_$PDBS` 的读取权限，按所选 PDB 配置 `CONTAINER_DATA`。PDB 账号需要读取 `V_$PDBS` 中的自身身份，以及所选表的 `SELECT`、`FLASHBACK` 权限。[Oracle 容器数据权限](https://docs.oracle.com/en/database/oracle/oracle-database/26/dbseg/managing-commonly-and-locally-granted-privileges.html)
3. 参照[配置示例](../../config/oracle-source-probe.yml)配置两个服务以及大写 `OWNER.TABLE` 表名。

```sh
java -cp 'rivomesh.jar:ojdbc8.jar' io.github.rivomesh.Main probe-oracle config/oracle-source-probe.yml
```

报告包含 DBID、resetlogs SCN、PDB GUID 和共同快照 SCN。每张表都在这个 SCN 做 Flashback 读取，并显示是否有主键。遇到 `ORACLE_WRITABLE_PDB_REQUIRED` 时，检查 PDB 是否处于读写状态，以及 CDB 根账号是否可见该 PDB。遇到 `ORACLE_PDB_AUTHORITY_MISMATCH` 时，核对配置的服务和 PDB 身份。

[实库夹具](../../tests/oracle-source-probe.py)验证有主键和无主键表、更新和删除后的历史值，以及将根服务配置为 PDB 时的拒绝行为。

追加 `--logminer` 运行原生归档验证。在同一提交 XID 看到更新前后 SQL 和无主键删除的撤销 SQL，已回滚的插入未出现在已提交集合。证据来自 LogMiner 查询。

设置 `archiveWindow.startScn` 和 `archiveWindow.endScnExclusive` 两个十进制字符串可检查指定归档区间。CDB 根账号需要读取 `V_$THREAD` 和 `V_$ARCHIVED_LOG`。检查绑定当前 resetlogs 世代和一个 redo 线程。`CATALOG_CONTIGUOUS` 表示目录里的归档段覆盖完整区间，`ORACLE_ARCHIVE_LOG_GAP` 表示至少一段缺失。恢复前保留原始区间，并用 LogMiner 验证物理日志。

设置 `archiveWindow.decodeCommitted: true` 后，预检用 LogMiner 读取已登记的归档文件。CDB 根账号额外需要 `LOGMINING`、`SYS.DBMS_LOGMNR` 执行权限和 `SYS.V_$LOGMNR_CONTENTS` 读取权限。结果包含已提交事务数、操作计数和有类型变更的 SHA-256。预检将 redo 和 undo 语句作为数据解析。目前接受支持范围内 `NUMBER(p,s)`、`VARCHAR2` 和 `DATE` 列的完整前后镜像，遇到跨区间事务、拆分语句、镜像缺失或未支持的操作及类型会拒绝。采集该区间之前，先为所选表配置相应的补充日志。

解码输出包含最后一笔事务的有类型提交坐标，包括提交 SCN 和 XID。解析器按完整事务分组，并在事务内编号事件。不同 XID 共用提交 SCN、顺序无法证明时会拒绝。实库还验证归档轮换后第二笔事务有独立提交坐标。

## V3 Oracle 到 PostgreSQL

按 [V3 样例](../../config/oracle-source-v3.yml) 设置 `source.type: ORACLE`。首批实验档案为 ARM64 Oracle 26ai Free 23.26.4.0.0 到 PostgreSQL 17.12。显式列出大写源 OWNER 和表、目标 schema 映射及单一可写 PDB。CDB 根账号除上文归档和 LogMiner 权限外，还需读取 `V_$TRANSACTION`、`V_$PDB_INCARNATION`。每张源表启用全列补充日志。PDB 账号读取表、元数据和共同 Flashback SCN。

```sh
java -cp 'rivomesh.jar:ojdbc8.jar' io.github.rivomesh.Main check config/oracle-source-v3.yml
java -cp 'rivomesh.jar:ojdbc8.jar' io.github.rivomesh.Main run config/oracle-source-v3.yml
```

使用相同类路径启动拥有 Worker 的进程（组合模式使用 `serve`，分离模式使用 `agent`）。可在界面导入 V3 配置，或在新建任务选择 **V3 → Oracle**，分别填 CDB 根和 PDB 连接、PDB 名称及大写 `OWNER.TABLE` 表名。每个源 owner 显式映射到 PostgreSQL Schema，启动前在选定 Agent 上执行 **检查**。受监督的 Worker 继承所属进程类路径中的外置 Oracle 驱动。此档案支持有明确精度的 `NUMBER(p,s)`、`VARCHAR2` 和 `DATE`，覆盖有主键及无主键表。每次开始采集使用共同快照 SCN。未完成的快照可以重取 Flashback 视图，完成的快照可复用。

采集轮询已关闭的归档段。断点绑定 CDB DBID 与 resetlogs、PDB GUID、UID 和世代、已采集 SCN 及最早所需归档 SCN。快照时仍打开的事务可使最早归档位置保留在较早处，直到归档包含该事务提交后才会投递整笔。只有归档覆盖后续活跃事务采样的 SCN，才会释放更老的保留范围。按任务显示的最早 SCN 保留源归档，并按实际负载验证轮换与保留策略。

归档缺失、事件格式或顺序无法证明、未审核 DDL 会停止推进并保留原断点，可从界面查看采集恢复情况。Oracle 切主、RAC、在线 redo、原生二进制 JSON、LOB、其他生成表达式、部分回滚及更多目标档案另行验收。SecureFiles LOB 的 LogMiner 记录缺少 `SQL_UNDO`，当前完整前后镜像采集在预检时拒绝 LOB 列。[Oracle LogMiner 数据类型与 SecureFiles](https://docs.oracle.com/en/database/oracle/oracle-database/26/sutil/oracle-logminer-utility.html) · [Oracle 活跃事务](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/V-TRANSACTION.html) · [PDB 世代](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/V-PDB_INCARNATION.html)

### 文本 JSON 列

Oracle `VARCHAR2` 列的声明长度上限为 4000 字节时，可通过一条启用、已验证且不可延迟的列级或表级 `CHECK` 将该列映射到 PostgreSQL `jsonb`。约束直接检查单列并要求严格语法及对象键唯一。其他 `VARCHAR2` 列沿用字符串映射。[Oracle SQL/JSON 约束](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/is-json-condition.html)

```sql
CREATE TABLE RIVO_APP.DOCUMENTS (
    ID NUMBER(12) PRIMARY KEY,
    PAYLOAD VARCHAR2(1000 BYTE)
        CHECK (PAYLOAD IS JSON (STRICT WITH UNIQUE KEYS))
);
ALTER TABLE RIVO_APP.DOCUMENTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

两个修饰词写在同一对括号内。Oracle Free 23.26.4 的 `CHECK` 会以 `ORA-02252` 拒绝拆开的 `IS JSON (STRICT) WITH UNIQUE KEYS`。

Flashback 和归档采集都验证 JSON 语法、完整单个值及重复对象键。SQL `NULL` 和 JSON `null` 分别保留。PostgreSQL `jsonb` 保存值语义，输出可能改变空白、成员顺序和数值表示。若源将文本 JSON 用作主键，预检返回 `ORACLE_TEXT_JSON_PRIMARY_KEY_UNSUPPORTED`，防止源上文本不同但目标 JSON 值等价的键冲突。Oracle 或 PostgreSQL 无法表示的值通过各自的错误和恢复流程处理。[PostgreSQL JSON 类型](https://www.postgresql.org/docs/17/datatype-json.html)

为已同步的列添加或删除这类约束会改变目标列的类型映射。先审核目标结构及已有数据，再按 DDL 恢复流程恢复采集。已有 `IS JSON` 约束时，Oracle 对同一列新增第二条会返回 `ORA-40664`。审核后可在同一次结构迁移中替换原约束。[Oracle ORA-40664](https://docs.oracle.com/en/error-help/db/ora-40664/)

## Oracle DATE 到 PostgreSQL

Oracle `DATE` 映射为 PostgreSQL `timestamp(6) without time zone`，保留年月日、时分秒与 SQL `NULL`。采集专用的 Oracle 会话固定使用公历和美式英语日期。PostgreSQL 读写使用 `LocalDateTime`，Worker 的时区在夏令时跳变期间也不会移动 Oracle 保存的本地时间。既有目标表的列同样须采用 `timestamp without time zone`。[Oracle DATE 类型](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html) · [Oracle 日期格式](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Format-Models.html) · [PostgreSQL JDBC 日期及时间类型](https://jdbc.postgresql.org/documentation/query/)

此档案接受 1582-10-15 至 9999-12-31 的日期。采集识别到更早日期时，包括公元前日期，会报告 `ORACLE_DATE_VALUE_OUT_OF_RANGE`。遇到尚未支持的 LogMiner 日期格式则报告 `ORACLE_DATE_FORMAT_UNSUPPORTED`。中断后先核对源值、受影响的表和持久化位点，再重试。Oracle 的各类 `TIMESTAMP` 另行验证。

用 `tests/oracle-source-probe.py --logminer --pipeline` 和冻结 JAR、Oracle JDBC 参数复现临时实库、强杀和恢复。加入 `--text-json` 验证严格文本 JSON，加入 `--date-values` 验证 Oracle DATE，均覆盖有主键、无主键表的 Flashback、归档 CDC 和恢复。[验收记录](../verification/README.md#oracle-archived-cdc-and-restart)

## 整数虚拟列

Oracle 26ai 任务可在向导选中 **同步受支持的整数虚拟列**，或在 V3 YAML 设置 `source.oracle.allowVirtualNumericColumns: true`。启动前先执行检查。

支持三种虚拟 `NUMBER(p,0)` 表达式。可以将一个物理整数列乘以 1 至 999 的整数常量，也可以对两个物理整数列求和或求差。操作数和结果都须明确声明精度，最大 18 位，结果精度须覆盖所有操作数的完整取值范围。例如 `AMOUNT NUMBER(12)`、`ADJUSTMENT NUMBER(12)` 和 `TOTAL NUMBER(16,0) GENERATED ALWAYS AS (AMOUNT + ADJUSTMENT) VIRTUAL`。虚拟主键、函数表达式和小数操作数需要单独支持。 [Oracle 列表达式](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Column-Expressions.html)

Worker 在全量阶段读取历史 SCN 的虚拟值并独立核对。增量阶段从归档 redo 和 undo 的物理列分别计算前后镜像。PostgreSQL 接收保存计算值的普通数值列。同一事务内多次更新和无主键重复行保留原有语义。任一操作数为 SQL NULL 时，计算结果为 SQL NULL。

在创建日志时开启此项。设置会改变源指纹。已有任务应先审阅表结构与断点迁移，再启用此功能。

以冻结应用包及 Oracle JDBC 参数运行 `tests/oracle-source-probe.py --logminer --pipeline --virtual-columns --startup-timeout 480`。实库夹具检查前后值、无主键重复行、原快照续传，以及对不支持表达式的拒绝。 [验证证据](../verification/README.md#oracle-virtual-column-feasibility)

## Oracle 虚拟生成列可行性

Oracle Free 26ai `23.26.4.0.0` 的目录显示虚拟 `NUMBER` 表达式 `"AMOUNT"*2` 没有物理段列。在同一历史 SCN，Flashback 读取了原生成值并保留无主键重复行数量。归档 redo 和 undo 包含同一行两次更新的基础值、SQL NULL 插入和无主键删除的前镜像，生成值未出现在归档语句中。独立的 `COLUMN_PRESENT` 探测对限定列标识返回 `ORA-00904`，无法使用该函数的存在标记。该次可行性试验确认了原生归档提供的操作数。

使用已冻结的应用包和 Oracle JDBC 参数运行 `tests/oracle-source-probe.py --logminer --virtual-evidence --startup-timeout 480`。该档案在临时 Oracle 实例中运行。 [验证证据](../verification/README.md#oracle-virtual-column-feasibility)
