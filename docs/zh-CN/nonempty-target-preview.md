# PostgreSQL 非空目标预检

如果已经向 PostgreSQL 导入过 MySQL 数据，可以在注册或启动同步任务前运行 `preview-adoption`。预检按映射比较源端有主键的表，汇总目标缺失、多出、变更及相等的行数。JSON、时间戳、十进制等数据使用与同步相同的规范化规则。

```sh
java -jar rivomesh.jar preview-adoption config/task.yml postgres
```

最后一个参数是 V3 配置中的目标 ID。目标需要指向 PostgreSQL 主库。给目标账号授予映射 schema 的 USAGE 权限、映射表及已有 Rivomesh 归属和任务表的 SELECT 权限。MySQL 账号需要[共同校验点](reconciliation-workflow.md)文档列出的读取和全局短锁权限。预检使用 PostgreSQL READ ONLY 事务，不打开同步日志库，也不写归属、业务数据或断点。

在维护窗口运行预检。它最多持有 MySQL 写入短锁 15 秒，用于建立源快照、锁定目标表并取得 PostgreSQL REPEATABLE READ 快照。开始扫描前释放写入短锁。读取期间的表锁可能延迟 DDL。预检要求保持原有的 MySQL 锁会话和快照会话。连接中断或驱动重连会使比较失效。报告对应已记录的两个快照，之后提交的数据可能改变比较结果。[PostgreSQL 事务隔离](https://www.postgresql.org/docs/17/sql-set-transaction.html)

- `MATCH_REQUIRES_VERIFIED_BASELINE` 表示已检查的行相等。保留报告用于规划，在接管前验证原始数据基线、可用源日志和目标归属。
- `DIFFERENCES_FOUND` 表示至少一个表存在缺失、多出或内容变更。通过稳定主键匹配对应行，再选择独立的对账或暂存方案。
- `BLOCKED` 表示至少一个表无法安全完成比较。查看各表阻断原因和已完成的比较。已有归属或断点、缺表、源表无主键、结构差异、继承表、分区、行安全、目标生成列、触发器或额外约束均可能阻断。

当前上限为发现 100 张源表、每张待比较表每侧 10 万行、单行解码或规范化后 4 MiB，以及每表计入预算的比较数据 64 MiB。两个读视图分别有 120 秒截止时间。数据上限适用于预检处理的解码值，JDBC 取数和 JVM 开销需要额外内存。超限、权限不足或视图失效会终止命令，无法生成最终比较结果。JSON 结果包含表名、数量和本次观察到的源端位点的 SHA-256 摘要。

## 写入交接前检查降级的 UNIQUE 索引

对于已有同步任务管理的 PostgreSQL 目标，先为该目标生成[共同校验点报告](reconciliation-workflow.md)并完成源目标比较。任务保持暂停，同时停止目标业务写入及 DDL。使用相同任务配置及该报告 ID 运行检查。

```sh
java -jar rivomesh.jar review-unique config/task.yml /private/reports <reportId> postgres
```

命令核对报告范围、日志位点、目标归属、已完成快照和目标物理断点。检查期间取得日志和目标断点锁，读取 PostgreSQL 后回滚锁事务。业务行、索引和断点保持原值。单次检查最多覆盖 100 张映射表和 128 项候选索引，期限为 120 秒。使用能够锁定断点行的同步账号。

对于已经降级的简单业务 UNIQUE，检查使用 PostgreSQL 目标列的排序规则及默认 `NULLS DISTINCT` 语义，统计冲突键组以及涉及的行数。复合键任一列为 NULL 时不计入冲突。报告展示列名和数量。MySQL 和 PostgreSQL 的行内容即使比较相等，排序规则差异仍可能造成唯一性冲突。[PostgreSQL 唯一索引](https://www.postgresql.org/docs/17/indexes-unique.html) · [ICU 排序规则](https://www.postgresql.org/docs/17/collation.html)

- `DUPLICATES_FOUND` 表示存在冲突。恢复约束前先对账处理。
- `REQUIRES_RECONCILIATION` 表示有跳过记录、历史校验差异，或需要单独规划的特殊索引。
- `NO_DUPLICATES_OBSERVED` 表示本次读视图中未发现冲突。审批约束恢复及写入交接前，核对最终位点、SQL 定义和业务语义。恢复同步或推进断点后，需要取得新的共同校验点再检查。
- `NO_DOWNGRADED_UNIQUE` 表示所选范围内没有相关候选索引。

检查覆盖普通 PostgreSQL 映射表和默认的 B-tree 键语义。GSI 全局唯一性、表达式、前缀长度、自定义操作符类及其他 NULL 语义需独立评估。约束创建、写入交接和回切按迁移方案执行。

## 在冻结边界生成约束恢复计划

恢复计划覆盖单张有主键的 InnoDB 表及一项已降级的业务 UNIQUE。源需直连单实例 MySQL 8.4，设置 `source.topology: AUTO` 且不配置候选节点。目标需为 UTF-8 编码的 PostgreSQL 17 主库。先停止两边业务写入，取得内容匹配的共同校验点并保持 Rivomesh 暂停，再由数据库运维人员将同一 MySQL 实例置为 `read_only` 和 `super_read_only`。源 UUID、GTID、binlog 文件及偏移必须与报告完全一致。MySQL 的只读模式仍允许复制线程写入，因此有入站复制通道配置或组复制成员时会拒绝生成计划。[MySQL 系统变量](https://dev.mysql.com/doc/refman/8.4/en/server-system-variables.html)

使用 `review-unique` 输出的表 ID 和 MySQL 原始索引名运行命令。

```sh
java -jar rivomesh.jar plan-unique-restore config/task.yml /private/reports <reportId> postgres <tableId> <sourceIndex>
```

命令核对已封存的匹配报告及源、目标跳过记录。在已冻结的 MySQL 上取得有时限的全局短锁，核对原会话并重算源行摘要。读取目标时持有日志暂停锁、目标表锁和断点锁，重新检查行摘要、归属、唯一性及已有普通索引。范围上限为 1 万行、单条规范化行 1 MiB、私有暂存 16 MiB，锁期限为 30 秒。源账号需要共同校验点的读取、短锁及复制状态权限。目标账号还需有权执行 `pg_catalog.pg_control_system()` 以识别物理实例。[PostgreSQL 控制信息函数](https://www.postgresql.org/docs/17/functions-info.html)

候选键支持映射成 PostgreSQL `bigint` 的有符号整数，或源端 `utf8mb4_0900_bin` VARCHAR 映射到显式使用 `pg_catalog."C"` 的 PostgreSQL `varchar` 或 `text`。检查已有普通 B-tree 索引的列、顺序、排序规则和默认操作符类。GSI、表达式索引及生成键列需要单独规划。此档案按原有语义识别大小写、尾随空格和 NULL。[MySQL 二进制排序规则](https://dev.mysql.com/doc/refman/8.4/en/charset-binary-collations.html) · [PostgreSQL 唯一索引](https://www.postgresql.org/docs/17/indexes-unique.html)

`RESTORE_PLAN_OBSERVED` 返回稳定的计划 SHA-256 摘要，绑定报告封存值、精确位点、当前表内容和目标物理身份。将输出保存用于人工审阅。核对审阅过的摘要时，以 `--expect-sha256 <digest>` 重跑命令。它会再次核验现场状态，普通索引重建等变化会使旧摘要失效。其他库产生的新 GTID 也会拒绝该校验点。现场重算摘要同时覆盖未写入 binlog 的源端数据修改和目标端外部修改。冻结、目标状态或断点变化后需重新验证。持久审批、执行约束恢复、业务写入交接及回切需要独立验收。

后续持久执行需要建立跨连接中断和源实例重启仍有效的写入隔离，并保持至运维完成业务交接。审批绑定计划摘要、目标身份和请求 ID。执行时重新核对冻结边界，在目标写入锁和断点锁内完成 PostgreSQL 约束事务。DDL 与执行回执在同一事务提交，重启后先查回执确认结果，再决定是否重试。

### 常见问题

**报告相等后可以在现有表上启动新任务吗？** 新任务仍要求兼容的空表。预检不会建立可续传位点或授予表归属。非空目标需另行验证基线并规划写入交接，也可先同步到独立的空 schema，对账后再切换。

**同时检查目标 schema 中其他表吗？** 预检只查看已配置的源库到目标 schema 映射中的同名表。迁移时另行盘点其余表和 schema。

**怎样修复数据或恢复曾降级的 UNIQUE？** 已经由任务管理的目标可以按[共同校验点修复](common-cut-repair.md)的校验流程处理。约束恢复、最终写入交接及回切按目标产品规划。

[English](../en/nonempty-target-preview.md)
