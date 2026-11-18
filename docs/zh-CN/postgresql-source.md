# PostgreSQL 源端

[English](../en/postgresql-source.md)

实验档案以一个 PostgreSQL 17 或 18 可写主库为源，通过原生 `pgoutput` 读取已提交事务。一个任务配置一个 PostgreSQL 数据库，`databases` 选择其中的 schema。目标映射仍通过 `targets[].mappings` 显式声明。

## 配置与运行

1. 在源实例启用 `wal_level=logical`，为任务的持久槽和导出快照的临时槽预留容量。源账号需要 `LOGIN`、`REPLICATION` 和已发布表的 `SELECT` 权限。
2. 创建明确列出同步表的发布，包含 INSERT、UPDATE 和 DELETE。对这些表设置 `REPLICA IDENTITY FULL`，以获取包括无主键行在内的完整前镜像。保持发布的表、列和行范围稳定，结构变更先暂停并审阅。
3. 按[示例配置](../../config/postgresql-source-v3.yml)设置源库、发布和目标。使用 PostgreSQL 目标时，连接角色需要读取 `pg_control_system()` 以核对物理连接身份。源库与目标库采用独立数据库。
4. 使用发布包执行 `check <配置文件>` 验证连接和发布范围，再以 `run <配置文件>` 启动。在管理界面可导入 V3 配置，也可从新建任务选择 **V3 → PostgreSQL**，填写一个数据库、源 Schema、发布名和逻辑槽前缀。向导提供 PostgreSQL 和 MySQL 目标选项。保存后在选定 Agent 上执行 **检查**，通过后再 **启动**。

数据库会在创建逻辑复制槽时提供一致位置。Rivomesh 导入导出的 MVCC 快照读取表数据，按原 LSN 捕获快照期间及之后的事务。提交已写入 Rivomesh 日志后才确认源槽。每个任务使用与日志世代绑定的槽名。[PostgreSQL 复制协议](https://www.postgresql.org/docs/17/protocol-replication.html) · [pgJDBC 复制 API](https://jdbc.postgresql.org/documentation/server-prepare/)

PostgreSQL 源使用 pgJDBC 42.7.14，并关闭驱动自动推进确认位置。普通保活和发布范围外的 WAL 不改变持久断点。发布表长时间无事务时，监控复制槽占用的 WAL 空间。[pgJDBC 下载](https://jdbc.postgresql.org/download/) · [复制流配置](https://github.com/pgjdbc/pgjdbc/blob/REL42.7.14/pgjdbc/src/main/java/org/postgresql/replication/fluent/ChainedCommonStreamBuilder.java)

## 支持边界与故障处理

- 初始类型包括有符号整数、布尔、有明确精度的小数、文本、JSON/JSONB 和 BYTEA。表可有主键，也可使用完整行身份的无主键表。
- PostgreSQL UUID 以小写标准格式写入目标 `VARCHAR(36)`。PostgreSQL 17.12 到 PostgreSQL 17.12 档案已验证 NULL 和 UUID 主键变更。夹具追加 `--uuid` 可复现。[PostgreSQL UUID 格式](https://www.postgresql.org/docs/17/datatype-uuid.html)
- PostgreSQL 18 的存储型生成列可发布到目标普通列。对发布设置 `ALTER PUBLICATION rivomesh_capture SET (publish_generated_columns=stored)`，表保留 `REPLICA IDENTITY FULL`。PostgreSQL 17 生成列、PostgreSQL 18 虚拟生成列、行或列过滤、分区路由、TRUNCATE 和自动 DDL 迁移需要独立档案。遇到不支持的类型或关系结构，预检或采集会给出明确错误。
- 原实例、时间线、槽和日志完好时，重启复用持续的 LSN 和已有快照。目标 SQL 错误可在修复目标原因后恢复该订阅。
- 显示 `POSTGRESQL_REQUIRED_WAL_UNAVAILABLE` 时，记录最后采集 LSN 并保留旧日志。评估原槽和所需 WAL 是否可恢复，必要时用新任务和经审阅的目标范围重建基线。
- 共同点对账、更多目标与切换档案继续由 SO-06 跟踪。

实库验证运行 `python3 tests/postgres-source-run.py --artifact <发布包> --output target/pg-source-proof`。夹具在独立容器中模拟 PostgreSQL 槽丢失。

当前构建与夹具统一使用 Temurin 21。早期多版本测试记录按原候选包留档。[验收记录](../verification/README.md#postgresql-source-runtime-matrix)

PostgreSQL 18.7 到 PostgreSQL 17.12 在 Temurin 21 下通过八项实库检查，覆盖有主键和无主键更新后的存储型生成列、目标普通列，以及发布遗漏生成列的拒绝行为。复现时追加 `--source-image postgres:18 --generated`。[生成列发布](https://www.postgresql.org/docs/18/logical-replication-gencols.html)

## 验证 TLS 连接

分别设置 PostgreSQL 源端和目标端的 JDBC URL。服务器启用支持通道绑定的 SCRAM 时，可在两个 URL 中设置 `sslmode=verify-full&sslrootcert=/run/rivomesh/trust/ca.pem&channelBinding=require`。把公开 CA 证书以只读方式挂载到执行 Agent。URL 使用的 DNS 名称必须与服务端证书匹配，使用数据库高可用服务时也要覆盖稳定写入口的名称。变更证书或挂载后，在执行 Agent 上重新执行 **检查**。[pgJDBC TLS](https://jdbc.postgresql.org/documentation/ssl/) · [pgJDBC 连接参数](https://jdbc.postgresql.org/documentation/use/)

验证 PostgreSQL 17.12 直连源和目标的 TLS 档案时，给实库夹具追加 `--source-image postgres:17.12 --tls`。夹具使用临时 CA 和服务端证书，查询真实 TLS 会话，并验证源域名或目标 CA 不匹配时预检会拒绝连接。[PostgreSQL TLS 统计](https://www.postgresql.org/docs/17/monitoring-stats.html#MONITORING-PG-STAT-SSL-VIEW)

## 同步槽与备库提升

创建新任务时在 `source.postgresql.url` 配置稳定的 PostgreSQL 写入口，在 `source.postgresql.failoverSlot` 设置 `true`。主库的 `synchronized_standby_slots` 列出用于物理备库的槽。备库设置对应的 `primary_slot_name`、`hot_standby_feedback=on`、`sync_replication_slots=on`，并在 `primary_conninfo` 配置有效的数据库名。写入口切换前，确认备库上的逻辑槽持久化、已同步且具备所需 WAL。[PostgreSQL 复制槽同步](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION) · [切换检查](https://www.postgresql.org/docs/17/logical-replication-failover.html)

数据库高可用服务隔离旧主、提升备库并更新写入口，Rivomesh 通过原入口重新连接。新主需要有相同的 system ID、覆盖原持久 LSN 和快照坐标的时间线历史，以及原 failover 槽。该槽须保有所需 WAL，确认位置不能领先 Rivomesh 日志。检查失败时保留旧 WAL 和日志。下一次切换前准备下一台物理备库。

PostgreSQL 17.12 在 Temurin 21 下实测强杀原主、原生备库提升、复用快照继续投递，以及误指回旧主时保留新时间线断点并拒绝续传。夹具用 `IDENTIFY_SYSTEM` 核对当前活动时间线。测试参数为 `--source-image postgres:17.12 --failover`，追加 `--reject-old-primary` 可复现旧主路由拒绝。负载均衡产品、级联拓扑和其他版本组合单独验收。[流复制协议](https://www.postgresql.org/docs/17/protocol-replication.html)
