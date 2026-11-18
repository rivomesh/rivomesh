# PostgreSQL 源端

[English](../en/postgresql-source.md)

实验档案以一个 PostgreSQL 17 或 18 可写主库为源，通过原生 `pgoutput` 读取已提交事务。一个任务配置一个 PostgreSQL 数据库，`databases` 选择其中的 schema。目标映射仍通过 `targets[].mappings` 显式声明。

## 配置与运行

1. 在源实例启用 `wal_level=logical`，为任务的持久槽和导出快照的临时槽预留容量。源账号需要 `LOGIN`、`REPLICATION` 和已发布表的 `SELECT` 权限。
2. 创建明确列出同步表的发布，包含 INSERT、UPDATE 和 DELETE。对这些表设置 `REPLICA IDENTITY FULL`，以获取包括无主键行在内的完整前镜像。保持发布的表、列和行范围稳定，结构变更先暂停并审阅。
3. 按[示例配置](../../config/postgresql-source-v3.yml)设置源库、发布和目标。使用 PostgreSQL 目标时，连接角色需要读取 `pg_control_system()` 以核对物理连接身份。源库与目标库采用独立数据库。
4. 使用发布包执行 `check <配置文件>` 验证连接和发布范围，再以 `run <配置文件>` 启动。也可以从管理界面导入 V3 配置。

数据库会在创建逻辑复制槽时提供一致位置。Rivomesh 导入导出的 MVCC 快照读取表数据，按原 LSN 捕获快照期间及之后的事务。提交已写入 Rivomesh 日志后才确认源槽。每个任务使用与日志世代绑定的槽名。[PostgreSQL 复制协议](https://www.postgresql.org/docs/17/protocol-replication.html) · [pgJDBC 复制 API](https://jdbc.postgresql.org/documentation/server-prepare/)

PostgreSQL 源使用 pgJDBC 42.7.14，并关闭驱动自动推进确认位置。普通保活和发布范围外的 WAL 不改变持久断点。发布表长时间无事务时，监控复制槽占用的 WAL 空间。[pgJDBC 下载](https://jdbc.postgresql.org/download/) · [复制流配置](https://github.com/pgjdbc/pgjdbc/blob/REL42.7.14/pgjdbc/src/main/java/org/postgresql/replication/fluent/ChainedCommonStreamBuilder.java)

## 支持边界与故障处理

- 初始类型包括有符号整数、布尔、有明确精度的小数、文本、JSON/JSONB 和 BYTEA。表可有主键，也可使用完整行身份的无主键表。
- PostgreSQL 18 的存储型生成列可发布到目标普通列。对发布设置 `ALTER PUBLICATION rivomesh_capture SET (publish_generated_columns=stored)`，表保留 `REPLICA IDENTITY FULL`。PostgreSQL 17 生成列、PostgreSQL 18 虚拟生成列、行或列过滤、分区路由、TRUNCATE 和自动 DDL 迁移需要独立档案。遇到不支持的类型或关系结构，预检或采集会给出明确错误。
- 原实例、时间线、槽和日志完好时，重启复用持续的 LSN 和已有快照。目标 SQL 错误可在修复目标原因后恢复该订阅。
- 显示 `POSTGRESQL_REQUIRED_WAL_UNAVAILABLE` 时，记录最后采集 LSN 并保留旧日志。评估原槽和所需 WAL 是否可恢复，必要时用新任务和经审阅的目标范围重建基线。
- 当前档案绑定一个 PostgreSQL 主库与单一时间线。跨时间线切换、槽同步、共同校验点和完整驱动矩阵由 SO-06 后续任务跟踪。

实库验证运行 `python3 tests/postgres-source-run.py --artifact <发布包> --output target/pg-source-proof`。夹具在独立容器中模拟 PostgreSQL 槽丢失。

已在 Java 8、11、17、21、25 上各运行七项实库检查。测试可用 `--jdk` 选择运行时。见[验收记录](../verification/README.md#postgresql-source-runtime-matrix)。

PostgreSQL 18.7 到 PostgreSQL 17.12 在 Java 8、11、17、21、25 各通过八项实库检查，包含有主键和无主键更新后的存储型生成列、目标普通列，以及发布遗漏生成列的拒绝行为。复现时追加 `--source-image postgres:18 --generated`。[生成列发布](https://www.postgresql.org/docs/18/logical-replication-gencols.html)
