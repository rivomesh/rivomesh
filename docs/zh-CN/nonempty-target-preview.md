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

### 常见问题

**报告相等后可以在现有表上启动新任务吗？** 新任务仍要求兼容的空表。预检不会建立可续传位点或授予表归属。非空目标需另行验证基线并规划写入交接，也可先同步到独立的空 schema，对账后再切换。

**同时检查目标 schema 中其他表吗？** 预检只查看已配置的源库到目标 schema 映射中的同名表。迁移时另行盘点其余表和 schema。

**能顺带修复数据或恢复曾降级的 UNIQUE 吗？** 预检只读。[共同校验点修复](common-cut-repair.md)适用于已经由任务管理的目标，并需要独立的校验。约束恢复、最终写入交接及回切按目标产品规划。

[English](../en/nonempty-target-preview.md)
