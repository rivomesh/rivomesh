# 连接器、Java 与 API 兼容档案

[English](../en/compatibility.md)

先选源版本和拓扑，再选目标产品与模式、Java 运行时。每行记录已实测范围或待验收事项。上线验收前，到[验证基线](../verification/README.md)和[机器可读记录](../verification/baseline.json)核对对应包的 SHA-256。

## 源版本和坐标

| 配置 | 实库范围 | 配置与恢复 |
|---|---|---|
| `MYSQL` 5.7.44 | Java 8。单机、GTID 主从提升和单主组复制。真实日志缺失时拒绝续传。 | [同步配置](pipeline-v3.md)、[日志缺失恢复](capture-recovery.md) |
| `MYSQL` 8.0.46 | Java 8。单机、GTID 主从提升和单主组复制。 | [同步配置](pipeline-v3.md)、[恢复手册](reconciliation-workflow.md) |
| `MYSQL` 8.2.0 | Java 8。单机、GTID 主从提升和单主组复制。 | [版本专项命令](pipeline-v3.md) |
| `MYSQL` 8.4.13 | Java 8。三种拓扑以及 binlog 准入检查。下方进程矩阵另含五种 JDK 的单机档案。 | [8.4 binlog 档案](pipeline-v3.md) |
| `POSTGRESQL` 17.12 | 实验性单主源。五种 JDK 分别通过 7 项 PostgreSQL 到 PostgreSQL 实库验证。Java 8 已通过同步槽切主，PostgreSQL 共同点对账待验证。 | [源指南](postgresql-source.md)、[任务样例](../../config/postgresql-source-v3.yml) |
| `POSTGRESQL` 18.7 | 实验性单主源，发布存储型生成列并物化为目标普通列。 | [源指南与发布](postgresql-source.md) |
| `ORACLE` 26ai Free 23.26.4.0.0 | Java 8，单独提供 Oracle JDBC 驱动。实测 SCN Flashback 和单线程归档 redo 解码。自动投递和 RAC 待实现。 | [预检权限与恢复](oracle-source.md)、[预检样例](../../config/oracle-source-probe.yml) |

MySQL 坐标使用逻辑源集群 ID 与无标签 GTID 集合。PostgreSQL 坐标使用逻辑源 ID、system ID、timeline、slot 和 LSN。Oracle 预检使用 DBID、resetlogs SCN、PDB GUID 和 SCN。`SourcePosition.formatVersion` 当前为 1，由 `sourceType` 对应的适配器解释。在同一来源身份和协议内比较坐标。[多库样例](../../config/pipeline-v3.yml)使用 `formatVersion: 3`、`source.clusterId`、`databases` 以及每个目标的显式 `mappings`。独立 MySQL 集群分别使用独立配置。

## 目标产品和一致性

| 目标标识 | 范围与投递语义 | 手册 |
|---|---|---|
| `MYSQL`、`POSTGRESQL` | MySQL 8.4 和 PostgreSQL 17.12 事务档案已实测。业务写入和目标断点在同一事务提交。 | [关系型目标](pipeline-v3.md) |
| `GAUSS` | openGauss 7.0.0-RC3 事务档案已实测。商用分布式 GaussDB 的约束、GSI 和部署验收待完成。 | [高斯索引策略与接管](gaussdb-index-policy.md) |
| `DM`、`KINGBASE`、`GBASE_8C` | 首批 Java 8 事务档案。确切版本、模式、驱动、类型转换和恢复范围见各产品手册。 | [达梦](dm-v3.md)、[金仓](kingbase-v3.md)、[GBase 8c](gbase8c-v3.md) |
| `GBASE_8A`、`GBASE_8S` | 独立产品，规划中。 | [实施状态](implementation-status.md) |
| `OCEANBASE` | CE 4.3.5.6 MySQL 租户在 Java 8 已通过原生事务、全量、增量、重启和 SQL 恢复。Oracle 模式和集群待完成。 | [OceanBase](oceanbase-v3.md) |
| `CLICKHOUSE`、`DORIS`、`HIVE` | 首批实库分析档案，采用版本化物化与回执。数据可见性和重放规则按引擎区分。 | [ClickHouse](clickhouse-v3.md)、[Doris](doris-v3.md)、[Hive](hive-v3.md) |
| `KAFKA`、`PULSAR`、`RABBITMQ`、`ROCKETMQ`、`ACTIVEMQ_CLASSIC`、`ACTIVEMQ_ARTEMIS`、`REDIS_STREAMS`、`NATS_JETSTREAM` | 限定 broker 档案，确认式至少一次投递。消费端按稳定事件 ID 去重，独立记录业务消费进度。 | [消息目标](messaging-v3.md)、[ActiveMQ Classic](classic-v3.md) |

向导读取 `GET /api/capabilities`。部署前按目标手册核对拓扑、权限、保留与备份边界。

## 源 × 目标 × Java 验证范围

| 实测源 → 目标 | Java 8 | Java 11 | Java 17 | Java 21 | Java 25 |
|---|---|---|---|---|---|
| MySQL 8.4 单机 → PostgreSQL 17.12 和独立 MySQL 8.4 | Controller、Agent、Worker | 同左 | 同左 | 同左 | 同左 |
| PostgreSQL 17.12 单主 → PostgreSQL 17.12 | 7 项 | 7 项 | 7 项 | 7 项 | 7 项 |
| PostgreSQL 18.7 单主 → PostgreSQL 17.12，包含存储型生成列 | 8 项 | 8 项 | 8 项 | 8 项 | 8 项 |
| MySQL 5.7.44 / 8.0.46 / 8.2.0 原生源档案 | 限定实库 | 待验收 | 待验收 | 待验收 | 待验收 |
| Oracle 26ai → 目标 | 仅源端预检 | 待验收 | 待验收 | 待验收 | 待验收 |
| 其他目标和拓扑 | 上表各产品实测范围 | 待验收 | 待验收 | 待验收 | 待验收 |

前三行对应[实测记录](../verification/baseline.json)里的 `fiveJdkProcesses`、`postgresqlSourceRuntimeMatrix` 和 `postgresqlStoredGenerated`。其他组合按实际 JDK 运行对应源、目标、存储和恢复测试。[复现命令](performance.md)

## 管理端请求

CLI 通过 Bearer 凭据访问，浏览器使用 HttpOnly 会话。浏览器写操作提供匹配的 Origin 与 `X-CSRF-Token`。VIEWER 按授权读取状态，OPERATOR 在授权范围内发出常规命令，ADMIN 配置任务、审阅敏感证据和批准恢复。[浏览器会话](browser-sessions.md) · [任务授权](task-authorization.md)

| 接口 | 请求与结果 |
|---|---|
| `/api/session`、`/api/session/logout`、`/api/nodes`、`/api/tasks` | 查询身份权限、节点和归属、分页任务。全局模式创建任务包含 `ownerNodeId`。[控制台接口](console-and-deployment.md) |
| `/api/tasks/{id}/configuration`、`/api/tasks/{id}/actions` | 变更绑定当前 `expectedRevision` 和原始 `X-Request-ID`。本地创建返回 201，本地更新配置返回 200，全局变更与操作受理返回 202。随后读取持久化回执。[配置编辑](configuration-editor.md) · [操作生命周期](execution-lifecycle.md) |
| `/api/tasks/{id}/capture-recovery`、`/api/tasks/{id}/failure-evidence` | 读取保留的采集故障代次和经授权的目标证据。[源审阅](capture-recovery.md) · [事务证据](failure-review.md) |
| `/api/tasks/{id}/recovery-plans`、`/api/tasks/{id}/gaps`、`/api/tasks/{id}/gap-verifications`、`/api/tasks/{id}/corrections`、`/api/tasks/{id}/validations` | 先检查证据和不可变计划引用，再提交对应操作和代次。[跳过](transaction-skip.md) · [缺口核验](gap-verification.md) · [手工补录](manual-correction.md) · [对账修复](managed-repair.md) |
| `/api/accounts`、`/api/audit` | ADMIN 账号目录与经授权的审计查询。[账号](account-management.md) · [审计](security-audit.md) |

停止示例：

```sh
curl --fail-with-body -X POST "$RIVOMESH_ORIGIN/api/tasks/orders/actions" \
  -H "Authorization: Bearer $RIVOMESH_TOKEN" \
  -H "X-Request-ID: orders-stop-001" -H 'Content-Type: application/json' \
  -d '{"action":"STOP","expectedRevision":"replace-with-current-revision"}'
```

从 `GET /api/tasks/orders` 获取版本。使用同一个请求 ID 和相同内容重试未确认的动作，并查询其回执。计划 ID、哈希、报告 ID、命令回执和管道或订阅故障代次都绑定于已审阅的源、目标及配置。[恢复手册](reconciliation-workflow.md)包含请求示例和完成判据。

400 表示输入无效，401 表示身份缺失或无效，403 表示权限或浏览器请求校验失败，404 表示任务不存在或不可访问。409 时按返回的冲突码重新核对版本或故障代次。503 表示共享权威或审计存储不可用。重试同一操作和相同请求内容时使用原请求 ID。[全局控制](global-control.md)

## 升级和切换

更换 JDK、驱动或部署配置前，记录包哈希、实测档案、活动配置版本和最近确认的断点。恢复或变更拓扑前停止相关操作，检查目标回执、日志保留和每处持久化存储。参见[首版基线](first-release.md)、[部署指南](console-and-deployment.md)和[切换手册](reconciliation-workflow.md)。新增或修改字段后，同步更新中英文档和对应配置或 API 样例。
