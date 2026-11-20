# 连接器、Java 与 API 兼容档案

[English](../en/compatibility.md)

先选源版本和拓扑，再选目标产品与模式、Java 运行时。每行记录已实测范围或待验收事项。上线验收前，到[验证基线](../verification/README.md)和[机器可读记录](../verification/baseline.json)核对对应包的 SHA-256。

## 源版本和坐标

| 配置 | 实库范围 | 配置与恢复 |
|---|---|---|
| `MYSQL` 5.7.44 | Temurin 21 单机及三节点单主组复制到 MySQL 与 PostgreSQL。异步 GTID 升主及缺失日志拒绝通过 PostgreSQL 目标验收。 | [同步配置](pipeline-v3.md)、[日志缺失恢复](capture-recovery.md) |
| `MYSQL` 8.0.46 | Temurin 21 单机及三节点单主组复制到两个 SQL 目标。异步 GTID 升主通过 PostgreSQL 目标验收。 | [同步配置](pipeline-v3.md)、[恢复手册](reconciliation-workflow.md) |
| `MYSQL` 8.2.0 | Temurin 21 单机及三节点单主组复制到两个 SQL 目标。异步 GTID 升主通过 PostgreSQL 目标验收。 | [版本专项命令](pipeline-v3.md) |
| `MYSQL` 8.4.13 | Temurin 21 单机、异步 GTID 升主及三节点单主组复制到两个 SQL 目标。binlog 准入及组复制旧主只读返回另有实库验证。 | [8.4 binlog 档案](pipeline-v3.md) |
| `POSTGRESQL` 17.12 | Temurin 21 实验档案。同步槽切主后自动续传，误指回旧时间线时拒绝。PostgreSQL 和 MySQL 两个目标通过同一 WAL 切点对账，并从原逻辑槽续传。 | [源指南](postgresql-source.md)、[任务样例](../../config/postgresql-source-v3.yml) |
| `POSTGRESQL` 18.7 | Temurin 21 实验档案。发布存储型生成列，PostgreSQL 17.12 目标物化为普通列。 | [源指南与发布](postgresql-source.md) |
| `ORACLE` 26ai Free 23.26.4.0.0 | Temurin 21，需单独提供 Oracle JDBC 驱动。单 PDB Flashback 和归档增量已实测 PostgreSQL、MySQL、金仓、达梦、ClickHouse、Doris、Hive 和 Redis Streams。RAC 和源切主待验收。 | [预检权限与恢复](oracle-source.md)、[V3 样例](../../config/oracle-source-v3.yml) |

MySQL 坐标使用逻辑源集群 ID 与无标签 GTID 集合。PostgreSQL 坐标使用逻辑源 ID、system ID、timeline、slot 和 LSN。Oracle 预检使用 DBID、resetlogs SCN、PDB GUID 和 SCN。`SourcePosition.formatVersion` 当前为 1，由 `sourceType` 对应的适配器解释。在同一来源身份和协议内比较坐标。[多库样例](../../config/pipeline-v3.yml)使用 `formatVersion: 3`、`source.clusterId`、`databases` 以及每个目标的显式 `mappings`。独立 MySQL 集群分别使用独立配置。

## 目标产品和一致性

| 目标标识 | 范围与投递语义 | 手册 |
|---|---|---|
| `MYSQL`、`POSTGRESQL` | MySQL 8.4 和 PostgreSQL 17.12 事务档案已实测。业务写入和目标断点在同一事务提交。 | [关系型目标](pipeline-v3.md) |
| `GAUSS` | openGauss 7.0.0-RC3 事务档案已实测。商用分布式 GaussDB 的约束、GSI 和部署验收待完成。 | [高斯索引策略与接管](gaussdb-index-policy.md) |
| `DM`、`KINGBASE`、`GBASE_8C` | 首批历史事务档案使用 Java 8。确切版本、模式、驱动、类型转换和恢复范围见各产品手册。 | [达梦](dm-v3.md)、[金仓](kingbase-v3.md)、[GBase 8c](gbase8c-v3.md) |
| `GBASE_8A`、`GBASE_8S` | 独立产品，规划中。 | [实施状态](implementation-status.md) |
| `OCEANBASE` | CE 4.3.5.6 MySQL 模式通过 Temurin 21 上的 MySQL、PostgreSQL 源全量、增量和重启。MySQL 源档案还覆盖独立 SQL 修复。Oracle 模式和集群另行验收。 | [OceanBase](oceanbase-v3.md) |
| `CLICKHOUSE`、`DORIS`、`HIVE` | 首批实库分析档案，采用版本化物化与回执。数据可见性和重放规则按引擎区分。 | [ClickHouse](clickhouse-v3.md)、[Doris](doris-v3.md)、[Hive](hive-v3.md) |
| `KAFKA`、`PULSAR`、`RABBITMQ`、`ROCKETMQ`、`ACTIVEMQ_CLASSIC`、`ACTIVEMQ_ARTEMIS`、`REDIS_STREAMS`、`NATS_JETSTREAM` | 限定 broker 档案，确认式至少一次投递。消费端按稳定事件 ID 去重，独立记录业务消费进度。 | [消息目标](messaging-v3.md)、[ActiveMQ Classic](classic-v3.md) |

V3 向导读取 `GET /api/capabilities` 判断源类型可用性，并按源引擎限制目标产品。PostgreSQL 和 Oracle 使用各自的版本、源身份、命名空间和凭据表单。部署前按目标手册核对拓扑、权限、保留与备份边界。

### 适配器验收与实库组合

MySQL、PostgreSQL 和 Oracle 将同一套带类型的表结构、快照和已提交事务写入日志库。日志库保留原生位点，并在分发前为无主键重复行分配稳定身份。关系库、分析库和消息目标读取共享模型，各自维护提交或投递回执。

扩展适配器时，通用行为复用事件和类型映射测试。新源用实库验收快照、日志解码、恢复和切主，再选代表性目标做端到端验证。新目标用统一事件和代表性源，实测存储、类型、行身份、提交回执和重试。转换规则、物理约束或恢复规则同时依赖源和目标时，再补对应组合，例如源小数精度超过目标上限，或目标排序规则可能合并原本不同的主键。实库清单记录已运行的具体版本和组合。 [实库验收记录](../verification/README.md)

## 首版 Java 运行时

**构建、运行和验收仅支持 Eclipse Temurin 21 LTS。** 发布包使用 Java 21 字节码。Linux、Docker 和 Kubernetes 的 Controller、Agent 和 Worker 都使用 Temurin 21。实库档案记录实际 JVM 版本、架构和包 SHA。新增源、目标、日志和恢复组合按对应的 Temurin 21 用例验收。[复现命令](performance.md)

上述源和目标档案仍对应各自的候选包与实测环境。早期运行时实验和 `fiveJdkProcesses`、`postgresqlSourceRuntimeMatrix`、`postgresqlStoredGenerated` 等档案保留原制品摘要和复现命令。[验收清单](../verification/baseline.json) · [固定历史快照](https://github.com/rivomesh/rivomesh/blob/419945c98f197bd6cc1c6cc3e75e9c6a4bb3188c/docs/verification/baseline.json)


### Java 21 外置 JDBC 驱动

五个已核验驱动包的入口类及基础字节码最高主版本为 52，对应 Java 8 的 class 文件格式。Rivomesh 固定使用 Temurin 21，服务加载和 URL 匹配均在该运行时验证。请在实际使用驱动的 Controller、Agent 或 Worker 上，以同一 classpath 执行连接检查。 [JVM class 文件版本](https://docs.oracle.com/javase/specs/jvms/se21/html/jvms-4.html#jvms-4.1) · [外置驱动打包](release-inventory.md)

| 诊断码 | 排查项 |
|---|---|
| `JDBC_DRIVER_UNAVAILABLE` | 检查驱动 JAR、读取权限、驱动类及构造器访问权限。 |
| `JDBC_DRIVER_JAVA_VERSION_UNSUPPORTED` | 更换为能够在 Temurin 21 加载的驱动或依赖构建。 |
| `JDBC_DRIVER_LINKAGE_INVALID` | 核对驱动依赖是否齐全，并检查 classpath 上的重复或冲突版本。 |

已验收外置 Connector/J 8.4.0 的 MySQL 8.4 TLS 1.3，以及 pgJDBC 42.7.14 的 PostgreSQL 17.12 域名与 CA 验证。两种档案在重启后均沿用原 CDC 断点。金仓 9.0.1 另有原生认证、精度和回执续传证据。各产品手册列出实际验证的服务端和客户端安全配置。 [MySQL TLS](pipeline-v3.md) · [PostgreSQL TLS](postgresql-source.md) · [金仓 JDBC](kingbase-v3.md)

### Temurin 21 目标产品实测档案

下表登记数据库或 Broker 版本、内置客户端或外置驱动及已验收的恢复操作。各行的制品和验收范围以产品手册和证据清单为准。

| 目标 | 服务端与客户端 | 原生验收 |
|---|---|---|
| `MYSQL` | 8.4.13 · Connector/J 8.4.0 | 原生事务、全量与增量、JSON、无主键、GTID 续传及 SQL 修复 |
| `POSTGRESQL` | 17.12 · pgJDBC 42.7.14 | 原生事务、JSONB、无主键、逻辑槽切换、证书校验和 SQL 修复 |
| `GAUSS` | openGauss 7.0.0-RC3 A · gsjdbc4 1.1 | 目标重启、表和索引一致性、唯一索引降级及原快照续传 |
| `DM` | 8.1.4.80 UTF-8 · 8.1.3.140 | 外置驱动、JSON 文本、宽数值、无主键、目标重启、Worker 重启与隔离修复 |
| `KINGBASE` | V009R001C001B0025 PG/UTF8 · 9.0.1 | 外置驱动鉴权、原生 JSON、精确数值、原回执和目标重启 |
| `GBASE_8C` | V6_S1.3.0B19 PG/UTF8 · 3.0.0B02 | 真实 MySQL 全量与增量、JSON、无主键、精确数值、重启续传和独立 SQL 修复 |
| `OCEANBASE` | CE 4.3.5.6 MySQL · Connector/J 8.4.0 | 真实 MySQL 全量与增量、JSON、无主键、精确数值、重启续传和独立 SQL 修复 |
| `CLICKHOUSE` | 25.8.33.6 · HTTP / RowBinary | 原生物化回执、重启、旧非空备份和原生前序回执核对 |
| `DORIS` | 4.1.3 · Connector/J 8.4.0 / Stream Load | 原生写入与回执、断点恢复，故障前确切批次核验 |
| `HIVE` | 4.0.1 · Hive JDBC 4.0.1 | 外置 PostgreSQL Metastore、ACID ORC、快照和增量、Worker 原断点重启与原生回执 |
| `KAFKA` | 3.9.2 / 4.1.2 · Kafka clients 3.9.2 / 4.1.2 | TLS/SCRAM、KRaft、原生 ACK、固定 UUID 事务缺口通知和断点恢复 |
| `PULSAR` | 4.0.14 · Pulsar SDK 4.0.14 | 私有 CA/JWT、多 Broker/BookKeeper、保留原 Ledger 和 Entry |
| `RABBITMQ` | 4.1.4 · AMQP client 5.36.0 | quorum 主节点切换与消费 ACK，独立回执流支持在消费后核验 |
| `ROCKETMQ` | 5.5.0 · RocketMQ client 5.5.1 | 本地同步刷盘、原生提交日志回执、独立 Controller 副本升主档案 |
| `ACTIVEMQ_CLASSIC` | 5.19.12 · OpenWire client 5.16.8 | KahaDB 共享存储主备及原生前序回执 |
| `ACTIVEMQ_ARTEMIS` | 2.57.0 · Core client 2.19.1 | 主备共享存储、非破坏队列原生前序回执 |
| `REDIS_STREAMS` | 7.4.2 / 8.0.2 · Jedis 8.0.2 | AOF、WAITAOF、外部升主、TLS、原生条目回执 |
| `NATS_JETSTREAM` | 2.11.8 · jnats 2.26.4 | TLS、三副本多数派与原生序列回执 |

商业分布式 GaussDB、OceanBase Oracle 租户以及 GBase 8a/8s 需要单独的原生验收。更多存储、安全和生产集群组合由各产品的 SDD 事项跟踪。 [证据清单](../verification/baseline.json) · [验收档案](../sdd/001-control-recovery-compatibility/acceptance.md)

## 管理端请求

CLI 通过 Bearer 凭据访问，浏览器使用 HttpOnly 会话。浏览器写操作提供匹配的 Origin 与 `X-CSRF-Token`。VIEWER 按授权读取状态，OPERATOR 在授权范围内发出常规命令，ADMIN 配置任务、审阅敏感证据和批准恢复。[浏览器会话](browser-sessions.md) · [任务授权](task-authorization.md)

| 接口 | 请求与结果 |
|---|---|
| `/api/session`、`/api/session/logout`、`/api/nodes`、`/api/tasks` | 查询身份权限、节点和归属、分页任务。全局模式创建任务包含 `ownerNodeId`。[控制台接口](console-and-deployment.md) |
| `/api/tasks/{id}/configuration`、`/api/tasks/{id}/actions` | 变更绑定当前 `expectedRevision` 和原始 `X-Request-ID`。本地创建返回 201，本地更新配置返回 200，全局变更与操作受理返回 202。随后读取持久化回执。[配置编辑](configuration-editor.md) · [操作生命周期](execution-lifecycle.md) |
| `/api/tasks/{id}/capture-recovery`、`/api/tasks/{id}/capture-exclusions`、`/api/tasks/{id}/failure-evidence` | 读取保留的采集故障代次和经授权的目标证据。[源审阅](capture-recovery.md) · [事务证据](failure-review.md) |
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
