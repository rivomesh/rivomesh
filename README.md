# Rivomesh

独立 Java 8 / Maven 数据同步服务。支持自动建表、可恢复的分块全量、GTID 增量，以及多节点主备接管。
与 `canal` 位于同级目录；不依赖 Canal Server，通过 `mysql-binlog-connector-java` 直接订阅复制协议。

## 需求演进与文档入口

项目目录已更名为 `rivomesh`，Java 包及 Maven groupId 为 `io.github.rivomesh`，可执行产物为 `rivomesh-app`。
CLI 和 Web 的 v3 配置支持多 database 显式映射、同源多目标；Web 同时保留**多份独立 v2 任务配置**，每份为一个 MySQL database → 一个 Gauss / PostgreSQL 目标。
已提供中英文任务向导、YAML 导入、检查、独立 Worker、共享暂停/恢复、持久启停意图，以及 Linux/Docker/Helm/原生 Kubernetes 部署文件。
平台需求已确定为多份独立任务配置、多源任务隔离、多库显式映射和同源多目标，未来源扩展 PostgreSQL/Oracle。
目标范围包含 MySQL/PostgreSQL、国产关系库、ClickHouse/Doris/Hive，以及 Kafka/Pulsar/RabbitMQ/RocketMQ/ActiveMQ/Redis Streams/NATS JetStream。
无主键、JSON、生成列、可选日志存储、既有目标、Web 工作台、Linux/Docker/Helm/原生 Kubernetes 清单均纳入方案。
v3 已接通多库/多目标、无主键多重集、JSON、受支持生成列及独立日志，提供多目标向导和独立恢复。Kafka、RabbitMQ、NATS JetStream、Redis Streams、Pulsar、RocketMQ 已实现确认后至少一次投递及重启恢复；Artemis Core 已通过独立传输和原生回执验收，七种消息目标的定点联合恢复已通过；ActiveMQ Classic 已通过独立原生传输、消费、重启和旧备份保护验收，MySQL＋PostgreSQL 联合链路的全量/增量、Worker 强杀、Broker 故障、SQL 错误重试及队列清空隔离也已通过；ClickHouse 首批版本物化已通过真实全量/增量和恢复链路；Doris 原生 MoW/Stream Load 已通过独立旧备份保护及 MySQL＋PostgreSQL 联合全量/增量、故障隔离和恢复验收；OceanBase MySQL 兼容业务租户实验适配已接通配置/界面，原生事务、跨库回滚、隔离及全量/部分增量已实测，完整重启恢复验收尚未通过；ADMIN 配置编辑、凭据轮换和版本恢复已通过真实数据库续传验收；其他厂商适配器、外部身份/外部不可变审计/修复和迁移切换仍在开发；已有 VIEWER/OPERATOR/ADMIN 角色、精确任务授权及全局审计，由 API 与界面共同限制。各目标使用独立投递线程，其一致性按实际协议区分。

Hive 实验适配已接通配置、界面和隔离 JDBC，首批独立物化、重启/旧备份保护及真实 MySQL＋PostgreSQL 联合全量/增量、目标停机、Worker 强杀、SQL 修复和视图丢失隔离均已通过；集群、安全及性能矩阵仍待验收。首个 Hive 配置要求服务端及所有读取会话关闭查询结果缓存，避免实测出现的旧缓存计数，详见 [Hive 手册](docs/zh-CN/hive-v3.md)。

| 文档 | 中文 | English |
|---|---|---|
| SDD需求基线、未完成事项、任务与验收 | [规格与差距](docs/sdd/001-control-recovery-compatibility/spec.md) | [SDD requirements](docs/sdd/001-control-recovery-compatibility/spec.en.md) |
| 已确认需求、实现边界、待确认事项 | [需求与决策](docs/zh-CN/requirements-and-decisions.md) | [Requirements and decisions](docs/en/requirements-and-decisions.md) |
| 完整范围、源/目标 SPI、MQ、UI、性能及部署方案 | [完整平台方案](docs/zh-CN/platform-design.md) | [Platform design](docs/en/platform-design.md) |
| 已实现的 Web、多任务、API 与四种部署方式 | [控制台与部署](docs/zh-CN/console-and-deployment.md) | [Console and deployment](docs/en/console-and-deployment.md) |
| 本批完成项、实际边界与下一批开发 | [实施状态](docs/zh-CN/implementation-status.md) | [Implementation status](docs/en/implementation-status.md) |
| 共享节点、账号、跨节点会话与边界 | [共享访问](docs/zh-CN/shared-access.md) | [Shared access](docs/en/shared-access.md) |
| 全局任务、持久命令与远程查询（接入中） | [全局控制进展与FAQ](docs/zh-CN/global-control.md) | [Global control progress and FAQ](docs/en/global-control.md) |
| V3 多库多目标、可选日志、生成列、恢复与 FAQ | [V3 运行手册](docs/zh-CN/pipeline-v3.md) | [V3 pipelines](docs/en/pipeline-v3.md) |
| Kafka / RabbitMQ / NATS JetStream / Redis Streams / Pulsar / RocketMQ / Artemis、消费协议、重放及 FAQ | [消息目标](docs/zh-CN/messaging-v3.md) | [Messaging targets](docs/en/messaging-v3.md) |
| ActiveMQ Classic 选择器、回执、恢复和 FAQ | [Classic](docs/zh-CN/classic-v3.md) | [Classic](docs/en/classic-v3.md) |
| OceanBase MySQL 租户、事务、索引和 FAQ | [OceanBase](docs/zh-CN/oceanbase-v3.md) | [OceanBase](docs/en/oceanbase-v3.md) |
| 达梦原生模式、类型转换与恢复 FAQ | [DM](docs/zh-CN/dm-v3.md) | [DM](docs/en/dm-v3.md) |
| 金仓 PG 模式、事务/索引、类型与 FAQ | [Kingbase](docs/zh-CN/kingbase-v3.md) | [Kingbase](docs/en/kingbase-v3.md) |
| Hive ACID 历史、视图、索引省略与实验边界 | [Hive](docs/zh-CN/hive-v3.md) | [Hive](docs/en/hive-v3.md) |
| Doris 类型、MoW 视图、索引、恢复和 FAQ | [Doris](docs/zh-CN/doris-v3.md) | [Doris](docs/en/doris-v3.md) |
| ClickHouse 类型、版本视图、恢复和 FAQ | [ClickHouse](docs/zh-CN/clickhouse-v3.md) | [ClickHouse](docs/en/clickhouse-v3.md) |
| 本地账号界面、令牌轮换、管理员保护与离线恢复 | [账号管理](docs/zh-CN/account-management.md) | [Accounts](docs/en/account-management.md) |
| 账号按任务授权、权限边界及撤销 | [按任务授权](docs/zh-CN/task-authorization.md) | [Task grants](docs/en/task-authorization.md) |
| 管理 API/异步操作审计、保留、查询和导出 | [安全审计](docs/zh-CN/security-audit.md) | [Security audit](docs/en/security-audit.md) |
| ADMIN 配置编辑、凭据轮换、版本恢复与 FAQ | [配置编辑](docs/zh-CN/configuration-editor.md) | [Configuration revisions](docs/en/configuration-editor.md) |
| 硬件条件、全量/增量实测工具及性能边界 | [性能测量](docs/zh-CN/performance.md) | [Performance](docs/en/performance.md) |
| GBase 8c 实验适配 | [GBase 8c](docs/zh-CN/gbase8c-v3.md) | [GBase 8c](docs/en/gbase8c-v3.md) |
| 校验与修复工作流（开发中） | [屏障、逐行比对与 CLI](docs/zh-CN/reconciliation-workflow.md) | [Barrier, row comparison and CLI](docs/en/reconciliation-workflow.md) |
| 管理界面校验作业（首版） | [操作、取消、重试与权限](docs/zh-CN/managed-validation.md) | [Managed validation](docs/en/managed-validation.md) |
| 数据修复（CLI 首版） | [共同校验点修复](docs/zh-CN/common-cut-repair.md) | [COMMON_CUT repair](docs/en/common-cut-repair.md) |
| 管理界面修复（首版） | [计划审阅、执行与取消](docs/zh-CN/managed-repair.md) | [Managed repair](docs/en/managed-repair.md) |
| Docker 开发预览 | [开发预览](docs/zh-CN/development-preview.md) | [Development preview](docs/en/development-preview.md) |
| UNIQUE/GSI 策略、恢复、业务接管及 FAQ | [Gauss 索引策略](docs/zh-CN/gaussdb-index-policy.md) | [Gauss index policy](docs/en/gaussdb-index-policy.md) |
| 完整事务跳过、计划恢复与缺口（开发中） | [事务跳过](docs/zh-CN/transaction-skip.md) | [Transaction skipping](docs/en/transaction-skip.md) |
| 更名后的构建、配置和任务恢复 | [更名迁移](docs/zh-CN/rename-migration.md) | [Rename migration](docs/en/rename-migration.md) |
| 当前恢复机制与验证 | [恢复与一致性](docs/recovery.md)、[验证记录](docs/validation.md) | [Recovery](docs/en/recovery.md), [Validation](docs/en/validation.md) |

部署入口位于 deploy/；tests/ 下的 Compose 仅用于数据库故障测试。完整平台、真实 K8s 集群和各厂商生产环境的验收仍需继续。

## Web 控制台

```bash
mvn -s build/settings.xml verify
export RIVOMESH_TOKEN="$(openssl rand -hex 32)"
java -Xmx512m -jar sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar serve config/platform.yml
```

打开 http://127.0.0.1:8080，使用令牌登录。通过向导创建/导入任务，检查后启动；也可将独立任务文件放到 config/tasks，重启加载。
默认最多同时运行 2 个独立 Worker，其余排队。平台重启保留启停意图；共享 PAUSED 不会被启动或重启绕过。
支持兼容单令牌与多用户 VIEWER/OPERATOR/ADMIN 配置；目录、API、角色范围、暂停和恢复限制见[操作文档](docs/zh-CN/console-and-deployment.md)。

## 支持的部署方式

| 方式 | 配置 | 行为 |
|---|---|---|
| 单实例 MySQL | `source.host/port`，`topology: AUTO` | 从持久化 GTID 断点重连 |
| 普通 GTID 主从 | `source.nodes`，`topology: ASYNC` 或 `AUTO` | 外部系统提升主库后自动发现、校验和续传 |
| InnoDB Cluster / Group Replication 单主 | 成员地址列表，`GROUP_REPLICATION` 或 `AUTO` | 仅连接 ONLINE、PRIMARY、可写的单主成员，选主后重新定位 |
| 多个同步服务节点 | 相同 jobId、逻辑源配置及目标，独立 nodeId | 一个节点执行，其他节点等待；故障后自动接管 |

同步服务不会执行数据库主库提升、关闭只读模式、修复 MySQL 复制、跳过 SQL 或修改源 GTID。
多主写入不支持。发现多个可写主库时停止数据处理，等待数据库高可用系统完成隔离。
普通主从的高可用系统必须先隔离旧主再提升新主；网络分区时，可连接的候选列表本身不能证明另一侧旧主已经停止写入。

## 构建和运行

```bash
mvn -s build/settings.xml verify
cp config/example.yml config/local.yml
export MYSQL_PASSWORD='替换为源密码'
export GAUSSDB_PASSWORD='替换为目标密码'

java -jar sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar check config/local.yml
java -jar sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar run config/local.yml
java -jar sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar status config/local.yml
# 仅在修复 PAUSED 原因后执行；会重试原失败事务，不跳过任何数据：
java -jar sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar resume config/local.yml
```

`check` 为连接、GTID 和表结构检查，不启动同步。建表权限和复制连接权限在 `run` 时验证。
`build/settings.xml` 使用公开 Maven 仓库；GaussDB 驱动从华为仓库下载后独立重定位。
密码及 nodeId 支持 `${ENV_NAME}`；其他字段直接填写配置值。进程、源会话及目标会话统一使用 UTC。

## MySQL 前提

已验证 MySQL 5.7.44 基础同步及 8.0 / 8.4；5.7 使用 `log_slave_updates`，适用的 8.x 使用 `log_replica_updates`。V3 完整链路及各拓扑的验证范围见验证记录。所有候选节点需要：

```ini
[mysqld]
server_id=1                         # 每个 MySQL 节点不同
log_bin=mysql-bin
gtid_mode=ON
enforce_gtid_consistency=ON
log_replica_updates=ON
binlog_format=ROW
binlog_row_image=FULL
binlog_row_value_options=
binlog_transaction_compression=OFF
```

候选节点必须属于同一完整复制拓扑，包含业务库的完整数据和 GTID 历史；不要使用过滤了业务数据的副本。
源用户需要业务库 SELECT、REPLICATION SLAVE、REPLICATION CLIENT，以及读取
`performance_schema.replication_group_members` 的权限。所有候选节点使用相同源账号。
客户端 `source.serverId` 必须区别于数据库节点及其他复制客户端；同一同步任务的热备进程由租约保证只有一个采集连接。

普通主从提升流程由现有数据库高可用系统负责。候选主库必须可写、已停止上游复制且无遗留 IO/SQL 错误。
服务会检查已采集 GTID、全量读取依赖的 GTID、新主库 executed/purged 集合，拒绝从落后的或缺失所需日志的节点继续。
数据库高可用策略需要保证必要的事务已复制到新主库；异步复制已经丢失、且没有其他副本或日志可恢复的事务，
同步服务无法凭空重建。不要为通过校验而手工伪造 GTID 或跳过失败事务。

GTID 使用非 tagged 形式；8.4 的可选 tagged GTID 暂不支持，会明确停止。MySQL Router/VIP 必须指向可写入口，
JDBC 和复制连接会校验同一物理后端；优先配置实际成员地址以便明确识别多主和角色。

## 全量、增量与恢复

任务阶段：`SNAPSHOT → CATCHUP → STREAMING`。运行状态另行记录：

| run_state | 含义及恢复方式 |
|---|---|
| READY / RUNNING | 等待接管 / 正常工作 |
| RETRYING | 连接中断、死锁、资源暂时不足等；退避重试 |
| WAITING_SOURCE | 尚无主库、存在多个写主、候选落后或源复制异常；等待源端修复后自动继续 |
| PAUSED | 确定性 SQL 错误、DDL、字段不兼容或所需 GTID 日志丢失；修复后执行 resume |

1. 记录起始 GTID，发现全部业务表及主键扫描上界。
2. 创建目标缺失表；检查已有空表的字段、长度、可空性及主键。
3. 按主键/联合主键分块读源库；分块数据、扫描游标、读取依赖的 GTID 在同一目标事务中提交。
4. 同时采集 binlog，在源事务提交边界将完整事件和采集 GTID 一起存入 GaussDB 队列。
5. 全量结束，等待采集经过结束 GTID，按源事务顺序回放，进入持续增量。

每个源事务在目标一个事务内应用，包括跨表变更。应用结果、应用序号和队列删除原子提交。
发生 SQL 错误时整个目标事务回滚，队列头保留；后续事务不会绕过失败事务。
采集 GTID 与已应用进度不同：SQL 应用失败时，已安全进入队列的后续采集进度可能已前进，业务应用进度不会越过错误。

全量阶段采用在线扫描配合 FULL 行镜像回放，不能把未追平的目标表当成一致性快照使用。
`STREAMING` 表示全量追平边界已越过，不代表此后任何时刻都没有复制延迟。
首次目标业务表必须为空；程序不自动清空已有数据。恢复已有任务继续使用原 jobId、clusterId 及目标配置。
源物理 UUID 可变化；逻辑 clusterId、database、读取选项及目标必须保持一致。候选地址可增减，不绑定恢复文件位点。

管理 schema 与业务 schema 必须不同：

| 管理表 | 内容 |
|---|---|
| sync_job_v2 | 阶段、运行状态、源地址、GTID、owner、租约、最近错误 |
| sync_table_v2 | 表结构、扫描上界、最后主键、已复制行数 |
| sync_transaction_v2 | 按源事务排序的待应用事件、GTID、来源 UUID 和诊断文件位点 |
| sync_error_v2 | 错误时间、节点、阶段、SQLState、数据库错误码、表、事务序号及 GTID |

全量及应用积压会增加目标队列大小，需要预留磁盘。成功应用的队列记录会同事务删除；错误历史不会自动删除。
旧版 `sync_job` 文件位点状态不会自动转换或覆盖；若同名旧任务存在，会拒绝启动。
升级前必须停止所有旧版进程，禁止 v1/v2 混跑同一目标。旧任务需制定迁移方案，或使用新的 jobId 与空目标 schema 重新全量，不要删除旧状态后覆盖非空目标表。

## 集群部署

2～3 个服务节点共用同一个 GaussDB 可写入口、业务 schema 和管理 schema，每个节点使用独立 nodeId。
节点可以配置不同的 MySQL 复制客户端 serverId；其余逻辑任务配置保持一致。
默认租约 30 秒，独立线程每 5 秒续期，长时间源查询不会因为主线程阻塞而停止心跳。
节点失败、暂停或长时间网络隔离后，其他节点获得租约，从共享游标和 GTID 继续。
每次工作会话使用新的随机 owner；所有数据和进度写事务均先校验 owner，旧进程恢复后不能继续写入。

这属于主备高可用；同一个任务不会分配给多个节点并行扫描或并行回放。
GaussDB 集群需要提供一致的可写入口，并保证业务表、队列、状态表一起持久化和恢复。

## SQL 错误处理

- 死锁、序列化冲突、连接中断、锁超时：重新加载共享进度、指数退避并加抖动重试。
- 数据溢出、约束冲突、语法/权限错误：PAUSED，保存失败上下文，不清空队列、不跳过事务。
- 源 MySQL 从库的 IO/SQL 错误：不自动修复数据库；有健康主库时继续使用主库，异常候选不会被选中。
- 缺少已读/已采集 GTID：等待追平；所需日志已经清理：暂停，恢复日志或选择正确主库后 resume。
- 不支持的 DDL、anonymous/tagged GTID、行镜像不完整或反序列化错误：停止推进失败事务的采集 GTID。

不支持的源 DDL 需要补充对应处理能力或重新制定迁移任务；单独执行 resume 不会跳过该 DDL。

`status` 返回 owner、源节点、两个 GTID 集合及 `error_*` 字段。修复后执行 resume，所有服务节点都能观察到恢复信号。
不要直接修改队列头、applied_sequence 或 GTID 字段来跳过错误。

## 类型及 GaussDB 兼容性

支持 InnoDB、有主键的普通表（含联合主键）；整数及 unsigned、DECIMAL、FLOAT/DOUBLE、YEAR、DATE、DATETIME/TIMESTAMP、
CHAR/VARCHAR/TEXT、BINARY/VARBINARY/BLOB、JSON。支持 utf8mb4、utf8/utf8mb3、ascii、latin1。

GaussDB 的 PostgreSQL 协议驱动已重定位，产物无需未重定位 gsJDBC JAR。
默认 A 模式的空字符串/NULL 差异通过连接级 `accept_empty_str` 处理，并保留原有兼容选项。
DATE 可能内部映射成 timestamp，按日期读写。VARCHAR 按最多每字符 4 个 UTF-8 字节扩容，避免中文或 emoji 超长。

本节描述 v2：不支持无主键、非 InnoDB、生成/不可见列、TIME、BIT、ENUM/SET、空间类型和零日期。v3 的无主键、生成列及索引支持见 [V3 手册](docs/zh-CN/pipeline-v3.md)。
不复制次级索引、外键、触发器、视图、默认值和自增序列；目标表由同步服务独占写入。
后续索引转换规则及其与当前实现的区别见 [Gauss 索引策略](docs/zh-CN/gaussdb-index-policy.md)。
运行中 DDL 暂停任务，包括其他 database 的 SQL 事件，以防跨库 SQL 引用漏判。`maxTransactionRows` 限制单事务缓冲操作数，UPDATE 算两次。

## 测试

```bash
mvn -s build/settings.xml verify  # 单元测试和打包，真实数据库测试默认禁用
tests/run.sh                    # 全量、重启、事务回滚、平台多 Worker、共享控制及 DDL 暂停
tests/ha-run.sh                 # 多节点、复制连接中断、SQL 修复、从库 SQL 错误、主库关闭后提升续传
tests/group-run.sh              # 全量期间关闭主库，三节点 Group Replication 选举后继续扫描及追平
tests/matrix.sh                 # MySQL 8.0 / 8.4 的基础、主从 HA、Group Replication 完整矩阵
npm ci --prefix tests/ui --ignore-scripts
npm test --prefix tests/ui       # 可选开发验证；Node 22.22.2+ 对应支持版本，不是服务运行依赖
```

所有测试仅创建独立容器，不占用宿主机数据库端口，结束清理容器和数据卷。
可设置 `SYNC_MAVEN_CACHE="$HOME/.m2/repository"` 复用缓存，`SYNC_MYSQL_IMAGE` 选择单个版本，
`SYNC_MYSQL_REGISTRY=public.ecr.aws/docker/library/mysql` 为矩阵使用官方 ECR 镜像源。
目标测试使用 openGauss；实际商业版 GaussDB 及特定分布式版本仍需实机联调。
2026-11-14 的 MySQL 8.0.46 / 8.4.13 完整矩阵共 44 次 Java 测试执行通过；另通过 3 项 DOM 测试及 Docker/Helm 检查，详见 [验证记录](docs/validation.md)。

详细机制见 [恢复与一致性](docs/recovery.md)。GTID 机制参考
[MySQL GTID auto-positioning](https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-auto-positioning.html)、
[Group Replication 成员状态](https://dev.mysql.com/doc/refman/8.4/en/group-replication-server-states.html)。

本地账号管理已提供中英文界面/API、显式托管启用、角色/任务范围、停用/删除、令牌轮换和离线恢复；原生升级/强杀/旧状态拒绝及真实 SQL 数据流连续性已通过，详见[账号手册](docs/zh-CN/account-management.md)。OIDC 和控制平台多写高可用仍待完成。

浏览器登录保持 / Browser sessions: [中文](docs/zh-CN/browser-sessions.md) · [English](docs/en/browser-sessions.md).

共享节点目录 / Shared node registry: [中文](docs/zh-CN/node-registry.md) · [English](docs/en/node-registry.md).
