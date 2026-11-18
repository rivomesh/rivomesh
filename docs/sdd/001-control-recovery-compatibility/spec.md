# Rivomesh SDD需求基线与未完成事项

RM-SDD-001 · v0.29 · 2026-11-18 · IMPLEMENTING

实施中：48项持续追踪，24项DONE、23项PARTIAL、1项NEW，余24项未闭环。RC-01、RC-02、RC-05、RC-03、RC-06、RC-07、RC-08、RC-09、SO-03、SO-04、SO-05 和 JV-01 限定档案已验收。自动接管、源范围恢复和完整兼容矩阵继续追踪。

[基线和决策](plan.md) · [任务](tasks.md) · [验收矩阵](acceptance.md) · [English](spec.en.md)

## 多节点统一控制

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| CP-01 | P0 / DONE | 统一节点注册与状态 | 共享节点目录已通过PostgreSQL/MySQL双节点、kill/重开及控制库中断验收；任务权威/远端命令仍属CP-02/03。 |
| CP-02 | P0 / PARTIAL | 全局任务、配置与操作权威状态 | 可选固定owner全局HTTP/界面、共享配置及历史已接入；旧任务升级导入、完整业务及规模验收仍未完成。 |
| CP-03 | P0 / PARTIAL | 跨节点持久命令路由 | 公开持久命令、离线STOP、真实多目标校验/修复及原UNKNOWN升级恢复已验收；完整取消/故障/迁移与容量矩阵仍未完成。 |
| CP-04 | P0 / PARTIAL | 故障接管、隔离与资源调度 | 已有Worker租约/epoch，缺少统一节点调度、远端生命周期和滚动升级协议。 |
| CP-05 | P0 / PARTIAL | 共享权限、审计和密钥引用 | 共享账号/授权/链式审计及Worker环境过滤已实现；完整远端关联、最小密钥引用和进程/文件隔离仍缺。 |
| CP-06 | P1 / PARTIAL | 全局日志、指标与诊断界面 | 双入口实测独立采集租约和脱敏诊断分页，Agent 重启后保留事件。全局节点及目标过滤、积压和容量验收待完成。 |
| CP-07 | P0 / DONE | 控制面HA及控制/数据面解耦 | 协议4分离档案已通过PG/MySQL控制库、管理全停/命令恢复、控制库中断、原Worker持续及非root验收；默认组合模式仍耦合，自动接管和其他存储/适配器档案另行追踪。 |

## 登录状态保持

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| SE-01 | P0 / DONE | 刷新后恢复登录态 | 已完成单控制器刷新/新页面恢复；跨控制器连续性仍属SE-04。 |
| SE-02 | P0 / DONE | 会话期限、退出和撤销 | 已完成期限、退出、目录修订撤销及可配置记住浏览器；当前内存存储重启失效。 |
| SE-03 | P0 / DONE | 安全会话传输和CSRF防护 | 已完成受保护Cookie、Origin/CSRF及TLS代理实测；生产TLS由代理终止。 |
| SE-04 | P0 / DONE | 跨控制节点的会话连续性 | 已通过PostgreSQL/MySQL共享会话双节点、HTTPS、撤销、kill/重开及存储中断恢复验收；全局任务控制仍属CP-02/03。 |
| SE-05 | P1 / DONE | CLI/API兼容及身份冲突规则 | 已完成Bearer兼容、不回退/身份冲突规则、权限重验、退出竞态及自身轮换。 |

## 人工恢复、跳过与补数据

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| RC-01 | P0 / DONE | 按采集阶段区分故障 | MySQL 源与首批 SQL 目标的阶段诊断已验收。源范围排除由 RC-04 跟踪。 |
| RC-02 | P0 / DONE | 失败事务检查与原始证据留存 | 已验收MySQL8.4、固定原Agent/LOCAL日志、PG/MySQL控制库的ADMIN完整事务审阅及持久证据：位置/历史列/镜像、授权分页、重启及日志GC后可读。自动证据目录管理、其他档案、跨Agent迁移与执行语义另行追踪。 |
| RC-03 | P0 / DONE | 目标应用阶段按完整事务跳过 | 协议7首批限定档案已验收：Java8、MySQL8.4双库源、原Agent/LOCAL日志、PostgreSQL业务目标、PG/MySQL控制库；完整跨库事务跳过、原生提交后源ACK失败、Agent强杀重开与跨入口确认均通过。其他目标/完整矩阵由RC-10/LG-01继续追踪。 |
| RC-04 | P0 / PARTIAL | 采集/解码阶段的受控缺口恢复 | 只读检查已列出源准入及全部潜在受影响订阅。范围证明、隔离和排除待实现。 |
| RC-05 | P0 / DONE | 不确定提交与断点核对 | 协议6原Agent/LOCAL日志、PG/MySQL控制库限定档案已验收：全任务暂停、新证据、原生围栏、管理员跨入口确认、ACK/回执原子提交及幂等重试，保持原暂停。跳过、补偿和其他完整矩阵另行追踪。 |
| RC-06 | P0 / DONE | 失败事务前镜像补偿 | 首批 SQL 档案通过。管理员跨入口审阅表范围及增删数量，按原事务前镜像恢复目标，随后显式恢复并重放。真实 CDC 验证 PostgreSQL 补偿，MySQL 通过原生合同。其他目标由 RC-10 追踪。 |
| RC-07 | P0 / DONE | 界面手工补数据与修正 | 首批 SQL 档案通过。逐行录入和 JSON 导入支持管理员审阅前后值、精确行定位、原子批次和原事务恢复。PostgreSQL 通过真实 CDC，MySQL 通过原生检查。其他目标由 RC-10 跟踪。 |
| RC-08 | P0 / DONE | 一致性缺口台账与真实水位 | 首批SQL档案已验收：持久缺口、审阅计划、跨入口审批/取消、全表重扫、原子凭据及安全覆盖/连续应用水位。核验不改写SKIP、不推进原应用前缀、不自动恢复；其他目标协议由RC-10/LG-03/LG-04继续追踪。 |
| RC-09 | P0 / DONE | 补偿作业的跨节点持久化和恢复 | 首批 SQL 档案通过跨节点计划、审批、取消与原请求恢复。真实提交边界测试以 PostgreSQL 17 为恢复目标。 |
| RC-10 | P1 / PARTIAL | 多目标恢复语义和回收保护 | SQL、分析库、消息队列一致性协议不同，不能复用一个伪事务实现。 |

## 源数据库兼容

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| SO-01 | P0 / PARTIAL | 多源协议与能力配置 | MySQL、PostgreSQL 与 Oracle 使用同一 V3 源适配器入口，保留各自坐标和版本。跨引擎能力探测、完整配置向导待完成。 |
| SO-02 | P1 / DONE | MySQL 5.7 完整支持档案 | 5.7.44 Java 8 V3 单机、GTID 主从、MGR 和未采集 binlog 缺失已实测。 |
| SO-03 | P1 / DONE | MySQL 8.0 当前V3能力补验 | MySQL 8.0.46 的当前 V3 单机、GTID 主从、单主组复制以及跨节点补偿和修正已验收。其他 JDK 由 JV-02 跟踪。 |
| SO-04 | P1 / DONE | MySQL 8.2 新增兼容与验收 | MySQL 8.2.0 的 Java 8 V3 单机、GTID 主从与单主组复制档案已验收。其他 JDK 由 JV-02 跟踪。 |
| SO-05 | P1 / DONE | MySQL 8.4 扩展兼容与回归 | MySQL 8.4.13 的 V3 三种拓扑、GTID/ROW/FULL 及拒绝档案已验收。其他目标组合由对应矩阵跟踪。 |
| SO-06 | P1 / PARTIAL | PostgreSQL 源适配器 | 17.12 单主已实测快照、增量、续传与槽丢失。跨时间线切换及共同点未验收。 |
| SO-07 | P1 / PARTIAL | Oracle 源适配器 | Oracle 26ai 单 PDB、归档 LogMiner 到 PostgreSQL 实测支持长事务、强杀续传、独立 SCN 和 PDB 世代检查。LOB、JSON、生成列、RAC 和切主仍待验收。 |

## Java运行时兼容

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| JV-01 | P0 / DONE | Java 8/11/17/21/25 可运行发布 | 同一候选包的 Controller、Agent 与 Worker 已在五个 JDK 完成真实同步。完整连接器矩阵和混合版本升级由 JV-02 至 JV-04 跟踪。 |
| JV-02 | P1 / PARTIAL | 源/目标/恢复跨JDK矩阵 | 五种 JDK 已验证 MySQL 8.4 基础双目标、PostgreSQL 17.12 七项和 18.7 存储型生成列八项实库档案。其他源、目标、TLS 和恢复组合待验收。 |
| JV-03 | P1 / PARTIAL | 驱动隔离、依赖与安全协议兼容 | 已有厂商SDK隔离和完整包检查，缺全部JDK组合验证。 |
| JV-04 | P1 / NEW | 多JDK部署及混合版本升级 | 现有基准可选JRE，生产发布/集群升级策略未完整验收。 |

## 完整文档

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| DO-01 | P0 / DONE | 双语SDD基线与范围可追踪 | 48 项需求、任务和验收用稳定 ID 对齐。证据档案、双语验收文本与进度计数由门禁核对。 |
| DO-02 | P1 / DONE | 四类部署与多节点运维文档 | 四种单机与集群部署路径、存储、会话、节点维护和备份升级已形成双语操作说明。跨节点卷和自动接管按对应实机需求继续验收。 |
| DO-03 | P1 / DONE | 恢复场景和FAQ完整手册 | 中英文故障手册覆盖切主、日志丢失、SQL/确认错误、节点、存储、会话、人工恢复与交接，按已有产品档案给出操作和边界。 |
| DO-04 | P1 / DONE | 配置/API/兼容性与升级文档 | 配置、API、权限、坐标、一致性、升级和实际跨 JDK 档案已整理为中英文索引。各未验收组合继续由对应兼容事项跟踪。 |
| DO-05 | P1 / PARTIAL | 文档和证据发布门禁 | 已有验收归档/哈希及预览检查，缺覆盖所有需求的统一门禁。 |

## 历史范围合并保留

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| LG-01 | P1 / PARTIAL | 国产事务库兼容矩阵 | OceanBase CE 4.3.5.6 MySQL 租户已通过原生事务和联合重启恢复。分布式 GaussDB、OceanBase Oracle 模式及 GBase 8a/8s 待验收。 |
| LG-02 | P1 / PARTIAL | 非空接管、约束恢复、切换回切 | 当前首用目标要求为空，修复成功也不是业务切换完成。 |
| LG-03 | P1 / PARTIAL | ClickHouse/Doris/Hive生产档案 | 首批物化及恢复档案已验证，含 OceanBase 单节点 MySQL 租户停机追平、两目标独立 SQL 修复和缺表隔离。集群、TLS、容量与完整 JDK 矩阵待验收。 |
| LG-04 | P1 / PARTIAL | 全部消息目标的集群与恢复协议 | Kafka/Pulsar/RabbitMQ/RocketMQ/Classic/Artemis/Redis Streams/NATS JetStream已有首批传输，完整集群/TLS/容量仍缺。 |
| LG-05 | P0 / PARTIAL | 可选存储和跨节点数据可达性 | 已有本地/外部日志，但本地权威文件、报告、会话、控制元数据的迁移/HA边界不统一。 |
| LG-06 | P1 / PARTIAL | 真实K8s/CSI与节点故障验收 | 已通过双节点 Kubernetes 的 Helm/原生 YAML、配置与 Secret 替换和 worker 停机恢复。节点本地 CSI 同卷、新租约及原断点续传已实测。跨节点 CSI 和旧 owner 隔离待验收。 |
| LG-07 | P1 / PARTIAL | 海量对象、大事务和硬件性能标准 | LOCAL 与 EXTERNAL 已各通过 10 万行全量及 12 万条增量内容校验。增量批次按字节限量并修复 BIGINT 主键扫描。多轮硬件矩阵、超大事务和海量表仍待验收。 |
| LG-08 | P1 / PARTIAL | 历史DDL、类型、索引与状态迁移 | 已有白名单对象和UNIQUE/GSI转换；完整动态DDL、复杂类型及备份恢复档案仍缺。首版不支持旧开发候选兼容。 |
| LG-09 | P1 / PARTIAL | 报告目录、保留清理与真实浏览器验收 | 原生 Chrome 已通过登录、刷新续登、诊断键盘与无障碍名称、移动端布局和退出。完整报告目录、scratch 清理和其他工作流仍待完成。 |
| LG-10 | P2 / PARTIAL | 外部身份、不可变审计与Apache准备 | 贡献指南、CycloneDX SBOM 与厂商二进制审查已建立。依赖分发结论、项目 LICENSE/NOTICE、外部身份和审计锚点仍待完成。 |

## 现状证据索引

| ID | 本地证据/实现位置（不代表本项完成） |
|---|---|
| CP-01 | [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) / [TaskWorker.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskWorker.java) |
| CP-02 | P0 / PARTIAL | 全局任务、配置与操作权威状态 | 可选固定owner全局HTTP/界面、共享配置及历史已接入；旧任务升级导入、完整业务及规模验收仍未完成。 |
| CP-03 | P0 / PARTIAL | 跨节点持久命令路由 | 公开持久命令、离线STOP、真实多目标校验/修复及原UNKNOWN升级恢复已验收；完整取消/故障/迁移与容量矩阵仍未完成。 |
| CP-04 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) |
| CP-05 | P0 / PARTIAL | 共享权限、审计和密钥引用 | 共享账号/授权/链式审计及Worker环境过滤已实现；完整远端关联、最小密钥引用和进程/文件隔离仍缺。 |
| CP-06 | P1 / PARTIAL | 全局日志、指标与诊断界面 | 双入口实测独立采集租约和脱敏诊断分页，Agent 重启后保留事件。全局节点及目标过滤、积压和容量验收待完成。 |
| CP-07 | [README.md](../../../docs/verification/README.md) / [OwnerRuntime.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/OwnerRuntime.java) |
| SE-01 | [app.js](../../../sync-app/src/main/resources/console/app.js) |
| SE-02 | [app.js](../../../sync-app/src/main/resources/console/app.js) / [account-management.md](../../../docs/zh-CN/account-management.md) |
| SE-03 | [app.js](../../../sync-app/src/main/resources/console/app.js) / [ConsoleServer.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/ConsoleServer.java) |
| SE-04 | [README.md](../../../docs/verification/README.md) / [browser-sessions.md](../../../docs/zh-CN/browser-sessions.md) |
| SE-05 | [app.js](../../../sync-app/src/main/resources/console/app.js) / [AccessControl.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AccessControl.java) |
| RC-01 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [FanoutDispatcher.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/FanoutDispatcher.java) |
| RC-02 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [managed-validation.md](../../../docs/zh-CN/managed-validation.md) |
| RC-03 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [JdbcSink.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcSink.java) |
| RC-04 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [reconciliation-workflow.md](../../../docs/zh-CN/reconciliation-workflow.md) |
| RC-05 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [FanoutDispatcher.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/FanoutDispatcher.java) / [README.md](../../../docs/verification/README.md) |
| RC-06 | [transaction-compensation.md](../../../docs/zh-CN/transaction-compensation.md) / [TransactionCompensation.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/TransactionCompensation.java) |
| RC-07 | [manual-correction.md](../../../docs/zh-CN/manual-correction.md) / [CorrectionBatch.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/CorrectionBatch.java) |
| RC-08 | [gap-verification.md](../../../docs/zh-CN/gap-verification.md) / [README.md](../../../docs/verification/README.md) |
| RC-09 | [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) / [README.md](../../../docs/verification/README.md) |
| RC-10 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [messaging-v3.md](../../../docs/zh-CN/messaging-v3.md) |
| SO-01 | [AdapterCatalog.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AdapterCatalog.java) / [SourceAdapter.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/SourceAdapter.java) |
| SO-02 | [validation.md](../../../docs/validation.md) / [matrix.sh](../../../tests/matrix.sh) |
| SO-03 | P1 / DONE | MySQL 8.0 当前V3能力补验 | MySQL 8.0.46 的当前 V3 单机、GTID 主从、单主组复制以及跨节点补偿和修正已验收。其他 JDK 由 JV-02 跟踪。 |
| SO-04 | P1 / DONE | MySQL 8.2 新增兼容与验收 | MySQL 8.2.0 的 Java 8 V3 单机、GTID 主从与单主组复制档案已验收。其他 JDK 由 JV-02 跟踪。 |
| SO-05 | P1 / DONE | MySQL 8.4 扩展兼容与回归 | MySQL 8.4.13 的 V3 三种拓扑、GTID/ROW/FULL 及拒绝档案已验收。其他目标组合由对应矩阵跟踪。 |
| SO-06 | [AdapterCatalog.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AdapterCatalog.java) / [SourceAdapter.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/SourceAdapter.java) |
| SO-07 | [AdapterCatalog.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AdapterCatalog.java) |
| JV-01 | [performance.md](../../../docs/zh-CN/performance.md) / [baseline.json](../../../docs/verification/baseline.json) |
| JV-02 | [performance.md](../../../docs/zh-CN/performance.md) |
| JV-03 | [verify-artifact.py](../../../tests/verify-artifact.py) / [performance.md](../../../docs/zh-CN/performance.md) |
| JV-04 | [performance.md](../../../docs/zh-CN/performance.md) / [values.yaml](../../../deploy/helm/rivomesh/values.yaml) |
| DO-01 | [requirements-and-decisions.md](../../../docs/zh-CN/requirements-and-decisions.md) / [implementation-status.md](../../../docs/zh-CN/implementation-status.md) |
| DO-02 | [rivomesh](../../../deploy/helm/rivomesh) / [kubernetes](../../../deploy/kubernetes) / [console-and-deployment.md](../../../docs/zh-CN/console-and-deployment.md) |
| DO-03 | [managed-repair.md](../../../docs/zh-CN/managed-repair.md) / [recovery.md](../../../docs/recovery.md) |
| DO-04 | [pipeline-v3.md](../../../docs/zh-CN/pipeline-v3.md) / [validation.md](../../../docs/en/validation.md) |
| DO-05 | [verify-artifact.py](../../../tests/verify-artifact.py) / [README.md](../../../docs/verification/README.md) |
| LG-01 | [implementation-status.md](../../../docs/zh-CN/implementation-status.md) / [gaussdb-index-policy.md](../../../docs/zh-CN/gaussdb-index-policy.md) |
| LG-02 | [platform-design.md](../../../docs/zh-CN/platform-design.md) / [gaussdb-index-policy.md](../../../docs/zh-CN/gaussdb-index-policy.md) |
| LG-03 | [clickhouse-v3.md](../../../docs/zh-CN/clickhouse-v3.md) / [doris-v3.md](../../../docs/zh-CN/doris-v3.md) / [hive-v3.md](../../../docs/zh-CN/hive-v3.md) |
| LG-04 | [messaging-v3.md](../../../docs/zh-CN/messaging-v3.md) / [classic-v3.md](../../../docs/zh-CN/classic-v3.md) |
| LG-05 | [pipeline-v3.md](../../../docs/zh-CN/pipeline-v3.md) / [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) |
| LG-06 | [rivomesh](../../../deploy/helm/rivomesh) / [kubernetes](../../../deploy/kubernetes) |
| LG-07 | [performance.md](../../../docs/zh-CN/performance.md) / [benchmark.py](../../../tests/benchmark.py) |
| LG-08 | [pipeline-v3.md](../../../docs/zh-CN/pipeline-v3.md) / [rename-migration.md](../../../docs/zh-CN/rename-migration.md) |
| LG-09 | [README.md](../../../docs/verification/README.md) / [managed-validation.md](../../../docs/zh-CN/managed-validation.md) |
| LG-10 | [account-management.md](../../../docs/zh-CN/account-management.md) / [security-audit.md](../../../docs/zh-CN/security-audit.md) |
