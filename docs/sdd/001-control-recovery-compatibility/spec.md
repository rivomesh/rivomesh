# Rivomesh SDD需求基线与未完成事项

RM-SDD-001 · v0.59 · 2026-11-19 · IMPLEMENTING

实施中：48项持续追踪，36项DONE、12项PARTIAL、0项NEW，余12项未闭环。RC-01、RC-02、RC-05、RC-03、RC-06、RC-07、RC-08、RC-09、SO-03、SO-04、SO-05 和 JV-01 限定档案已验收。自动接管、更多源恢复档案和完整兼容矩阵继续追踪。

[基线和决策](plan.md) · [任务](tasks.md) · [验收矩阵](acceptance.md) · [English](spec.en.md)

## 多节点统一控制

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| CP-01 | P0 / DONE | 统一节点注册与状态 | 共享节点目录已通过PostgreSQL/MySQL双节点、kill/重开及控制库中断验收；任务权威/远端命令仍属CP-02/03。 |
| CP-02 | P0 / DONE | 全局任务、配置与操作权威状态 | PostgreSQL 与 MySQL 双控制库实测跨入口任务修订一致、全局任务 ID 唯一、两个独立源及配置隔离，和单机原任务强杀续传。 |
| CP-03 | P0 / DONE | 跨节点持久命令路由 | PostgreSQL 与 MySQL 各通过七项实库验收，包括跨入口启停、原请求去重、离线 202、撤权拒绝及跨 Agent 代次的未知结果。既有实库档案覆盖目标重试和校验修复。 |
| CP-04 | P0 / PARTIAL | 故障接管、隔离与资源调度 | 双 Agent 同库切换、报告及回执接收、双 SQL 围栏和候选显式启动已实测，已审批排空的交接也能在 Controller 退出后续办。原 Agent 强制终止时，两入口区分节点租约过期与任务状态未知。原目录重启先执行离线 STOP，再经显式 START 续传，旧 Agent 和采集代次均已隔离。已实测原 Agent 仅失去同库控制和日志路径时停止旧 Worker，原进程重连后先办离线 STOP，显式启动后双目标各应用一次。原 Agent 永久失联、在途事务、独立库可达性和完整排空矩阵仍待验。 |
| CP-05 | P0 / DONE | 共享权限、审计和密钥引用 | PostgreSQL 和 MySQL 控制库实测跨入口撤权、按请求/任务/Agent/计划追踪及真实浏览器脱敏。执行 Agent 启用任务凭据授权后，Worker 仅得到所需且授权的引用。 |
| CP-06 | P1 / DONE | 全局日志、指标与诊断界面 | 实测双入口按节点、任务、目标诊断和分页、Agent 重启及限流。PostgreSQL 与 MySQL 8.4 直写提供已验证提交延迟，其余不可测时间明确显示未知。 |
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
| RC-04 | P0 / DONE | 采集/解码阶段的受控缺口恢复 | MySQL 8.4 单笔完整 binlog 行事务档案通过。已支持受影响目标确认、原始证据留存、显式批准排除、持久缺口与原位续传。无主键、其他协议和无法重验的范围需恢复原日志或审阅重建方案。 |
| RC-05 | P0 / DONE | 不确定提交与断点核对 | 协议6原Agent/LOCAL日志、PG/MySQL控制库限定档案已验收：全任务暂停、新证据、原生围栏、管理员跨入口确认、ACK/回执原子提交及幂等重试，保持原暂停。跳过、补偿和其他完整矩阵另行追踪。 |
| RC-06 | P0 / DONE | 失败事务前镜像补偿 | 首批 SQL 档案通过。管理员跨入口审阅表范围及增删数量，按原事务前镜像恢复目标，随后显式恢复并重放。真实 CDC 验证 PostgreSQL 补偿，MySQL 通过原生合同。其他目标由 RC-10 追踪。 |
| RC-07 | P0 / DONE | 界面手工补数据与修正 | 首批 SQL 档案通过。逐行录入和 JSON 导入支持管理员审阅前后值、精确行定位、原子批次和原事务恢复。PostgreSQL 通过真实 CDC，MySQL 通过原生检查。其他目标由 RC-10 跟踪。 |
| RC-08 | P0 / DONE | 一致性缺口台账与真实水位 | 首批SQL档案已验收：持久缺口、审阅计划、跨入口审批/取消、全表重扫、原子凭据及安全覆盖/连续应用水位。核验不改写SKIP、不推进原应用前缀、不自动恢复；其他目标协议由RC-10/LG-03/LG-04继续追踪。 |
| RC-09 | P0 / DONE | 补偿作业的跨节点持久化和恢复 | 首批 SQL 档案通过跨节点计划、审批、取消与原请求恢复。真实提交边界测试以 PostgreSQL 17 为恢复目标。 |
| RC-10 | P1 / PARTIAL | 多目标恢复语义和回收保护 | Kafka 双入口复核物理 Topic 并重放受阻审批。托管 Worker 已在真实 Kafka 上为启用后首次可核验的 COMMIT 持久记录 Topic UUID、原生位点、Agent 成员租约及日志 UTC 时间。PostgreSQL、MySQL 和 H2 日志已通过，业务与消费位点保持独立。两个管理入口可在当前故障的精确前序 ACK 查验签名回执。真实 Kafka 查询、历史全量归属、正式发送与恢复、跨适配器补偿待验。 |

## 源数据库兼容

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| SO-01 | P0 / DONE | 多源协议与能力配置 | V3 管理界面已按能力提供 MySQL、PostgreSQL、Oracle 源类型、映射和版本表单。按各自身份和坐标预检，明确拒绝其他源类型的专用配置。 |
| SO-02 | P1 / DONE | MySQL 5.7 完整支持档案 | 5.7.44 Java 8 V3 单机、GTID 主从、MGR 和未采集 binlog 缺失已实测。 |
| SO-03 | P1 / DONE | MySQL 8.0 当前V3能力补验 | MySQL 8.0.46 的当前 V3 单机、GTID 主从、单主组复制以及跨节点补偿和修正已验收。Java 21 其余组合由 JV-02 跟踪。 |
| SO-04 | P1 / DONE | MySQL 8.2 新增兼容与验收 | MySQL 8.2.0 的 Java 8 V3 单机、GTID 主从与单主组复制档案已验收。Java 21 其余组合由 JV-02 跟踪。 |
| SO-05 | P1 / DONE | MySQL 8.4 扩展兼容与回归 | MySQL 8.4.13 的 V3 三种拓扑、GTID/ROW/FULL 及拒绝档案已验收。其他目标组合由对应矩阵跟踪。 |
| SO-06 | P1 / PARTIAL | PostgreSQL 源适配器 | Temurin 21 已实测 PostgreSQL 17.12 逻辑槽切主、UUID、DATE 、微秒时间戳和 TIME(6) 双目标续传，以及 18.7 存储型生成列。更多类型、动态 DDL 和多目标共同对账点待验。 从逻辑槽停在结构变化前的 LSN 时，已实测当前 PostgreSQL 发布的只读结构差异。 PostgreSQL 17.12 扩展 NUMERIC 标度已实测 PostgreSQL 和 MySQL 双目标，涉及源端舍入、65 位精度、无主键重数及 NaN 整笔暂停。 |
| SO-07 | P1 / PARTIAL | Oracle 源适配器 | Oracle 26ai 单 PDB 到 PostgreSQL 已验证长事务、原 SCN 续传、严格文本 JSON、DATE 和显式启用的有界整数虚拟列乘法、加法及减法。无主键重复行、SQL NULL 和原快照归档重放通过。其他表达式、原生二进制 JSON、LOB 和 RAC 故障切换待验。 Oracle Free 已验证 BasicFiles 和 SecureFiles 更新的 LogMiner 记录缺少可靠 LOB 前镜像，受限账号预检分别返回对应原因。LOB 数据采集仍待完整镜像方案。 嵌套保存点和回滚在实库归档中已核对。两路 PostgreSQL 订阅在进程强杀后沿用原快照和 SCN，提交行与同 SCN 的 Flashback 一致。 |

## Java运行时兼容

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| JV-01 | P0 / DONE | Temurin 21 首版运行基线 | 现行候选使用 Java 21 字节码。已实测两个 Controller、Agent、Worker 及 MySQL 双目标同步。后续连接器与部署组合由 JV-02 至 JV-04 跟踪。 |
| JV-02 | P1 / PARTIAL | Java 21 源、目标及恢复组合 | 首版仅使用 Temurin 21 LTS。已实测四个 MySQL 版本的单机全量与增量、MySQL 8.4.13 异步 GTID 双目标续传与三节点单主组复制，以及 PostgreSQL 17/18 和 Oracle 单 PDB。其他目标、TLS 和恢复场景继续按 Temurin 21 验收。 |
| JV-03 | P1 / PARTIAL | 驱动隔离、依赖与安全协议兼容 | Java 21 实测 MySQL/PostgreSQL TLS 与重连、达梦外置驱动及 SQL 修正、金仓外置驱动的身份验证、精度、故障重连和原回执续传。五路 JDBC 已核对外置加载。其他厂商 TLS 及集群组合继续验收。 Pulsar 4.0.14 已实测单 Broker 的私有 CA、Token、拒绝错误主机名和重启续传。 |
| JV-04 | P1 / DONE | Java 21 部署及滚动替换 | Temurin 21 已通过手动 Linux 启动、非 root Docker 实库、Helm 和原生 Kubernetes 部署。同一构建通过发布证据门禁，滚动替换和原目录续传也有实库记录。 |

## 完整文档

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| DO-01 | P0 / DONE | 双语SDD基线与范围可追踪 | 48 项需求、任务和验收用稳定 ID 对齐。证据档案、双语验收文本与进度计数由门禁核对。 |
| DO-02 | P1 / DONE | 四类部署与多节点运维文档 | 四种单机与集群部署路径、存储、会话、节点维护和备份升级已形成双语操作说明。跨节点卷和自动接管按对应实机需求继续验收。 |
| DO-03 | P1 / DONE | 恢复场景和FAQ完整手册 | 中英文故障手册覆盖切主、日志丢失、SQL/确认错误、节点、存储、会话、人工恢复与交接，按已有产品档案给出操作和边界。 |
| DO-04 | P1 / DONE | 配置/API/兼容性与升级文档 | 配置、API、权限、坐标、一致性、升级和实际跨 JDK 档案已整理为中英文索引。各未验收组合继续由对应兼容事项跟踪。 |
| DO-05 | P1 / DONE | 文档和证据发布门禁 | 门禁核对候选包 SHA、25 份示例、双语文档和真实 Chrome、独立 Agent 原生恢复、两类实机 Kubernetes 证据。不同候选包的清单已验证会被拒绝。 |

## 历史范围合并保留

| ID | 优先级/状态 | 需求 | 尚缺内容 |
|---|---|---|---|
| LG-01 | P1 / PARTIAL | 国产事务库兼容矩阵 | OceanBase CE 4.3.5.6 MySQL 租户已通过原生事务和联合重启恢复。分布式 GaussDB、OceanBase Oracle 模式及 GBase 8a/8s 待验收。 openGauss 7.0.0-RC3 A 模式已实测预检与原生索引一致、复合主键写入、目标原端点重启续传，商业分布式仍独立验收。 |
| LG-02 | P1 / DONE | 非空接管、约束恢复、切换回切 | 限定档案已验收。冻结 MySQL 的同一非空 PostgreSQL 单表完成差异审阅、原切点接管、恢复已审批的 BIGINT UNIQUE、业务切换及首次提交写入前回切，并复用原快照继续 CDC。另一个同切点档案验证了二进制排序、NO PAD 的 VARCHAR UNIQUE(email)，保留原普通索引并继续 CDC。更广产品及写后逆向迁移分别跟踪。 |
| LG-03 | P1 / PARTIAL | ClickHouse/Doris/Hive生产档案 | ClickHouse、Doris 和内置目录 Hive 已有原生恢复档案。Hive 4.0.1 外置 PostgreSQL Metastore 通过全量、增量、原目录身份、整组重启、Worker 原快照续传、四批回执与独立 SQL 错误重放。分布式仓库、生产集群、旧备份恢复和容量仍在验收。 |
| LG-04 | P1 / PARTIAL | 全部消息目标的集群与恢复协议 | NATS TLS、RabbitMQ 多数派、Kafka KRaft 和 Redis Streams 外部提升及消费恢复已有原生档案。Pulsar 4.0.14 已验证单 Broker 私有 CA/Token，以及双 Broker、三 BookKeeper 下的原 Topic 恢复、确认不足时暂停确认和同事件重试。双 Broker 私有 CA/Token 与三节点 ZooKeeper 主节点故障后的元数据写入也已通过。跨主机存储故障域、内部链路加密、负载容量和其他消息目标档案待验。 Artemis 2.57.0 主进程强杀后由共享卷备进程接管。相同入口复用原生回执及队列身份并确认同一重试。 ActiveMQ Classic 5.19.12 已实测同一 KahaDB 卷的主备切换。固定 OpenWire 和 Jolokia 入口切换后，发行包发布器核验原归属和回执，确认同一重试事件。旧主重启保持备用。 |
| LG-05 | P0 / PARTIAL | 可选存储和跨节点数据可达性 | 可选日志与 PostgreSQL 同库双证据交接已实测。候选丢失独立日志库链路时控制租约原地续期，原生切点拒绝。复联后可重新核验并暂存，双入口拒绝跨库写入接管。释放后原 Agent 从原断点向两目标续传。异库持久围栏、其他存储和目标拓扑待验。 |
| LG-06 | P1 / DONE | 真实K8s/CSI与节点故障验收 | 双节点 ARM64 实测通过 Helm 和原生 YAML 的镜像升级、RWO CSI 原卷迁移及自动续传。另实测物理 Worker 故障恢复、Secret 轮换、外部日志接管及旧 Worker 写入隔离。 |
| LG-07 | P1 / DONE | 海量对象、大事务和硬件性能标准 | Temurin 21 限定资源档案已验收。含 12 轮硬件实测、256 库、超过 8 MiB 的单笔事务、独立日志限额下的慢目标背压和强杀追平。硬件手册记录测量口径及实际资源。 |
| LG-08 | P1 / PARTIAL | 历史DDL、类型、索引与状态迁移 | 对象白名单及 UNIQUE/GSI 转换已有。已实测 MySQL 到 PostgreSQL 的协调冷备和原断点恢复；MySQL 原生 DDL 差异可在两入口按日志与当前目录只读核对。经审阅执行动态 DDL、复杂类型和其他产品待验。 PostgreSQL 17.12 关系变化已实测停留原 LSN。界面审阅包括原生类型声明变化、列顺序和目录已有的索引定义。 JDBC 目标已回归验证身份列换序时仍保护 UNIQUE，显式全局主键和身份 GSI 在建表前要求分布方案。MySQL 源的 Gauss 目标可在管理预检审阅索引动作，覆盖完整统计及有界详情。商用多 CN/DN 和目标目录行为仍需实机验收。 openGauss 7.0.0-RC3 已核对源 UNIQUE(email) 降级与索引 OID 保留，目标主键和同步回执的唯一性保留。 |
| LG-09 | P1 / DONE | 报告目录、保留清理与真实浏览器验收 | 双入口历史报告/回执、审批、键盘分页、归档和恢复已实测。限定档案回收经停止、保留期、摘要和报告锁检查后只移除已安全归档的重复行文件。 |
| LG-10 | P2 / PARTIAL | 外部身份、不可变审计与Apache准备 | 签名检查点与完整原始审计事件都可独立核验并写入 S3 Object Lock。本地 MinIO 已实测精确版本、COMPLIANCE 保留、同版重试、跨批次连续性及离线还原。SBOM 与五路外置 JDBC 包已有证据。真实 AWS 权限、项目授权、LICENSE/NOTICE 及外部身份仍待完成。 精确发行包扫描已识别外置版本的 151 个包内组件，并记录 46 个 Maven BOM 缺项。显式分发审查会阻止未知嵌套制品。 单固定 HTTPS OIDC 提供方已通过模拟实测及账号界面绑定和撤销，同一共享控制库的两个入口均执行本地账号权限和撤权。真实身份提供方、入口代理及提供方主动撤销仍待验。 |

## 现状证据索引

| ID | 本地证据/实现位置（不代表本项完成） |
|---|---|
| CP-01 | [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) / [TaskWorker.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskWorker.java) |
| CP-02 | P0 / DONE | 全局任务、配置与操作权威状态 | PostgreSQL 与 MySQL 双控制库实测跨入口任务修订一致、全局任务 ID 唯一、两个独立源及配置隔离，和单机原任务强杀续传。 |
| CP-03 | P0 / DONE | 跨节点持久命令路由 | PostgreSQL 与 MySQL 各通过七项实库验收，包括跨入口启停、原请求去重、离线 202、撤权拒绝及跨 Agent 代次的未知结果。既有实库档案覆盖目标重试和校验修复。 |
| CP-04 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) |
| CP-05 | P0 / DONE | 共享权限、审计和密钥引用 | PostgreSQL 和 MySQL 控制库实测跨入口撤权、按请求/任务/Agent/计划追踪及真实浏览器脱敏。执行 Agent 启用任务凭据授权后，Worker 仅得到所需且授权的引用。 |
| CP-06 | P1 / DONE | 全局日志、指标与诊断界面 | 实测双入口按节点、任务、目标诊断和分页、Agent 重启及限流。PostgreSQL 与 MySQL 8.4 直写提供已验证提交延迟，其余不可测时间明确显示未知。 |
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
| SO-03 | P1 / DONE | MySQL 8.0 当前V3能力补验 | MySQL 8.0.46 的当前 V3 单机、GTID 主从、单主组复制以及跨节点补偿和修正已验收。Java 21 其余组合由 JV-02 跟踪。 |
| SO-04 | P1 / DONE | MySQL 8.2 新增兼容与验收 | MySQL 8.2.0 的 Java 8 V3 单机、GTID 主从与单主组复制档案已验收。Java 21 其余组合由 JV-02 跟踪。 |
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
| DO-05 | [release-evidence-gate.py](../../../tests/release-evidence-gate.py) / [README.md](../../../docs/verification/README.md) |
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
