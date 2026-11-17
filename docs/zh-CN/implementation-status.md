# 实施状态与下一批工作

2026-11-17 RC-03限定档案已验收：完整跨库事务跳过、目标提交后源ACK失败、原Agent强杀重开与跨入口恢复、后续APPLY保留缺口均通过。最终发布包51项专项、96 DOM、PG/MySQL各34项原生合同及两种控制库各26项真实同步检查通过。首次全量回归有1项测试夹具租约过期，51项定向重跑通过，未将其写成一次全量无错运行。[验收证据](../verification/2026-11-17-managed-skip/README.md)。当前SDD为10 DONE、32 PARTIAL、6 NEW，剩余38项；缺口验证关闭及补偿仍未完成。

2026-11-17 包名增量：Java/Maven 基础命名空间已改为 `io.github.rivomesh`，已推送 GitHub `main`。全部模块重建、457项应用测试、2项SDK测试、87项DOM、PG/MySQL各33项原生合同及PG控制库23项多节点CDC检查通过。[更名验收](../verification/2026-11-17-package-namespace/README.md)。后续跳过界面已接线，96项DOM及36项管理/HTTP专项通过；协议7部署开关、计划恢复、显式缺口提示及双语操作手册已补入，完整跨节点跳过验收仍未完成。[事务跳过（开发中）](transaction-skip.md)。

开发预览按需启动，默认不常驻。2026-11-16已按用户要求清理预览容器；历史记录中的预览地址当前未启动。
更新：2026-11-17。English: [Implementation status](../en/implementation-status.md)。完整需求见[平台方案](platform-design.md)，操作方式见[控制台手册](console-and-deployment.md)。

最新只读核对增量99a1：事务审阅已可显示目标断点核对，397项发布 JAR 应用测试、85项DOM、PG/MySQL各17项合同与PG控制库22项多节点CDC检查通过。[证据](../verification/2026-11-17-checkpoint-inspection/README.md)。[人工提交确认](commit-acknowledgement.md)已通过[限定验收](../verification/2026-11-17-managed-commit/README.md)：416发布JAR用例、87 DOM、每产品38原生合同、两个控制库各23项多节点检查。RC-05已关闭，继续推进事务跳过与缺口记录。

较早bcaa增量：管理界面事务审阅已验收，可从任一控制入口保存证据、查看历史表/列/类型与前后镜像、显式分页读取原始/规范化事件；ADMIN权限、退出清空及晚到响应保护已覆盖。原日志GC及Agent强杀重开后仍可读。383项应用构建/发布包优先测试、83项DOM、PG/MySQL各23项存储/协议契约、五JRE各52项，以及两个控制库各21项真实同步/审阅链路通过。[操作/部署/FAQ](failure-review.md) · [证据](../verification/2026-11-17-managed-failure-review/README.md)。RC-02限定档案已关闭，手工跳过/补偿/缺口仍未完成。

较早ea18增量：375项应用构建/发布包优先回归、79项DOM、PG/MySQL各21项日志/证据契约、五JRE各44项基础契约通过。已增加[内部私有持久证据](failure-evidence.md)和按需历史结构读取；原日志回收后仍能审阅原记录。[验收](../verification/2026-11-17-failure-evidence/README.md)。首次准备/快照的真实双入口故障与重启已另行通过[662d验收](../verification/2026-11-17-snapshot-diagnostics/README.md)。界面原始事务审阅、人工跳过、补偿和缺口台账仍未完成。

较早增量4a07：358项应用构建/发布包优先测试、78项DOM、PG/MySQL各9项真实日志契约、五JRE各27项基础契约通过；实际双控制入口、SQL暂停后Worker重启、同错误版本/序号保留、健康目标继续投递及显式恢复追平已验证。新增[故障阶段与FAQ](failed-transactions.md)，受限事务检查仍是内部接口；人工跳过、补数、持久证据及缺口台账未完成。[证据](../verification/2026-11-17-failure-inspection/README.md)。

协议4分离档案已通过348项应用构建/发布包优先测试、76项DOM及五JDK各152项专项；PG/MySQL控制库、真实多库双目标、管理全停、控制库中断、接收入口死亡后原命令恢复、非root三进程和SQL并发角色切换均已验收。自动跨Agent接管、完整日志/适配器/JDK矩阵仍待完成。 [Evidence](../verification/2026-11-17-execution-lifecycle/README.md).

SDD跟踪：48项中10项DONE，余38项（6NEW、32PARTIAL）。CP-01、5项会话、CP-07分离档案和RC-02事务审阅及RC-05提交确认限定档案已关闭；CP-02/03/05仍部分完成，自动跨Agent接管仍未完成。[清单](../sdd/001-control-recovery-compatibility/tasks.md)。

## 本批已实现

| 范围 | 交付 |
|---|---|
| 多配置 | 各配置绑定独立源，独立 Worker JVM；持久启停意图、并发/堆限制、退出退避、配置身份检查和目标重叠检查 |
| 访问权限 | VIEWER/OPERATOR/ADMIN 独立令牌身份，API/界面权限、持久操作者记录和 Helm/原生 Secret 角色配置；全局 API/异步操作链式审计、保留/界面/流式导出已实现；精确任务授权已实现；本地账号/角色/授权编辑、令牌轮换与离线恢复已实测；外部身份/不可变锚点待完成 |
| 控制界面 | 中英文登录、任务向导、YAML 导入、能力目录、检查/启停、全量及积压状态、操作记录；ADMIN 配置编辑、凭据轮换、版本历史及预览恢复 |
| 共享控制 | 管理表 PAUSED、旧 owner 隔离、按错误版本恢复、请求去重和乐观版本检查 |
| 配置与状态 | 原子写入、单控制目录文件锁、敏感配置私有权限、Worker 配置快照、日志轮转、状态失败提示 |
| 扩展核心 | Source/Sink/Journal SPI、类型精度/字段存在性、持久日志及独立目标确认；v2 状态保持兼容，v3 已接入 CLI/Web，SQL 与消息目标按实际协议区分 |
| 部署 | Linux 服务示例、非 root Docker、Helm Chart、由同一 Chart 渲染的原生 YAML、健康检查及持久卷 |
| 回归 | Java 单元/API、真实库多 Worker/暂停恢复/重启、原有 HA/MGR、DOM 交互、镜像及清单检查；最终数量见验证记录 |

Web 与 CLI 均已接通 v3，多目标向导、独立错误恢复和托管本地日志已通过真实 HTTP 测试。v2 单库任务继续保留。V3 支持多库映射、多个事务目标、可选日志、无主键/JSON/受支持生成列和次级索引策略；详见 [V3 手册](pipeline-v3.md)。真实 MySQL 8.4 → PostgreSQL 17 + openGauss 7 双实例链路已通过单目标 SQL 故障隔离及进程强制终止恢复。独立 MySQL 8.4、PostgreSQL 17、openGauss 7 三目标完整链路已通过两种日志模式；v3 普通主从及 MGR 故障用例也已通过，完整版本矩阵仍在扩展。Kafka 3.9.2、RabbitMQ 4.1.4、NATS 2.11.8、Redis 7.4.2 和 Pulsar 4.0.14 的确认发布、快照/增量及 Worker/Broker 重启已实测，见[消息手册](messaging-v3.md)。RocketMQ 原生 5.5.1 客户端 / 5.5.0 Broker 的本地同步刷盘、重启和 Topic 丢失保护已通过独立验收，联合全量/增量、Worker 强杀及三轮 Broker 重启已通过（49 项执行）。RocketMQ 原生位置/内容回执也已通过实际旧非空备份回退拒绝，以及 MySQL/PostgreSQL/openGauss 日志保存验证。Artemis Core 传输和界面已接入，独立 Java 8/当前 Broker 传输和原生回执验收通过，包含相同资源身份的旧非空备份拒绝；实际发布包的七种消息目标定点链路用例已通过全量/增量、Worker 强杀、三轮全部 Broker 停止/启动及 Artemis 队列丢失后的独立隔离。此前超时记录仍为失败证据；客户端传输错误现已与真实 HTTP 响应区分。ClickHouse 原生版本物化已通过首批 MySQL＋PostgreSQL 多目标恢复配置，见 [ClickHouse 手册](clickhouse-v3.md)；普通 BTREE/业务 UNIQUE/简单 GSI 到跳数索引的转换、FINAL 精确查询和旧任务显式 NONE 模式已实现，表达式/前缀/全文/空间索引仍需转换计划。ActiveMQ Classic 已接入并通过独立原生传输/消费/重启/旧备份保护验收，MySQL＋PostgreSQL 联合全量/增量、进程强杀、Broker 故障、SQL 约束修复重试和清空隔离也已通过；Doris 4.1.3 ARM64/Java8 原生 MoW/Stream Load 已通过独立重启/旧非空备份拒绝，以及真实 MySQL8.4＋PostgreSQL17 全量/增量、目标停机自动追平、Worker 强杀、SQL错误修复重试及视图丢失隔离，详见[Doris手册](doris-v3.md)。OceanBase MySQL 兼容业务租户实验适配已实现，原生强读/跨库回滚/归属/epoch/全量及部分增量已实测；完整联合恢复尚未通过，仍标记实验状态，见[OceanBase手册](oceanbase-v3.md)。Hive 实验适配已接通配置、隔离 JDBC、ACID ORC 历史/视图和索引省略计划；首轮独立原生物化/重启/旧备份保护、确认丢失/大小写及真实 MySQL＋PostgreSQL 联合全量/增量、停机追平、Worker 强杀、SQL 修复和视图丢失隔离已通过，生产集群/安全/性能矩阵尚未完成，见[Hive 手册](hive-v3.md)。金仓 PG/UTF8 实验适配已接入 CLI/界面与厂商 Java8 驱动，首批原生事务及真实 MySQL＋PostgreSQL 联合全量/增量、带符号微秒 TIME、无主键、停机追平、Worker 强杀、SQL 修复和缺失表隔离已通过，见[金仓手册](kingbase-v3.md)；金仓其他模式、集群、安全/性能仍待完成。达梦原生0/UTF8/blank1实验适配已通过首批 MySQL＋PostgreSQL 联合全量/增量、宽小数/JSON文本计划、既有空表容量/精度拒绝、文本主键大小写/尾空格、TIME、停机追平、Worker强杀、SQL修复及缺表隔离，见[达梦手册](dm-v3.md)；其他模式、集群、安全/性能仍待完成。GBase 8c V6 S1.3.0B19 PG/UTF8 厂商驱动、CLI/界面和连接语义检查已实现，首批真实 MySQL＋PostgreSQL 联合全量/增量、TIME/无主键/文本主键、停机追平、强杀恢复、SQL 修复和缺表隔离已通过，见[GBase 8c手册](gbase8c-v3.md)；8a、8s 仍待分别实现。

ADMIN 配置编辑及版本恢复已通过独立真实发布包链路，包含 PostgreSQL 密码轮换、幂等保存、历史恢复和投影文件缺失后的控制器重启续传，见[配置编辑手册](configuration-editor.md)。全局 API/异步操作审计、保留与导出已通过独立 Java8 HTTP/强杀恢复及真实数据库链路校验，见[审计手册](security-audit.md)。本地账号管理的原生升级/强杀/旧状态拒绝及 SQL 连续性已通过，见[账号手册](account-management.md)；外部不可变锚点、OIDC及迁移仍未完成。

精确任务授权已接通任务列表及直接操作路由，VIEWER/OPERATOR可限制任务ID；空列表无任务权限，ADMIN全局管理，见[授权手册](task-authorization.md)。本地账号/授权编辑界面及 API 已实现，具有管理员保留、版本/审计锚点及离线恢复保护，见[账号手册](account-management.md)。

## 下一批核心实现

1. 接入外部身份并扩展修复和配置迁移工作流，并实现 v2 显式迁移/回退；完成可选日志、主从/MGR、多节点联合故障矩阵。
2. 实现超大事务分片、海量目录分页/有界缓存、物理容量监测和回执压缩；完善共同追平屏障与监控指标。
3. 扩展生成表达式/类型、历史 DDL、索引转换计划和非空目标接管；主键/身份键不能降级，业务 UNIQUE/GSI 的转换边界已写入手册。
4. 完成 OceanBase 重启恢复验收，完善 ClickHouse/Doris 的集群/性能配置，扩展 Hive 的集群/安全/性能验收并实现其他厂商目标；扩展已实现 Kafka/RabbitMQ/NATS/Redis/Pulsar 的集群、TLS、容量故障矩阵。
5. 补全角色、凭据生命周期/配置迁移、校验/修复、切换和回退工作流，发布硬件基准与长稳/升级/真实集群验收矩阵。

阶段化不改变已确认范围，也不把界面“规划中”当成已经实现。

## 工作区补充待办

工作区 TODO 还记录了手工补数据/跳过，以及 Java 8、11、17、21、25 支持。原文件保持独立，本页仅记录实施关系。

手工修复与跳过需要单独设计：保留原事件、定位失败事务及所有受影响目标、操作权限/审计、幂等补偿、回放和后续数据对账。跳过后必须显式标记一致性缺口；它不是普通 resume，也不能通过直接修改 GTID 冒充完整同步。当前界面默认恢复仍重试原失败事务，尚未实现手工跳过或数据编辑。

构建和 Broker 故障验证基于 Java 8；Temurin 8/11/17/21/25 的 MySQL→PostgreSQL 主键表全量/增量/字节一致性，已在 LOCAL 和 PostgreSQL EXTERNAL 日志下通过 10 个真实配置，见[原始报告](performance.md)。其他驱动、消息、TLS 和故障切换仍需逐运行时验收。

## 验证边界

实际集群 Kubernetes/CSI、剩余目标和已实现消息目标的完整集群矩阵、OIDC及外部身份集成、完整迁移接管、源 PostgreSQL/Oracle 和硬件性能标准尚未验收。当前通过的单项记录见[验证记录](../validation.md)。真实浏览器窗口在当前环境不可用，已完成 DOM/HTTP 验证，截图级外观与可访问性待补验。

开发预览已按用户要求停止；后续使用临时验证环境，完成即清理。历史预览说明见[开发预览](development-preview.md)。

共同校验屏障基础已实现并通过 LOCAL/H2、外部 PostgreSQL/MySQL/openGauss 四种日志下的真实库验证，包含跨 database 的同一 MVCC 视图、源写锁/元数据锁、超时/强杀释放、采集/规范化/目标 ACK 原子追平与持久暂停、所选目标隔离及未选目标真实停机恢复。现有 GBase 8c 联合故障链路也回归通过。已继续实现私有磁盘源行密封、事务型 SQL 目标逐行读取、缺失/多余/变更报告及 CLI；其新增验收单独记录，不能将屏障测试视为完整对账验收。管理端校验作业/UI的新增验收见下文；修复、接管切换仍待完成，见[校验修复工作流](reconciliation-workflow.md)。

SQL逐行校验及CLI首批真实验收现已通过上述四种日志配置，包含源库停机后的比对、校验进程强杀恢复和精确差异计数；目标限定MySQL8.4/PostgreSQL17。[证据](../verification/2026-11-15-row-reconciliation/README.md)。管理端UI验收见下文；修复仍未完成。


2026-11-16：管理端校验首版已通过四种日志模式真实 HTTP/Worker/数据库联合验收，提供目标和预算选择、后台记录、取消、继续比对、分表摘要、权限及报告审计。最终187项根构建测试（185应用+2SDK）、185项发布包优先测试及44项DOM通过。MySQL新日志库的首次只读状态/启动问题已修正。原始差异行、修复、完整目录/保留、接管切换和其余完整目标仍待完成。[管理校验](managed-validation.md) / [证据](../verification/2026-11-16-managed-validation/README.md)。

2026-11-16：管理员差异分页、原始值详情已通过四种日志模式真实SQL/HTTP权限验收及原有屏障回归。191项根构建（189应用+2SDK）、189项发布包优先测试和46项DOM通过；Docker预览已更新。修复、跳过和切换仍待完成。[差异详情证据](../verification/2026-11-16-difference-details/README.md)。

2026-11-16：共同校验点修复CLI首版已通过四种真实日志模式、MySQL8.4/PostgreSQL17业务目标验收：显式计划/摘要确认、目标变化拒绝、SQL回滚、无主键身份恢复、提交响应丢失、提交前后强杀及凭据续跑，CDC位点保持不变。200项根构建（198应用+2SDK）、198项发布包优先测试和46项DOM通过；Docker预览已更新。修复界面/持久作业、依赖检查、失败事务补偿/跳过、接管切换及完整项目目标仍待完成。[修复手册](common-cut-repair.md) / [证据](../verification/2026-11-16-common-cut-repair/README.md)。

2026-11-16：管理端修复首版已验收：显式选表、预定计划ID持久化、认证管理员审阅/摘要确认、修订号/请求去重、取消与重试、提交凭据和审计。MySQL/PostgreSQL目标上的触发器、外键与元数据权限拒绝已有实测；完整依赖规划、预算编辑、独立凭据目录、失败事务补偿/跳过、接管切换、其他厂商及完整HA/容量矩阵仍待完成。见[管理端修复](managed-repair.md)。

2026-11-17: CP-02/03 now PARTIAL: internal global task/control foundations passed307actual-JAR-first tests and native PostgreSQL/MySQL control fixtures. Historical 1e47 candidate used LOCAL public control; later candidates add the opt-in global profile. [Evidence](../verification/2026-11-17-global-control-foundations/README.md).

2026-11-17 开发增量：可选 sharedTasks 固定owner全局HTTP/界面、共享配置/历史、持久命令及远程查询已接入。重启前STOP/PAUSE屏障及超过50条命令分页回归已补入；新候选应用构建336项通过，真实跨节点业务CDC和新包五JDK验收进行中。CP-02/03仍PARTIAL，旧任务导入和自动接管未完成。 [Operations](global-control.md).
