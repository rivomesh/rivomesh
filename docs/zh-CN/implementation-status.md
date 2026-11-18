# 当前实施状态

首版尚未发布。Java/Maven命名空间为`io.github.rivomesh`。[完整SDD](../sdd/001-control-recovery-compatibility/spec.md) · [任务清单](../sdd/001-control-recovery-compatibility/tasks.md) · [English](../en/implementation-status.md)。

48项需求：**21项完成、25项部分完成、2项未实现，剩余27项**。完成项仅适用于各自声明的验收档案，不代表所有数据库版本和部署组合均已通过。

## 已验收

| 范围 | 当前结果 |
|---|---|
| DO-01 | 双语 SDD 的 48 项需求、验收、证据与进度自动核对。 |
| CP-01 | 统一节点目录、角色、版本、心跳及归属 |
| SE-01～05 | 刷新登录、期限/撤销、安全Cookie/CSRF、跨入口会话、API身份规则 |
| CP-07 | 控制/执行分离；限定档案下管理故障不影响原Worker安全同步 |
| RC-02 | 持久事务证据、管理员分页审阅、历史结构及镜像 |
| RC-05 | 目标已提交但ACK丢失时核对原生凭据并恢复确认 |
| RC-03 | 完整跨库事务跳过；原生提交后ACK失败、原Agent强杀重开、跨入口恢复；保持暂停并显示缺口 |
| RC-08 | 持久缺口、审阅核验、跨入口确认/取消及真实增量水位；限定SQL档案 |
| RC-06 | 前镜像补偿还原受影响行，跨入口审批并保持暂停。限定 SQL 档案 |
| RC-07 | 手工录入、JSON 导入、前后值预览与跨入口审批。限定 SQL 档案 |
| RC-01 | MySQL 源连接、解码、DDL、日志及目标应用阶段诊断，跨入口和重启验证 |
| RC-09 | 补偿与修正跨入口取消、提交前后强杀恢复，并按原请求核对凭据 |
| JV-01 | 五个 JDK 的 Controller、Agent 和 Worker 实际启动并完成 SQL 全量和增量 |
| SO-02 | MySQL 5.7.44 的 Java 8 V3 三种拓扑、真实主从 SQL 线程故障和 binlog 缺失。 |
| SO-03 | MySQL 8.0.46 的 V3 三种拓扑及跨入口人工恢复 |
| SO-05 | MySQL 8.4.13 的 V3 三种拓扑、日志准入和会话级不支持事件的处理 |
| SO-04 | MySQL 8.2.0 在 Java 8 的 V3 单机、GTID 主从和单主组复制实库档案 |

[单一验证基线](../verification/README.md)保存验收范围和可追溯摘要，不再维护开发迭代的逐次副本。清理后的最新回归与旧验收记录分开标记。

## 已有能力及边界

| 能力 | 当前状态 |
|---|---|
| 同步链路 | MySQL多库映射、同源多目标、自动建表、分块全量转增量、持久断点；无主键/JSON/受支持生成列 |
| PostgreSQL 源 | 17.12 单主实验档案，涵盖 pgoutput、JSONB、无主键、LSN 续传及槽缺失拒绝。见[配置与恢复](postgresql-source.md) |
| 采集故障恢复 | 管理界面和 CLI 提供只读检查、受影响订阅及原断点复验。范围隔离待实现。见[操作说明](capture-recovery.md) |
| 管理 | 中英文向导、配置/修订、独立Worker、跨入口持久命令、账号/权限/审计 |
| 对账修复 | 共同校验点、行差异、管理员修复计划；限定MySQL/PostgreSQL档案 |
| 缺口核验 | 审阅计划、管理员确认、重新扫描、原子凭据和跨入口取消；限定PG/MySQL，验收进度见[手册](gap-verification.md) |
| 事务补偿 | PostgreSQL 和 MySQL 的管理员预览、确认、目标原子凭据和原事务重放。见[操作说明](transaction-compensation.md) |
| 关系目标 | MySQL/PostgreSQL/openGauss已有实际档案；达梦/金仓/GBase8c为限定实验档案；OceanBase完整恢复未验收；分布式GaussDB待实机 |
| 分析目标 | ClickHouse/Doris/Hive已有初步物化和恢复档案；生产集群、安全及性能矩阵待补 |
| 消息目标 | Kafka/Pulsar/RabbitMQ/RocketMQ/ActiveMQ Classic/Artemis/Redis Streams/NATS JetStream已有不同范围的确认/恢复档案；全部集群/TLS矩阵未完成 |
| 部署 | Linux、Docker、Helm、原生Kubernetes YAML；已实测双节点 Kubernetes 的 Helm/原生 YAML 与 RWO CSI 重挂，跨节点存储和旧 owner 隔离待验收 |
| Java | Java8构建基线；五JRE已有部分档案，完整源×目标×驱动×TLS×恢复矩阵未完成 |

具体产品约束以[平台方案](platform-design.md)、[控制台](console-and-deployment.md)和各产品手册为准。协议和数据格式编号是内部状态标识，不是对开发版本升级的承诺。

## 当前清理

已删除旧二进制探针、自动控制模式迁移及开发快照。控制命名空间按首次配置直接创建；模式不匹配拒绝启动，不静默升级或回退。73项相关回归已通过。传输SDK已进入Maven缓存，应用层迭代无需重复打包所有SDK。

V2/V3双执行链、对应向导和旧配置仍有交叉引用，将作为下一批独立清理；当前不宣称已经全部删除。用户数据、业务表和有效状态完整性检查未被清理。

## 下一步

1. 源端受控范围恢复及其他目标恢复协议。
2. 自动跨Agent接管、排空迁移、完整全局诊断与存储可达性。
3. PostgreSQL 源的主备切换和共同校验点，以及 Oracle 源。
4. 分布式GaussDB、其他厂商/消息/分析目标生产矩阵。
5. 大事务/海量表/硬件基准、真实K8s、完整JDK矩阵与双语文档。

开发预览不常驻；测试结束清理临时容器，保留实际需要的依赖缓存。每小时汇报的对比基线记录在[progress-reports.json](../sdd/001-control-recovery-compatibility/progress-reports.json)，独立定时通知工具当前不可用。
