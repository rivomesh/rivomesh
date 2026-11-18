# Rivomesh

独立Java/Maven数据同步平台，包名与Maven groupId为`io.github.rivomesh`。首版尚未发布；开发候选的兼容分支和重复快照正在清理。

当前主链路使用MySQL复制协议采集，支持多database显式映射、同源多目标、自动建表、分块全量转增量与持久断点。管理端提供中英文配置、节点/任务管理、权限审计、事务审阅、限定SQL事务跳过和对账修复。

## 启动

```sh
mvn -s build/settings.xml package
export RIVOMESH_TOKEN='replace-with-at-least-24-random-bytes'
java -jar sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar serve config/platform.yml
```

默认管理地址为`http://127.0.0.1:8080`。认证令牌由配置提供；配置生产HTTPS/Cookie/Origin后再开放访问。创建同步任务可用向导或导入[当前多库多目标配置](config/pipeline-v3.yml)。

| 文档 | 中文 | English |
|---|---|---|
| 当前能力、完成项和限制 | [实施状态](docs/zh-CN/implementation-status.md) | [Status](docs/en/implementation-status.md) |
| 首次部署与控制模式 | [首版基线](docs/zh-CN/first-release.md) | [First-release baseline](docs/en/first-release.md) |
| SDD规格、任务、验收 | [规格](docs/sdd/001-control-recovery-compatibility/spec.md) | [Specification](docs/sdd/001-control-recovery-compatibility/spec.en.md) |
| UI/Linux/Docker/Helm/Kubernetes | [控制台与部署](docs/zh-CN/console-and-deployment.md) | [Console/deployment](docs/en/console-and-deployment.md) |
| 同步、映射、索引与恢复 | [同步手册](docs/zh-CN/pipeline-v3.md) | [Pipelines](docs/en/pipeline-v3.md) |
| 多节点与持久命令 | [全局控制](docs/zh-CN/global-control.md) | [Global control](docs/en/global-control.md) |
| 事务证据与提交确认 | [审阅](docs/zh-CN/failure-review.md)、[确认](docs/zh-CN/commit-acknowledgement.md) | [Review](docs/en/failure-review.md),[confirmation](docs/en/commit-acknowledgement.md) |
| 完整事务跳过与缺口 | [跳过](docs/zh-CN/transaction-skip.md) | [Skipping](docs/en/transaction-skip.md) |
| 共同点缺口核验 | [核验与FAQ](docs/zh-CN/gap-verification.md) | [Verification and FAQ](docs/en/gap-verification.md) |
| 失败事务补偿 | [补偿操作](docs/zh-CN/transaction-compensation.md) | [Compensation](docs/en/transaction-compensation.md) |
| 手工补数据 | [录入与导入](docs/zh-CN/manual-correction.md) | [Manual correction](docs/en/manual-correction.md) |
| 对账与管理员修复 | [校验](docs/zh-CN/managed-validation.md)、[修复](docs/zh-CN/managed-repair.md) | [Validation](docs/en/managed-validation.md),[repair](docs/en/managed-repair.md) |
| 目标产品与能力边界 | [平台方案](docs/zh-CN/platform-design.md) | [Platform design](docs/en/platform-design.md) |
| 当前验证范围 | [单一验证基线](docs/verification/README.md) | [Verification baseline](docs/verification/README.md) |

目标范围包含MySQL/PostgreSQL、openGauss/分布式GaussDB、达梦/金仓/OceanBase/GBase、ClickHouse/Doris/Hive及Kafka/Pulsar/RabbitMQ/RocketMQ/ActiveMQ/Redis Streams/NATS JetStream。**实现和实测范围按产品档案区分；不能把产品名称、JDBC连通或同协议当成完整支持。** PostgreSQL/Oracle源、缺口验证关闭、失败事务补偿、完整集群/JDK/TLS/性能矩阵仍在实现。

## 开发迭代

应用层修改前可先安装当前SDK，随后只构建/测试应用模块：

```sh
mvn -s build/settings.xml -pl '!sync-app' -DskipTests install
mvn -s build/settings.xml -f sync-app/pom.xml test
```

SDK源码或依赖变化后重新安装SDK。测试临时数据库使用`tests/`中的夹具，不常驻开发预览。V2/V3双执行链和旧向导还有交叉引用，将单独合并；目前不宣称已全部移除。

Rivomesh尚未获得Apache项目身份；贡献、许可证/NOTICE/SBOM和完整发布门禁仍待完成。
