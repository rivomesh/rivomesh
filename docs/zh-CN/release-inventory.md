# 发布包清单与 Apache 准备

[English](../en/release-inventory.md)

构建候选包后，在项目根目录生成 CycloneDX Maven SBOM，并将它与最终的 shaded JAR 核对。[CycloneDX Maven 插件](https://cyclonedx.github.io/cyclonedx-maven-plugin/)

```sh
mvn -B -s build/settings.xml \
  org.cyclonedx:cyclonedx-maven-plugin:2.9.1:makeAggregateBom \
  -DoutputFormat=json -DoutputName=rivomesh-cyclonedx \
  -DincludeTestScope=false -Dcyclonedx.skipAttach=true
python3 tests/release-inventory.py \
  --artifact sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar \
  --maven-bom target/rivomesh-cyclonedx.json \
  --output target/release-inventory.json
```

清单记录两个文件的 SHA-256、JAR 内嵌 Maven 坐标，以及识别出的许可文件、原生库和嵌套 JAR 摘要。`requiredComponentReviews` 列出包内第三方坐标、JDBC 标记和 Maven BOM 中声明的许可名称。遇到空列表时应查验上游发布包。核对 BOM 遗漏项，以及经重定位但未保留 Maven 元数据的依赖。内嵌 Hive SDK 的审核要登记嵌套 JAR 的摘要和其内部依赖。Oracle 源预检使用部署方提供的驱动。

Temurin 21 候选包 `cf103fa235ea3fca9b67d0100b3328b0e219426a775a1c337258b8e4cf25bf40` 有 109 个内嵌坐标，其中 6 个未列入含 175 个组件的 Maven BOM。清单识别出 111 个原生二进制文件及嵌套 JAR、20 个许可证和通知文件。该包的 `META-INF/LICENSE` 和 `META-INF/NOTICE` 来自单个依赖。当前源码在 `META-INF/rivomesh-licenses/` 保留 GaussDB 与 GBase8c 的上游许可原文。识别到对应驱动标记时，清单会核对内嵌许可的字节摘要。

## ASF 分发审查

开发包内嵌 MySQL Connector/J 8.4.0 社区版，适用 GPLv2 和 Universal FOSS Exception。ASF 当前的 Category X 政策覆盖 GPL 及未获明确允许的例外。发现包内驱动时检查返回 `BUNDLED_MYSQL_CONNECTOR_J`。Apache Flink CDC 采用部署方单独安装驱动的方案。Rivomesh 的外置 JDBC 候选包采用这一打包方式，集成方式和其余分发内容仍需 ASF 审查。[Connector/J 8.4.0 许可证](https://github.com/mysql/mysql-connector-j/blob/8.4.0/LICENSE) · [ASF 第三方许可证政策](https://www.apache.org/legal/resolved.html#category-x) · [Flink CDC MySQL 文档](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/mysql-cdc/)

### 外置 MySQL JDBC 驱动的候选包

构建独立命名的候选 JAR，并结合 Maven 聚合 BOM 检查包内内容。

```sh
mvn -B -s build/settings.xml -Pexternal-mysql-jdbc -pl sync-app -am -DskipTests package
candidate=sync-app/target/rivomesh-app-0.1.0-SNAPSHOT-external-mysql-jdbc.jar
python3 tests/release-inventory.py \
  --artifact "$candidate" \
  --maven-bom target/rivomesh-cyclonedx.json \
  --output target/external-mysql-inventory.json \
  --expect-external-mysql
```

该构建配置从候选包中排除 Connector/J，包括 JDBC 服务声明和随该依赖打包的通知。Maven BOM 仍记录构建依赖。结合 `mavenComponentsWithoutEmbeddedCoordinates` 和实际二进制清单，核对构建依赖与分发内容。`--expect-external-mysql` 检查候选包及内嵌 JAR。binlog 读取组件独立于 JDBC 驱动保留在包内。

手动部署时，由部署方按适用许可单独获取受支持的 Connector/J JAR。源库、目标库或控制库使用 MySQL 协议 JDBC 时，由部署方提供该文件。通过显式 classpath 和主类启动。

```sh
mysql_driver=/opt/rivomesh-drivers/mysql-connector-j-8.4.0.jar
java -cp "$candidate:$mysql_driver" io.github.rivomesh.Main serve /etc/rivomesh/platform.yml
```

执行节点将 `serve` 换成 `agent`，其 Worker 继承同一 JDBC classpath。systemd 服务使用此候选包时，用绝对路径写入上述 `ExecStart`。仓库的标准 Docker 构建继续打包开发版 JAR。

用同一个 classpath 执行 `check` 并传入 MySQL 流水线配置。`JDBC_DRIVER_UNAVAILABLE` 表示 JVM 无法加载该驱动类。检查部署方驱动文件的路径及进程的读取权限。

用一次性 MySQL 8.4 环境验收 Controller、Agent 和 Worker 的外置驱动路径。该用例使用 MySQL 控制库、PostgreSQL 和 MySQL 两个目标，并执行滚动重启。

```sh
python3 tests/global-control-pipeline.py --artifact "$candidate" \
  --external-mysql-driver "$mysql_driver" \
  --output target/external-mysql-native \
  --separate-execution --rolling-restart --control-product MYSQL
```

### 外置厂商 JDBC 驱动的候选包

单独使用 `external-vendor-jdbc` 构建。应用 JAR 保留 binlog 读取组件、PostgreSQL 和 H2 驱动，排除 MySQL Connector/J、GaussDB 和 GBase8c 驱动模块，以及金仓和达梦 JDBC。独立的 `external-mysql-jdbc` 配置继续内嵌四个目标驱动。Maven 构建 BOM 仍记录构建和测试依赖，Maven 多模块构建也可能单独生成驱动 JAR。[Maven Shade 构件筛选](https://maven.apache.org/plugins/maven-shade-plugin/examples/includes-excludes.html)

```sh
mvn -B -s build/settings.xml -Pexternal-vendor-jdbc -pl sync-app -am -DskipTests package
candidate=sync-app/target/rivomesh-app-0.1.0-SNAPSHOT-external-vendor-jdbc.jar
python3 tests/verify-artifact.py "$candidate" --jdbc-profile external-vendor-jdbc
python3 tests/release-inventory.py --artifact "$candidate" \
  --maven-bom target/rivomesh-cyclonedx.json \
  --output target/external-vendor-inventory.json --expect-external-vendor-jdbc
```

清单检查候选包及嵌套 JAR 中已知的类、JDBC 服务声明、Maven 元数据和可识别的上游通知。该检查覆盖候选应用二进制包。源码归档和部署镜像分别记录其实际内容及审核证据。

部署方仅提供任务所需的驱动，并自行管理文件路径。GaussDB 或 openGauss 使用项目单独构建的已重定位 GaussDB 驱动模块，允许同一 JVM 加载内嵌的 PostgreSQL 驱动。GBase8c 可使用已核对的原厂 `gbase8c-jdbc-3.0.0B02.jar`。当前验证的金仓和达梦驱动版本为 `kingbase8-9.0.1.jar`、`DmJdbcDriver18-8.1.3.140.jar`。

```sh
driver_dir=/opt/rivomesh-drivers
mysql_driver="$driver_dir/mysql-connector-j-8.4.0.jar"
gauss_driver="$driver_dir/rivomesh-gaussdb-driver-0.1.0-SNAPSHOT.jar"
gbase_driver="$driver_dir/gbase8c-jdbc-3.0.0B02.jar"
kingbase_driver="$driver_dir/kingbase8-9.0.1.jar"
dameng_driver="$driver_dir/DmJdbcDriver18-8.1.3.140.jar"
classpath="$candidate:$mysql_driver:$gauss_driver:$gbase_driver:$kingbase_driver:$dameng_driver"
java -cp "$classpath" io.github.rivomesh.Main serve /etc/rivomesh/platform.yml
```

执行 Agent 或独立流水线使用相同启动 classpath，Agent 将其传给 Worker。离线验收使用同一个应用 JAR，分别验证缺少厂商驱动及提供五个驱动文件时的类来源、URL 匹配、驱动缺失错误和达梦错误分类。

```sh
java -cp "$candidate" tests/roles/ExternalVendorJdbcAcceptance.java
java -cp "$classpath" tests/roles/ExternalVendorJdbcAcceptance.java \
  "$mysql_driver" "$gauss_driver" "$gbase_driver" "$kingbase_driver" "$dameng_driver"
```

达梦实库档案使用同一种候选包，部署方的 classpath 只添加外置 Connector/J 和达梦驱动。实测涵盖连接认证、驱动加载来源、缺驱动拒绝、达梦原生错误处理和数据库重启后重连。[达梦实库复现](dm-v3.md#java-21-外置达梦驱动实库验收)

### 分发审核记录

人工审核完成后，运行清单命令并附加 `--asf-review target/candidate/distribution-review.json`。审核记录包含以下字段。

| 字段 | 审核证据 |
| --- | --- |
| `formatVersion` | 整数 `1` |
| `artifactSha256`、`mavenBomSha256` | 本次候选包清单中的对应 SHA-256 值 |
| `sourceRightsEvidence` | 源码来源、权利和贡献审查记录的引用 |
| `packagedContentsEvidence` | 实际分发内容的审查记录，包含嵌套或重定位依赖、原生库、通知和其他素材 |
| `packageLicenseFiles` | 对象，键为 `META-INF/LICENSE` 和 `META-INF/NOTICE`，值为 JAR 内已审核文件的 SHA-256 |
| `componentReviews` | 对象，键与 `requiredComponentReviews` 一致。每项含适用许可条款字符串 `license` 及证据引用字符串 `evidence` |

审查记录缺项、摘要不匹配、分发包许可文件缺失、厂商通知缺失或修改，或包内仍含 Connector/J 时，输出 `ASF_DISTRIBUTION_REVIEW_BLOCKED` 并返回退出码 1。`ASF_DISTRIBUTION_REVIEW_RECORDED` 表示记录与实际 JAR 匹配且扫描未发现已知阻断项。适用许可和完整内容由审核人员核对。每个对外分发的源码包、二进制包和镜像都分别审查。[ASF 分发包许可要求](https://www.apache.org/legal/release-policy.html#licensing)

项目级 LICENSE 和 NOTICE 取决于源码权利审查。准备这两个文件前，先记录代码、素材和厂商组件的来源及贡献权。新孵化项目的初始代码按 Incubator 的 podling IP 指南办理。[Podling IP 审查指南](https://incubator.apache.org/guides/ip_clearance.html)

准备阶段使用项目名 **Rivomesh** 和命名空间 `io.github.rivomesh`。后续提案、名称和品牌审查以 ASF 指南为准。[提案指南](https://incubator.apache.org/guides/proposal.html) · [名称检索](https://incubator.apache.org/guides/names.html) · [品牌指南](https://incubator.apache.org/guides/branding.html)

## 候选包功能验收

冻结一份候选 JAR，并将它用于 [Chrome](../../tests/ui/browser-smoke.mjs)、[原生 CDC](../../tests/global-control-pipeline.py) 和 [Kubernetes](../../tests/kubernetes-csi-runtime.py) 实测。运行 Kubernetes 用例时传入 `--artifact-sha256`，核对两种部署中实际运行的 JAR。将原候选包和各项生成的清单传给门禁。

```sh
python3 tests/release-evidence-gate.py \
  --artifact target/candidate/rivomesh.jar \
  --browser target/candidate/browser/manifest.json \
  --kubernetes target/candidate/kubernetes/manifest.json \
  --native-cdc target/candidate/native-cdc/manifest.json
```

门禁同时检查 SDD 状态与证据、文档链接、中英文 API 和连接器覆盖、发布包资源，并以测试凭据解析随包提供的 YAML 示例。单独检查这些内容时使用 `--examples-only`。完整命令要求真实浏览器、独立 Agent 原生恢复，以及两种 Kubernetes 实机部署均使用同一候选包。其他适配器或部署档案的原生结果分别登记在[验收基线](../verification/README.md)。分发审查单独登记并使用同一候选包摘要。
