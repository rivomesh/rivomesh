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

清单保存两个文件的 SHA-256、JAR 内嵌 Maven 坐标、许可证和通知文件摘要，以及已识别的厂商 JDBC 标记。审核时检查仅出现在 JAR 内的坐标和缺少许可元数据的依赖。GBase8c 构建下载的 ZIP/JAR、重定位进入包内的 GaussDB 驱动需要依据实际分发内容审查。Oracle 源预检使用部署方提供的驱动。

本次生成的 Maven 清单有 175 个组件，JAR 内有 109 个坐标，其中 6 个坐标未被 Maven 聚合清单列出，另有 9 个 Maven 组件未声明许可证，识别出 2 个厂商标记。发布审核继续核对源码权利归属、每种分发包的实际内容和对应的 LICENSE、NOTICE。[ASF 第三方许可证政策](https://www.apache.org/legal/resolved)

准备阶段使用项目名 **Rivomesh** 和命名空间 `io.github.rivomesh`。后续提案、名称和品牌审查以 ASF 指南为准。[提案指南](https://incubator.apache.org/guides/proposal.html) · [名称检索](https://incubator.apache.org/guides/names.html) · [品牌指南](https://incubator.apache.org/guides/branding.html)
