# 首版验证基线 / First-release verification baseline

仅保留当前基线索引；开发过程中的重复源码/脚本副本、失败尝试和逐次快照已从工作树移除。Git提交历史保留追溯。操作文档见[中文实施状态](../zh-CN/implementation-status.md)和[English](../en/implementation-status.md)。

The compact [baseline](baseline.json) preserves the exact bounded acceptance provenance. Earlier results are not relabelled as fresh validation of cleanup changes. Current validation status is tracked in baseline.json. No production compatibility or full product matrix is implied.

| SDD | 已验收范围 / Accepted scope | 原验收产物 SHA-256 |
|---|---|---|
| CP-01 | Bounded profile | `e42cd685bcc9fb38a1e0e82819230ac58869a54f40c4417d39cd596337b91d11` |
| CP-07 | Java8 real CDC;fixed Agent;LOCAL/H2 journal;MySQL8.4 source;PostgreSQL17/MySQL8.4 targets;PostgreSQL17/MySQL8.4 control | `aa17cf5b6d9c21e302af818c5f48ff7c46bbb32d3a532ab1fa0491abe43403bf` |
| SE-01 | Bounded profile | `95127453ff4e6b9a6b0f16d80ea1a16c2c5861b4a260226e875bab6bc72c60a3` |
| SE-02 | Bounded profile | `95127453ff4e6b9a6b0f16d80ea1a16c2c5861b4a260226e875bab6bc72c60a3` |
| SE-03 | Bounded profile | `95127453ff4e6b9a6b0f16d80ea1a16c2c5861b4a260226e875bab6bc72c60a3` |
| SE-04 | Bounded profile | `a1ec34fc060b4078cdaa10f357d5933f4df60f5b9616519433c851ddad69fa4a` |
| SE-05 | Bounded profile | `95127453ff4e6b9a6b0f16d80ea1a16c2c5861b4a260226e875bab6bc72c60a3` |
| RC-02 | Bounded profile | `bcaae9309898a33ecfe919d69bfa6c43227647524498f8269682df44942cbcbe` |
| RC-03 | Java8;MySQL8.4 two-database source;original fixed Agent;LOCAL/H2 journal;PostgreSQL17 business skip target;PG17/MySQL8.4 control stores;native PG/MySQL contracts;both pauses retained | `656245c6ed5d8c80c3017ed319e3802d8974f03e69f432a2ec64f0566660ac74` |
| RC-05 | Java8;MySQL8.4 source;original fixed Agent;LOCAL/H2 journal;PostgreSQL17/MySQL8.4 control stores;public real-CDC PostgreSQL confirmation;native PG/MySQL transaction contracts;whole task paused | `9f97b4d98c103e895c9d1262cd19dcd0946b21b6fc0f72fff2267e59c727de00` |

最新清理前的功能档案：Java8、MySQL8.4双库源、原Agent/LOCAL日志、PostgreSQL业务目标，PG/MySQL控制库各26项真实同步检查，PG/MySQL原生合同各34项、96 DOM及发布包51项专项。34项中含现已删除的开发版兼容探针；清理后的原生恢复回归改为32项，不能将两者相加。首次全量回归的租约过期与51项定向重跑情况在baseline.json中如实保留。

The cleaned baseline removes automatic control-mode migration and historical binary tests. Namespace mode remains fixed and mismatches refuse;lease fencing,data integrity and current recovery tests remain. V2/V3 execution consolidation is a separate pending cleanup batch.

当前公开缺口核验构建包：`3404041b03ed0a407777366056b509778f4c93e12dea0a4f14572b01670a63ce`。

- 87项扩展合同通过，最终改动后23项专项通过；重叠项不相加。
- PostgreSQL17/MySQL8.4日志及目标各54项发布包优先合同通过。
- 102项DOM重新运行通过，覆盖计划范围、摘要审批、取消、响应丢失重试、权限和退出登录。
- PG17/MySQL8.4控制库各27项真实CDC检查通过：Java8、MySQL8.4双库源、原固定Agent/LOCAL-H2日志、PostgreSQL17业务核验目标、MySQL8.4健康订阅；两管理入口审阅和重新扫描两张表，原子记录两条缺口。测试资源均清理。

RC-08首批SQL档案已验收；其他目标由RC-10/LG-03/LG-04跟踪，更广源/JDK/K8s矩阵仍未验收。[中文核验指南](../zh-CN/gap-verification.md) / [English](../en/gap-verification.md)。

The same candidate passes bounded native and public real-CDC gap verification. Historical results above are provenance rather than fresh certification. No complete connector/source/JDK/K8s recertification is claimed.

RC-08水位收尾候选：`abee42cfb26ee3623c419f9d43d9dbe6a429508a8dd01da33da38338dcdcf372`。22项定向检查、103项DOM及PG/MySQL各58项原生合同通过。当前包复验只读水位、API/界面与原生恢复合同；未改变的审批/事务写入链路复用上面的3404041真实CDC27项/控制产品结果，不将旧结果改称本包重跑。复用理由及确切版本记录在baseline.json。

Coverage includes explicit SKIP;the contiguous SQL applied prefix stops before the first skip and is unchanged by historical verification. Broker/materializer acknowledgments have distinct semantics and never claim downstream-consumer completion. All incremental sequence fields used by this view are exact decimal strings.

RC-06 补偿候选包 `d1b1595b8efbe4a6e2258f899836fa53cf33b2c11d31f621c5fc4ca9955b67a3` 通过 32 项 Java 定向检查、104 项 DOM、PostgreSQL 17 和 MySQL 8.4 各 4 项原生检查，以及 18 项真实 CDC 检查。真实链路使用 MySQL 8.4 双库源、PostgreSQL 控制库、原 Agent 与 LOCAL/H2 日志。跨入口补偿 PostgreSQL 目标的主键冲突，恢复了三张表的四个受影响标识，再由正常 CDC 重放。MySQL 是健康对照订阅，MySQL 目标的补偿结论来自原生检查。

RC-07 手工修正候选包 `33a1d4dca6a381664855c0ae96100fa9da004a61070155b0ed7a646c6a2c4f25` 通过 39 项 Java 定向检查、107 项 DOM、PostgreSQL 17 和 MySQL 8.4 各 4 项原生检查及 18 项真实 CDC 检查。真实链路使用两个管理入口、原 Agent、LOCAL/H2 日志和 PostgreSQL 控制库。用原始 JSON 批次补回目标缺失行，按 ROW_ID 修改无主键行并删除多余行。管理员审批后显式恢复并重放原事务。任务记录只带范围和摘要。

### SO-04 · MySQL 8.2.0

Java 8 的 V3 反应堆测试通过 56 项，包括多库全量与增量、无主键、JSON、生成列、LOCAL 和外部日志及原事务恢复。冻结包 `cce6a099cd6edc00fa0713689aa20a80a1b67a6443dfd756fa23186a340b6829` 另通过两项原生 HA 检查。GTID 主从提升后从已有断点继续，单主组复制在全量中途选主后只重取未完成快照。验证源为 ARM64 的 `mysql:8.2.0`，启用 GTID、ROW binlog 和 FULL 行镜像。

The Java 8 reactor passed 56 V3 tests. The frozen package passed one native async promotion scenario and one native single-primary group election scenario. The HA profiles used PostgreSQL and openGauss targets. Reproduce all three source profiles with `tests/matrix.sh --v3 8.2.0`. JV-02 tracks the other JDK combinations.

### RC-09 · 跨节点恢复作业 / Distributed recovery jobs

候选包 `c5a11f6ec7a52f94bd9b305708a1d0c08c8b2ce6387ced762ec8ecfac05bb9a3` 通过 42 项 Java 检查、108 项界面检查、4 项 PostgreSQL 原生补偿、4 项 MySQL 原生修正和 14 项真实 CDC 检查。两个控制器以原请求 ID 取消排队和运行中的作业。补偿及手工修正均在等待目标 SQL 锁时强杀 Agent，验证无部分写入，以原请求继续。目标提交后、Agent 写入本地结果前再强杀 Agent，并关闭原管理入口。重启后由仍在线的入口读取同一原生回执，目标保持暂停，日志 ACK 不动。

Real CDC used MySQL 8.4 source and healthy peer, PostgreSQL 17 recovery destination and control storage, Java 8, and the original Agent with a LOCAL/H2 journal. PostgreSQL compensation and MySQL correction separately passed four native checks each. The process fault fixture is test-only.

### JV-01 · 五 JDK 真实进程 / Five-JDK runtime

候选包 `c5a11f6ec7a52f94bd9b305708a1d0c08c8b2ce6387ced762ec8ecfac05bb9a3` 在 ARM64 上实际启动两台 Controller、一台 Agent 和一台 Worker。五个 JVM 版本分别为 1.8.0_512, 11.0.33, 17.0.21, 21.0.13, 25.0.5。每个版本通过 5 项检查，包含 MySQL 8.4 源到 PostgreSQL 17 / MySQL 8.4 的全量与增量、进程版本和夹具清理。

All four running processes used the expected JVM on each JDK. The candidate passes the class-file level 52 and packaged driver checks. JV-02 and JV-03 track the broader database, connector and TLS combinations.

### RC-01 · 源与目标故障阶段 / Pipeline fault phases

候选包 `f7b7dfb7c88785a8622077edbed3ec2f6d800f8c9a035ea1a4e73217322f90a3` 通过 48 项 Java 定向检查及 109 项界面检查。真实 CDC 的源断线与 DDL 档案 6 项、解码档案 5 项、目标准备/快照/应用档案 19 项分别通过。MySQL 原生事务档案 4 项通过，并启动了含阶段记录表的 MySQL 日志库。

Source connection loss is observed from both management entries and CDC resumes after the source returns. DDL and partial row images retain the original GTID and error generation across Agent restart. Destination preparation, snapshot and apply failures keep their own phases. Focused tests distinguish normalization from a journal ACK failure after a native destination commit.

### SO-03 · MySQL 8.0.46

Java 8 候选包 `f7b7dfb7c88785a8622077edbed3ec2f6d800f8c9a035ea1a4e73217322f90a3` 通过 5 项 V3 单机实库测试、GTID 主从提升和单主组复制选主各 1 项，以及 14 项跨入口恢复检查。恢复测试覆盖 PostgreSQL 目标与 MySQL 健康对照、全量转增量、前镜像补偿、类型化修正、取消及强杀后原请求恢复。

The source was MySQL 8.0.46 with GTID, ROW and FULL enabled. Run the three topology profiles with `tests/matrix.sh --v3 8.0.46`. Pass `--source-image` to the global CDC recovery runner to select the same MySQL patch version.

### SO-01 · 源配置与适配入口 / Source configuration

候选包 `4722408f57a63e883147b979c3c706dd20d1ff6cf9aefcc6cba973fbb8c96ee2` 通过 Java 43 项、界面 109 项、MySQL 原生 2 项和真实 CDC 5 项检查。原生检查确认可按主次版本或精确补丁验证目标 MySQL 实例。双入口用 `source.type: MYSQL` 和 `source.expectedVersion: "8.4"` 创建任务，并完成全量与增量。

Capture, preflight and common-cut validation share the source adapter entry point. PostgreSQL and Oracle source execution remain tracked in SO-06 and SO-07.

### SO-05 · MySQL 8.4.13

候选包 `d7fc2e8bad6f4ddcb4a7a1e4eda248214df6c88976d0c02449e37e4651b543b0` 通过 41 项 Java 检查、109 项界面检查、6 项 V3 单机实库检查和两项真实选主检查。独立日志档案验证了非 FULL、匿名模式、部分 JSON、压缩事务和带标签历史的拒绝。部分 JSON 与压缩事务也通过了采集中的原生事件拒绝测试。该包在五个 JDK 复跑 Controller、Agent 和 Worker 启动、全量与增量。

Native MySQL version was 8.4.13. Reproduce the profile with `tests/mysql84-profile-run.sh` and `tests/matrix.sh --v3 8.4`.

## SO-06 PostgreSQL 17 source pilot

Candidate `ff8208b7758f335a103202e21e1370c44505fb488a926a7502b032cb071ccb66` passed 42 Java checks, 110 console checks and seven native cases on PostgreSQL 17.12. PostgreSQL uses pgJDBC 42.7.14 with automatic flush advancement disabled. Verified snapshot and CDC across two schemas, JSONB, keyless rows, idle keepalive, SQL recovery, same-slot LSN restart and lost-slot refusal. Promotion, generated columns and common-cut verification remain open. See the [English guide](../en/postgresql-source.md) and [Chinese guide](../zh-CN/postgresql-source.md).

## SO-02 MySQL 5.7 V3

Artifact `d7fc2e8bad6f4ddcb4a7a1e4eda248214df6c88976d0c02449e37e4651b543b0` passed six standalone tests, one asynchronous GTID promotion test and one single-primary MGR test on Java 8. The asynchronous fixture reproduces replica SQL error 1032, recovers and promotes, then removes a binlog containing an uncaptured transaction. Rivomesh refuses restart with `SOURCE_LOG_GAP`, preserves the original journal position and sequence, and leaves the missing row unapplied. The container is MySQL 5.7.44 linux/amd64. Reproduce with `tests/matrix.sh --v3 5.7.44`.

## RC-04 capture review

Candidate `ff8208b7758f335a103202e21e1370c44505fb488a926a7502b032cb071ccb66` passed six native MySQL 8.4 checks with two controllers and an independent Agent. Both entries returned the same read-only review after a decode fault and Agent restart, with every affected subscription listed and the original error generation and GTID retained. PostgreSQL missing-slot review kept the original LSN and left the slot absent. Source-range proof and approved exclusion remain open. See the [operator guide](../en/capture-recovery.md).

The same candidate also passed the MySQL 5.7.44 PostgreSQL-only asynchronous promotion fixture. After a real uncollected binlog was purged, the review returned `SOURCE_LOG_GAP`, the affected subscription and the original captured sequence. Reproduce with `SYNC_MYSQL57=1 SYNC_MYSQL57_IMAGE=mysql:5.7.44 SYNC_V3_HA_POSTGRES_ONLY=1 tests/v3-ha-run.sh`.

## LG-06 live Kubernetes RWO CSI

The image built from JAR `16efe0b21bad57fda1418195484595ac20b8e5579f4763f4a109e474beef44a9` passed four live checks each on Helm and native YAML in a two-node Kubernetes v1.31.0 ARM64 cluster. The checks exercised browser refresh, real PostgreSQL snapshot/CDC, RWO CSI remount across scale 0/1, configuration rollout and Secret rotation. Node-local CSI and single controller are the tested profile. See the [deployment guide](../en/console-and-deployment.md#two-node-live-verification).

The native profile also stopped a separate worker while the PostgreSQL fixture remained available on the control plane. It confirmed that the target stayed at its previous value, recovered the same CSI claim on the original node and delivered queued data under a newer journal lease.

## DO-01 bilingual SDD verifier

Verifier SHA-256 `d2a9fb5e16014825a51fbbd0569e885eeb00d7954ccf57c7724b5bd1fa8aef08`. The check verifies stable IDs, unique tasks and acceptance rows, criteria rendered in both languages, acyclic dependencies, progress totals and archived evidence. The criteria checksum detects a changed acceptance scope.

## SO-07 Oracle SCN preflight

Artifact `4e19cc6dd662de282ebc63b3da1ce6dbea6d918ab230de35c1299d56c8f5d27c` passed four Oracle Free 23.26.4.0.0 ARM64 native checks on Java 8. The probe validates root and PDB authority, checks Flashback on keyed and keyless tables and refuses a mismatched service. The fixture observed the original values and duplicate multiplicity after a later update and delete. CDC integration remains open. See the [preflight guide](../en/oracle-source.md).

## PostgreSQL source runtime matrix

Artifact `4e19cc6dd662de282ebc63b3da1ce6dbea6d918ab230de35c1299d56c8f5d27c` ran seven native PostgreSQL 17.12 source cases on each of Java 8, 11, 17, 21 and 25, a total of 35 successful cases. Runtime versions and image IDs are in the baseline. Run `python3 tests/postgres-source-run.py --jdk <8|11|17|21|25> --artifact <candidate jar> --output target/pg-source-proof` for an individual profile.

The same Oracle 26ai image also passed the native `--logminer` archive check. The source exposes redo/undo SQL, a common commit SCN and XID for keyed update and keyless delete, and excludes the rolled-back insert.

## DO-03 bilingual recovery runbook

Artifact SHA-256 `f69e54391a22d1ff4564deb2f272a06d4850c5ea7dfe75f8f86a32601c68ee23` covers the Chinese and English Markdown in that order. Both versions cover promotion, missing logs, target SQL and uncertain commit, node and journal outage, browser state, skips, compensation, correction, and cutover. The scope and relative links were checked against the current operator guides.

## LG-10 release inventory

JAR `5c33b78a9fec625db629d11c65453d04ea1ea5279c708b92348fd0c6413ec2c8` and CycloneDX BOM `8361ded69144d17b670a3695ecee29bd2e303a1d088a3775e813d69bc84aba44` were compared. The inventory records 175 Maven components, 109 embedded coordinates, six without an aggregate coordinate, nine without declared license metadata, two vendor markers, and nine notice/license file hashes. See the [release inventory guide](../en/release-inventory.md).
