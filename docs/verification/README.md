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

Candidate `ff8208b7758f335a103202e21e1370c44505fb488a926a7502b032cb071ccb66` passed six native MySQL 8.4 checks with two controllers and an independent Agent. Both entries returned the same read-only review after a decode fault and Agent restart, with every affected subscription listed and the original error generation and GTID retained. PostgreSQL missing-slot review kept the original LSN and left the slot absent. A later candidate verified bounded MySQL source-range proof and approved exclusion. See the [operator guide](../en/capture-recovery.md).

The same candidate also passed the MySQL 5.7.44 PostgreSQL-only asynchronous promotion fixture. After a real uncollected binlog was purged, the review returned `SOURCE_LOG_GAP`, the affected subscription and the original captured sequence. Reproduce with `SYNC_MYSQL57=1 SYNC_MYSQL57_IMAGE=mysql:5.7.44 SYNC_V3_HA_POSTGRES_ONLY=1 tests/v3-ha-run.sh`.

## LG-06 live Kubernetes RWO CSI

The image built from JAR `16efe0b21bad57fda1418195484595ac20b8e5579f4763f4a109e474beef44a9` passed four live checks each on Helm and native YAML in a two-node Kubernetes v1.31.0 ARM64 cluster. The checks exercised browser refresh, real PostgreSQL snapshot/CDC, RWO CSI remount across scale 0/1, configuration rollout and Secret rotation. Node-local CSI and single controller are the tested profile. See the [deployment guide](../en/console-and-deployment.md#two-node-live-verification).

The native profile also stopped a separate worker while the PostgreSQL fixture remained available on the control plane. It confirmed that the target stayed at its previous value, recovered the same CSI claim on the original node and delivered queued data under a newer journal lease.

The shared NFS CSI 4.13.5 profile tested Kubernetes v1.31.0 ARM64 with NFS v4.0. Helm and native YAML each passed six checks. Both started JAR `b95d036f0d5f633de1a311581f70ad67c126af0b22b72f26240fa5d6934c6d32`, then replaced it with `9cc5da0987cc78609988d7a75d5c3de9ae0129e334d16e8abdf1ba31c6373365`. Each upgrade retained the claim UID, CSI volume handle and snapshot ID. A new lease resumed SQL delivery after replacement. Scale-down removed the old pod before mounting the same claim on the other node, which delivered an update written during downtime without a manual START. The epochs advanced 2 → 3 on replacement and 3 → 4 on the move. The earlier physical worker outage and stale Worker profiles cover node failure and lease fencing. [Manifest](baseline.json) · [CSI test fixture](../../tests/kubernetes-nfs-fixture.yaml)

## DO-01 bilingual SDD verifier

Verifier SHA-256 `d2a9fb5e16014825a51fbbd0569e885eeb00d7954ccf57c7724b5bd1fa8aef08`. The check verifies stable IDs, unique tasks and acceptance rows, criteria rendered in both languages, acyclic dependencies, progress totals and archived evidence. The criteria checksum detects a changed acceptance scope.

## SO-07 Oracle SCN preflight

Artifact `4e19cc6dd662de282ebc63b3da1ce6dbea6d918ab230de35c1299d56c8f5d27c` passed four Oracle Free 23.26.4.0.0 ARM64 native checks on Java 8. The probe validates root and PDB authority, checks Flashback on keyed and keyless tables and refuses a mismatched service. The fixture observed the original values and duplicate multiplicity after a later update and delete. CDC integration remains open. See the [preflight guide](../en/oracle-source.md).

## PostgreSQL source runtime matrix

Artifact `4e19cc6dd662de282ebc63b3da1ce6dbea6d918ab230de35c1299d56c8f5d27c` ran seven native PostgreSQL 17.12 source cases on each of Java 8, 11, 17, 21 and 25, a total of 35 historical cases. The baseline records their runtime versions and image IDs. The current fixture runs Temurin 21 with `python3 tests/postgres-source-run.py --artifact <candidate jar> --output target/pg-source-proof`.

The same Oracle 26ai image also passed the native `--logminer` archive check. The source exposes redo/undo SQL, a common commit SCN and XID for keyed update and keyless delete, and excludes the rolled-back insert.

## DO-03 bilingual recovery runbook

Artifact SHA-256 `f69e54391a22d1ff4564deb2f272a06d4850c5ea7dfe75f8f86a32601c68ee23` covers the Chinese and English Markdown in that order. Both versions cover promotion, missing logs, target SQL and uncertain commit, node and journal outage, browser state, skips, compensation, correction, and cutover. The scope and relative links were checked against the current operator guides.

## LG-10 release inventory

JAR `5c33b78a9fec625db629d11c65453d04ea1ea5279c708b92348fd0c6413ec2c8` and CycloneDX BOM `8361ded69144d17b670a3695ecee29bd2e303a1d088a3775e813d69bc84aba44` were compared. The inventory records 175 Maven components, 109 embedded coordinates, six without an aggregate coordinate, nine without declared license metadata, two vendor markers, and nine notice/license file hashes. See the [release inventory guide](../en/release-inventory.md).

## Oracle archive coverage

Artifact `5c33b78a9fec625db629d11c65453d04ea1ea5279c708b92348fd0c6413ec2c8` passed seven Oracle Free 23.26.4.0.0 Java 8 checks. The probe returned a contiguous catalog interval after archive rotation and refused a wider interval containing unarchived changes. The separate LogMiner test confirmed committed transaction contents from actual redo.

## Oracle typed archived changes

Oracle Free 23.26.4.0.0 ARM64 on Java 8 passed eight native checks on artifact `a59076b9c8d364a47a2848a59179ca70b05ceae8b52e655427f9d15ae3e0404e`. The scoped JDBC account opened the archived files with LogMiner. The same Oracle update and keyless delete decoded into one typed transaction with identical content hashes on two reads. A rolled-back insert stayed outside the committed set and the unarchived suffix was refused. See the [native fixture](../../tests/oracle-source-probe.py) and [manifest](baseline.json).

## Capture lease through two controllers

Artifact `5c33b78a9fec625db629d11c65453d04ea1ea5279c708b92348fd0c6413ec2c8` passed 18 native PostgreSQL control-store and MySQL 8.4.13 source checks with two controllers and a distinct Agent/Worker. Both entries displayed the same journal lease independently of Worker process liveness. The disconnected owner produced unknown lease state. A durable STOP accepted while the owner was offline prevented Worker restoration and left the lease inactive. The console passed 111 DOM checks. [Native manifest](baseline.json)

## Large-payload journal performance

Artifact `c2459566253bee13e7435565d05bac136118703b13314cf6ebffdc190dead410` passed 43 focused Java tests. On an ARM64 Apple M1 Pro Docker Desktop host, both LOCAL and EXTERNAL reached source/target hash equality on 100,000 snapshot rows and 120,000 changes. Each fixture used one CPU per container and a 256 MiB Worker heap, and both cleaned up successfully. During diagnosis, native PostgreSQL EXPLAIN showed a sequential scan for a NUMERIC parameter against the BIGINT key. Binding the same key as BIGINT used the primary-key index. [Timing and environment](../en/performance.md) · [Raw reports](baseline.json)

## Bilingual connector and API compatibility reference

English and Chinese reference files concatenated in that order have SHA-256 `c8408cd8f98974b6aaf4e3de66b2a5312d686daf1c99fe03d6cd16af8ef4ec3a`. Both languages index the configured source, destination and five-JDK profiles, typed positions, access rules, request and plan identities and upgrade boundaries. The documentation check resolved relative links in all 124 Markdown files and verified the shared connector and endpoint inventory. [English reference](../en/compatibility.md) · [中文](../zh-CN/compatibility.md)

## PostgreSQL 18 stored generated columns

Artifact `72357d90c1d87d99fbdf767927a0ca122286df7c7f3defc60f9abd21622b898c` passed 36 focused Java tests, eight native checks on each of Java 8, 11, 17, 21 and 25 from PostgreSQL 18.7 to 17.12, plus seven native PostgreSQL 17.12 regression checks on Java 8. With the PostgreSQL 18 publication enabling stored generated columns, keyed and keyless source updates changed the corresponding ordinary destination columns. A preflight without that publication coverage refused the configuration. [Reproduction](../en/postgresql-source.md) · [Manifest](baseline.json)

## Bilingual deployment and maintenance reference

Combined guide SHA-256 `32cad011e732673d4e33857945269cee8b81933f082cb3a15584a8d53df6797e`. Both languages document Linux, Docker, Helm and native Kubernetes examples for standalone and separated execution, plus node enrollment, an operator-driven drain, stores, secrets, sessions and coordinated backup and upgrade. Both Compose configurations passed render validation. Helm lint passed, and the standalone, Controller A, Controller B and Agent A renders matched their four native files byte for byte. The live Kubernetes evidence above covers separately scoped CSI and interruption tests. [English guide](../en/execution-lifecycle.md) · [中文](../zh-CN/execution-lifecycle.md)

## Native PostgreSQL 17 synchronized-slot promotion

Artifact `49ed903ee7a7d75f57908c05a96990fe790af785a1015550cf1f8501cf5d717b` passed 18 focused Java checks and two PostgreSQL 17.12 Java 8 profiles with 8 and 7 native checks. The original primary was killed, the physical standby promoted after validating its synchronized slot, and the writer endpoint routed to the standby. Delivery resumed automatically from the prior WAL position with the same snapshot IDs. The two profiles then refused a lost slot and a route back to the earlier primary, retaining the latest journal position. [Operations and reproduction](../en/postgresql-source.md) · [Manifests](baseline.json)

## Worker diagnostic events and Chrome smoke

Artifact `3355bdc100775f7ce813a61309a1306ab6a03cd4a7078d6a68ba1784065777d9` passed 24 Java tests, 112 DOM tests and seven native MySQL/PostgreSQL checks across two controllers and a separate Agent. After a real incomplete-binlog-image fault and Agent restart, both entries returned the same page for an anchored cursor with only typed events, timestamps and phases. VIEWER and invalid-page requests were rejected. Chrome 157 exercised keyboard login, cookie reload, the accessible diagnostic dialog, a 390px viewport and actual sign-out. Wide, mobile and diagnostic screenshots were reviewed. [Manifests](baseline.json)

## OceanBase MySQL tenant recovery

Artifact `58cc370518c0f92271f39fafab02b8c508beb59bf736bdb797eb3a8add050ff4` passed 501 Java tests with 32 optional cases skipped. The actual OceanBase CE 4.3.5.6 ARM64 fixture passed native transactional/checkpoint rollback, epoch and reopen checks with Java 8. In mixed MySQL 8.4.13 to PostgreSQL 17.12 and OceanBase delivery, it passed snapshot and CDC including generated and keyless rows, target restart and catch-up, Worker SIGKILL, independent SQL constraint recovery, persistent failure generation and missing-target-table isolation. The fixture cleaned up its own containers and volumes. [Reproduction](../en/oceanbase-v3.md) · [Manifest](baseline.json)

## Oracle typed commit ordering

Artifact `e6186b17a8b357db1b1a577aa7f1372b1f19241509f17c2a5b8ceafcd6f33f52` passed nine Oracle Free 26ai Java 8 checks. LogMiner changes are grouped into complete source transactions with repeatable per-transaction event IDs. The scoped probe retained the original authority and emitted typed SCN/XID commit coordinates. After rotating the archive, the second transaction appeared as a distinct later coordinate. [Manifest](baseline.json)

## Oracle archived CDC and restart

Artifact `9b555ae4a107fb34cba96c3f4b392f136ed290b6a2aac2e3b0c3fa3cc7ace029` passed 13 native Oracle 26ai checks and emitted a durable SCN to a PostgreSQL 17.12 destination. The root and PDB were independently authenticated. The Worker held the archive floor for a transaction open before Flashback, delivered it after commit and rotation, reused both snapshots after process SIGKILL, and refused an unsupported DDL before advancing the saved SCN. The profile used Java 8 with an external Oracle driver, one archive thread and LOCAL storage. [Guide and reproduction](../en/oracle-source.md) · [Manifest](baseline.json)

### Exact-candidate release evidence and stale Worker acceptance

Java 8 candidate `54485ac2862a4d12efec26c8e319f4c811b023c33774be995fd3b5fbe0d6cfab` passed [the release evidence gate](../../tests/release-evidence-gate.py). The run parsed 25 YAML examples, checked the bilingual SDD and API reference, verified five real Chrome checks and seven native MySQL checks with separate management and execution processes. Kubernetes v1.31.0 ran eight Helm and checked-in YAML checks on two nodes, including CSI remount, configuration rollout and Secret rotation. Both workloads verified the SHA-256 of the running JAR. A real Chrome manifest from an earlier candidate was rejected. Results are archived as `releaseEvidenceGate` in [baseline.json](baseline.json).

On the same artifact, the disposable two-node cluster passed three additional checks using one external PostgreSQL journal. Both Workers used the same pipeline identity. Freezing the first container stopped its heartbeat, the second Worker advanced the epoch from 1 to 2 and delivered through the original source slot. After unfreezing, the stale Worker observed the fence and the newer owner kept writing against the original snapshot. This verifies externally started Worker takeover. Agent placement remains separate from the accepted same-volume Kubernetes relocation. [Fixture](../../tests/kubernetes-csi-runtime.py)

### NATS JetStream TLS and quorum

Java 8 candidate `411dafb3780dabe7b718458c8e05ddbb99146113d8abbf79e72bb5a9098fab04` passed six native [NATS fixture](../../tests/nats-cluster-run.py) checks with three separate NATS 2.11.8 instances and file-backed replicas. Rivomesh confirmed trusted traffic, rejected an absent CA and an endpoint name missing from the certificate, continued after killing the elected stream leader, and received no confirmation with one replica left. Restoring two members allowed the original event ID to succeed and be retried within the deduplication window while preserving all three message bodies. Scope is the confirmed publisher transport. [Baseline](baseline.json)

### Mixed Java rollout

Candidate `411dafb3780dabe7b718458c8e05ddbb99146113d8abbf79e72bb5a9098fab04` passed eight native checks with MySQL 8.4.13, PostgreSQL and separate Controller/Agent processes. Controllers Java 8 and 21 ran with Agent/Worker Java 11. Replacing Java 8 with Java 25 left the original Worker delivering native CDC while the Java 21 Controller admitted a durable check. Reopening the drained Agent on Java 17 preserved the earlier STOP and GTID. Its Java 17 Worker resumed both SQL targets with all original snapshot IDs. The fixture identifies the actual running JVM executables and image IDs at each phase. [Fixture](../../tests/global-control-pipeline.py) · [Baseline](baseline.json)

### Source capability wizard

Candidate `0c553e56c7c4402bd298984cd77ca4dfa5f34ec253fa9426ded4b5adb1556f8b` passed seven real Chrome cases, including PostgreSQL and Oracle forms submitting stopped tasks to the live API, password masking and mobile Oracle layout. The DOM suite passed 115 checks. Twenty-three focused Java checks verify source profile validation and native position decoding, and 25 configuration examples passed the shipped parsers. The preceding build passed 502 Java cases with 32 optional cases skipped. On candidate `dd11a9bb3b63fdac6f02cdfe6c5061c7c3c5c5de014979b6eb15daecee912ecc`, PostgreSQL 17.12 passed seven native checks and Oracle 26ai passed thirteen, including original LSN/SCN restart and refusal at missing or unsupported input. [Wizard browser fixture](../../tests/ui/browser-smoke.mjs) · [Baseline](baseline.json)

### MySQL decode header review

Candidate `6d629310028898525a76a54683910b8db4fad01e919f28b46359ae444fd4c2ab` passed nine native MySQL 8.4.13 cases, 21 focused Java checks and 116 DOM checks. Both management entries read the same GTID, native commit boundary and table after an Agent restart. The target and capture checkpoint stayed paused at the original position. Purging the synthetic source log removed the header evidence, reported the source gap and preserved the checkpoint. The report contains header metadata and still requires separately retained row contents for a future skip review. [Fixture](../../tests/global-control-pipeline.py) · [Baseline](baseline.json)

### Global task filters and destination backlog

Candidate `ec6ef04ed53a54e479df1e6f9ad60fa76bb01d6eba3f3c4619b319aa3dd61b0c` passed seven native two-controller checks and seven real Chrome checks, plus 503 Java checks with 32 optional skips and 118 DOM checks. The management entries ran exact task and execution-node filters in shared SQL. A native PostgreSQL constraint blocked one delivery while the MySQL destination caught up. Both entries reported the same destination-specific backlog, and generation-bound resume brought both data sets and their acknowledgment positions back into agreement. [Native fixture](../../tests/global-control-pipeline.py) · [Baseline](baseline.json)

### Retained MySQL binlog contents

Candidate `b95d036f0d5f633de1a311581f70ad67c126af0b22b72f26240fa5d6934c6d32` passed eleven native MySQL 8.4.13 checks. The Java 8 CLI read the affected bytes through the actual binlog protocol, verified each frame CRC32, and returned a SHA-256 matching the independently read byte range of the original on-server binlog. Two repeats produced a single private archive with the same checkpoint binding and exactly the original event bytes. After purging the synthetic log, a new proof was rejected and the previous archive remained byte-identical. [Fixture](../../tests/global-control-pipeline.py) · [Baseline](baseline.json)

### Managed MySQL capture evidence

Candidate `9cc5da0987cc78609988d7a75d5c3de9ae0129e334d16e8abdf1ba31c6373365` passed thirteen native MySQL 8.4.13 checks, seven Chrome checks and 119 DOM tests. Administrators retained the original event frames from either Controller. Other roles and a stale boundary were refused. A retry after source log purge and Agent SIGKILL reused the same archive and receipt without changing the capture checkpoint. A simulated abandoned staging file was removed during publication. The isolated rerun passed after an earlier concurrent run ended with a control-store 503 during review. [Operator guide](../en/capture-recovery.md#retain-from-a-management-entry) · [Manifest](baseline.json)


### Approved exclusion of a MySQL source transaction

Candidate `784468155a13763bf75e532cfb736269df34719a4a77942d8736390021356ffe` passed fourteen approval and continuation checks and fourteen log-purge checks on native MySQL 8.4.13. Administrators reviewed the original bytes and exact impact, then excluded the one GTID while both real SQL target rows and acknowledgment positions stayed unchanged. The Agent reopened the paused state and gap receipt. Later transactions reached PostgreSQL and MySQL after Resume, with the contiguous applied prefixes held at the gap. A matching retained archive alone could not authorize the source range after purge. The same artifact passed seven Chrome checks, 35 focused Java tests and 120 DOM tests. [Runbook](../en/capture-recovery.md#approve-a-source-transaction-skip) · [Manifests](baseline.json)


### Temurin 21 release baseline

Candidate `cb8183632cc1297ed12332c3b9c8580ff3651309c6e648d00f5ed086ed0245cc` passed eight native checks after compiling to Java 21 class-file major 65. The fixture read the actual Controller, Agent and Worker JVMs on ARM64. Replacing one Controller left the other entry available and the original Worker continued delivery. The drained Agent used the same directory and restarted the original snapshots and GTID for PostgreSQL and MySQL. An earlier candidate built at the same class level passed 503 Java tests with 32 optional integration cases skipped. [Deployment guide](../en/console-and-deployment.md#java-21-deployment) · [Manifests](baseline.json)


### Console branding and session bootstrap

Candidate `15a81de54070880f0eaa211e4f00dca3e90696f3334f74492506a6a0248502bd` passed seven live Chrome cases on Temurin 21, 121 DOM cases and parsing of 25 configuration examples. The console uses the Rivomesh site palette and compact mobile navigation. Cookie authentication survives reload, expiry still opens sign-in, and a temporary session lookup error can recover through the same browser cookie. Login and restore each load the task list once. The real browser verified the 390px workspace, source wizard submissions and sign-out. [Browser fixture](../../tests/ui/browser-smoke.mjs) · [Manifests](baseline.json)


### Java 21 exact release candidate

The Dockerfile produced nonroot ARM64 image `sha256:7fd83307056d33e6e87b6734de8da5be77c8fe07324b7bb00c3c74cda11792c8` with Temurin 21.0.13+9 and JAR `be6406c60f2b2c5ab0bd00ecc64cbf28cfd09e8e764d137e19c38f26cdc7dbdb`. The same JAR started as UID 10001 on Linux with the shipped manual configuration and writable state paths. The image passed 19 live MySQL, PostgreSQL and distributed management checks, including controller loss, control-store interruption, target SQL recovery, isolated owner restart, validation and repair. Helm and native YAML each passed four actual Kubernetes v1.31.0 cases for login refresh, CSI remount, configuration rollout and Secret rotation. Chrome passed seven and the gate parsed all 25 configuration examples. SHA checks bound the three live manifests to the same JAR. [Release verifier](../../tests/release-evidence-gate.py) · [Manifests](baseline.json)


### Durable validation report directory

Candidate `480c85fa9a20bb1e6d331a360fdefbf4e1b9a9e768aac5c85d13579e2ef06794` passed nineteen native cases, eight real Chrome cases, 122 DOM tests and 35 focused Java tests. Both Controllers found the same actual validation report after Agent restart and paged its metadata without exposing row contents. The report could still be opened for authorized inspection and repair. The browser exercised the actual catalog endpoint, with DOM coverage for opening a retained report whose operation has aged out. The workspace lock protects active projections, and reopening a report removes abandoned summary staging files. [Report guide](../en/managed-validation.md) · [Manifests](baseline.json)


### Capacity-bound Agent assignment

Candidate `cf103fa235ea3fca9b67d0100b3328b0e219426a775a1c337258b8e4cf25bf40` passed seven native cases each with PostgreSQL and MySQL control stores. Two Controllers and two registered Agents used Temurin 21. The first automatically assigned task delivered snapshots and native incremental rows to both SQL destinations. Simultaneous creates from the two entries competed for the remaining slot and only one acquired it. Further creates were refused before a new task appeared. The first request replayed its original owner after both Agents filled. Console DOM coverage passed 123 cases, including the single automatic placement payload. [Global control](../en/global-control.md#automatic-assignment-for-new-tasks) · [Handoff design](../sdd/001-control-recovery-compatibility/design-agent-handoff.en.md) · [Manifests](baseline.json)

### Temurin 21 source profile regression

The same artifact passed 48 native checks on Temurin 21.0.13. MySQL 5.7.44, 8.0.46, 8.2.0 and 8.4.13 each ran five standalone checks with two Controllers, an Agent, a Worker and two SQL destinations, including cross-schema CDC and keyed, keyless, JSON and generated values. PostgreSQL 17.12 passed seven checks including synchronized-slot promotion and old-route refusal. The fixture verifies the active timeline with `IDENTIFY_SYSTEM`, which avoids reading older checkpoint metadata while the post-promotion checkpoint is still running. PostgreSQL 18.7 passed eight checks including stored generated columns and missing-slot protection. Oracle 26ai Free 23.26.4.0.0 passed 13 single-PDB checks including long archive transactions, original-SCN restart and DDL refusal with the external Oracle JDBC driver. [Compatibility](../en/compatibility.md) · [PostgreSQL replication protocol](https://www.postgresql.org/docs/17/protocol-replication.html) · [Manifests](baseline.json)

### Destination diagnostics and branded console

Candidate `186cddb562acb16d913670dee1564d0d4f2942ba7978b0ccd531ed7056fc622c` passed 10 native `--separate-execution --task-views` checks. Both Controllers observed a PostgreSQL destination pause and a separate MySQL destination retry and recovery while the unaffected destination continued. Destination-specific events stayed redacted and independently pageable through an Agent SIGKILL. A viewer received 403 and using a cursor from another destination returned 409. Twenty-one combined diagnostic and source focused cases included 16 diagnostic tests, and the diagnostic DOM suite passed 124 cases. [Global control](../en/global-control.md) · [Manifests](baseline.json)

The normal Temurin 21 Maven package `f49f52d6ce34862664c70bbed9f8815517d1bf115bec6d7b6adb95689c535488` passed 11 cases in Chrome 157 with final console resources. The official Rivomesh site marks were checked at desktop and mobile widths. Browser cases covered system appearance, a manual preference through reload with the authenticated cookie, keyboard navigation at 320 CSS pixels, the new target selector, historical report discovery and logout. The light muted text was adjusted for contrast on console panels. [Console appearance and session use](../en/console-and-deployment.md) · [Manifests](baseline.json)

### Browser recovery through two management entries

Candidate `186cddb562acb16d913670dee1564d0d4f2942ba7978b0ccd531ed7056fc622c` passed 15 combined native cases, including six real Chrome interactions. The task used a separate Agent, two Controllers, MySQL 8.4 source and PostgreSQL and MySQL destinations. A cookie issued by one Controller opened the same task in the other. After Agent restart, the browser found the retained report, navigated 27 differences across pages, required explicit reveal for raw values and sent the reviewed repair through both Controllers. Reload retained the two target receipts. A second workflow imported typed corrections without rounding an unsigned 64-bit JSON integer and required an approval checkbox and reason. Both entries showed the receipt with business values redacted. The fixture checked exact target SQL rows, CDC resumption, retained acknowledgment, and cleanup. A later DOM check independently verified the dialog's accessible name and keyboard dismissal. [Managed validation](../en/managed-validation.md) · [Manifests](baseline.json)

### Offline audit checkpoints

The verified export tool writes only the audit store identity, sequence and chain hash to a new private checkpoint file. Verification against an independently retained checkpoint rejected a genuinely earlier valid export and accepted a same-store continuation through SIGKILL using Temurin 21 and embedded H2. The offline suite passed 11 tests including chain rewrite, store mismatch, missing overlap and protection against overwriting a checkpoint. External custody and storage policy remain operational requirements. [Audit export](../en/security-audit.md) · [Manifests](baseline.json)

### Oracle strict textual JSON

Candidate `886a29b36957c8a964029eef5848d34bd3b8f598dc41e8578f6ae3cdfbc5ea05` passed 18 native Oracle Free 23.26.4.0.0 to PostgreSQL 17.12 cases on Temurin 21, with five focused Oracle Java tests. A validated, enabled, nondeferrable `VARCHAR2` constraint using `IS JSON (STRICT WITH UNIQUE KEYS)` authorizes JSONB materialization. Flashback and archived LogMiner preserved large integers, Unicode, embedded quotes, SQL NULL versus JSON null, and keyless multiplicity. Native binary JSON and JSON keys were refused. The pipeline retained its original snapshots after SIGKILL and continued archived JSON delivery. [Oracle source](../en/oracle-source.md) · [Manifests](baseline.json)

### Verified standalone writer handoff and read-only adoption

Candidate `109361ade9b29a7caacb7da04cbd7b09e13c2f87b2893998290f41975b3bab8e` passed five native handoff cases and 55 focused Java tests. A PostgreSQL journal stored a single new writer assignment and its proof for two independently mapped SQL destinations. The fixture refused a live lease, wrong journal generation, substituted snapshot, newer target epoch and earlier binary. Retrying the transfer returned its original receipt. A suspended old Worker was fenced after thaw and the replacement resumed an actual cross-database transaction with the original snapshots. [Standalone handoff](../en/pipeline-v3.md#verified-standalone-writer-handoff) · [Manifests](baseline.json)

Candidate `f49f52d6ce34862664c70bbed9f8815517d1bf115bec6d7b6adb95689c535488` passed nine MySQL 8.4 to PostgreSQL read-only adoption preview checks. Matching, changed, missing and extra rows were counted without opening destination ownership or journal state. PostgreSQL connected with read-only privileges. Losing the physical MySQL snapshot session during destination pinning stopped the comparison despite reconnect being enabled. The fixture also covered empty tables, row security, constraints, existing ownership and checkpoint, and unsupported target products. [Read-only adoption preview](../en/nonempty-target-preview.md) · [Manifests](baseline.json)

### PostgreSQL delivery samples and UUID keys

The Temurin 21 candidate `c3605293a956f0b8656d9e7a0ec60a8c08d1a29da1953b62515cf1f9d9484a13` passed 38 focused Java checks, including the HTTP assertion for the official wordmark and title, and 10 live PostgreSQL 17.12 checks. The timestamp from pgoutput matched `pg_xact_commit_timestamp` down to its microseconds. The last per-target sample survived SIGKILL, stayed historical during a SQL constraint pause and advanced only after a direct confirmed apply. UUIDs retained their canonical text through a primary-key move, delete, and nullable values. Destination timing, throughput and recovery coverage are reported separately. [Source guide](../en/postgresql-source.md) · [Timing](../en/global-control.md) · [Manifests](baseline.json)

### Optional external MySQL JDBC package

The opt-in Maven artifact `432389ac96f728432a7512568c8c8e32833326672df40c3514a76d6039a2b07f` left the standard package SHA-256 unchanged. Its nested archive inventory verified the driver exclusion and both exact upstream vendor notices. A separately mounted Connector/J ran eight native cases with MySQL 8.4.13 source and control store, separate Controllers and Agent, and PostgreSQL and MySQL destinations. Rolling replacement preserved the STOP receipt, GTID and snapshots. Three additional checks covered an accidentally bundled driver, explicit missing-JDBC admission and the original unavailable-endpoint diagnosis. The full artifact-specific distribution review remains pending. [Release inventory](../en/release-inventory.md) · [Apache third-party license policy](https://www.apache.org/legal/resolved.html) · [Manifests](baseline.json)

### Repeated capacity reference and clean reactor

A clean archive of commit `2257425` built `5c94d5468757992376f8d99a0d8d043e215138c9d99c5684922aa45d7183d137` with 514 passing Java tests and 32 optional integration skips. On that exact artifact, all 12 native capacity trials passed snapshot and CDC content validation. The matrix used three repetitions per journal and CPU quota on an Apple M1 Pro with Docker Desktop. The benchmark verifies final key boundaries before measuring visibility and checks ordered content at each phase. [Hardware results and reproduction](../en/performance.md#repeated-temurin-21-capacity-reference) · [Manifests](baseline.json)

The same artifact then passed 256 MySQL database mappings to 256 PostgreSQL schemas, including 512 exact rows and one 768-change transaction with 8,505,279 native binlog bytes. The first durable snapshot set completed in 25.84 seconds. The transaction reached the target in 4.32 seconds. After SIGKILL, an offline 256-change transaction arrived in 21.42 seconds measured from the restart request, with every original snapshot identifier preserved. [Catalog fixture](../../tests/catalog-benchmark.py) · [Hardware and content manifest](baseline.json)

LG-07 also passed a sustained slow-target profile on the same artifact. With 32 mappings and a separate PostgreSQL journal, pausing the target held the capture sequence at 3 and the target acknowledgment at 0 for 5.34 seconds across nine observations. The logical journal high-water mark was 3,244,602 bytes under a configured 3,538,944-byte budget. PostgreSQL relation size was recorded separately. After SIGKILL the Worker acquired a newer epoch, retained all snapshot IDs, applied the full source range through acknowledgment 24, and collected the pending payload to zero. Exact row contents matched across source and target. [Slow-target reproduction](../en/performance.md) · [Manifests](baseline.json)

### Report lookup and keyboard focus through polling

Candidate `5726e5c369f06bc6abcf622f7bf6672440b0e5eafaa1a142331bc1c015cb5c40` passed 16 native checks, including seven real Chrome interactions across two Controllers. The browser found the original report by ID after Agent restart, returned to its directory page and kept keyboard focus through a real periodic refresh before opening native differences. It verified named correction controls, 27-row paging, explicit raw reveal, two repair receipts and typed correction with exact JSON integer precision. The same Maven artifact passed 12 standalone Chrome checks and the DOM suite passed 127 cases. [Report operations](../en/managed-validation.md) · [Manifests](baseline.json)

### Managed Agent handoff assessment

The exact Temurin 21 candidate `43b01b78bf4aa661119f9f49ae8a1057e92d98f9a52c5d2dbf0d1eb1c2d72597` passed 41 focused Java tests and eight native cases on each of PostgreSQL and MySQL control stores. Two real Controllers read and replayed the same assessment for two real Agents and a caught-up external PostgreSQL journal. The original Worker remained assigned. One of two simultaneous refreshes replaced the expected assessment and the persisted result survived SIGKILL of a Controller. The response exposes the checkpoint hash and reports the required evidence checks before actual transfer. [Global control API](../en/global-control.md#assess-transfer-to-another-agent) · [Manifests](baseline.json)

### MySQL direct-origin delivery timing

Candidate `5726e5c369f06bc6abcf622f7bf6672440b0e5eafaa1a142331bc1c015cb5c40` passed six MySQL 8.4.13 cases and five MySQL 5.7.44 cases. An independent reference reader checked CRC32 and positions in the native binlog transaction and verified the ordinary GTID's original commit timestamp. Two native SQL destinations emitted matching observed source times and independent confirmed sequences. The samples survived Agent restart. MySQL 5.7 retained explicit unknown timing while both destinations continued to receive live CDC. [Timing profile](../en/global-control.md) · [MySQL timestamp reference](https://dev.mysql.com/doc/refman/8.4/en/replication-delayed.html) · [Manifests](baseline.json)

### PostgreSQL business UNIQUE restoration preview

The same Temurin 21 candidate passed five native MySQL 8.4.13 to PostgreSQL 17.12 checks and two offline admission guards. A completed common-cut report had matching row content, while the target's case-insensitive collation exposed a conflicting group with two rows. The review retained native NULL-distinct semantics, checked only counts while the source was offline, and preserved target data, indexes and checkpoints. It refused incomplete or outdated validation evidence. [Read-only constraint review](../en/nonempty-target-preview.md) · [Manifests](baseline.json)

### RabbitMQ three-member quorum recovery

Candidate `5726e5c369f06bc6abcf622f7bf6672440b0e5eafaa1a142331bc1c015cb5c40` passed six native RabbitMQ 4.1.4 cases with a PostgreSQL sibling target. After the connected leader was killed, original event IDs redelivered with identical serialized bytes. A lone surviving replica could not advance the publication receipt. The SQL target continued through both the minority outage and Worker restart. Restoring the original RabbitMQ members in sequence recovered quorum. Manual acknowledgment drained 19 deliveries with 18 distinct IDs, and four complete transaction envelopes matched the retained native source receipts. [Messaging operations](../en/messaging-v3.md) · [Manifests](baseline.json)

### Global task and configuration authority

CP-02 passed ten cases on each of native PostgreSQL 17.12 and MySQL 8.4.13 control stores, using the same Temurin 21 artifact `5726e5c369f06bc6abcf622f7bf6672440b0e5eafaa1a142331bc1c015cb5c40`. An active configuration created and edited through Controller A was read at the same revision through B. Concurrent creation of one global task ID admitted one authority row. Two independent MySQL sources copied same-named tables with isolated configurations, destination schemas and credential references. Stale revisions and cross-task references were refused. A standalone task reused the same identifier in its own authority scope and continued original CDC after SIGKILL. [Shared control](../en/global-control.md) · [Manifests](baseline.json)

### Candidate Agent evidence probe and assessment UI

Candidate `24dfeb6ee634f580b4f7f923639f6e59fa01a748cc1625dda8f452ed79d2782a` passed ten native cases per control store and 71 focused Java tests. The original and candidate Agents used authenticated read-only queries bound to the assessment, task revision and both enrollments. The candidate rejected its own mismatched credentials, then verified the original PostgreSQL journal and MySQL GTID plus both native SQL receipts. The original Agent inventoried private evidence and retained explicit custody refusal. The journal, target positions and task assignment stayed unchanged.

Artifact `729a1e0c8fd2b86c1fcbf2746fda6c53f5a7135d2cd809e8407af98eca6c8884` passed 11 real cases including three Chrome interactions across both Controllers and their Agents. The assessment dialog preserved keyboard focus through polling, let an administrator select an authorized live candidate and reopened the same receipt at the other entry. The existing owner and worker state were retained. Standalone Chrome passed 13 cases and the DOM suite passed 131. [Global handoff operations](../en/global-control.md#assess-transfer-to-another-agent) · [Manifests](baseline.json)

### Cross-entry command outcomes

CP-03 passed seven cases on each of PostgreSQL 17.12 and MySQL 8.4.13 control stores using the same Temurin 21 artifact `5726e5c369f06bc6abcf622f7bf6672440b0e5eafaa1a142331bc1c015cb5c40`. Pausing through one Controller and resuming through the other changed live CDC, and replay of the original pause returned the same receipt while CDC kept running. HTTP 202 recorded a pending request while its Agent was offline. Revoking its issuer before Agent rejoin returned the same-request `REJECTED` outcome at both entries. An isolated fault fixture advanced the local revision without the original execution receipt. Agent generation 3 exposed `UNKNOWN` consistently after generation 2 and an identical replay preserved that outcome. Earlier native profiles cover target retry, offline stop and validation/repair. [Global control](../en/global-control.md) · [Manifests](baseline.json)

### Global diagnostic filters and bounded reads

Candidate `4edb16a83fb1a004dc466712dae89e180d1df40f13e7b1c776fc21e8237f15ab` passed ten native cases across two Controllers and a separate Agent, with independent target pause and recovery, filtered and redacted logs, scoped cursors and stable pages after Agent restart. The normal console passed a concurrent HTTP test for rate limiting and the `Retry-After` header. The Agent budget is eight pages per second with a burst of 16. The exact artifact passed 28 focused Java tests, 13 real Chrome checks and 131 DOM tests. Native PostgreSQL and direct-origin MySQL 8.4 timing use verified commit evidence. MySQL 5.7 and delivery profiles without evidence show unknown timing. [Global diagnostics](../en/global-control.md) · [Manifests](baseline.json)

### Replicated Kafka publication and offsets

Artifact `5726e5c369f06bc6abcf622f7bf6672440b0e5eafaa1a142331bc1c015cb5c40` passed five native Kafka 3.9.2 KRaft checks with three controllers and one broker-only node. Killing the partition leader elected a surviving replica. At one in-sync replica, two live controllers still committed fresh metadata while the publisher and journal acknowledgment held at sequence 2. Restoring a replica delivered the retained transaction at sequence 3 with its original ID and bytes. The consumer resumed at its committed offset 7. After processing six records and committing offset 13, a reopened consumer found no pending records and all 13 original records retained their bytes. The source fixture uses deterministic transactions in the packaged JDBC journal. Live CDC, broker TLS and control majority loss are separate profiles. [Messaging operations](../en/messaging-v3.md) · [Manifests](baseline.json)

### Frozen-source PostgreSQL UNIQUE restoration plan

Artifact `860e01d05556b9e865cef684135ed0e397ba7b8ab7405805a8d6749ce60bb678` passed nine native checks with MySQL 8.4.13 and PostgreSQL 17.12 and 49 focused Java tests. `plan-unique-restore` binds the completed report, common checkpoint, source GTID and target index identity to a review digest while checking live frozen-source and target row content. The profile refused an unfrozen source, an ordinary index rebuilt after review, incompatible PostgreSQL collation, altered rows with the same GTID or target checkpoint, and a changed source GTID. Native business data and indexes were preserved. The integrated compile and 24 focused tests used artifact `93740e78e05df5d83d821bd4f6c3583f9a21d1ae2c85c5d62538fdc84d2ff01a`. [Restoration workflow](../en/nonempty-target-preview.md) · [Manifests](baseline.json)

### Shared validation report custody

Artifact `ef74695d3da6e8b50edb03edfa01e85a7d777b73073d8a38a60544d6332288aa` passed 12 native cases per PostgreSQL and MySQL control store and 30 focused Java tests. The original Agent archived a real completed report under its operating-system lock on an opt-in private shared mount. A candidate with a disjoint mount could not verify the exact archive. With both Agents using the shared mount, the candidate authenticated the assessment and checked the original summary and database bytes. The test preserved the task owner, journal and both target checkpoints. Candidate access to source evidence, failure evidence and closed operation receipts remains separately guarded. The scoped integration artifact `57d5286f1477a8d4f1548bcaccd7efca0745296e140ac695036f0b733de4eb14` passed 30 focused Java cases. [Evidence custody](../en/global-control.md#assess-transfer-to-another-agent) · [Manifests](baseline.json)

### MySQL 8.4 native asynchronous source failover on Java 21

Artifact `4edb16a83fb1a004dc466712dae89e180d1df40f13e7b1c776fc21e8237f15ab` passed eight real checks with MySQL 8.4.13 and Temurin 21.0.13. The topology uses an external PostgreSQL journal, separate Controllers and Agent, and independent PostgreSQL and MySQL targets. A least-privilege capture account could not promote the replica. After primary SIGKILL, the replica stayed read-only and both Controllers observed the native receiver fault. The journal and each target held sequence 1. An external database promotion let Rivomesh automatically acknowledge sequence 2 in the original generation, with both target receipts bound to the same promoted GTID. Exact keyed, keyless, JSON and generated data matched. The task revision, configuration and all three source and per-target snapshots were preserved. [Source failover runbook](../en/pipeline-v3.md#automatic-continuation-after-mysql-84-primary-failure) · [Manifests](baseline.json)

### Historical operation receipts after Agent loss

Candidate `f4e283959eba1d3ef6b022ef6a82155a7c53a10b64a12c74404b3da3186af207` passed 21 native checks, including ten real Chrome interactions, plus 29 focused Java and 134 DOM tests. Two Controllers found a real report, paged 27 differences, reviewed and approved precise PostgreSQL and MySQL repairs, and reviewed typed correction with exact JSON integers. After 21 later terminal commands and Agent shutdown, both Controllers returned the earlier repair and correction receipts by authorized task and request ID. Chrome reopened the correction receipt without returning original business values. The storage view reports protected file metadata. The staged app including shared custody passed 47 Java and 134 DOM tests on `daa2daab521d7dc050eb55e1fdd4111bd43fc4b543f8fcb11cfcdd5f9cbe12d2`. [Report and receipt operations](../en/managed-validation.md) · [Manifests](baseline.json)

### Redis Streams promotion and pending-entry recovery

Artifact `93740e78e05df5d83d821bd4f6c3583f9a21d1ae2c85c5d62538fdc84d2ff01a` passed six real Redis 7.4.2 checks. After the old primary was fenced and stopped, the configured endpoint refused the read-only replica until an operator promoted its original AOF. While the new primary lacked a replica fsync, the original journal receipt stayed at sequence 1 with one ambiguous BEGIN. A new replica restored fsync and the original transaction advanced the receipt to sequence 2, with 11 stream entries for ten event IDs and exact bytes for the repeated entry. Previously confirmed consumer acknowledgements stayed effective. The replacement consumer claimed three original pending records. Cold restart of the promoted server restored the exact stream IDs, group cursor and empty pending-entry list from its AOF. [Redis operations](../en/messaging-v3.md) · [Manifests](baseline.json)

### Cross-entry authorization, audit correlation and Agent secret references

CP-05 passed 13 native checks per PostgreSQL and MySQL control store on artifact `ba12f19fbc4657f8f2470d2dda4de9d70d14c709b6d6c7b063844469ad05acab`, including Chrome 157 opening the real configuration and audit views at both entries. Revoking access at one Controller immediately denied a new command at the other. Both audit exports correlated an actual repair through the request hash, task, assigned Agent and plan ID after Agent restart. With the optional per-task policy enabled, the actual Worker received its authorized and used variable only. Another task's credential or moving a credential reference into a public identity field was denied. Configuration and audit Chrome views withheld original names and values. Oracle root/PDB reference paths were covered by Java tests. The native artifact passed 49 focused Java and 23 focused DOM cases. The integrated artifact `fd0077cab4c5a0e88e1cff4f63320452459617c82bab548c7441623e99948d6c` passed 54 Java and 134 DOM cases. [Shared access](../en/shared-access.md) · [Manifests](baseline.json)

A fresh Temurin 21.0.13+9 reactor from `4e86f79` built normal artifact `9a58c7f72c0789ac329c21c6c664f95d733b394bb8d5f5cf125dd11b7241b684`. The app passed 526 Java cases, with 32 optional external-profile cases skipped, and the isolated transport passed two. [Reactor record](baseline.json)

### PostgreSQL certificate verification and resumed TLS delivery

The normal reactor artifact `9a58c7f72c0789ac329c21c6c664f95d733b394bb8d5f5cf125dd11b7241b684` passed 11 native PostgreSQL 17.12 checks on Java 21.0.13. Packaged pgJDBC 42.7.14 used `sslmode=verify-full` and `channelBinding=require`. The source and destination native views showed TLS 1.3, including the active logical replication session. The real preflight refused an incorrect source DNS name and an unrelated target CA. Snapshots and CDC matched. The profile replayed a transaction after an explicit destination SQL recovery and restarted after SIGKILL with the same LSN stream, original snapshot IDs and verified TLS. Removing the original slot was refused at the last durable checkpoint. [PostgreSQL TLS setup](../en/postgresql-source.md#verified-tls-connections) · [Manifests](baseline.json)

### Reviewed holds for retained validation reports

Artifact `4716ce9a2012053d536d2f97574094a8a454898a26b52fd103fff0fb6571a4a3` passed eight native cases including two real Chrome interactions, 39 focused Java and 135 DOM tests. Administrators used both Controllers to approve an opt-in report retention hold, while a viewer could not extend it. The current report lock serialized the review attachment and the original report bytes stayed identical. Its exact command receipt remained available after Agent loss and its review remained visible on restart. Helm rendered both default and enabled policy. The integrated artifact `f8610ff55840df7ff8bbc12ed726dfdf9fde3ba3ec51130085e573906db89ed7` passed 48 Java and 135 DOM cases. This policy protects evidence and records a minimum retention age. Final report archival or reclamation requires a separately verified lifecycle. [Retention operations](../en/managed-validation.md) · [Manifests](baseline.json)

### Coordinated local state restore and damage refusal

The Java 21 artifact `daa2daab521d7dc050eb55e1fdd4111bd43fc4b543f8fcb11cfcdd5f9cbe12d2` copied a stopped LOCAL H2 journal and matched H2 JDBC test target. The restored process kept the generation, schema, snapshot, source position and native destination receipt. Retained synthetic MySQL GTID transactions replayed once and advanced sequence 1 to 3. A mixed-time target copy returned `SINK_JOURNAL_CHECKPOINT_DIVERGED`. Modified pending raw data returned `JOURNAL_CHECKSUM_MISMATCH`. The refusal paths preserved the saved source position, subscriber acknowledgement, target receipt and business rows. [Backup and restore](../en/first-release.md) · [Manifests](baseline.json)

### Candidate custody of closed operation receipts

Artifact `5195f04ef4299cf18da38e69c0c76ab50d01d39ad4aafbe6f6f158dd962893c4` passed 14 native checks on each control store. The original Agent matched authenticated global commands to exact private local receipts. It sealed closed records in the protected report archive, and the candidate verified the exact bytes through its own authenticated scope. The operator response exposes counts and digests. After original Agent restart with one private receipt missing, the candidate reported incomplete receipt custody and retained prior report evidence, source progress, synchronized targets and the original writer. The staged integration with retention and per-task authorization passed 48 Java tests. This bounded profile covers up to 64 global commands. Actual request migration and task assignment are still outside the tested custody phase. [Handoff operations](../en/global-control.md#assess-transfer-to-another-agent) · [Manifests](baseline.json)

### Retain exact original journal events while recovery remains open

Artifact `97b3d326989e8e4845829b70adf3739efa41b0ce16c4cbe07ebfc005fa999c7f` passed 22 native cases each on PostgreSQL 17.12, MySQL 8.4.13 and MySQL 8.0.42, plus five H2 peer initialization and reopen repetitions. Open gaps and prepared skip intents pin the exact raw event, normalized payload and associated retained metadata. Unrelated acknowledged transactions remain eligible for collection. A transaction releases its bytes only after the job lock validates the exact closed recovery receipt. The native tests covered restart, sibling acknowledgements, quota and release. MySQL selected the new collector indexes with 4,096 gap and intent rows in its query plan. The latest staged artifact `27b6d9be244c34bcdd954b24afb6955ecf0480101bcad9a2884be677a9c694cb` passed 41 focused Java cases, with six external-enabled cases skipped in that particular build. Analytics and message-target recovery protocols are tracked separately. [Skip and retention operations](../en/transaction-skip.md) · [Manifests](baseline.json)

### Native MySQL and PostgreSQL coordinated LOCAL restore

Java 21 artifact `f8610ff55840df7ff8bbc12ed726dfdf9fde3ba3ec51130085e573906db89ed7` passed four live MySQL 8.4.13 to PostgreSQL 17.12 checks. The fixture waited for the engine to exit, performed native H2 shutdown and independently confirmed that H2 could open read-only and the destination had no Worker sessions. It copied the journal and matched it to an actual PostgreSQL dump. Transactional restore in the same PostgreSQL database kept the native OID, generation and both original snapshots. An offline MySQL GTID reached the original target once and advanced the journal and native checkpoint from 1 to 2. A clone restored to a different configured database URL was refused with `JOURNAL_IDENTITY_MISMATCH`; both target evidence and rows stayed intact. [Coordinated backup procedure](../en/first-release.md) · [Manifests](baseline.json)

### Transactional PostgreSQL UNIQUE restoration

Artifact `da92a97512d7ff77eb0eb8bd1e8f23089475dd0488e91d667a8ad658965a2a38` passed 62 focused Java and 11 native MySQL 8.4.13 and PostgreSQL 17.12 cases. The bounded profile uses an explicitly and persistently frozen standalone MySQL source and one PostgreSQL database hosting both the destination and external journal. The reviewed plan digest and explicit operation ID bind the approval. PostgreSQL commits the new UNIQUE constraint and receipt together while retaining the ordinary index. An exact MySQL session kill after native DDL but before commit rolled back both writes. A forced lost PostgreSQL acknowledgment produced `COMMIT_UNKNOWN` and the same operation replay verified the original committed receipt. The restored constraint enforced duplicates after restart. Writer handover, failback and multi-target adoption have separate gates. [Constraint restore operation](../en/nonempty-target-preview.md) · [Manifests](baseline.json)

### Oracle civil DATE snapshots and archived CDC

Oracle Free 23.26.4.0.0 and PostgreSQL 17.12 passed 22 native cases on JAR `99d287b9ed1829c22ebb16fc7d5c0bc02722672a30c943e8a8960bf887f6cb77` and seven focused Java parser tests. Leap day, second precision, SQL NULL and civil time in a daylight-saving gap remained consistent for keyed and keyless rows. Archived redo and undo matched, and SIGKILL resumed with the original snapshot identities. Strict text JSON continued to work and unsupported BCE dates were refused. [Oracle source](../en/oracle-source.md) · [Manifests](baseline.json)

### MySQL TLS 1.3 binlog and destination reconnect

The complete staged build `c28a871f409b5f46936f063093a245b9b802003d414885d0d63117f1afc371df` passed 41 Java tests and four native MySQL 8.4.13 cases, with one optional MySQL profile skipped by the local Java test command. The fixture confirmed that a mismatched DNS name was refused and that the original address and trust succeeded. MySQL native views showed TLS 1.3 for the actual binlog stream and JDBC target session. Verified TLS copied exact JSON and generated values, then continued from the original GTID and snapshot after Worker SIGKILL. The capture client negotiates the JDK protocols for its CA and identity-verified modes. [MySQL TLS operations](../en/pipeline-v3.md#verified-tls-for-mysql-capture-and-delivery) · [Manifests](baseline.json)

### Sealed validation archive and exact restore

Artifact `507a42969f4dbaf9cd321f871ad82454b53b04a13d33afcf77b04a5aa6a18624` passed 15 native cases including six real Chrome interactions, plus 44 Java and 135 DOM cases. An administrator approved a private recovery copy of a completed validation report. The archive and original files matched exactly. Both Controllers traced the approving operator and request to the sealed descriptor digest. The fixture removed one disposable hot report file. Other readers saw its durable restore fence and the report remained in the directory. A separate approval recovered the exact original bytes and both entries reopened the report. Archive and restore receipts survived Agent loss. The integrated artifact `3f7109c6395d94b89b4729ae60044c5404f45f131d85f7375ecba29bf552dd34` passed 50 Java and 135 DOM cases including the direct handoff-reader fence. Final hot report reclamation has a separate eligibility and protection requirement. [Report archive operations](../en/managed-validation.md) · [Manifests](baseline.json)

### Explicit per-target recovery modes and ClickHouse native receipt review

The integrated artifact `1d484dc78701059e93ac7b2815b65fa2100bbad3ac1395614bc894704d83dcb4` passed 72 Java, 136 DOM and 14 real Chrome checks, with two external-profile cases gated in the Java command. The adapter API and bilingual console distinguish reviewed SQL skip/compensation from products that require original transaction recovery and a separately planned baseline. ClickHouse candidate `cb301b3c403e4c634656b4a9e7fbfa750827cc6116418a7a25365c22dbf6cc7c` passed a 36-case run with native MySQL source and ClickHouse/PostgreSQL targets. The read-only command verified the exact preceding ClickHouse commit receipt. It refused a changed native receipt or a target with evidence of a later partial batch. The SQL sibling continued and both targets retained their acknowledged positions during review. [ClickHouse recovery](../en/clickhouse-v3.md) · [Manifests](baseline.json)

### LG-09 audited validation report reclamation and restore

Artifact `08e755568ee40d57d8e4ccf776a3974bd02c9d6130a686843e428c10734a9e34` passed 18 native cases including eight real Chrome interactions, 47 Java and 136 DOM tests. The bounded opt-in profile requires an eligible completed differences report, a stopped task, expired minimum retention, one reviewed exact archive SHA and exclusive report access. The native fixture refused a running task, unexpired hold and mismatched hash. A confirmed request reclaimed only the duplicate hot row database while retaining the summary, archive, approval and durable restore marker. Shared audit linked operator, request, report and archive digest. After Agent restart, Chrome restored the exact archived bytes. Replaying the old reclaim request preserved those restored bytes. The Java suite exercised interruption after unlink before terminal completion, audit failure and locked or referenced evidence. Earlier native profiles cover authorized historical search, original sessions, approved repair and 27-row keyboard pagination. [Report runbook](../en/managed-validation.md) · [Manifests](baseline.json)

### Reviewed reservation for managed Agent handoff

Artifact `403d01af44aac54f0ab16ec707fee7bf90cf90e660c7a12c83485379d31a55f3` passed 16 native cases per PostgreSQL/MySQL control store and 59 Java tests. Preparation binds an exact stopped cut, assessment, revision and custody evidence. It reserves one task and Worker slot for 15 minutes on the database clock. Both entries replayed the same request, refused new commands and exhausted capacity, and could release the hold after a Controller restart. Artifact `8817f9f83c68fa22b4d3dc0779330f211bb54b45ac8dbe766111eb7737ac8d7d` passed 18 native cases including four real Chrome interactions. A keyboard user reserved capacity through one Controller, checked the same receipt and released through the other. Original owner, revision and source, journal and SQL target positions were preserved. Integrated artifact `c67bd52d678ad99eca226619f710a9b6c34ee3374f2397c5f3d26331b162d287` passed 69 Java and 140 DOM tests with the report reclamation guards. Execution authority remains a separate transfer phase. [Agent preparation operations](../en/global-control.md) · [Manifests](baseline.json)

### Exact-cut plan for an existing PostgreSQL table

Artifact `07e70a2c8bf0a21bc2e142dc6aab9bb0e64a787211faaec54f83bd47d46674d9` passed seven native MySQL 8.4.13 to PostgreSQL 17.12 and 64 focused Java cases. The bounded one-keyed-table plan requires persisted source freeze, exact physical source cut, matching row content and target catalog, and absent competing ownership and metadata namespaces. Repeating the reviewed digest reproduces the plan. Insufficient freeze, changed rows or structure, REINDEX, oversized input, existing ownership or a second mapped table are refused. Native preflight leaves business and metadata bytes intact. A transactional adoption bootstrap is tracked separately. [Existing target operations](../en/nonempty-target-preview.md) · [Manifests](baseline.json)

### PostgreSQL civil DATE with two SQL destinations

Artifact `d27ebc75279169bed072345c768b2dfc9b513a0722d332b999ef1586693f95c2` passed nine native checks on PostgreSQL 17.12 to PostgreSQL and MySQL 8.4.13, plus 13 focused Java tests. Civil DATE and SQL NULL matched in both snapshot and pgoutput, including Gregorian boundary, leap day and a date skipped by the JVM time zone. DATE key changes and keyless multiplicity matched. SIGKILL preserved the original slot and both snapshot sets. Infinite DATE stopped before the observed LSN advanced or either destination changed. The integration artifact `d8c7bcb2e39f492505522b512584e1a6e0e1c6902b2f7480f7f02b1ea246bfec` passed 43 focused Java tests. [PostgreSQL source](../en/postgresql-source.md) · [Manifests](baseline.json)

### Doris 4.1.3 Java 21 live recovery and native receipts

The exact artifact `08e755568ee40d57d8e4ccf776a3974bd02c9d6130a686843e428c10734a9e34` passed the native Doris pipeline and receipt acceptance on Temurin `Temurin 21.0.12.1+1` with a MySQL 8.4.13 source and PostgreSQL 17.12 sibling target. Snapshot and CDC preserved the bounded typed values and keyless multiplicity. PostgreSQL progressed during the Doris outage, and Doris caught up at the saved receipt. A Worker SIGKILL resumed with the original native marker. The fixture checked the four native receipt tokens against their original Doris metadata tablets and confirmed that the SQL sibling's error replay did not alter the latest Doris receipt. The disposable fixture has one FE and one BE. [Doris operations](../en/doris-v3.md) · [Manifests](baseline.json)

### Authenticated historical failure evidence custody

Artifact `be849a63d7a06b651a4ef4e71d9b825b79ca71de580400cde015aa1798bcb5d7` passed 15 live checks each on PostgreSQL and MySQL control stores. A real MySQL transaction failed a PostgreSQL constraint, producing signed original Agent evidence. After SQL repair and exact replay, readiness checked the stopped cut. The original Agent authenticated its authority and record contents under the private lock. A candidate with a different mount refused custody and then verified exact archived bytes on the shared mount. Archive contents excluded the original signing key. Original ownership and positions were preserved. Integrated artifact `b4dd675432e35dccd84cb47db6ed039d91310085625eff432c39dcbadd6f277f` passed 84 focused Java cases, including interlocks with report reclamation. Authenticated raw source archives and writer activation are separate. [Managed custody](../en/global-control.md) · [Manifests](baseline.json)

### External vendor JDBC candidate

Temurin 21 artifact `02906330f9cc73efa8e4516ef3838f3ee3607a12f943e0352664a8db5711b5f3` passed the three-profile package comparison. The optional `external-vendor-jdbc` package externalizes MySQL Connector/J, GaussDB and GBase8c wrapper drivers, Kingbase and DM JDBC. The operator classpath probe verified all five expected providers, their class origins, URL matching and the DM exception diagnostic. With the drivers absent, requests returned the explicit unavailable-driver diagnostic. The development and MySQL-only variants kept their SHA-256 digests across builds. The combined normal artifact `b4dd675432e35dccd84cb47db6ed039d91310085625eff432c39dcbadd6f277f` and external artifact `520aecc1fff7de42aaf4c51ea45b61d56a2a333675a88008bae56a3b0f3807ce` both passed binary checks. Vendor endpoint connectivity and distribution rights remain separately scoped. [External JDBC build and operations](../en/release-inventory.md) · [Manifests](baseline.json)

### Shared handoff volume deployment configuration

The deployment checker rendered two separate Agent releases with the same existing ReadWriteMany claim for evidence and distinct existing claims for private state. The new configuration kept the nonroot execution identity and omitted Pod fsGroup for this optional profile to preserve owner-only filesystem mode. It refused Controller placement, missing private claims, reused claims and invalid claim names. Default Helm/native manifests still matched, and the standard Docker, separated execution and review-overlay configurations rendered with fixture inputs. This check covers deployment templates and POSIX configuration requirements. The independent native custody fixture covers the shared-file behavior. [Kubernetes and shared custody](../en/global-control.md)

### Atomic bootstrap of an existing PostgreSQL target

Artifact `6065ddcb304b9cf4e76a8c1065f7fd75fb64c42646d9ca68b3e73d65419fe1f7` passed eight native MySQL 8.4.13 and PostgreSQL 17.12 cases and 67 focused Java tests. The reviewed profile starts with one keyed unowned table and a persistently frozen standalone MySQL source. PostgreSQL registers target ownership, journal cut, complete snapshot and operation receipt in one transaction. Loss of the pinned MySQL session after PostgreSQL DDL rolled everything back. A locally flushed commit with a lost acknowledgment was recovered by the original request after PostgreSQL restart while MySQL was offline. Replay refused drifted business rows or a changed snapshot checkpoint. The ordinary Worker resumed the approved cut, reusing the snapshot and applying new insert/update/delete with zero copy parts. Staged integration `338c88717f4b1656c6638d36da3e35fa986ba61b6cdb02418527b90050047de0` passed 62 focused Java tests. Broader targets and business write authority cutover/failback remain separate. [Existing target bootstrap](../en/nonempty-target-preview.md) · [Manifests](baseline.json)

### Original Agent authentication of retained MySQL binlog bytes

Artifact `ce7af9356aed6cd316bf803bd0e38c9babd75c0ab52e9ea2f0ee69ac20ae2976` passed 14 native cases with MySQL 8.4.13 and PostgreSQL control, plus 37 focused Java tests. Both Controllers returned the original archive for a stable request. After Agent restart and source binlog purge, a fresh request verified the original signed receipt and reused the exact bytes. The source-range exclusion gate still refused the purged range. Local tests exercised corrupted files, interruption between receipt and archive publication, missing keys and legacy unsigned data. The integrated artifact `46a221010f9f9e6eb2fccf12d3fe49716f8f60a80025f41213238ac9e0d44815` passed 28 Java tests. [Capture evidence operations](../en/capture-recovery.md) · [Manifests](baseline.json)

### MySQL 8.4 single-primary group election and automatic continuation

Temurin 21 artifact `b4dd675432e35dccd84cb47db6ed039d91310085625eff432c39dcbadd6f277f` passed six native MySQL 8.4.13 and PostgreSQL 17.12 cases. After SIGKILL of the original group primary, a surviving majority elected a different writable member. The same Worker advanced the durable sequence, journal ACK and destination receipts from 1 to 2 to 3, with exact GTIDs and row checks across two mapped databases. The journal generation and both original snapshots stayed stable. MySQL refused writes on the restarted former primary. The package refused its configured source endpoint. A separate fixed-endpoint repeat passed six checks on the same JAR. The real packaged JDBC driver reached the original host address after restart and confirmed the former primary UUID and both read-only flags. The packaged preflight then returned `SOURCE_NO_PRIMARY`. The former primary rejoined and caught up as a read-only secondary while the Worker continued on the elected primary. [Group recovery operations](../en/pipeline-v3.md#mysql-84-single-primary-group-replication-recovery) · [Manifests](baseline.json)

### Hive 4.0.1 recovery with original native receipt identity

Artifact `08e755568ee40d57d8e4ccf776a3974bd02c9d6130a686843e428c10734a9e34` passed six native markers on Temurin 21 with MySQL 8.4.13, Hive 4.0.1 and PostgreSQL 17.12. PostgreSQL continued while Hive was stopped. Hive and Worker restart preserved the original snapshot and four exact batch receipts, including their Metastore owner, resource and warehouse location. The SQL sibling's separate failure and replay kept the Hive receipts intact. Removing the Hive business view left an explicit fence. The profile uses one HiveServer2, embedded Derby and a local warehouse. [Hive operations](../en/hive-v3.md) · [Manifests](baseline.json)

### Kafka native review of preceding COMMIT receipts

Artifact `f30a038dd9dee55de64de300e5e62a079af53a8837427f62c9cfb3f1a297327e` passed two native review checks and 45 Java tests. Four Kafka 3.9.2 KRaft brokers preserved both namespace COMMIT offsets and hashes in repeated read-only reviews. A later partial BEGIN produced an uncertain-publication refusal. Failed target ACK remained 1, healthy peer ACK remained 2, and no consumer offset moved. The integrated artifact `62f4a8cd9ce2488812e3732d888e837503bbe769b37558b78cbc31791e587ff7` passed 49 Java tests. [Kafka receipt review](../en/messaging-v3.md) · [Manifests](baseline.json)

### Managed writer enrollment and same-PostgreSQL handoff preparation

Artifact `495a11d365b1492f8e2bb29c0924e4a4888c3492b0cce0ad3af293cc5c85b0f9` passed 20 native checks and 40 focused Java cases. Only the original Agent admitted the explicitly initialized writer. Native CDC preserved the original snapshots and transported a two-database transaction. The standalone transfer path refused the managed writer. After candidate proof, a changed native activation epoch caused preparation to roll back. The valid shared-control transaction verified the writer and data cut, bound a successor and reserved capacity until release. The original Agent retained the writer, business data and acknowledged positions. Integrated artifact `ce3308fe6e559e00c8d2d6d390ea89ece805bec780a2e854410ab7030dc313df` passed 58 focused Java tests. Activation and old-owner retirement are separate. [Handoff operations](../en/global-control.md) · [Manifests](baseline.json)

### External Dameng JDBC on a native Java 21 pipeline

External packaging artifact `520aecc1fff7de42aaf4c51ea45b61d56a2a333675a88008bae56a3b0f3807ce` passed 12 native cases with MySQL 8.4.13, Dameng 8.1.4.80 and PostgreSQL 17.12. The real Java 21 process loaded Connector/J 8.4.0 and DM JDBC 8.1.3.140 from separate operator-supplied JARs. The driver-absent check refused promptly. Real Dameng missing-table error -2106 used the vendor diagnostic. Typed copy and CDC preserved DECIMAL(65,30), UINT64, JSON, binary, TIME and keyless multiplicity. Target restart recovered at the identical localhost endpoint. Worker SIGKILL and independent transactional repairs retained native checkpoints. [Dameng operations](../en/dm-v3.md) · [Manifests](baseline.json)

### Candidate Agent authentication of native MySQL binlog evidence

Artifact `0a51471fa1d9239872dc0319a73facaa06fde55a11bb74a09207b94a53b78c15` passed 15 native and 53 focused Java checks. A real MySQL 8.4.13 transaction hit the configured row limit. The original Agent authenticated the retained wire bytes against the native binlog range. A separately reviewed limit change replayed the exact GTID to PostgreSQL and MySQL with the original configuration fingerprint, journal generation and snapshots. The caught-up task stopped without an open gap. A disjoint mount refused proof. The candidate verified exact archive and receipt bytes on the shared mount, while the original Agent retained its key, ownership and cut. Integrated artifact `b7222b01278873932f76f7b92b082743eeb631c5746a327bc0dcb57263ff37a2` passed 82 focused Java tests. [Raw archive operations](../en/capture-recovery.md) · [Manifests](baseline.json)

### Reviewed readiness for an existing PostgreSQL target writer

Artifact `132db52244df5fa4cdbc0403961c738daad766341ce15465c06f858cc72baaaf` passed six native checks on MySQL 8.4.13 and PostgreSQL 17.12, plus 67 focused Java tests. From an existing adopted table at the original paused cut, the command sealed a durable readiness record tied to the live source freeze and one dedicated PostgreSQL login. It refused ownership, memberships, direct or indirect write privileges and active writer sessions. PostgreSQL restart preserved the original receipt. Changed privileges or source thaw invalidated readiness without replacing it. The operation kept target grants and write authority unchanged. Integrated artifact `b5e543895f8a4423dbd34cf2eea866e14857794f59092cdc5736dd040f349a9f` passed 68 Java tests. [Existing target writer preparation](../en/nonempty-target-preview.md) · [Manifests](baseline.json)

### PostgreSQL civil and UTC microsecond timestamps with two SQL destinations

Artifact `16ebaaea67f73bfb89f1e57921751d8c6ca6d679f9bfdf8d3c8046f0affe5869` passed nine native PostgreSQL 17.12 to PostgreSQL 17.12 and MySQL 8.4.13 scenarios and 55 Java tests. A civil timestamp in the JVM's skipped DST hour retained its exact clock value. Two offsets during a repeated local hour produced distinct UTC values. Snapshot and redo retained microseconds, NULL, primary-key movement and exact keyless duplicates. SIGKILL kept the original source slot and both snapshot sets. One infinite timestamp refused the complete transaction before either destination or the original LSN advanced. Integrated artifact `3e98145dd6abe48467aeaef2e19a16b1af73da99a47189ccd0fef428dbf6ab96` verified packaging and passed 57 focused tests. The intermediate full reactor `5a3c4361588b0e99d07496e9da71cd5a39e06c32669f103d91bd0c2a5357d8a1` passed 559 app and two isolated transport tests, with 34 optional integration skips. [Timestamp mapping and recovery](../en/postgresql-source.md) · [Manifests](baseline.json)

### Internal atomic writer and placement transfer on shared PostgreSQL

The scoped PostgreSQL 17.12 native test passed five checks with synthetic signed admission. A live old lease refused the transition. An induced failure after the joined writes rolled back writer history, epoch, task configuration, owner and preparation. Commit advanced the writer epoch once across both stores. The old signed configuration was visible only to the original Agent directory. The old assignment failed to reacquire its lease and the successor claimed a newer epoch. The integrated application `902fe9ba792da8073d83a6c7966940597cb1d74305cc57ce20d2a56b4bd81ffa` passed 61 Java tests. The native profile exercises the shared storage transaction. Real candidate configuration installation and public transition have separate gates. [Manifests](baseline.json)

### Candidate private report and receipt staging

Artifact `58298164f4ab3a05ad8fc7e54161a3e0541703f073b8cf9fa331fc82bd18d186` passed 19 native tests and 30 focused Java. The prepared Agent staged exact report and receipt bytes under private names. The existing validation CLI refused the staging directory without creating an ordinary report, and the other Controller revalidated the same stage while the shared archive was unavailable. Restart, release and missing original receipt were checked. Original ownership, registry and source and target cuts stayed stable. [Agent preparation operations](../en/global-control.md) · [Manifests](baseline.json)

### Two-entry Kafka native receipt review

Artifact `6bbdfed85c59d6b4b142144f397df163693e56fdb15ed44518cd9a674ab75d1a` passed two native Kafka KRaft cases and 42 focused Java. The authenticated management action stores a reviewed receipt. The second entry replayed the same operation and could query the protected result after the Agent went offline. Native broker offsets and both subscription acknowledgements stayed stable. A later partial BEGIN refused proof. The combined package `d7a0775b9220ea26d18abe76e697a5e97dc9e83f9c97c740082d40c0e1a837b3` passed artifact checks, 72 focused Java cases and 141 DOM tests. Its optional PostgreSQL-native contract was run independently. [Kafka operations](../en/messaging-v3.md) · [Manifests](baseline.json)

### Actual PostgreSQL business writer grants and pre-write failback

Artifact `57da4be48cffc57f6b0a0145be7f416cf40c65455df571dce11a8cf1eb4340da` passed seven native cases on MySQL 8.4.13 and PostgreSQL 17.12 with 68 focused Java tests. At the approved paused adopted cut, one dedicated role obtained the native grants and Rivomesh ordinary resume was fenced. The original request recovered a locally committed grant after a lost response. Failback first revoked PostgreSQL grants. It allowed previously rolled-back DML, rejected active role sessions and refused uncertain unlogged continuity after a real PostgreSQL crash. A committed write that returned rows to their earlier value remained visible in the durable witness after restart and fenced failback. Integrated JAR `23176e1092da24b99381c921688daa4e3ae4595b44f1a2a3469c247a580c7680` passed 68 Java tests. The original MySQL freeze stays protected. Combined restoration of a downgraded constraint at the same adopted cut is tracked separately. [Writer cutover and failback](../en/nonempty-target-preview.md) · [Manifests](baseline.json)

### LG-02 existing PostgreSQL target from constraint review through failback

Artifact `10d0fde41214d6ef5919dde2b9078a071e3b3113112af9e1afbb308433f3d48f` passed four native end-to-end checks and 68 focused Java tests. One keyed existing PostgreSQL 17.12 table with one signed BIGINT business UNIQUE used the same frozen MySQL 8.4.13 cut throughout. The workflow reviewed actual differences, adopted the table, restored the approved UNIQUE and retained the original ordinary index. Unattested constraints and changed physical indexes refused admission. The same restored receipt authorized business writer handoff and pre-write failback. Ordinary CDC then retained the original snapshot and enforced restored UNIQUE. SDD v0.58 accepts this first bounded profile. [Existing target runbook](../en/nonempty-target-preview.md) · [Manifests](baseline.json)

The earlier exact `18d7a77` application passed a full Temurin 21 packaging reactor with 565 app tests and two isolated transport tests, 35 optional skips, and final package checks. The normal package lifecycle builds the relocated GaussDB driver before checking coexistence with other PostgreSQL-protocol drivers. [Manifests](baseline.json)

### Hive external PostgreSQL Metastore, partial native profile

The Hive 4.0.1 fixture uses stock catalog schema 4.0.0 on a dedicated PostgreSQL 17.12 server and a separate PostgreSQL business destination. It passed stock initialization and validation, native snapshot/CDC and coordinated service outage recovery. Catalog bytes and physical PostgreSQL, Hive table, owner, resource and warehouse identity remained identical after the catalog server restarted. The SQL destination advanced during the outage, and Hive caught up when the services returned. Host power logs recorded Clamshell Sleep during Worker restart, after which the test timed out. All named fixtures and private test configurations were cleaned. Worker continuation and subsequent native SQL recovery remain open for this external profile. [Hive external Metastore](../en/hive-v3.md) · [Manifests](baseline.json)

### MySQL DDL comparison in both management entries

Artifact `729fb2106d22572789e52d26d5bb242e7bf03386d90064991fba0431bb0a481a` passed eight native checks and 37 focused Java tests. A real MySQL ALTER stopped capture at the original GTID. Both entries compared the journal schema and current catalog, then showed a later ALTER in the observed source comparison. Both target schemas and the original task checkpoint stayed unchanged. The combined package `0ce387c8dc554b17e70fff3ed82a399c713eeec4ef472dd43d3d2f5c272a7bb3` passed 37 Java, 145 DOM and binary package verification. [Capture review](../en/capture-recovery.md) · [Manifests](baseline.json)

### Console controls for private evidence staging

The staged console candidate `5370a8018ae954e8d2aa7ecd5d4dcaa77538e6e27ae8babff5d889a72c6a67e6` passed 144 DOM and 14 standalone Chrome cases. Six Chrome checks with a synthetic two-entry API passed exact operation replay and revalidation. The two live cluster attempts stopped before the browser staging interaction. Their disposables were cleaned. The independent native staging API proof above covers the exact evidence behavior on real Agents. [Private staging operations](../en/global-control.md) · [Manifests](baseline.json)
