# Rivomesh SDD requirements and incomplete scope

RM-SDD-001 · v0.59 · 2026-11-19 · IMPLEMENTING

Implementation:48requirements,36 DONE, 12 PARTIAL and 0 NEW, with 12 open. RC-01/RC-02/RC-05/RC-03/RC-06/RC-07/RC-08/RC-09/SO-03/SO-04/SO-05/JV-01 bounded profiles are accepted. Automatic takeover, broader source recovery and complete matrices remain tracked.

[Baseline/decisions](plan.en.md) · [Tasks](tasks.en.md) · [Acceptance](acceptance.en.md) · [中文](spec.md)

## Unified cluster control

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| CP-01 | P0 / DONE | Global node registry and state | Shared registry passes PostgreSQL/MySQL two-node,kill/rejoin and storage-outage acceptance;global task authority/commands remain CP-02/03. |
| CP-02 | P0 / DONE | Global task/configuration authority | Both control stores verified cross-entry configuration revisions, global task-ID uniqueness, physical source/configuration isolation and standalone restart with the original task. |
| CP-03 | P0 / DONE | Durable cross-node command routing | Seven native checks per control store covered cross-entry lifecycle, replay, pending HTTP 202, rejection on revocation, and UNKNOWN across Agent generations. Prior native evidence covers target retry, validation and repair. |
| CP-04 | P0 / PARTIAL | Failover,fencing and placement | Native tests cover same-store dual-Agent transfer, evidence, dual SQL fencing, approved Worker continuation and an approved drained handoff after Controller loss. When the original Agent is killed, both entries distinguish the expired node lease from unknown task state. Same-directory restart first executes the queued STOP. Explicit START consumes the retained backlog while former Agent and capture epochs remain fenced. A native control and journal path partition stopped the former Worker. The original process completed queued STOP after reconnecting. Explicit START consumed the backlog once on each target. Permanent loss, in-flight transactions, independently reachable stores and broader drain acceptance remain. |
| CP-05 | P0 / DONE | Shared authorization,audit and secret references | Both control stores verified cross-entry revocation, audit correlation by request/task/Agent/plan and real browser redaction. The optional Agent-level policy passes only task-authorized and actually used references to each Worker. |
| CP-06 | P1 / DONE | Cluster logs,metrics and diagnostics | Distributed diagnostics verified node/task/destination scope, paging, restart, redaction and bounded reads. Native PostgreSQL and direct-origin MySQL 8.4 expose verified commit lag. Unavailable timing is explicitly unknown. |
| CP-07 | P0 / DONE | Control-plane HA and worker independence | Protocol4 separated profile accepted on PG/MySQL control stores,all-controller loss/pending-command recovery,store outage,same-Worker continuation and nonroot runtime;default combined remains coupled and failover/other storage-adapter profiles are separate. |

## Persistent login

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| SE-01 | P0 / DONE | Session survives page reload | Single-controller reload/new-page restoration complete;cross-controller continuity remains SE-04. |
| SE-02 | P0 / DONE | Session expiry,logout and revocation | Expiry,logout,directory-revision revocation and optional browser persistence complete;memory sessions expire on controller restart. |
| SE-03 | P0 / DONE | Secure session transport and CSRF protection | Protected cookies,Origin/CSRF and real TLS-proxy acceptance complete;the proxy terminates production TLS. |
| SE-04 | P0 / DONE | Session continuity across controllers | Shared SQL sessions passed two-controller HTTPS/revocation/kill/reopen/store-outage acceptance on PostgreSQL/MySQL; global task control remains CP-02/03. |
| SE-05 | P1 / DONE | CLI/API compatibility and identity conflicts | Bearer compatibility,no-fallback/conflict rules,grant checks,logout races and self-rotation complete. |

## Manual recovery,skip and corrective data

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| RC-01 | P0 / DONE | Phase-aware fault classification | Bounded MySQL source and SQL destination failure phases are accepted. RC-04 tracks source range exclusion. |
| RC-02 | P0 / DONE | Failed-transaction inspection and evidence retention | Accepted bounded MySQL8.4,fixed original Agent/LOCAL journal,PG/MySQL control profile:ADMIN whole-transaction review,positions/historical columns/images,authorized pages and durable evidence surviving restart/journal GC. Catalog automation,other profiles,Agent migration and execution semantics remain separately tracked. |
| RC-03 | P0 / DONE | Skip a complete destination transaction | Protocol7 bounded profile accepted:Java8,MySQL8.4 two-database source,original Agent/LOCAL journal,PostgreSQL business target and PG/MySQL control stores. Complete transaction skip,native commit/source ACK failure,Agent SIGKILL/reopen and cross-entry recovery pass. Other targets/full matrices remain RC-10/LG-01. |
| RC-04 | P0 / DONE | Controlled capture/decode gap recovery | Accepted for one complete MySQL 8.4 row transaction. The workflow verifies affected targets, retained bytes, explicit approval, durable gap and continuation. Keyless, other protocols and unprovable ranges require log recovery or a reviewed rebuild. |
| RC-05 | P0 / DONE | Uncertain commit and checkpoint reconciliation | Protocol6 original-Agent/LOCAL-journal bounded profile accepted on PG/MySQL control stores:whole-task pause,fresh evidence,native fencing,cross-entry ADMIN confirmation,atomic ACK/receipt and idempotent retry,with both pauses retained. Skip,compensation and full additional matrices remain separate. |
| RC-06 | P0 / DONE | Failed-transaction before-image compensation | The bounded SQL profile passed. Administrators review table scope and mutation counts across entries, restore the before-images and explicitly resume the original transaction. PostgreSQL passes real CDC and MySQL passes native contracts. RC-10 tracks other destinations. |
| RC-07 | P0 / DONE | Guided manual corrective data entry | The bounded SQL profile passed. Typed entry and JSON import support administrator review of before/after values, exact identities, atomic batches and resumption of the original transaction. PostgreSQL passes real CDC and MySQL passes native checks. RC-10 tracks other destinations. |
| RC-08 | P0 / DONE | Consistency-gap ledger and honest watermarks | First SQL profile accepted:durable gaps,reviewed plans,cross-entry approval/cancellation,fresh full-table scans,atomic receipts and distinct safe coverage/contiguous application. Verification preserves SKIP and the original applied prefix without resuming. Other target protocols remain RC-10/LG-03/LG-04. |
| RC-09 | P0 / DONE | Durable distributed recovery jobs | The bounded SQL profile passes cross-entry planning, approval, cancellation and original-request recovery. Native commit-boundary tests use PostgreSQL 17 as the recovery destination. |
| RC-10 | P1 / PARTIAL | Destination-specific recovery and retention | Both Kafka entries review physical topics and replay the blocked approval. A managed Worker records the Topic UUID, native offset, Agent membership lease and journal UTC for the first eligible newly observed COMMIT. Native Kafka with PostgreSQL, MySQL and H2 journals passed. Both management entries can verify the signed receipt of the current failure's exact preceding ACK. Joint native Kafka readback, complete historical coverage, dispatch and recovery, and cross-adapter compensation remain. |

## Source compatibility

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| SO-01 | P0 / DONE | Source protocol and capability configuration | The V3 console offers capability-scoped MySQL, PostgreSQL and Oracle source forms, mappings and versions. Each preflight uses its own identity and position protocol and rejects foreign source options. |
| SO-02 | P1 / DONE | MySQL5.7 supported profile | 5.7.44 Java 8 V3 standalone, GTID async, MGR, and native binlog-loss refusal passed. |
| SO-03 | P1 / DONE | MySQL8.0 current V3 acceptance | MySQL 8.0.46 V3 standalone, GTID async, single-primary MGR and distributed compensation/correction are accepted. JV-02 tracks remaining Java 21 combinations. |
| SO-04 | P1 / DONE | MySQL8.2 compatibility and acceptance | MySQL 8.2.0 Java 8 V3 standalone, GTID async and single-primary MGR are accepted. JV-02 tracks remaining Java 21 combinations. |
| SO-05 | P1 / DONE | MySQL8.4 expanded compatibility | MySQL 8.4.13 V3 topologies and GTID/ROW/FULL admission and refusal are accepted. Additional destination combinations are tracked separately. |
| SO-06 | P1 / PARTIAL | PostgreSQL source adapter | Temurin 21 verified PostgreSQL 17.12 logical slot promotion, UUID and two-target DATE, microsecond timestamp and TIME(6) recovery, plus 18.7 stored generated columns. Further types, dynamic DDL and common-cut verification remain. A native read-only comparison shows the current PostgreSQL publication while capture stays at the pre-change LSN. PostgreSQL 17.12 extended NUMERIC scales passed PostgreSQL and MySQL native delivery, including source rounding, 65 digits, keyless multiplicity and whole-transaction refusal of NaN. |
| SO-07 | P1 / PARTIAL | Oracle source adapter | Oracle 26ai single-PDB archive CDC to PostgreSQL covers long transactions, SCN restart, strict text JSON, civil DATE and opt-in bounded virtual integer multiplication, addition and subtraction. Keyless duplicates, SQL NULL and original-snapshot archive replay passed. Other expressions, native binary JSON, LOB and RAC failover remain. Native Oracle Free evidence shows incomplete LOB before-images for BasicFiles and SecureFiles changes. Restricted-role preflight exposes the matching refusal. Full LOB capture remains open. Native archived CDC covers nested savepoints and partial rollback. Both PostgreSQL subscriptions reuse their original snapshots and SCN after process kills, and committed rows match Oracle Flashback at the same SCN. |

## Java runtime compatibility

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| JV-01 | P0 / DONE | Temurin 21 first-release runtime | The current line builds Java 21 bytecode. Two Controllers, Agent, Worker and live MySQL fan-out were verified. JV-02 through JV-04 track connector and deployment profiles. |
| JV-02 | P1 / PARTIAL | Java 21 source, sink and recovery profiles | Temurin 21 LTS is the sole first-release runtime. Native evidence covers four standalone MySQL versions, MySQL 8.4.13 async GTID continuation to two targets and three-node single-primary Group Replication, PostgreSQL 17/18, and single-PDB Oracle. Additional destinations, TLS and recovery profiles remain in the Temurin 21 matrix. |
| JV-03 | P1 / PARTIAL | Driver isolation,dependencies and TLS compatibility | Java 21 verified MySQL/PostgreSQL TLS and reconnect, external Dameng native SQL repair, and external Kingbase native authentication, precision, reconnect and receipt continuation. Five external JDBC paths have verified classpath admission. Further vendor TLS and cluster profiles remain. Pulsar 4.0.14 passed a single-broker private-CA and JWT profile, wrong-host refusal and broker restart. |
| JV-04 | P1 / DONE | Java 21 deployment and rolling replacement | Temurin 21 passed manual Linux startup, nonroot Docker with native data, Helm and native Kubernetes. The same build passed the release evidence gate. Rolling replacement and original-directory resume have live evidence. |

## Complete documentation

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| DO-01 | P0 / DONE | Bilingual traceable SDD baseline | All 48 requirements, tasks and verdicts are aligned by stable ID. The verifier checks bilingual criteria, archived evidence and progress totals. |
| DO-02 | P1 / DONE | Deployment and cluster operations docs | Bilingual instructions cover four standalone and cluster deployment paths, stores, sessions, node maintenance, backups and upgrades. Cross-node volumes and automatic takeover continue under live platform requirements. |
| DO-03 | P1 / DONE | Recovery scenarios and FAQ | The bilingual runbook covers promotion, log loss, SQL/ack, node, storage, sessions, manual recovery and cutover with actions and product boundaries. |
| DO-04 | P1 / DONE | Configuration/API/compatibility and upgrade docs | Bilingual reference covers configuration, API, grants, coordinates, consistency, upgrades and cross-JDK evidence. Connector combinations continue under their corresponding compatibility requirements. |
| DO-05 | P1 / DONE | Documentation and evidence release gates | The gate verifies candidate SHA, 25 examples, bilingual docs, real Chrome, separate-Agent native recovery and both live Kubernetes paths. A different candidate manifest is rejected. |

## Preserved earlier scope

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| LG-01 | P1 / PARTIAL | Domestic transactional database matrix | OceanBase CE 4.3.5.6 MySQL tenant passed native transactions and mixed restart recovery. Distributed GaussDB, OceanBase Oracle mode and GBase 8a/8s remain. Native openGauss 7.0.0-RC3 mode A verified catalog parity, composite keys and original-endpoint target restart. Commercial distributed deployments require their own acceptance. |
| LG-02 | P1 / DONE | Nonempty adoption,constraints,cutover/failback | Bounded single-table PostgreSQL profile accepted. At one frozen MySQL cut the fixture reviewed differences, adopted, restored an approved BIGINT UNIQUE, transferred business writes, performed pre-write failback and reused the original snapshot. A second profile at the same cut verifies binary, NO PAD VARCHAR UNIQUE(email), retains the ordinary index and continues CDC. Broader products and post-write reverse migration are separate. |
| LG-03 | P1 / PARTIAL | ClickHouse/Doris/Hive production profiles | ClickHouse, Doris and embedded Hive have native recovery evidence. Hive 4.0.1 with an external PostgreSQL Metastore passed snapshot, CDC, original catalog identity, service restart, original-snapshot Worker recovery, four batch receipts and isolated SQL error replay. Distributed storage, production clusters, old backup recovery and capacity remain. |
| LG-04 | P1 / PARTIAL | Queue cluster and recovery profiles | Native NATS TLS, RabbitMQ quorum, Kafka KRaft and Redis Streams external promotion and consumer recovery passed. Pulsar 4.0.14 covers single-broker TLS and Token and a two-broker, three-bookie profile with original topic recovery, withheld confirmations below quorum and stable-event retry. Two-broker private TLS and Token, and new metadata after loss of a leader in the three-node ZooKeeper quorum, also passed. Cross-host storage failure domains, internal network cryptography, capacity and other queue profiles remain. The Artemis 2.57.0 shared-store backup activated after primary SIGKILL. The same ingress reused the native receipt and queue identity and confirmed the same retry. ActiveMQ Classic 5.19.12 passed shared KahaDB failover. The packaged publisher uses stable OpenWire and Jolokia entries to verify the original ownership and receipt and confirm the exact retry. The restarted former primary stays passive. |
| LG-05 | P0 / PARTIAL | Optional storage and cross-node reachability | Optional journals and combined PostgreSQL same-database custody passed. During independent journal loss, the candidate keeps its control lease and cannot prove a native cut. After reconnect it can recheck and stage offline history. Both entries refuse cross-database writer transfer. Release lets the original Agent continue to both targets from its original cut. Durable split-store fencing and other storage and destination profiles remain. |
| LG-06 | P1 / DONE | Live K8s/CSI and node-fault acceptance | Live ARM64 accepted Helm and native YAML image replacement, same-claim RWO CSI relocation and automatic CDC. Separate native cases cover physical worker loss, Secret rotation, external-journal takeover and stale Writer fencing. |
| LG-07 | P1 / DONE | Large catalogs,transactions and hardware standards | The bounded Temurin 21 resource profile is accepted, with 12 hardware trials, 256 mapped databases, a transaction over 8 MiB and slow-target backpressure plus restart recovery through a separate journal. |
| LG-08 | P1 / PARTIAL | Historical DDL,types,indexes and state migration | Allowlisted objects, UNIQUE/GSI policies and coordinated MySQL-to-PostgreSQL restore have native evidence. Two management entries expose read-only MySQL DDL differences between recorded schema and current catalog. Reviewed DDL execution, more types and other products remain. PostgreSQL 17.12 relation drift retains the original LSN. Read-only catalog review includes native type modifiers, existing column order and collected index definitions. JDBC regressions cover reordered identity UNIQUE and pre-DDL refusal of global primary or identity indexes. Managed Gauss preflight for MySQL reports complete plan counts and bounded details. Native commercial multi-CN/DN and target catalog behavior still need deployment validation. openGauss 7.0.0-RC3 verified UNIQUE(email) downgrade and stable index OIDs while retaining business and receipt primary keys. |
| LG-09 | P1 / DONE | Report retention and real-browser acceptance | Dual-entry report/receipt discovery, approvals, keyboard pagination, archival and exact restore passed. The bounded reclaim profile removes only an archived duplicate row file after stop, retention, digest and active-lock checks. |
| LG-10 | P2 / PARTIAL | External identity,immutable audit and Apache preparation | The checkpoint and complete original audit events can be verified independently and retained with S3 Object Lock. Local MinIO verified exact versions, COMPLIANCE retention, identical retries, continuity and offline retrieval. SBOM and five external JDBC drivers have evidence. Live AWS permissions, source rights, LICENSE/NOTICE and external identity remain. Exact external-vendor package inventory identifies 151 packaged coordinates and 46 absent from the Maven BOM. Explicit distribution review blocks unidentified embedded JARs. The fixed HTTPS OIDC provider and Accounts UI binding and revocation passed the local fixture. Both shared-store Controllers enforce local account grants and revocation. Production identity providers, ingress and provider-driven revocation remain. |

## Baseline evidence index

| ID | Existing evidence/code (not completion) |
|---|---|
| CP-01 | [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) / [TaskWorker.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskWorker.java) |
| CP-02 | P0 / DONE | Global task/configuration authority | Both control stores verified cross-entry configuration revisions, global task-ID uniqueness, physical source/configuration isolation and standalone restart with the original task. |
| CP-03 | P0 / DONE | Durable cross-node command routing | Seven native checks per control store covered cross-entry lifecycle, replay, pending HTTP 202, rejection on revocation, and UNKNOWN across Agent generations. Prior native evidence covers target retry, validation and repair. |
| CP-04 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) |
| CP-05 | P0 / DONE | Shared authorization,audit and secret references | Both control stores verified cross-entry revocation, audit correlation by request/task/Agent/plan and real browser redaction. The optional Agent-level policy passes only task-authorized and actually used references to each Worker. |
| CP-06 | P1 / DONE | Cluster logs,metrics and diagnostics | Distributed diagnostics verified node/task/destination scope, paging, restart, redaction and bounded reads. Native PostgreSQL and direct-origin MySQL 8.4 expose verified commit lag. Unavailable timing is explicitly unknown. |
| CP-07 | [README.md](../../../docs/verification/README.md) / [OwnerRuntime.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/OwnerRuntime.java) |
| SE-01 | [app.js](../../../sync-app/src/main/resources/console/app.js) |
| SE-02 | [app.js](../../../sync-app/src/main/resources/console/app.js) / [account-management.md](../../../docs/zh-CN/account-management.md) |
| SE-03 | [app.js](../../../sync-app/src/main/resources/console/app.js) / [ConsoleServer.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/ConsoleServer.java) |
| SE-04 | [README.md](../../../docs/verification/README.md) / [browser-sessions.md](../../../docs/en/browser-sessions.md) |
| SE-05 | [app.js](../../../sync-app/src/main/resources/console/app.js) / [AccessControl.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AccessControl.java) |
| RC-01 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [FanoutDispatcher.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/FanoutDispatcher.java) |
| RC-02 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [managed-validation.md](../../../docs/zh-CN/managed-validation.md) |
| RC-03 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [JdbcSink.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcSink.java) |
| RC-04 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [reconciliation-workflow.md](../../../docs/zh-CN/reconciliation-workflow.md) |
| RC-05 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [FanoutDispatcher.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/FanoutDispatcher.java) / [README.md](../../../docs/verification/README.md) |
| RC-06 | [transaction-compensation.md](../../../docs/en/transaction-compensation.md) / [TransactionCompensation.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/TransactionCompensation.java) |
| RC-07 | [manual-correction.md](../../../docs/en/manual-correction.md) / [CorrectionBatch.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/CorrectionBatch.java) |
| RC-08 | [gap-verification.md](../../../docs/en/gap-verification.md) / [README.md](../../../docs/verification/README.md) |
| RC-09 | [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) / [README.md](../../../docs/verification/README.md) |
| RC-10 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [messaging-v3.md](../../../docs/zh-CN/messaging-v3.md) |
| SO-01 | [AdapterCatalog.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AdapterCatalog.java) / [SourceAdapter.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/SourceAdapter.java) |
| SO-02 | [validation.md](../../../docs/validation.md) / [matrix.sh](../../../tests/matrix.sh) |
| SO-03 | P1 / DONE | MySQL8.0 current V3 acceptance | MySQL 8.0.46 V3 standalone, GTID async, single-primary MGR and distributed compensation/correction are accepted. JV-02 tracks remaining Java 21 combinations. |
| SO-04 | P1 / DONE | MySQL8.2 compatibility and acceptance | MySQL 8.2.0 Java 8 V3 standalone, GTID async and single-primary MGR are accepted. JV-02 tracks remaining Java 21 combinations. |
| SO-05 | P1 / DONE | MySQL8.4 expanded compatibility | MySQL 8.4.13 V3 topologies and GTID/ROW/FULL admission and refusal are accepted. Additional destination combinations are tracked separately. |
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
