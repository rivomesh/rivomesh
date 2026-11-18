# Rivomesh SDD requirements and incomplete scope

RM-SDD-001 · v0.56 · 2026-11-18 · IMPLEMENTING

Implementation:48requirements,34 DONE, 14 PARTIAL and 0 NEW, with 14 open. RC-01/RC-02/RC-05/RC-03/RC-06/RC-07/RC-08/RC-09/SO-03/SO-04/SO-05/JV-01 bounded profiles are accepted. Automatic takeover, broader source recovery and complete matrices remain tracked.

[Baseline/decisions](plan.en.md) · [Tasks](tasks.en.md) · [Acceptance](acceptance.en.md) · [中文](spec.md)

## Unified cluster control

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| CP-01 | P0 / DONE | Global node registry and state | Shared registry passes PostgreSQL/MySQL two-node,kill/rejoin and storage-outage acceptance;global task authority/commands remain CP-02/03. |
| CP-02 | P0 / DONE | Global task/configuration authority | Both control stores verified cross-entry configuration revisions, global task-ID uniqueness, physical source/configuration isolation and standalone restart with the original task. |
| CP-03 | P0 / DONE | Durable cross-node command routing | Seven native checks per control store covered cross-entry lifecycle, replay, pending HTTP 202, rejection on revocation, and UNKNOWN across Agent generations. Prior native evidence covers target retry, validation and repair. |
| CP-04 | P0 / PARTIAL | Failover,fencing and placement | Both entries and both Agents verified original journal, source, target, reports and bounded closed receipts. Capacity reservations, source/failure evidence, dual fencing and actual placement commit remain. |
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
| RC-10 | P1 / PARTIAL | Destination-specific recovery and retention | PostgreSQL and MySQL verified exact original-event pins for open gaps and pending skips, backpressure, verified reclamation and independent target progress. Analytical and messaging receipt and gap-notification protocols remain. |

## Source compatibility

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| SO-01 | P0 / DONE | Source protocol and capability configuration | The V3 console offers capability-scoped MySQL, PostgreSQL and Oracle source forms, mappings and versions. Each preflight uses its own identity and position protocol and rejects foreign source options. |
| SO-02 | P1 / DONE | MySQL5.7 supported profile | 5.7.44 Java 8 V3 standalone, GTID async, MGR, and native binlog-loss refusal passed. |
| SO-03 | P1 / DONE | MySQL8.0 current V3 acceptance | MySQL 8.0.46 V3 standalone, GTID async, single-primary MGR and distributed compensation/correction are accepted. JV-02 tracks remaining Java 21 combinations. |
| SO-04 | P1 / DONE | MySQL8.2 compatibility and acceptance | MySQL 8.2.0 Java 8 V3 standalone, GTID async and single-primary MGR are accepted. JV-02 tracks remaining Java 21 combinations. |
| SO-05 | P1 / DONE | MySQL8.4 expanded compatibility | MySQL 8.4.13 V3 topologies and GTID/ROW/FULL admission and refusal are accepted. Additional destination combinations are tracked separately. |
| SO-06 | P1 / PARTIAL | PostgreSQL source adapter | PostgreSQL 17.12 passed snapshot, failover-slot continuation and old-timeline refusal. 18.7 passed stored generated columns. Broader types, DDL and multi-target common cuts remain. |
| SO-07 | P1 / PARTIAL | Oracle source adapter | Temurin 21 verified Oracle 26ai single-PDB long transactions, archived SCN restart, strict text JSON and civil DATE to PostgreSQL. Native binary JSON, LOB, generated columns and RAC failover remain. |

## Java runtime compatibility

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| JV-01 | P0 / DONE | Temurin 21 first-release runtime | The current line builds Java 21 bytecode. Two Controllers, Agent, Worker and live MySQL fan-out were verified. JV-02 through JV-04 track connector and deployment profiles. |
| JV-02 | P1 / PARTIAL | Java 21 source, sink and recovery profiles | Java 21 passed four single-primary MySQL releases, MySQL 8.4.13 async GTID primary failover with two SQL destinations, PostgreSQL 17/18 and single-PDB Oracle profiles. More topologies, destinations, TLS and recovery combinations remain. |
| JV-03 | P1 / PARTIAL | Driver isolation,dependencies and TLS compatibility | Java 21 verified external MySQL JDBC, PostgreSQL endpoint TLS, and MySQL snapshot/binlog/destination TLS 1.3 with certificate identity refusal and reconnect. Further vendors and profiles remain. |
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
| LG-01 | P1 / PARTIAL | Domestic transactional database matrix | OceanBase CE 4.3.5.6 MySQL tenant passed native transactions and mixed restart recovery. Distributed GaussDB, OceanBase Oracle mode and GBase 8a/8s remain. |
| LG-02 | P1 / PARTIAL | Nonempty adoption,constraints,cutover/failback | Nonempty comparison and actual frozen-MySQL UNIQUE restoration with a same-database PostgreSQL journal passed, including rollback and uncertain-commit reconciliation. Formal adoption, multi-target constraints, writer handover and failback remain. |
| LG-03 | P1 / PARTIAL | ClickHouse/Doris/Hive production profiles | Bounded materialization and recovery profiles include OceanBase MySQL tenant outage catch-up, independent SQL repair and missing-table isolation. Clusters, TLS, capacity and remaining JDK combinations need verification. |
| LG-04 | P1 / PARTIAL | Queue cluster and recovery profiles | Native NATS TLS, RabbitMQ quorum, Kafka KRaft and Redis Streams external promotion of the original AOF replica with consumer recovery passed. Other cluster, TLS and capacity profiles remain. |
| LG-05 | P0 / PARTIAL | Optional storage and cross-node reachability | Local and external journals are available. Two Agents verified exact report and closed receipt custody on a shared mount. Original events, failure evidence and actual transfer remain. |
| LG-06 | P1 / DONE | Live K8s/CSI and node-fault acceptance | Live ARM64 accepted Helm and native YAML image replacement, same-claim RWO CSI relocation and automatic CDC. Separate native cases cover physical worker loss, Secret rotation, external-journal takeover and stale Writer fencing. |
| LG-07 | P1 / DONE | Large catalogs,transactions and hardware standards | The bounded Temurin 21 resource profile is accepted, with 12 hardware trials, 256 mapped databases, a transaction over 8 MiB and slow-target backpressure plus restart recovery through a separate journal. |
| LG-08 | P1 / PARTIAL | Historical DDL,types,indexes and state migration | Allowlisted objects and UNIQUE/GSI conversions exist. Java 21 verified coordinated LOCAL journal and PostgreSQL backup/restore, original MySQL 8.4.13 continuation and identity refusal. Broader DDL, types and other products remain. |
| LG-09 | P1 / PARTIAL | Report retention and real-browser acceptance | Historical report/receipt lookup, paging, correction and repair passed across entries. Configured minimum retention, reviewed holds and exact file preservation passed. Reversible final-report archival and reclamation remain. |
| LG-10 | P2 / PARTIAL | External identity,immutable audit and Apache preparation | Contribution guide, CycloneDX SBOM and packaged vendor inventory are in place. Distribution review, project LICENSE/NOTICE, external identity and audit anchors remain. |

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
