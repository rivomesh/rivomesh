# Rivomesh SDD requirements and incomplete scope

RM-SDD-001 · v0.9 · 2026-11-17 · IMPLEMENTING

Implementation:48requirements,10DONE,32PARTIAL,6NEW;38open. RC-02/RC-05/RC-03 bounded profiles are accepted;verified gap closure,automatic takeover,compensation and complete matrices remain tracked.

[Baseline/decisions](plan.en.md) · [Tasks](tasks.en.md) · [Acceptance](acceptance.en.md) · [中文](spec.md)

## Unified cluster control

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| CP-01 | P0 / DONE | Global node registry and state | Shared registry passes PostgreSQL/MySQL two-node,kill/rejoin and storage-outage acceptance;global task authority/commands remain CP-02/03. |
| CP-02 | P0 / PARTIAL | Global task/configuration authority | Opt-in fixed-owner global HTTP/UI,shared configuration and history are wired;legacy import,complete business and scale acceptance remain open. |
| CP-03 | P0 / PARTIAL | Durable cross-node command routing | Public durable commands,offline STOP,actual multi-target validation/repair and original UNKNOWN upgrade recovery accepted;complete cancellation/fault/migration/capacity matrices remain open. |
| CP-04 | P0 / PARTIAL | Failover,fencing and placement | Worker leases/epochs exist;global scheduling,remote lifecycle and rolling-upgrade protocols do not. |
| CP-05 | P0 / PARTIAL | Shared authorization,audit and secret references | Shared accounts/grants/hash-chained audit and Worker environment filtering exist; complete remote correlation, least-secret references and process/filesystem isolation remain open. |
| CP-06 | P1 / PARTIAL | Cluster logs,metrics and diagnostics | Global observations,owner/unknown state,pagination and maintenance results exist;complete node/target filters,remote logs and diagnostics search remain open. Worker process liveness versus journal-claim readiness needs clearer presentation. |
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
| RC-01 | P0 / PARTIAL | Phase-aware fault classification | Initial destination preparation/snapshot and delivery phases pass dual-entry native fault/restart acceptance;complete source phases and manual-action diagnostics remain open. |
| RC-02 | P0 / DONE | Failed-transaction inspection and evidence retention | Accepted bounded MySQL8.4,fixed original Agent/LOCAL journal,PG/MySQL control profile:ADMIN whole-transaction review,positions/historical columns/images,authorized pages and durable evidence surviving restart/journal GC. Catalog automation,other profiles,Agent migration and execution semantics remain separately tracked. |
| RC-03 | P0 / DONE | Skip a complete destination transaction | Protocol7 bounded profile accepted:Java8,MySQL8.4 two-database source,original Agent/LOCAL journal,PostgreSQL business target and PG/MySQL control stores. Complete transaction skip,native commit/source ACK failure,Agent SIGKILL/reopen and cross-entry recovery pass. Other targets/full matrices remain RC-10/LG-01. |
| RC-04 | P0 / NEW | Controlled capture/decode gap recovery | Destination skipping cannot fix failures before complete events exist. |
| RC-05 | P0 / DONE | Uncertain commit and checkpoint reconciliation | Protocol6 original-Agent/LOCAL-journal bounded profile accepted on PG/MySQL control stores:whole-task pause,fresh evidence,native fencing,cross-entry ADMIN confirmation,atomic ACK/receipt and idempotent retry,with both pauses retained. Skip,compensation and full additional matrices remain separate. |
| RC-06 | P0 / NEW | Failed-transaction before-image compensation | COMMON_CUT repair exists but does not reconstruct the failed transaction pre-state. |
| RC-07 | P0 / PARTIAL | Guided manual corrective data entry | Report-driven repair exists without independent failed-transaction/manual patches. |
| RC-08 | P0 / PARTIAL | Consistency-gap ledger and honest watermarks | Durable plans/native SKIP receipts/gap ledger and public warnings pass native PG/MySQL contracts and real recovery on both control stores. Normal APPLY retains gaps. Verified gap closure,compensation and complete destination semantics remain open. |
| RC-09 | P0 / PARTIAL | Durable distributed recovery jobs | COMMON_CUT has local durable operations/receipts;distributed/new modes do not. |
| RC-10 | P1 / PARTIAL | Destination-specific recovery and retention | SQL,analytical and queue consistency protocols differ. |

## Source compatibility

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| SO-01 | P0 / PARTIAL | Source protocol and capability configuration | Neutral events/positions exist but factories/UI remain MySQL-oriented. |
| SO-02 | P1 / PARTIAL | MySQL5.7 supported profile | 5.7.44 basic evidence does not cover the required current-platform object/recovery/runtime matrix. |
| SO-03 | P1 / PARTIAL | MySQL8.0 current V3 acceptance | Historical8.0.46 V2/HA evidence is not current full V3 acceptance. |
| SO-04 | P1 / NEW | MySQL8.2 compatibility and acceptance | No dedicated complete8.2 native evidence found;8.0/8.4 cannot substitute. |
| SO-05 | P1 / PARTIAL | MySQL8.4 expanded compatibility | Representative8.4 V3/recovery evidence is not every log/auth/topology/destination combination. |
| SO-06 | P1 / NEW | PostgreSQL source adapter | Catalog says PLANNED;a PG sink driver is not a PG source. |
| SO-07 | P1 / NEW | Oracle source adapter | Catalog says PLANNED;no native snapshot/CDC implementation. |

## Java runtime compatibility

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| JV-01 | P0 / PARTIAL | Java8/11/17/21/25 runnable releases | Java8 baseline and an older ten-profile SQL matrix exist;current full release is not covered. |
| JV-02 | P1 / PARTIAL | Cross-JDK source/sink/recovery matrix | Ten profiles cover one keyed ASCII table/PG sink,not all adapters. |
| JV-03 | P1 / PARTIAL | Driver isolation,dependencies and TLS compatibility | Vendor isolation/artifact checks exist without all-JDK coverage. |
| JV-04 | P1 / NEW | Multi-JDK deployment and mixed-version upgrades | Benchmarks select JREs;production/mixed-cluster upgrade policies are not accepted. |

## Complete documentation

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| DO-01 | P0 / PARTIAL | Bilingual traceable SDD baseline | This change creates the v0.5 inventory;module designs,spec change control and implementation evidence closure remain open. |
| DO-02 | P1 / PARTIAL | Deployment and cluster operations docs | Existing deployment artifacts do not cover new cluster control/session models. |
| DO-03 | P1 / PARTIAL | Recovery scenarios and FAQ | Existing reconciliation FAQ does not cover new skip,patch and distributed faults. |
| DO-04 | P1 / PARTIAL | Configuration/API/compatibility and upgrade docs | Partial handbooks lack new APIs and a unified profile index. |
| DO-05 | P1 / PARTIAL | Documentation and evidence release gates | Archives/hash/preview checks exist without a complete requirement release gate. |

## Preserved earlier scope

| ID | Priority/status | Requirement | Gap |
|---|---|---|---|
| LG-01 | P1 / PARTIAL | Domestic transactional database matrix | Initial openGauss/DM/Kingbase/GBase8c profiles exist;distributed GaussDB,complete OceanBase recovery and GBase8a/8s remain open. |
| LG-02 | P1 / PARTIAL | Nonempty adoption,constraints,cutover/failback | Initial destinations must be empty;repair completion is not cutover. |
| LG-03 | P1 / PARTIAL | ClickHouse/Doris/Hive production profiles | Initial materialization/recovery exists;cluster/security/capacity/JDK/new recovery profiles remain open. |
| LG-04 | P1 / PARTIAL | Queue cluster and recovery profiles | Initial transports exist for all confirmed queues;complete cluster/TLS/capacity coverage is open. |
| LG-05 | P0 / PARTIAL | Optional storage and cross-node reachability | Local/external journals exist;authority/reports/sessions/control-store relocation and HA boundaries are not unified. |
| LG-06 | P1 / PARTIAL | Live K8s/CSI and node-fault acceptance | Helm/native static checks do not establish live-cluster safety. |
| LG-07 | P1 / PARTIAL | Large catalogs,transactions and hardware standards | Small benchmark tools exist;large-transaction/catalog/cache/capacity standards remain incomplete. |
| LG-08 | P1 / PARTIAL | Historical DDL,types,indexes and state migration | Supported objects/index conversion and v2 compatibility exist;full dynamic DDL,types and explicit migration/rollback remain open. |
| LG-09 | P1 / PARTIAL | Report retention and real-browser acceptance | Summaries/details/DOM/HTTP exist;full catalogs,cleanup,visual/accessibility evidence do not. |
| LG-10 | P2 / PARTIAL | External identity,immutable audit and Apache preparation | Local accounts/hash-chained audit exist;external identity/anchors,licenses/NOTICE/SBOM/contribution workflows remain open. |

## Baseline evidence index

| ID | Existing evidence/code (not completion) |
|---|---|
| CP-01 | [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) / [TaskWorker.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskWorker.java) |
| CP-02 | P0 / PARTIAL | Global task/configuration authority | Opt-in fixed-owner global HTTP/UI,shared configuration and history are wired;legacy import,complete business and scale acceptance remain open. |
| CP-03 | P0 / PARTIAL | Durable cross-node command routing | Public durable commands,offline STOP,actual multi-target validation/repair and original UNKNOWN upgrade recovery accepted;complete cancellation/fault/migration/capacity matrices remain open. |
| CP-04 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) |
| CP-05 | P0 / PARTIAL | Shared authorization,audit and secret references | Shared accounts/grants/hash-chained audit and Worker environment filtering exist; complete remote correlation, least-secret references and process/filesystem isolation remain open. |
| CP-06 | P1 / PARTIAL | Cluster logs,metrics and diagnostics | Global observations,owner/unknown state,pagination and maintenance results exist;complete node/target filters,remote logs and diagnostics search remain open. Worker process liveness versus journal-claim readiness needs clearer presentation. |
| CP-07 | P0 / DONE | Control-plane HA and worker independence | Protocol4 separated profile accepted on PG/MySQL control stores,all-controller loss/pending-command recovery,store outage,same-Worker continuation and nonroot runtime;default combined remains coupled and failover/other storage-adapter profiles are separate. |
| SE-01 | [app.js](../../../sync-app/src/main/resources/console/app.js) |
| SE-02 | [app.js](../../../sync-app/src/main/resources/console/app.js) / [account-management.md](../../../docs/zh-CN/account-management.md) |
| SE-03 | [app.js](../../../sync-app/src/main/resources/console/app.js) / [ConsoleServer.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/ConsoleServer.java) |
| SE-04 | P0 / DONE | Session continuity across controllers | Shared SQL sessions passed two-controller HTTPS/revocation/kill/reopen/store-outage acceptance on PostgreSQL/MySQL; global task control remains CP-02/03. |
| SE-05 | [app.js](../../../sync-app/src/main/resources/console/app.js) / [AccessControl.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AccessControl.java) |
| RC-01 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [FanoutDispatcher.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/FanoutDispatcher.java) |
| RC-02 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [managed-validation.md](../../../docs/zh-CN/managed-validation.md) |
| RC-03 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [JdbcSink.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcSink.java) |
| RC-04 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [reconciliation-workflow.md](../../../docs/zh-CN/reconciliation-workflow.md) |
| RC-05 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [FanoutDispatcher.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/FanoutDispatcher.java) / [README.md](../../../docs/verification/2026-11-16-managed-repair/README.md) |
| RC-06 | [README.md](../../../docs/verification/2026-11-16-managed-repair/README.md) / [common-cut-repair.md](../../../docs/zh-CN/common-cut-repair.md) |
| RC-07 | [README.md](../../../docs/verification/2026-11-16-managed-repair/README.md) / [RepairRequest.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/RepairRequest.java) |
| RC-08 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [reconciliation-workflow.md](../../../docs/zh-CN/reconciliation-workflow.md) |
| RC-09 | [TaskService.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/TaskService.java) / [README.md](../../../docs/verification/2026-11-16-managed-repair/README.md) |
| RC-10 | [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) / [messaging-v3.md](../../../docs/zh-CN/messaging-v3.md) |
| SO-01 | [AdapterCatalog.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AdapterCatalog.java) / [SourceAdapter.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/SourceAdapter.java) |
| SO-02 | [validation.md](../../../docs/validation.md) / [mysql57-run.sh](../../../tests/mysql57-run.sh) |
| SO-03 | [validation.md](../../../docs/validation.md) / [matrix.sh](../../../tests/matrix.sh) |
| SO-04 | [AdapterCatalog.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AdapterCatalog.java) / [matrix.sh](../../../tests/matrix.sh) |
| SO-05 | [validation.md](../../../docs/validation.md) / [pipeline-v3.md](../../../docs/zh-CN/pipeline-v3.md) |
| SO-06 | [AdapterCatalog.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AdapterCatalog.java) / [SourceAdapter.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/SourceAdapter.java) |
| SO-07 | [AdapterCatalog.java](../../../sync-app/src/main/java/io/github/rivomesh/platform/AdapterCatalog.java) |
| JV-01 | [performance.md](../../../docs/zh-CN/performance.md) / [matrix.json](../../../docs/benchmarks/2026-11-15-runtime/matrix.json) |
| JV-02 | [performance.md](../../../docs/zh-CN/performance.md) |
| JV-03 | [verify-artifact.py](../../../tests/verify-artifact.py) / [performance.md](../../../docs/zh-CN/performance.md) |
| JV-04 | [performance.md](../../../docs/zh-CN/performance.md) / [values.yaml](../../../deploy/helm/rivomesh/values.yaml) |
| DO-01 | [requirements-and-decisions.md](../../../docs/zh-CN/requirements-and-decisions.md) / [implementation-status.md](../../../docs/zh-CN/implementation-status.md) |
| DO-02 | [rivomesh](../../../deploy/helm/rivomesh) / [kubernetes](../../../deploy/kubernetes) / [console-and-deployment.md](../../../docs/zh-CN/console-and-deployment.md) |
| DO-03 | [managed-repair.md](../../../docs/zh-CN/managed-repair.md) / [recovery.md](../../../docs/recovery.md) |
| DO-04 | [pipeline-v3.md](../../../docs/zh-CN/pipeline-v3.md) / [validation.md](../../../docs/en/validation.md) |
| DO-05 | [verify-artifact.py](../../../tests/verify-artifact.py) / [README.md](../../../docs/verification/2026-11-16-managed-repair/README.md) |
| LG-01 | [implementation-status.md](../../../docs/zh-CN/implementation-status.md) / [gaussdb-index-policy.md](../../../docs/zh-CN/gaussdb-index-policy.md) |
| LG-02 | [platform-design.md](../../../docs/zh-CN/platform-design.md) / [gaussdb-index-policy.md](../../../docs/zh-CN/gaussdb-index-policy.md) |
| LG-03 | [clickhouse-v3.md](../../../docs/zh-CN/clickhouse-v3.md) / [doris-v3.md](../../../docs/zh-CN/doris-v3.md) / [hive-v3.md](../../../docs/zh-CN/hive-v3.md) |
| LG-04 | [messaging-v3.md](../../../docs/zh-CN/messaging-v3.md) / [classic-v3.md](../../../docs/zh-CN/classic-v3.md) |
| LG-05 | [pipeline-v3.md](../../../docs/zh-CN/pipeline-v3.md) / [JdbcJournal.java](../../../sync-app/src/main/java/io/github/rivomesh/cdc/JdbcJournal.java) |
| LG-06 | [rivomesh](../../../deploy/helm/rivomesh) / [kubernetes](../../../deploy/kubernetes) |
| LG-07 | [performance.md](../../../docs/zh-CN/performance.md) / [benchmark.py](../../../tests/benchmark.py) |
| LG-08 | [pipeline-v3.md](../../../docs/zh-CN/pipeline-v3.md) / [rename-migration.md](../../../docs/zh-CN/rename-migration.md) |
| LG-09 | [README.md](../../../docs/verification/2026-11-16-managed-repair/README.md) / [managed-validation.md](../../../docs/zh-CN/managed-validation.md) |
| LG-10 | [account-management.md](../../../docs/zh-CN/account-management.md) / [security-audit.md](../../../docs/zh-CN/security-audit.md) |

v0.5: CP-02/03 PARTIAL · [Internal foundations evidence](../../verification/2026-11-17-global-control-foundations/README.md). Historical foundations scope; newer public routing evidence follows.

2026-11-17: fixed-owner public routing/CDC incremental acceptance, with a known maintenance projection gap. [Evidence](../../verification/2026-11-17-global-routing/README.md). CP-02/03 remain PARTIAL.

2026-11-17: [Global maintenance and original UNKNOWN upgrade](../../verification/2026-11-17-global-maintenance/README.md).339application tests,5x143maintenance controls and native business evidence;CP-02/03 remain PARTIAL.

v0.6: CP-07 explicit separated profile accepted;7DONE/33PARTIAL/8NEW,41open. [Evidence](../../verification/2026-11-17-execution-lifecycle/README.md).

v0.7: RC-02 bounded managed review accepted;8DONE/32PARTIAL/8NEW,40open. [Evidence](../../verification/2026-11-17-managed-failure-review/README.md).

v0.8:RC-05 proven-commit confirmation bounded profile accepted;9 DONE,33 PARTIAL,6 NEW;39 open. [Evidence](../../verification/2026-11-17-managed-commit/README.md).

v0.9:RC-03 bounded profile accepted;10 DONE,32 PARTIAL,6 NEW;38 open. [Evidence](../../verification/2026-11-17-managed-skip/README.md).
