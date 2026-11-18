# Current implementation status

The first release is not published. Java/Maven namespace:`io.github.rivomesh`. [SDD](../sdd/001-control-recovery-compatibility/spec.en.md) · [Tasks](../sdd/001-control-recovery-compatibility/tasks.en.md) · [中文](../zh-CN/implementation-status.md).

48 requirements: **22 DONE, 25 PARTIAL and 1 NEW, with 26 open**. Each accepted item is limited to its declared profile rather than every product/version/deployment combination.

## Accepted

| Scope | Result |
|---|---|
| DO-01 | All 48 bilingual SDD requirements, acceptance profiles, evidence and totals are checked automatically. |
| DO-03 | Bilingual runbook covers promotion, logs, storage, nodes, sessions and manual recovery. |
| CP-01 | Unified registry,roles,versions,heartbeat and ownership |
| SE-01–05 | Reload persistence,expiry/revocation,secure cookies/CSRF,cross-entry sessions and API identity rules |
| CP-07 | Separated control/execution;bounded original-Worker continuity during management outages |
| RC-02 | Durable transaction evidence,authorized pagination,historical schemas and images |
| RC-05 | Native commit verification and acknowledgment recovery without reapplying rows |
| RC-03 | Complete cross-database skip,native commit/source ACK failure,original-Agent SIGKILL/reopen and cross-entry recovery;pauses/gaps retained |
| RC-08 | Durable gaps,reviewed verification,cross-entry approval/cancellation and honest incremental progress;bounded SQL profile |
| RC-06 | Before-image compensation with cross-entry approval and explicit resume. Bounded SQL profile |
| RC-07 | Typed entry, JSON import, before/after preview and cross-entry approval. Bounded SQL profile |
| RC-01 | MySQL source, journal and target fault phases across entries and restart |
| RC-09 | Cross-entry cancellation and original-request recovery across compensation and correction commit boundaries |
| JV-01 | Actual Controller, Agent and Worker on all five JDKs with SQL snapshot and CDC verification |
| SO-02 | MySQL 5.7.44 Java 8 V3 across three topologies, replica SQL fault and native binlog-loss refusal. |
| SO-03 | MySQL 8.0.46 V3 across three topologies and cross-entry manual recovery |
| SO-05 | MySQL 8.4.13 V3 topologies, log profile admission and unsupported live events |
| SO-04 | MySQL 8.2.0 on Java 8 with native V3 standalone, GTID async and single-primary MGR profiles |

The [single verification baseline](../verification/README.md) retains scoped provenance rather than repeated development snapshots. Cleanup validation is distinguished from earlier acceptance.

## Existing capabilities and limits

| Capability | Current boundary |
|---|---|
| Replication | MySQL multi-database mappings/fan-out,table creation,chunked snapshot-to-stream,durable progress,keyless/JSON/supported generated columns |
| PostgreSQL source | 17.12 and 18.7 pgoutput profiles. The 17.12 synchronized-slot promotion and 18.7 stored generated columns passed. [Operator guide](postgresql-source.md) |
| Oracle source | 26ai Free single-PDB Flashback and archived LogMiner CDC to PostgreSQL with durable SCN restart. [Source guide](oracle-source.md) |
| Capture recovery | Original checkpoint and impacted targets, same-file MySQL GTID headers, CRC32 and private archival of original event bytes. Approved range exclusion remains open. [Operator guide](capture-recovery.md) |
| Management | Bilingual wizard,configuration revisions,isolated Workers,durable cross-entry commands,accounts/grants/audit |
| Compare/repair | Common-cut row comparison and reviewed repair plans;bounded MySQL/PostgreSQL profiles |
| Gap verification | Reviewed plans,administrator confirmation,fresh scans,atomic receipts and cross-entry cancellation;bounded PG/MySQL,see [guide](gap-verification.md) for acceptance |
| Transaction compensation | Administrator review and approval for PostgreSQL and MySQL with atomic target receipts and original transaction replay. See the [operator guide](transaction-compensation.md) |
| Relational targets | MySQL, PostgreSQL and openGauss have native profiles. DM, Kingbase, GBase8c and OceanBase MySQL mode have bounded native transaction and mixed restart profiles. Distributed GaussDB and other vendor modes remain open. |
| Analytical targets | Initial ClickHouse/Doris/Hive materialization/recovery;production clustering,security/performance remain open |
| Messaging | Eight broker profiles use confirmed at-least-once delivery. NATS JetStream passed three-member TLS and quorum recovery. Other cluster and security profiles remain open. |
| Deployment | Linux, Docker, Helm and native Kubernetes. Two live nodes verified same-volume remount, image replacement, configuration rollout, Secret rotation, cross-node automatic CDC and stale Worker fencing with an external journal. |
| Java | Java 8 build baseline. Java 8, 11, 17, 21 and 25 ran together with Controller replacement and drained Agent restart. Broader connector and protocol-version profiles remain open. |

See [design](platform-design.md),[console operations](console-and-deployment.md) and product guides for constraints. Protocol/data-format numbers identify internal modes,not compatibility promises for unreleased builds.

## Cleanup

Historical-binary probes,automatic control-mode migration and duplicate development snapshots are removed. Namespaces are created in the configured mode and refuse mismatches without silent migration/fallback.73 affected contracts pass. SDKs are installed in the Maven cache,so application-only iterations avoid rebuilding them.

The V2/V3 execution branches,wizard and configuration fixtures still share dependencies;their consolidation is a separate pending batch. Business data and current state-integrity checks were not removed.

## Next work

1. Controlled source range recovery and recovery protocols for other destinations.
2. Automatic Agent relocation,draining,global diagnostics and storage reachability.
3. Broader PostgreSQL and Oracle types, Oracle failover, and the PostgreSQL common-cut workflow.
4. Distributed GaussDB and complete vendor/messaging/analytical production matrices.
5. Large transactions/catalogs,hardware benchmarks,liveK8s,complete JDK matrix and bilingual docs.

No permanent preview is retained. Temporary fixtures are cleaned;useful dependency caches remain. Hourly report comparison state lives in[progress-reports.json](../sdd/001-control-recovery-compatibility/progress-reports.json);no independent notification scheduler is currently available.
