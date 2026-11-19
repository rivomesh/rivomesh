# Current implementation status

The first release is not published. Java/Maven namespace:`io.github.rivomesh`. [SDD](../sdd/001-control-recovery-compatibility/spec.en.md) · [Tasks](../sdd/001-control-recovery-compatibility/tasks.en.md) · [中文](../zh-CN/implementation-status.md).

The SDD contains the current totals and requirements. The verification archive defines the database, version and deployment profile for each accepted result. [Current totals](../sdd/001-control-recovery-compatibility/spec.en.md) · [Verification archive](../verification/README.md)

## Accepted

| Scope | Result |
|---|---|
| DO-01 | All 48 bilingual SDD requirements, acceptance profiles, evidence and totals are checked automatically. |
| DO-03 | Bilingual runbook covers promotion, logs, storage, nodes, sessions and manual recovery. |
| CP-01–03 | Global node registry, shared task configuration and request-bound cross-entry commands. |
| CP-05–06 | Cross-entry revocation, audit correlation, scoped Worker credentials and bounded, redacted diagnostics. |
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
| JV-01 | Temurin 21 is the current release baseline for Controller, Agent and Worker. |
| LG-07 | Verified capacity matrix, 256 database mappings, large transactions and shared-journal backpressure with resume. |
| SO-02 | MySQL 5.7.44 V3 in three historical topologies, including replica SQL failure and binlog-loss refusal. Temurin 21 passed the standalone profile. |
| SO-03 | MySQL 8.0.46 V3 across three topologies and cross-entry manual recovery |
| SO-05 | MySQL 8.4.13 V3 topologies, log profile admission and unsupported live events |
| SO-04 | MySQL 8.2.0 V3 with historical standalone, GTID async and single-primary MGR results. Temurin 21 passed the standalone profile. |

The [single verification baseline](../verification/README.md) retains scoped provenance rather than repeated development snapshots. Cleanup validation is distinguished from earlier acceptance.

## Existing capabilities and limits

| Capability | Current boundary |
|---|---|
| Replication | MySQL multi-database fan-out, automatic tables, chunked snapshot, durable progress, keyless rows, JSON and supported generated columns. MySQL 8.4.13 async GTID promotion and single-primary Group Replication passed on Temurin 21. |
| PostgreSQL source | Synchronized-slot promotion on 17.12 and stored generated columns on 18.7 passed. Native 17.12 REAL and DOUBLE PRECISION replicate into PostgreSQL and MySQL. [Operator guide](postgresql-source.md) |
| Oracle source | Oracle Free 26ai single-PDB Flashback and archived LogMiner CDC. Two PostgreSQL subscriptions preserve original SCNs through savepoint rollback and forced restart. The profile accepts strict text JSON and blocks native JSON when redo images are missing. [Source guide](oracle-source.md) |
| Capture recovery | Inspect the original position and affected targets, verify MySQL GTID boundaries and original binlog bytes, and approve one verified source transaction exclusion on the bounded profile. [Operator guide](capture-recovery.md) |
| Management | Bilingual wizard, shared revisions, isolated Workers and cross-entry control. Real Chrome verified retained sessions, report and receipt lookup, and approved report recovery. Keycloak 26.7.4 passed HTTPS code login with S256 PKCE and cross-entry revocation. [Global control](global-control.md) · [OIDC](oidc-login.md) |
| Interrupted journal ACK | With a PostgreSQL journal, native tests cover a committed MySQL or PostgreSQL target. After the original Agent reconnects, explicit START completes only the missing ACK. Permanent original Agent loss still requires a durable fence and archived recovery evidence. [Operator guide](global-control.md#asymmetric-control-network-partition) |
| Compare/repair | Common-cut row comparison and reviewed repair plans;bounded MySQL/PostgreSQL profiles |
| Gap verification | Reviewed plans,administrator confirmation,fresh scans,atomic receipts and cross-entry cancellation;bounded PG/MySQL,see [guide](gap-verification.md) for acceptance |
| Transaction compensation | Administrator review and approval for PostgreSQL and MySQL with atomic target receipts and original transaction replay. See the [operator guide](transaction-compensation.md) |
| Relational targets | MySQL, PostgreSQL and openGauss have native profiles. DM, Kingbase, GBase8c and OceanBase MySQL mode have bounded native transaction and mixed restart profiles. Distributed GaussDB and other vendor modes remain open. |
| Analytical targets | Initial ClickHouse/Doris/Hive materialization/recovery;production clustering,security/performance remain open |
| Messaging | Eight broker profiles use confirmed at-least-once delivery. Native profiles include Kafka signed receipt readback, ActiveMQ Classic and Artemis shared-store failover, NATS TLS/quorum, RabbitMQ quorum and externally promoted Redis Streams. [Messaging guide](messaging-v3.md) |
| Deployment | Linux, Docker, Helm and native Kubernetes. Two live nodes verified same-volume remount, image replacement, configuration rollout, Secret rotation, cross-node automatic CDC and stale Worker fencing with an external journal. |
| Java | Eclipse Temurin 21 LTS is the sole supported build and runtime. Connector, deployment and security protocol acceptance uses the exact Java 21 candidate. |

See [design](platform-design.md),[console operations](console-and-deployment.md) and product guides for constraints. Protocol/data-format numbers identify internal modes,not compatibility promises for unreleased builds.

## Cleanup

The first-release runbooks describe the current artifact and state formats. Use coordinated Controller, journal and target backups and verify their identity before restoring. Temporary fixture containers and duplicate build artifacts are cleaned after verification. [First-release operations](first-release.md)

## Next work

1. Authoritative drained handoff when the original Agent is permanently unavailable, with durable fencing and complete recovery evidence.
2. Reviewed report reclamation, formal nonempty adoption and writer handover or failback.
3. Per-target recovery protocols and native database, analytical and broker compatibility profiles.
4. Reviewed PostgreSQL/Oracle schema changes, Oracle RAC and additional vendor modes. Production ingress and audit retention, distribution licensing and required notices remain under review.

[Complete task list](../sdd/001-control-recovery-compatibility/tasks.en.md)

No permanent preview is retained. Temporary fixtures are cleaned;useful dependency caches remain. Hourly report comparison state lives in[progress-reports.json](../sdd/001-control-recovery-compatibility/progress-reports.json);no independent notification scheduler is currently available.
