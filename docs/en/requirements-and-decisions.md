# Rivomesh requirements, decisions, and implementation boundaries

Updated: 2026-11-18. 中文：[需求与决策](../zh-CN/requirements-and-decisions.md).

This document records scope and implementation status. **Accepted requirements are not released features.** See the [README](../../README.md) for current operation and [platform design](platform-design.md) for capabilities and scenarios.

New/clarified scope and gaps use [SDD baseline RM-SDD-001](../sdd/001-control-recovery-compatibility/spec.en.md):48traceable requirements,tasks and acceptance contracts. Current behavior remains governed by [implementation status](implementation-status.md) and evidence.

## Confirmed decisions

| ID | Decision | Current implementation |
|---|---|---|
| D01 | Each independent task configuration binds one logical source, initially MySQL; the platform may manage multiple configurations/independent sources, with standalone task deployment retained | Web/CLI support v2/v3, multi-database/target wizard, independent recovery and journal selection |
| D02 | Select multiple databases per topology with explicit target instance/database/schema mappings | Web/CLI v3 implement `databases` and per-target `mappings`; see V3 manual |
| D03 | Fan out one source to multiple targets/products | Web/CLI v3 implement independent target ACKs, fault isolation and recovery |
| D04 | Validate MySQL 5.7, 8.0, 8.2 and 8.4 as standalone, asynchronous GTID and single-primary MGR sources | All four versions have archived native topology profiles. Java 21 verification expands by connector and recovery case |
| D05 | Cover MySQL, PostgreSQL, openGauss, distributed GaussDB, DM, Kingbase, OceanBase and GBase | OceanBase CE in MySQL mode passed outage and SQL recovery. Distributed GaussDB, OceanBase Oracle mode and GBase 8a/8s need native verification |
| D06 | Relax Gauss business secondary UNIQUE to ordinary indexes, avoiding GSI solely for those indexes; permit relaxation for compatibility issues on other targets | V3 ordinary secondary indexes/business UNIQUE downgrade implemented; GSI has model plans; distributed hardware acceptance pending |
| D07 | Only the synchronization service writes subscribed target tables; validate and restore required constraints before takeover | Exclusive writes currently required; takeover workflow planned |
| D08 | Automatically retry from the original checkpoint. Manual skip and compensation require evidence and a durable gap. Database HA performs primary promotion | Native MySQL promotion and resume, destination skip, bounded source exclusion, compensation and typed correction are verified |
| D09 | Ship Linux, Docker, Helm and native Kubernetes instructions with bilingual manuals and FAQ | Both Kubernetes formats passed two-node image replacement, same-volume remount and CDC continuation. External-journal stale Worker fencing passed |
| D10 | Final name Rivomesh, directory `rivomesh`, Java package/Maven groupId `io.github.rivomesh` | Renamed in this change; no claim of domain/trademark clearance or Apache status |
| D11 | Include no-PK tables, JSON, and STORED/VIRTUAL generated columns in first-release scope | V3 keyless multisets, JSON, STORED and whitelisted VIRTUAL materialization implemented/tested |
| D12 | Optional independent log storage; deployment with or without it | V3 H2 local and PostgreSQL/openGauss/MySQL external journals with independent ACKs/safe reclamation implemented |
| D13 | Broad support for existing nonempty targets, object differences, and takeover | Initial targets must be empty;COMMON_CUT repair exists while nonempty adoption/merge remains pending |
| D14 | Publish reproducible hardware/workload/performance standards instead of universal TPS promises | Benchmark tooling and small SQL/JDK smoke evidence exist;representative capacity standards and soak remain |
| D15 | Provide a bilingual console, wizards, diagnostics and guided recovery | Cross-entry control, cookie reload, three source forms, reviews, skips and correction are available. Nonempty adoption and automatic Agent placement are open |
| D16 | Government/enterprise domestic-database migration, staged switching, and parallel environments | One-way mirroring is the foundation; ownership/takeover/failback boundaries in platform design |
| D17 | Support ClickHouse, Doris, Hive and future analytical connectors | Initial materialization and recovery profiles exist for all three. Cluster, TLS, capacity and Java 21 coverage remain |
| D18 | Kafka,Pulsar,RabbitMQ,RocketMQ,ActiveMQ Classic/Artemis,Redis Streams and NATS JetStream | All have initial transport/recovery evidence;complete cluster/TLS/capacity and new skip/compensation protocols need per-adapter acceptance |
| D19 | PostgreSQL and Oracle sources share the neutral SPI, event model and source coordinates | PostgreSQL passed native snapshot, logical CDC and synchronized-slot promotion. Oracle passed single-PDB archived CDC and SCN recovery |
| D20 | Manage authorized remote tasks from any management entry | Shared node inventory, configuration and durable command routing are available. Stale Worker isolation passed. Automatic Agent placement remains |
| D21 | Preserve login on reload with logout, expiry, revocation and cross-entry sessions | HttpOnly sessions passed actual Chrome reload and logout checks. Cross-entry shared sessions are verified |
| D22 | Review transaction skips, source gaps and manual corrective data while retaining evidence | Native evidence covers destination skip, a keyed MySQL source exclusion and reviewed correction. Each runbook describes its supported profile |
| D23 | Use only Eclipse Temurin 21 LTS for the first release | Java 21 bytecode, actual Controller/Agent/Worker and dual-target resume passed. Acceptance expands by source, target, driver and deployment topology |
| D24 | Trace requirements, design, tasks and acceptance in SDD and maintain bilingual manuals | Use current requirements.json, task and acceptance documents and the hourly progress record |

The platform boundary is **one source per task, multiple tasks per platform**. No implicit source merging or cross-source transactions. Isolated workers may run under one console or as separate deployments.

`source.nodes` still lists candidates in one topology. Isolate task IDs, state namespaces, quotas, and replication client server IDs; reject overlapping target tables by default.

## Multiple databases and targets

This example expresses product intent. For runnable configuration see the [V3 manual](pipeline-v3.md) and `config/pipeline-v3.yml`:

| Task | MySQL database | Target instance | Target namespace |
|---|---|---|---|
| sales | orders | gauss-a | app / orders |
| sales | crm | gauss-a | app / crm |
| sales | orders | doris-b | analytics / orders |
| legacy | finance | dm-c | Explicit product-specific namespace |

Version mappings and check case rules, reserved words, shortened names, and collisions. New subscriptions establish their own baseline rather than reusing another cursor. Independent configurations capture separately by default; sharing requires explicit association and compatible source identity, filters, security, and storage policies.

Within a v3 task, the runner shares ordered logs and routes events with independent snapshot, delivery/application/visibility positions and errors per target. One target's SQL failure should not pause others; shared decoding failures may affect all targets in that task.

Apply complete source transactions atomically where the relational target supports that scope. No cross-target XA is introduced. Probe Doris single/multi-table transaction support per version; use dedicated batch/materialization protocols for ClickHouse/Hive. Reject unmet task consistency requirements, or require an explicit weaker mode explained by the UI.

## Recovery and log storage

Preserve atomic data/local-checkpoint commits and owner fencing for transactional sinks. Other sinks require replayable batches, resolved commit acknowledgments, deduplication/versioning, and visibility watermarks. Advancing an external checkpoint first is not a substitute.

| Mode | No extra independent store | Failure boundary |
|---|---|---|
| Local durable journal | Yes; persistent directory/volume | Process recovery; permanent disk loss is not guaranteed lossless; cross-node takeover requires original logs and reliable fencing |
| Target-hosted journal | Yes, for targets with required transactional/state capabilities | Failure of that target can block capture; arbitrary analytics engines cannot host the current Gauss metadata unchanged |
| Independent shared journal | No; an explicitly supported HA backend | Enables cross-node worker recovery, subject to journal availability/capacity |

Persist logs before capture GTID advances. Reclaim only after all active subscriptions and baseline dependencies permit it. Alert/backpressure at quotas rather than silently discard unconsumed data. Per-task quotas must prevent one offline target from exhausting the platform.

Failover verifies captured and all relevant snapshot-read dependency GTIDs. CDC cannot recover permanently lost source transactions; editing GTIDs must not bypass gaps. Distributed metadata needs compatible keys/constraints and a durable stream sequence, not CN-generated IDs, timestamps, or lexical GTID ordering.

## Compatibility and index policy

Broad support means capability detection and a matrix identifying product/version/mode/engine/driver, objects, and consistency level. Distinguish verified, experimental, unverified, and unsupported. JDBC connectivity does not prove replication support; a feature in one release cannot be assumed for older versions.

Other targets may relax incompatible business secondary UNIQUE constraints into ordinary indexes or supported access structures, with an explicit difference manifest. Report the absence of an equivalent structure honestly. Candidate identity keys on no-PK tables, technical keys, and Doris/ClickHouse materialization keys are not optional business secondary constraints. See [Gauss index policy](gaussdb-index-policy.md) and [platform design](platform-design.md).

## Decisions no longer blocking design

Broad product coverage, hardware-based performance standards, optional storage, no-PK/JSON/generated columns, existing targets, conditional UNIQUE relaxation, Rivomesh naming, independent configurations, and Web UI are confirmed. Do not repeatedly ask whether these features are needed.

An onboarding wizard obtains actual versions, modes, namespaces, volumes, and privileges and produces compatibility/sizing reports. Listing every future customer environment now is unnecessary. Conflict priorities for existing data, destructive changes, and production switching still require task-specific choices; broad compatibility does not authorize arbitrary overwrites/deletions.

Classify DDL by operation/capability: additions, compatible changes, destructive changes, generated-expression changes, and partition changes, with automatic, planned-migration, or blocked outcomes. Including the three requested object types does not imply every MySQL engine/function/DDL is automatically compatible.

## Delivery and acceptance

Suggested order: rename → capability/task isolation → identity/JSON/generated columns/DDL → optional journals/adapters → UI recovery/validation → takeover → full benchmark/delivery matrix. Deliver UI operations alongside core features rather than waiting for every adapter.

Test task isolation, cross-database transactions, target-specific recovery, journal GC/backpressure, duplicate counts on keyless tables, historical generated expressions, JSON precision, existing-target reconciliation, analytics retry/visibility, source failover, and stale owners. [Validation](validation.md) separately records v2 HA, v3 three-target/two-journal, HTTP/DOM and broker tests. Repeated executions are not summed as distinct cases; remaining scope needs dedicated acceptance.

## FAQ

**Can configurations connect to independent MySQL sources?** Yes. The platform now loads independent configurations and manages isolated workers/checkpoints. Standalone CLI remains. See [console manual](console-and-deployment.md) for boundaries.

**Do parallel environments imply writes to the same table on both sides?** No. Start with one writer/multiple readers, shadow validation, or explicit domain ownership. Independent writers require additional source capture, conflict resolution, loop prevention, and application routing beyond current one-way MySQL CDC.

**Can every target share identical DDL/write SQL?** No. Reuse the event model; adapters own types, indexes, transactions, batch commits, and reading semantics.

**Are all new features available now?** No. Renaming, v3 fan-out, keyless/JSON/supported generated columns, optional journals, Web and Kafka/RabbitMQ/NATS/Redis Streams/Pulsar/RocketMQ are implemented. Complete DDL, remaining adapters, permissions/repair and business switching remain in development. Enable features according to the implementation/validation matrices.

## Specification changes and acceptance

Keep the same requirement ID across [requirements](../sdd/001-control-recovery-compatibility/requirements.json), both specifications, tasks and acceptance tables. Git retains specification versions and changes. The archive binds each accepted profile to its artifact SHA-256 and the hash of both language versions of its criteria. Reopen a requirement or provide fresh evidence when its acceptance scope expands. Run `python3 tests/sdd-check.py` to verify the records.
