# Connector, runtime and API profiles

[中文](../zh-CN/compatibility.md)

Choose a profile by source version and topology, target product and mode, then Java runtime. Each row records the scope with native evidence or identifies work still pending. Check the exact candidate hash in the [verification baseline](../verification/README.md) and [machine-readable manifests](../verification/baseline.json) before selecting a build for acceptance.

## Sources and coordinates

| Configuration | Native scope | Configuration and recovery |
|---|---|---|
| `MYSQL` 5.7.44 | Temurin 21 standalone snapshot and CDC to MySQL and PostgreSQL. Earlier archives cover asynchronous GTID promotion, single-primary Group Replication and missing binlog refusal. | [Pipeline configuration](pipeline-v3.md), [missing-log recovery](capture-recovery.md) |
| `MYSQL` 8.0.46 | Temurin 21 standalone snapshot and CDC to both SQL targets. Earlier archives cover asynchronous GTID promotion and single-primary Group Replication. | [Pipeline configuration](pipeline-v3.md), [recovery runbook](reconciliation-workflow.md) |
| `MYSQL` 8.2.0 | Temurin 21 standalone snapshot and CDC to both SQL targets. Earlier archives cover asynchronous GTID promotion and single-primary Group Replication. | [Version-specific test commands](pipeline-v3.md#mysql-820-source-regression) |
| `MYSQL` 8.4.13 | Temurin 21 standalone snapshot and CDC to both SQL targets. Earlier archives cover all three topologies and binlog admission. | [8.4 binlog profile](pipeline-v3.md#mysql-84-binlog-profile) |
| `POSTGRESQL` 17.12 | Experimental Temurin 21 profile. Synchronized-slot promotion resumes automatically and an earlier timeline is refused. Common-cut verification is pending. | [Source guide](postgresql-source.md), [task example](../../config/postgresql-source-v3.yml) |
| `POSTGRESQL` 18.7 | Experimental Temurin 21 profile. PostgreSQL 17.12 materializes published stored generated columns as ordinary columns. | [Source guide and publication](postgresql-source.md) |
| `ORACLE` 26ai Free 23.26.4.0.0 | Temurin 21 with a separately supplied Oracle JDBC driver. Single-PDB Flashback and single-thread archived CDC to PostgreSQL. RAC and source promotion remain pending. | [Probe privileges and recovery](oracle-source.md), [V3 example](../../config/oracle-source-v3.yml) |

MySQL uses the logical source cluster ID and untagged GTID sets. PostgreSQL uses the logical source ID plus system ID, timeline, slot and LSN. The Oracle probe uses DBID, resetlogs SCN, PDB GUID and SCN. `SourcePosition.formatVersion` is currently 1 and is interpreted by `sourceType`. Compare coordinates within the original authority and protocol. The [multi-database sample](../../config/pipeline-v3.yml) uses `formatVersion: 3`, `source.clusterId`, `databases` and an explicit `mappings` object per target. Use independent configurations for separate MySQL clusters.

## Targets and consistency

| Target identifiers | Scope and delivery contract | Manual |
|---|---|---|
| `MYSQL`, `POSTGRESQL` | MySQL 8.4 and PostgreSQL 17.12 native transactional targets with business writes and checkpoint in one commit. | [Relational pipeline](pipeline-v3.md) |
| `GAUSS` | openGauss 7.0.0-RC3 native transactional profile. Commercial distributed GaussDB constraints, GSI behavior and deployment acceptance are pending. | [Gauss index policy and takeover](gaussdb-index-policy.md) |
| `DM`, `KINGBASE`, `GBASE_8C` | Initial Java 8 native transactional profiles. Consult each guide for the exact server mode, driver, field conversions and recovery checks. | [DM](dm-v3.md), [Kingbase](kingbase-v3.md), [GBase 8c](gbase8c-v3.md) |
| `GBASE_8A`, `GBASE_8S` | Planned separate products. | [Product status](implementation-status.md) |
| `OCEANBASE` | CE 4.3.5.6 MySQL-mode native transactions, snapshot, CDC, restart and SQL recovery passed on Java 8. Oracle mode and clusters remain pending. | [OceanBase](oceanbase-v3.md) |
| `CLICKHOUSE`, `DORIS`, `HIVE` | First native analytical profiles. Versioned materialization and receipts, with visibility and replay rules defined per engine. | [ClickHouse](clickhouse-v3.md), [Doris](doris-v3.md), [Hive](hive-v3.md) |
| `KAFKA`, `PULSAR`, `RABBITMQ`, `ROCKETMQ`, `ACTIVEMQ_CLASSIC`, `ACTIVEMQ_ARTEMIS`, `REDIS_STREAMS`, `NATS_JETSTREAM` | Bounded broker profiles with confirmed at-least-once publishing. Deduplicate stable event IDs and track consumer completion separately. | [Messaging](messaging-v3.md), [ActiveMQ Classic](classic-v3.md) |

The V3 wizard reads `GET /api/capabilities` for source availability and limits destinations to the selected engine’s supported products. PostgreSQL and Oracle source forms preserve their separate version, source identity, namespaces and credentials. For each planned or experimental deployment, check the product manual's topology, authorization, retention and backup scope.

## Release runtime

Build, run and accept new candidates on Temurin 21 LTS. Record the real JVM version, architecture and artifact SHA on each live profile. The source and destination tables above include results from their tested development artifact. Earlier records are available as `fiveJdkProcesses`, `postgresqlSourceRuntimeMatrix` and `postgresqlStoredGenerated` in the [verification manifests](../verification/baseline.json). Use the matching Java 21 fixture for unverified source, destination, journal or recovery combinations. [Reproduction commands](performance.md)

The standard release uses Java 21 class files and requires Java 21 or later. The standard Docker and Kubernetes configurations use Java 21. Temurin 25.0.5 on ARM64 passed a bounded MySQL 8.4.13 profile with two Controllers, an Agent and a Worker. PostgreSQL and MySQL targets received initial and incremental data and resumed from the original GTID after external replica promotion. Controller and Agent rolling replacement also preserved the original snapshots.

Build an optional Java 17 bundle from source with a JDK 17 and Maven.

```sh
mvn -B -s build/settings.xml -Pjava17 package
python3 tests/verify-artifact.py --java-release 17
```

This bundle passed native snapshot, CDC, and Controller and Agent rolling replacement with Temurin 17.0.21 on ARM64, MySQL 8.4.13 source and PostgreSQL and MySQL targets. The Worker reused the original GTID, snapshots and STOP receipt. A separate native run also verified both targets resume from the promoted GTID after MySQL 8.4.13 asynchronous failover. Use the verified bundle with Java 17 for that profile. The Java 8 and 11 process results in the historical manifests refer to earlier packages. Current bundles for those runtimes remain pending. [Verification and reproduction](../verification/baseline.json)

## Management requests

Use Bearer credentials for CLI or an HttpOnly browser session. Browser writes supply the configured Origin and `X-CSRF-Token`. VIEWER reads task-scoped status, OPERATOR can issue routine commands within its grants, and ADMIN can configure, inspect sensitive row evidence and authorize data repair. [Browser sessions](browser-sessions.md) · [Grants](task-authorization.md)

| Endpoint family | Request and result |
|---|---|
| `/api/session`, `/api/session/logout`, `/api/nodes`, `/api/tasks` | Identity and scope, node/owner observation and task pagination. Global creation includes `ownerNodeId`. [Console API](console-and-deployment.md) |
| `/api/tasks/{id}/configuration` and `/api/tasks/{id}/actions` | Mutations bind to the current `expectedRevision` and original `X-Request-ID`. Local create returns 201 and local configuration update returns 200. Global mutations and action admission return 202. Follow the persisted operation receipt. [Configuration](configuration-editor.md) · [Command lifecycle](execution-lifecycle.md) |
| `/api/tasks/{id}/capture-recovery`, `/api/tasks/{id}/capture-exclusions`, `/api/tasks/{id}/failure-evidence` | Read the retained source failure generation and the authorized target evidence. [Source review](capture-recovery.md) · [Transaction evidence](failure-review.md) |
| `/api/tasks/{id}/recovery-plans`, `/api/tasks/{id}/gaps`, `/api/tasks/{id}/gap-verifications`, `/api/tasks/{id}/corrections`, `/api/tasks/{id}/validations` | Inspect evidence and immutable plan references, then submit the matching action and generation. [Skip](transaction-skip.md) · [Verification](gap-verification.md) · [Correction](manual-correction.md) · [Managed repair](managed-repair.md) |
| `/api/accounts`, `/api/audit` | ADMIN account directory and authorized audit reads. [Accounts](account-management.md) · [Audit](security-audit.md) |

For example:

```sh
curl --fail-with-body -X POST "$RIVOMESH_ORIGIN/api/tasks/orders/actions" \
  -H "Authorization: Bearer $RIVOMESH_TOKEN" \
  -H "X-Request-ID: orders-stop-001" -H 'Content-Type: application/json' \
  -d '{"action":"STOP","expectedRevision":"replace-with-current-revision"}'
```

Use the revision from `GET /api/tasks/orders`. A repeated request ID reuses the same committed command identity with matching content. Retry uncertain delivery under that ID and reconcile its receipt. Plan IDs, hashes, report IDs, command receipts and pipeline/subscription error generations remain tied to the reviewed source, target and configuration. The [recovery runbook](reconciliation-workflow.md) shows payloads and completion conditions.

A 400 response indicates invalid input, 401 missing or invalid credentials, 403 missing permission or rejected browser authority, and 404 an absent or inaccessible task. A 409 response requires inspecting the returned conflict code and refreshing the revision or generation. A 503 response indicates unavailable shared authority or audit persistence. Resume with the same request ID only if retrying the same action and payload. [Global control](global-control.md)

## Upgrade boundaries

Record the artifact hash, profile, active configuration version and last confirmed checkpoint before changing JDKs, drivers or deployment settings. Quiesce operations and review destination receipts, journal retention and every persistent store before restoring or changing a topology. Follow the [first-release baseline](first-release.md), [deployment guide](console-and-deployment.md) and [cutover runbook](reconciliation-workflow.md). Update both language versions and their configuration or API example when a field or profile changes.
