# Connector, runtime and API profiles

[中文](../zh-CN/compatibility.md)

Choose a profile by source version and topology, target product and mode, then Java runtime. Each row records the scope with native evidence or identifies work still pending. Check the exact candidate hash in the [verification baseline](../verification/README.md) and [machine-readable manifests](../verification/baseline.json) before selecting a build for acceptance.

## Sources and coordinates

| Configuration | Native scope | Configuration and recovery |
|---|---|---|
| `MYSQL` 5.7.44 | Temurin 21 standalone and three-member single-primary Group Replication to MySQL and PostgreSQL. Async GTID promotion and missing binlog refusal passed with a PostgreSQL target. | [Pipeline configuration](pipeline-v3.md), [missing-log recovery](capture-recovery.md) |
| `MYSQL` 8.0.46 | Temurin 21 standalone and three-member single-primary Group Replication to both SQL targets. Async GTID promotion passed with a PostgreSQL target. | [Pipeline configuration](pipeline-v3.md), [recovery runbook](reconciliation-workflow.md) |
| `MYSQL` 8.2.0 | Temurin 21 standalone and three-member single-primary Group Replication to both SQL targets. Async GTID promotion passed with a PostgreSQL target. | [Version-specific test commands](pipeline-v3.md#mysql-820-source-regression) |
| `MYSQL` 8.4.13 | Temurin 21 standalone, async GTID promotion and three-member single-primary Group Replication to both SQL targets. Binlog admission and the former group primary returning read-only have separate native proofs. | [8.4 binlog profile](pipeline-v3.md#mysql-84-binlog-profile) |
| `POSTGRESQL` 17.12 | Experimental Temurin 21 profile. Synchronized-slot promotion resumes automatically and an earlier timeline is refused. The bounded PostgreSQL and MySQL profile compares both targets at one WAL cut and resumes the original slot. | [Source guide](postgresql-source.md), [task example](../../config/postgresql-source-v3.yml) |
| `POSTGRESQL` 18.7 | Experimental Temurin 21 profile. PostgreSQL 17.12 materializes published stored generated columns as ordinary columns. | [Source guide and publication](postgresql-source.md) |
| `ORACLE` 26ai Free 23.26.4.0.0 | Temurin 21 with a separately supplied Oracle JDBC driver. Native single-PDB Flashback and archived CDC to PostgreSQL, MySQL, Kingbase, Dameng, ClickHouse, Doris, Hive and Redis Streams. RAC and source promotion remain pending. | [Probe privileges and recovery](oracle-source.md), [V3 example](../../config/oracle-source-v3.yml) |

MySQL uses the logical source cluster ID and untagged GTID sets. PostgreSQL uses the logical source ID plus system ID, timeline, slot and LSN. The Oracle probe uses DBID, resetlogs SCN, PDB GUID and SCN. `SourcePosition.formatVersion` is currently 1 and is interpreted by `sourceType`. Compare coordinates within the original authority and protocol. The [multi-database sample](../../config/pipeline-v3.yml) uses `formatVersion: 3`, `source.clusterId`, `databases` and an explicit `mappings` object per target. Use independent configurations for separate MySQL clusters.

## Targets and consistency

| Target identifiers | Scope and delivery contract | Manual |
|---|---|---|
| `MYSQL`, `POSTGRESQL` | MySQL 8.4 and PostgreSQL 17.12 native transactional targets with business writes and checkpoint in one commit. | [Relational pipeline](pipeline-v3.md) |
| `GAUSS` | openGauss 7.0.0-RC3 native transactional profile. Commercial distributed GaussDB constraints, GSI behavior and deployment acceptance are pending. | [Gauss index policy and takeover](gaussdb-index-policy.md) |
| `DM`, `KINGBASE`, `GBASE_8C` | The first archived native transactional profiles used Java 8. Consult each guide for the exact server mode, driver, field conversions and recovery checks. | [DM](dm-v3.md), [Kingbase](kingbase-v3.md), [GBase 8c](gbase8c-v3.md) |
| `GBASE_8A`, `GBASE_8S` | Planned separate products. | [Product status](implementation-status.md) |
| `OCEANBASE` | CE 4.3.5.6 MySQL mode passes native transactions, snapshot, CDC, restart and SQL recovery on Temurin 21. Oracle mode and clusters remain pending. | [OceanBase](oceanbase-v3.md) |
| `CLICKHOUSE`, `DORIS`, `HIVE` | First native analytical profiles. Versioned materialization and receipts, with visibility and replay rules defined per engine. | [ClickHouse](clickhouse-v3.md), [Doris](doris-v3.md), [Hive](hive-v3.md) |
| `KAFKA`, `PULSAR`, `RABBITMQ`, `ROCKETMQ`, `ACTIVEMQ_CLASSIC`, `ACTIVEMQ_ARTEMIS`, `REDIS_STREAMS`, `NATS_JETSTREAM` | Bounded broker profiles with confirmed at-least-once publishing. Deduplicate stable event IDs and track consumer completion separately. | [Messaging](messaging-v3.md), [ActiveMQ Classic](classic-v3.md) |

The V3 wizard reads `GET /api/capabilities` for source availability and limits destinations to the selected engine’s supported products. PostgreSQL and Oracle source forms preserve their separate version, source identity, namespaces and credentials. For each planned or experimental deployment, check the product manual's topology, authorization, retention and backup scope.

### Adapter verification and native combinations

MySQL, PostgreSQL and Oracle deliver the same typed table, snapshot and committed-transaction model to the journal. The journal preserves the source coordinate and assigns stable identities to keyless row occurrences before dispatch. SQL, analytical and messaging targets consume the shared model and maintain their own commit or delivery receipts.

When extending an adapter, use shared event and projection tests for common behavior. A new source needs real-engine evidence for native snapshot, change decoding, restart and failover, plus end-to-end delivery to representative targets. A new target needs real-engine evidence for storage, type and key semantics, commit receipts and retry, using common events and a representative source. Add a native source-to-target case when a projection, physical constraint or recovery rule depends on both ends. Examples include source decimal precision that exceeds the target limit and a target collation that can merge distinct source keys. The verification ledger records the exact combinations that ran. [Verification ledger](../verification/README.md)

## Release runtime

**Eclipse Temurin 21 LTS is the sole supported build and runtime.** The release uses Java 21 class files. Controller, Agent and Worker processes use Temurin 21 on Linux, Docker and Kubernetes. Record the actual JVM version, architecture and artifact SHA in each native run. Use the matching Temurin 21 fixture to verify a new source, destination, journal or recovery profile. [Reproduction commands](performance.md)

Each result in the source and destination tables applies to its original candidate and environment. Earlier runtime experiments and profiles such as `fiveJdkProcesses`, `postgresqlSourceRuntimeMatrix` and `postgresqlStoredGenerated` retain their original artifact hashes and reproduction commands in the archive. [Verification manifests](../verification/baseline.json) · [Fixed historical snapshot](https://github.com/rivomesh/rivomesh/blob/419945c98f197bd6cc1c6cc3e75e9c6a4bb3188c/docs/verification/baseline.json)


### Operator-supplied JDBC on Java 21

The five checked driver artifacts have entry-point class major 52 and a base-class bytecode ceiling of 52. The JVM specification associates major 52 with Java 8. This records their bytecode floor. Rivomesh deploys on Temurin 21 and verifies actual service loading and explicit URL routing there. Run the connection check on the same Controller, Agent or Worker classpath that will use the driver. [JVM class file versions](https://docs.oracle.com/javase/specs/jvms/se21/html/jvms-4.html#jvms-4.1) · [External driver packaging](release-inventory.md#candidate-with-external-vendor-jdbc-drivers)

| Diagnostic | What to check |
|---|---|
| `JDBC_DRIVER_UNAVAILABLE` | The required JAR, read access, matching driver class and constructor accessibility. |
| `JDBC_DRIVER_JAVA_VERSION_UNSUPPORTED` | Replace the incompatible driver or dependency with a build that runs on Temurin 21. |
| `JDBC_DRIVER_LINKAGE_INVALID` | Use the matching driver dependency set. Check for a missing transitive class or conflicting copies on the classpath. |

Verified transport profiles include external Connector/J 8.4.0 for MySQL 8.4 TLS 1.3 and pgJDBC 42.7.14 for PostgreSQL 17.12 TLS with hostname and CA validation. Both retain the original CDC checkpoint across restart. Kingbase 9.0.1 has separate authentication, precision and native receipt recovery evidence. Use the product guides for the tested server and client security settings. [MySQL TLS](pipeline-v3.md) · [PostgreSQL TLS](postgresql-source.md) · [Kingbase JDBC](kingbase-v3.md)

### Target evidence on Temurin 21

The profile records identify the exact database or broker, the bundled client or externally admitted driver and the verified recovery operations. Scope and test artifacts follow the linked product guide and evidence ledger.

| Target | Server and client | Native acceptance |
|---|---|---|
| `MYSQL` | 8.4.13 · Connector/J 8.4.0 | Native transactions, snapshot and CDC, JSON, keyless tables, GTID continuation and SQL repair |
| `POSTGRESQL` | 17.12 · pgJDBC 42.7.14 | Native transactions, JSONB, keyless tables, logical-slot promotion, TLS verification and SQL repair |
| `GAUSS` | openGauss 7.0.0-RC3 A · gsjdbc4 1.1 | Target restart, table/index consistency, business UNIQUE downgrade and original-snapshot continuation |
| `DM` | 8.1.4.80 UTF-8 · 8.1.3.140 | External JDBC, JSON text, wide numerics, keyless tables, target and Worker restarts and isolated repair |
| `KINGBASE` | V009R001C001B0025 PG/UTF8 · 9.0.1 | External JDBC authentication, native JSON, exact numerics, original receipts and target restart |
| `GBASE_8C` | V6_S1.3.0B19 PG/UTF8 · 3.0.0B02 | Native MySQL snapshot/CDC, JSON, keyless rows, exact values, restart and isolated SQL repair |
| `OCEANBASE` | CE 4.3.5.6 MySQL · Connector/J 8.4.0 | Native MySQL snapshot/CDC, JSON, keyless rows, exact values, restart and isolated SQL repair |
| `CLICKHOUSE` | 25.8.33.6 · HTTP / RowBinary | Native materialization receipts, restart, older nonempty backup refusal and exact predecessor review |
| `DORIS` | 4.1.3 · Connector/J 8.4.0 / Stream Load | Native writes and receipts, checkpoint recovery and exact previous-batch verification |
| `HIVE` | 4.0.1 · Hive JDBC 4.0.1 | External PostgreSQL Metastore, ACID ORC, snapshot and CDC, Worker checkpoint restart and native receipts |
| `KAFKA` | 3.9.2 / 4.1.2 · Kafka clients 3.9.2 / 4.1.2 | TLS/SCRAM, KRaft, native ACK, fixed-UUID transactional gap notices and checkpoint recovery |
| `PULSAR` | 4.0.14 · Pulsar SDK 4.0.14 | Private CA/JWT, multi-broker and BookKeeper recovery, original ledger and entry receipts |
| `RABBITMQ` | 4.1.4 · AMQP client 5.36.0 | Quorum leader recovery and consumer ACK, optional native receipt stream verified after consumer ACK |
| `ROCKETMQ` | 5.5.0 · RocketMQ client 5.5.1 | Local SYNC_FLUSH, native commit-log receipts and the separate Controller replica-promotion profile |
| `ACTIVEMQ_CLASSIC` | 5.19.12 · OpenWire client 5.16.8 | KahaDB shared-store failover and exact native preceding receipts |
| `ACTIVEMQ_ARTEMIS` | 2.57.0 · Core client 2.19.1 | Primary/backup shared store and exact native receipts on a non-destructive queue |
| `REDIS_STREAMS` | 7.4.2 / 8.0.2 · Jedis 8.0.2 | AOF, WAITAOF, externally promoted primary, TLS and native entry receipts |
| `NATS_JETSTREAM` | 2.11.8 · jnats 2.26.4 | TLS, three-replica quorum and native sequence receipts |

Commercial distributed GaussDB, OceanBase Oracle tenants and GBase 8a/8s require their own accepted profiles. Additional storage, security and production-cluster combinations are tracked by the SDD product requirements. [Evidence ledger](../verification/baseline.json) · [Acceptance profiles](../sdd/001-control-recovery-compatibility/acceptance.en.md)

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
