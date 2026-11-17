# Managed failed-transaction review / 管理界面事务证据审阅

2026-11-17 · SHA256 `bcaae9309898a33ecfe919d69bfa6c43227647524498f8269682df44942cbcbe` ·187969804bytes.

AC-RC-02 is accepted for the bounded profile below. This closes inspection/retention,not uncertain-commit mutation,skip,compensation or gap recovery.

| Check | Result | Evidence |
|---|---|---|
| App build / actual JAR first |383pass each,zero failures/errors/skips;conditional integration classes explicitly excluded offline |[build](app-build.log),[unit](release-unit.log)|
| Console / artifact |83DOM:durable create request,summary before explicit raw read,pagination,text escaping,logout/late responses,bilingual named/typed images and SQL NULL/JSON null/missing distinction;packaged resources/Java8 class gate |[UI](ui.log),[artifact](artifact.log)|
| PostgreSQL17 storage/protocol |23contracts:10paused-journal,12evidence(3SQL) and1actual-SQL protocol3/4→5 contract;live/expired refusal,normal STOP upgrade,old-profile refusal |[log](postgresql-storage.log),[manifest](postgresql-storage-manifest.json)|
| MySQL8.4 storage/protocol |same23contracts on actual MySQL/InnoDB storage |[log](mysql-storage.log),[manifest](mysql-storage-manifest.json)|
| Five JREs |8/11/17/21/25,52H2/filesystem/managed-HTTP cases each=260,arm64/256MiB |[manifest](runtime-manifest.json)|
| PostgreSQL17 control-store business |two controllers+independent Agent/real Worker,MySQL8.4 two databases→PostgreSQL17/MySQL8.4,LOCAL/H2 journal;21checks |[log](postgresql-business.log),[manifest](postgresql-business-manifest.json)|
| MySQL8.4 control-store business |same real21check lifecycle and review profile,independent control instance |[log](mysql-business.log),[manifest](mysql-business-manifest.json)|
| Deployment |Helm/default legacy native parity,new review profiles/negative guards;Linux/Compose examples;review overlay preserves three original volumes |[deployment](deployment.log)|

New real acceptance creates evidence through managementB for a failed PostgreSQL transaction. Both entries return one durable evidence ID/hash and actual raw/normalized binlog changes with historical table/column metadata. Summaries exclude row values. OPERATOR and VIEWER receive403for both summary/raw endpoints through both entries. Retrying the original create request throughA preserves the ID and leaves the target paused. After fixing the SQL cause,Agent SIGKILL/reopen,offline durable STOP and actual journal payload GC,the same retained transaction/hash/raw values remain readable from the other entry. Existing preparation/snapshot faults,management/store outages,configuration/history and actual common-cut validation/repair also pass.

Acceptance profile:Java8 production path,fixed original Agent/directory,LOCAL/H2 journal,MySQL8.4 source,two mapped databases,PostgreSQL17/MySQL8.4 destinations,and either dedicated PostgreSQL17 or MySQL8.4 control storage. Snapshot keyed/keyless/JSON/generated values and subsequent cross-database DML are checked;the deliberately failed transaction is a keyed INSERT. Five-JRE contracts are H2/fixture controls,not a full real-source/destination/driver/TLS matrix. External data-journal and other destination profiles remain separately tracked. Lifecycle HTTP is loopback/plain HTTP;no new HTTPS,nonroot-image or live Kubernetes acceptance is claimed for this JAR.

Protocol5 is explicit`cluster.recoveryReview:true`;defaultoff preserves prior3/4profiles. Durable-command and owner-query admission both reject new review requests when disabled. Existing members must normally STOP before upgrade;expired RUNNING members still block. SQL upgrade tests use this JAR with old/new protocol configurations,not an actual mixed-binary rolling deployment. Agents still expose probes only. Original node IDs/PVCs/directories are preserved;automatic relocation is not implemented.

The owner persists evidence ID,original task revision and authorization revision before capture,reauthorizes before work,and publishes safe summaries only. Evidence reads reauthorize/audit through the existing shared authority/owner-query protocol. A missing read directory/authority does not initialize new evidence state. Local pending-operation restart tests reuse the original ID. Bounded files,quotas,HMAC/identity,scoped historical schemas,whole-event pages and retention after journal GC are covered by the underlying contracts. Truncated/orphan files simulate incomplete publication;physical power loss and exact kill-at-fsync remain unaccepted.

Source identity,complete transaction/event range,original source-position tokens,table/column types,subscriber ACK,epochs,raw/normalized hashes and full/partial/absent images are inspectable. `configurationRevision` records the action's original task revision;the configuration fingerprint is separate. It is not a configuration-history restore reference or execution approval. Source-position summaries whitelist known tokens rather than forwarding arbitrary payload fields. Reading never normalizes,activates a destination,advances ACK/GTID or automatically resumes.

RC-01 remains PARTIAL for complete source-phase diagnosis. RC-03/04/05/06/07/08/09/10 retain their outstanding skip,gap,manual checkpoint/compensation,cross-node execution and retention semantics. Evidence directory management/automatic GC/key rotation,large-event streaming,other compatibility profiles and visual/browser accessibility acceptance remain open under their own tasks. No manual mutation endpoint exists.

[Candidate](candidate-manifest.json) binds frozen JAR/tests/[sources](source-manifest.json). Seven SDK JARs and17source/POM hashes were [reused unchanged](sdk-reuse.log);no new full reactor claim. [Harnesses](harness/) match final native/runtime manifests. Failed [role fixture](attempts/protocol-role-fixture.log) and [summary assertion](attempts/summary-assertion.log) remain failed records:the former omitted a required old-member role,the latter mistook availability metadata for row-image fields. Production checks were retained. The [intermediate e63](intermediate-e63/candidate-manifest.json) passed its earlier JSON-only scope and PG lifecycle,but lacks final typed presentation;reconstructed harness snapshots match its recorded hashes exactly.

本轮关闭RC-02上述已验收档案，SDD为8DONE、32PARTIAL、8NEW，余40项。临时容器/卷/网络已清理，没有常驻页面或新运行镜像。手工跳过、补偿、缺口台账及完整项目仍未完成。

[中文操作/FAQ](../../zh-CN/failure-review.md) · [English operations/FAQ](../../en/failure-review.md).
