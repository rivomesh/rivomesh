# Durable failed-transaction evidence / 失败事务持久证据

2026-11-17 · SHA256 `ea1837cbc7fbb02a019ecba62d8d02b31a76901e0942c7e4b79c3014a0357767` ·187960677bytes.

| Check | Accepted scope | Evidence |
|---|---|---|
| App build / actual JAR first |375pass each,zero failures/errors/skips;conditional native integration classes explicitly excluded |[build](app-build.log),[unit](release-unit.log)|
| UI / artifact |79DOM;existing phase UI unchanged,packaged resources and Java8 class-version gate pass |[UI](ui.log),[artifact](artifact.log)|
| PostgreSQL17 |21contracts:10paused-journal plus11evidence tests;3evidence tests use the actual selected SQL journal |[log](postgresql.log),[manifest](postgresql-manifest.json)|
| MySQL8.4 |same21contracts on actual MySQL/InnoDB;file-only cases do not become vendor-target tests |[log](mysql.log),[manifest](mysql-manifest.json)|
| Five JREs |Java8/11/17/21/25,44H2/filesystem/foundation cases each=220,arm64/256MiB |[manifest](runtime-manifest.json)|

New internal APIs retain a signed immutable observation,raw/existing normalized transactions and referenced historical schemas under original task/owner/directory identity. Bounded capture never activates a business target,normalizes or acknowledges a transaction. Historical lookup reads only requested versions and refuses missing,corrupt,misidentified or over-budget schemas;an unrelated corrupt/large schema does not prevent scoped capture.

Evidence acceptance covers reopening,private0700/0600 files,idempotency/conflicts,tampered content/key,missing keys,changed directory identity,record/directory quotas retaining old evidence,symlink rejection,concurrent publication,safe summaries and bounded whole-event pages. Retained evidence remains readable after actual journal ACK/collection;repeating the original capture request returns the retained record without rereading a missing journal. Truncated/trailing JSON refuses;unpublished temporary files are never treated as records or silently deleted. Those cases simulate interrupted artifacts;they do not establish process-kill-at-fsync or physical power-loss acceptance.

[Candidate](candidate-manifest.json) binds frozen JAR/tests/[source hashes](source-manifest.json). [SDK hashes](sdk-reuse.log) verify seven unchanged SDK JARs and17source/POM paths;this was an app-only build,not a fresh full reactor. [Harness snapshots](harness/) match final native/runtime manifests. Every disposable fixture was removed;no permanent preview or new runtime image.

The real initial DDL/snapshot/apply fault and two-controller/Agent lifecycle was accepted separately on [662d2b08](../2026-11-17-snapshot-diagnostics/README.md). This ea18 increment does not rerun the real source CDC lifecycle or claim commercial targets,TLS/cluster/JDK matrices,new nonroot images or Kubernetes acceptance. Native evidence capture uses constructed complete transactions and actual journal storage,not new source-binlog decoding.

RC-01/02 remain PARTIAL. Raw UI review,public creation/queries,shared authorization/audit integration,new-request protocol gates,manual native-checkpoint reconciliation,SKIP/compensation receipts,gaps and cross-node recovery remain open. No new public command,endpoint or checkpoint semantics is exposed. Evidence is not an execution credential or proof of destination non-commit. Automatic evidence GC/key rotation and original-owner migration remain open.

本轮持久证据属于内部基础交付，不代表界面原始事务审阅、手工跳过或补数已完成；SDD仍7DONE、33PARTIAL、8NEW。新增证据不会自动改变原事务、ACK、GTID或任务运行状态。

[中文操作/FAQ](../../zh-CN/failure-evidence.md) · [English operations/FAQ](../../en/failure-evidence.md).
