# Paused failure inspection and phase summaries / 暂停事务检查与阶段摘要

2026-11-17 · SHA256 `4a0716e040b09dc3cf9cce9c8eb3dd265a46893070f990bada376bbc72614f64` ·187937377bytes.

[Candidate](candidate-manifest.json) binds the frozen JAR,compiled tests and [source hashes](source-manifest.json). App-only Java8 build reuses seven unchanged SDK artifacts;[hash checks](sdk-reuse.log) do not imply a new full reactor or adapter matrix.

| Check | Result | Evidence |
|---|---|---|
| App build / actual JAR first |358pass each,zero failures/errors/skips;conditional native integration classes explicitly excluded |[build](app-build.log),[unit](release-unit.log)|
| UI / packaged assets |78DOM pass;phase guidance,unknown/corrupt diagnostics,both languages,text escaping;JAR resource/class-version gate passes |[UI](ui.log),[artifact](artifact.log)|
| PostgreSQL17 journal contracts |9pass,zero skips;restart,atomic pause,stale/legacy state,corruption,quota/forged length,missing history and concurrent resume/GC |[log](postgresql.log),[manifest](postgresql-manifest.json)|
| MySQL8.4 journal contracts |same9pass,zero skips,on real InnoDB storage |[log](mysql.log),[manifest](mysql-manifest.json)|
| Five JRE foundation |Java8/11/17/21/25,27cases each=135executions,arm64/256MiB |[manifest](runtime-manifest.json),[summary](runtime.log)|
| Real separated business lifecycle |2controllers,independent Agent/Worker,LOCAL/H2 journal,MySQL8.4 two databases to PostgreSQL17/MySQL8.4,PostgreSQL17 control;18checks |[business](business.log),[manifest](business-manifest.json)|

New business acceptance:inject a real PostgreSQL CHECK violation,both management entries expose TARGET_APPLY and failedSequence=ACK+1 without row values. Stop/start Worker under unchanged task configuration;deliver a fresh source commit to the healthy MySQL target while PostgreSQL remains paused at the same failed sequence/error generation. Remove the constraint,explicitly resume with that generation and verify exact content catch-up. The lifecycle also rechecks management/control-store outages,offline durable STOP,private-journal restart,configuration/history,actual validation/differences and reviewed common-cut repair followed by explicit resume.

`pauseFailure` stores the diagnostic with subscriber pause/error generation in one journal transaction. The additive diagnostic table does not change checkpoint semantics or enable new command kinds;missing legacy phase records remain UNKNOWN. This is not an actual mixed-binary rolling-upgrade acceptance. Failure during activation is distinct from apply,and a failure on the second transaction identifies the second transaction. Source normalization/journal exceptions retain their global failure routing.

Internal inspection locks journal/subscriber in the established order,binds generation/epoch/fingerprint/error generation/ACK,and validates raw/existing normalized payloads. It neither normalizes nor activates a business target. Raw+normalized budget is4MiB,with bounded character/UTF-8 verification independent of stored lengths. It refuses truncation,corruption,missing head/history and stale requests. Status summaries never read raw transaction bodies and expose no transaction IDs,hashes or row values;corrupt diagnostics become visibly UNKNOWN while value-bearing inspection refuses.

RC-01/02 remain PARTIAL. Initial target preparation/snapshot and complete source connection/read/decode/DDL phases still need coverage. Raw UI review,authorization/pagination,private durable evidence,checkpoint reconciliation for manual operations,SKIP/compensation receipts,gap accounting,retention and cross-node recovery plans remain open. The read result is not an execution credential. No manual skip/patch endpoint exists. No new data-format semantics were introduced;future SKIP must gate incompatible old Workers before mutation.

Five-JRE runs use H2/fixture adapters;they are not a source/destination/JDK/TLS/SDK matrix. The business profile uses plain HTTP loopback,Java8 and a fixed Agent;it does not reaccept nonroot images,Kubernetes/CSI,commercial products,source promotion or automatic Agent relocation for this JAR. Prior CP-07 profile evidence remains [aa17](../2026-11-17-execution-lifecycle/README.md).

The [intermediate b63 candidate](intermediate-b63/candidate-manifest.json) passed357build/unit and8native cases per SQL product. It did not include the later UI-safe phase-summary implementation and was superseded by4a07. Final [harness snapshots](harness/) are hash-bound by their manifests. All temporary containers/networks/volumes were removed;no permanent preview or new runtime image was retained.

本轮是RC-01/02部分交付，不新增DONE项；SDD仍7DONE、33PARTIAL、8NEW。实际SQL故障/重启/多入口诊断与健康目标继续同步已验证；不可据此宣称人工跳过、补数或完整数据库矩阵已经完成。

[中文操作与FAQ](../../zh-CN/failed-transactions.md) · [English operations/FAQ](../../en/failed-transactions.md).
