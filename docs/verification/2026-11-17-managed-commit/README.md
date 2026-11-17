# RC-05 managed commit acknowledgment / 人工确认已提交事务

Accepted bounded profile,2026-11-17. SDD RC-05 closes for this profile. Skipping,compensation/manual correction,gap ledgers,other connectors,automatic Agent relocation and full compatibility matrices remain separately open.

Final JAR SHA256: `9f97b4d98c103e895c9d1262cd19dcd0946b21b6fc0f72fff2267e59c727de00`,187993279 bytes,frozen at `target/release-evidence/managed-commit-first.jar`. Test classes hash: `a7a90a6ecfae0989a238619256b0e6ecb77cc6bf2705ccb34e6f0c7e57859898`. Source manifest hash: `c9e9a1554f2d777def799930c83a6cfb2736eb96194419875f4dab12efd6bb49`.

## Accepted evidence

- 416 final actual-JAR-first Java8 application tests;0 failed/errors/skipped. Conditional `*IntegrationTest` excluded explicitly. Before the last additive migration change:415 Maven application tests;final migration/public-control focused26 tests passed. Final JAR416 suite covers the final production binary. Seven unchanged SDK modules were reused;this is not a fresh full-reactor build.
- 87 DOM tests:historical comparison,exact evidence binding,authorization/capability gating,pause/error/epoch freshness,reason plus review checkbox,one evidence-bound confirmation action without automatic resume,and existing logout/escaping/language contracts.
- PostgreSQL17 and MySQL8.4 each38 native contracts:13 commit acknowledgment,14 checkpoint inspection,10 paused journal,and1 actual SQL protocol3/4/5-to6 upgrade contract. Covers real target commit leading ACK,identity/hash/epoch/error refusals,receipt+ACK rollback together,reopen/GC/native-metadata-loss replay,conflicting/concurrent requests,transactional-engine guard,and targeted old-journal receipt-table migration.
- PostgreSQL17 and MySQL8.4 control stores each23 real CDC/control checks:two controllers,original fixed Agent,LOCAL/H2 data journal,MySQL8.4 source with two databases and PostgreSQL17/MySQL8.4 destinations. A test-only injector commits the real paused PostgreSQL normalized binlog transaction and deliberately withholds journal ACK. After whole-task pause/fencing and fresh evidence,ADMIN confirms via one controller and retries via the other:one ACK advance,same durable receipt,no duplicate rows,both pauses retained. OPERATOR/VIEWER refuse. Explicit resume catches up. Existing controller/store loss,Worker/Agent restart,GC survival and reviewed row-repair lifecycle also pass.
- Helm default/native parity and recoveryCommit overlay/invalid-policy gates passed. No actual Kubernetes/CSI rollout is claimed.

## Boundaries

Only proven native APPLY commits can be confirmed. Require whole-pipeline pause,fresh evidence,equal native/journal fenced epochs and exactly ACK+1 with matching transaction ID/projected hash. An unadvanced checkpoint is not proof of absent business rows. The operation leaves task/subscriber paused and increments the subscriber error generation. New protocol6 admission requires all original members normally STOPPED;expired RUNNING records refuse upgrade. Actual mixed-old-binary rollout was not exercised:the SQL tests use old/new protocol modes of this candidate.

The public real-CDC confirmation path targets PostgreSQL;MySQL target transaction semantics are covered by the native SQL contracts. Do not relabel this as every product/version/JDK/TLS/nonroot/storage profile. This increment uses Java8;earlier five-JRE evidence remains tied to its original artifacts.

Receipt-table preparation creates only additive metadata for an authorized,verified paused operation. MySQL DDL implicitly commits,so confirmation reacquires all fences and revalidates afterward. Core confirmation never recreates missing native metadata. An initial H2 protocol fixture assigned a separated role while configured combined;the fixture was corrected to separated mode without weakening production admission (see attempts).

本档案完成的是“已提交事务确认”闭环：界面、API、跨节点协议、权限、围栏、ACK/回执、恢复和双语文档。不是“跳过事务”，不改源GTID，不重写业务行，也不自动恢复。两个控制库的临时容器、卷和网络均已清理；没有保留常驻预览。操作说明见[中文](../../zh-CN/commit-acknowledgement.md)和[English](../../en/commit-acknowledgement.md)。
