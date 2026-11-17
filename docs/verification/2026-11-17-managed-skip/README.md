# Managed complete-transaction skipping / 完整事务跳过限定验收

2026-11-17 · RC-03 accepted for the bounded profile below. RC-08 verified gap closure,compensation and complete product/runtime matrices remain open.

Candidate SHA-256: `656245c6ed5d8c80c3017ed319e3802d8974f03e69f432a2ec64f0566660ac74` (188594455 bytes). Binary/test snapshots are local ignored artifacts;manifest.json and sources.json bind the tested inputs.

## Profile and results

- Java8;MySQL8.4 source with2 databases;original fixed Agent;LOCAL/H2 journal;PostgreSQL17 business skip target and independent healthy MySQL8.4 business target;PostgreSQL17 and MySQL8.4 control-store profiles.
- Full reactor executed459 app cases:458 passed,one existing30-second fixture lease expired during a678-second setup span;29 integration cases skipped. The complete focused51-case retry passed without weakening production fences or changing the fixture lease.2 SDK cases passed. This is not represented as one clean full-reactor run.
- Actual final JAR-first51 focused contracts pass,including checkpoint inspection,session/HTTP baseline,approval booleans and the public gap-status regression.
- 96 DOM cases pass. Three experimental protocol7 Helm/native YAML profiles and unchanged default parity pass. This is not real-browser visual or liveK8s acceptance.
- PostgreSQL17/MySQL8.4 each34 native contracts pass on the final artifact. Includes journal/target/coordinator atomicity,controlled retry/cancel,receipt recovery,format4 refusal by ACTUAL older9f97 JAR,protocol3–6 normal-STOP upgrades to7 and ACTUAL older protocol6 JAR refusal after upgrade.
- Each control-store profile passes26 actual multi-node CDC checks. The23 earlier lifecycle/review/repair/confirmation checks pass plus complete cross-database skip,source-ACK-loss+Agent-SIGKILL recovery,and later APPLY retaining durable gaps.

## Behavior proven

An ADMIN retains fresh evidence after whole-task pause,explicitly approves one complete cross-database transaction and records exactly one selected-destination SKIPPED receipt/gap. Business rows for both tables remain unchanged;the healthy target receives the original transaction. Request replay returns the same receipt. Both task and selected subscriber stay paused. OPERATOR/VIEWER mutation and plan reads refuse;readers still see the public gap warning metadata.

A second native skip commits while a test-only source ACK constraint fails. The original plan stays PREPARED,the source ACK stays behind and ordinary RESUME fails. The original Agent is killed and reopened using its private persistent directory. Another management entry proves COMMITTED and restores exactly one source gap/ACK,retaining the original native execution identity. Subsequent explicit resume and a later source transaction clear only the current native skip marker;both gap records remain even when the business data converges again.

The first live attempt exposed publicStatus filtering out gap fields. The final candidate passes a targeted HTTP reader regression and both complete CDC profiles after the fix. Initial actual-older-control probes used getConstructor against a package-private constructor;the reflection-only test helper was corrected to use the declared constructor. No production admission rule was relaxed. Attempts are retained separately.

All final native and CDC fixtures report cleanup exit0. No permanent preview or new deployment image is retained.

## Boundaries / 范围

RC-03仅关闭上述首批PostgreSQL业务目标档案；MySQL目标的原生事务合同已通过，其完整公开业务跳过矩阵及其他产品能力由RC-10/LG-01继续追踪。无跨Agent自动迁移、采集/解码跳过、失败前镜像补偿或验证关闭缺口承诺。消息/分析目标不能套用本地SQL事务协议。未跑新的五JRE、全部驱动/TLS、非root镜像或真实K8s矩阵。

Scripts and source hashes are archived exactly as executed. Historical package-namespace/UI candidates and failed attempts remain separate;their test totals are not added into a fictitious single run.
