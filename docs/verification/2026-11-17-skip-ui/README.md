# Experimental transaction recovery UI / 事务跳过界面开发验收

2026-11-17 · Increment after source commit e606b65. Source inputs and test classes are hash-bound in manifest.json/sources.json.

Candidate SHA-256: `c4f44a65762a8a01a2fbdd36dac7018b2e86b7f1d89b3dfc098db3e6d117af9e` (188594185 bytes).

- 96 DOM cases pass,including9 new recovery workflows:explicit reason/loss acceptance,current revision,no auto-resume,stale/capability/snapshot guards,native COMMITTED/ABSENT/UNKNOWN action selection,separate retry/cancel approvals,escaped values,read-only role gap warnings,bounded string cursors,logout/late-response protection and live language switching.
- 36 focused management/HTTP tests pass against both Maven classes and the actual packaged JAR first. A new HTTP test rejects coerced/missing approval booleans before command acceptance and accepts a literal boolean. Static recovery.js serving and index wiring are verified.
- Helm/default native parity remains unchanged. Three new experimental protocol7 controller/Agent YAML profiles match their overlays;invalid dependency flags reject.
- Bilingual transaction-skip operations,API,upgrade and FAQ guides are added. Shared-task clusters require explicit recoverySkip/Commit/Review flags and protocol7 quiescence. Standalone controllers expose administrator actions with the same evidence/approval guards.

Initial negative test correctly received400 REQUEST_INVALID;its assertion incorrectly expected an internal validation code. The test expectation was corrected without broadening error disclosure. The failed attempt is preserved;the corrected36-case run passes.

This is UI/API/deployment development verification,not full public skip acceptance. Fake TaskBackend tests do not prove real multi-node protocol7 CDC/crash behavior. Native PG/MySQL33-case internal skip contracts and protocol6 CDC23 belong to the separate package-namespace candidate. No new five-JRE/full-driver/TLS/liveK8s acceptance is claimed. RC-03/RC-08 remain PARTIAL;verified gap closure remains open.

本批已完成界面接线及专项验证，完整跨节点公开跳过和缺口关闭尚未验收。没有启动常驻预览；临时测试容器使用--rm并已清理。
