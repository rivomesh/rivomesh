# RC-05 read-only checkpoint inspection / 只读断点核对

Accepted bounded increment,2026-11-17. RC-05 remains PARTIAL;manual commit acknowledgment,skip and compensation are not accepted by this increment.

Application JAR SHA256: `99a1e86599c9b09c50df7d1de8be5521205e5e5742cbd25db2c7f514803e7433` (187979360 bytes),frozen at `target/release-evidence/checkpoint-inspection-first.jar`.

- 397 actual-JAR-first Java8 application tests;0 failures/errors/skips. Conditional `*IntegrationTest` classes excluded explicitly. Original frozen test classes hash: `003082ab1dba6f8114e6038a31412ebafd07938edcb2594dff8aca352b69ff4d`.
- 85 DOM contracts,including evidence binding,committed/unacknowledged wording,historical observation warning,older member fallback and language switching.
- 17 native contracts each on PostgreSQL17 and MySQL8.4:14 checkpoint contracts use native journal and target;3 automatic fanout regressions use H2. Revised native fixture test classes hash: `7e4c4fbdda53adcac609bd3d37a5ec9b7cfb8f428f8a2cee64d1ad3aae572c3f`.
- 22 real CDC/control checks:MySQL8.4 with two databases → PostgreSQL17/MySQL8.4 destinations,LOCAL/H2 journal,two management JVMs and a fixed independent Agent,PostgreSQL17 control store. Real binlog evidence is compared to the paused PostgreSQL native checkpoint. Target epoch stays unchanged;review never advances ACK or resumes. Includes Worker/Agent/controller restarts,SQL faults,evidence retention after journal GC and existing reviewed repair lifecycle.
- Before final metadata hardening:396 Maven application contracts. Final changed production code:23 related Maven contracts;subsequent fixture-only corrections:14 focused contracts. The final JAR-first397 test suite covers the final application binary. Seven unchanged packaged SDK modules were reused;this was an app-only build,not a fresh full reactor.

`checkpoint-inspection-fixed-sources.json` binds the final native fixture/harness source snapshot before the next increment. The original source manifest in the candidate manifest binds the first test snapshot. Production JAR is identical in both. Later source changes for manual acknowledgment belong to a separate increment and do not alter this acceptance.

Initial attempts are preserved:one fixture reopened a sink without loading its table schema;the initial MySQL native fixture used an oversized MyISAM key and expected a SQL exception where the new metadata guard returns an explicit missing-table error. Production guards were preserved. A first real CDC fixture stopped before creating tasks because readiness used the temporary MySQL initialization socket;final harness probes TCP instead. Its manifest is a failed infrastructure attempt,not accepted business coverage. No mixed-binary rollout,real Kubernetes,TLS/nonroot rerun,additional targets or new five-JRE matrix is claimed.

所有本档案容器、网络及测试卷均已清理。此档案验收的是只读核对和界面展示，不能将其写成“人工恢复完成”；SDD 仍为 8 DONE、32 PARTIAL、8 NEW。核对结果仅表示观察时的原生断点，不证明业务数据不存在，也不是跳过或补偿授权。
