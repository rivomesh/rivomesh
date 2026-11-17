# Package namespace acceptance / 包名更改验收

2026-11-17 · Source commit `e606b650d36c448ab02f625811ab1b07b4997b58`.

Java base package and Maven groupId: `io.github.rivomesh`. All nine modules were rebuilt after removing module compiler outputs; no earlier SDK JAR was reused. Vendor download caches and historical evidence were retained.

Candidate SHA-256: `48de0f43ef9fb57ed4b826283682dab2efa1ffb02e5ed07e1035b2ee159fc3e3` (188593845 bytes). Binary/class snapshots are local ignored build artifacts; archived sources.json binds the candidate inputs.

- Reactor: 457 executed application tests and2 SDK tests;zero failures/errors.28 environment-dependent application tests were skipped and are not claimed as passed.
- UI:87 DOM tests;Helm/native manifest parity and invalid configuration checks passed.
- Actual release JAR: new entry point/JDBC service/transport classes verified;old application namespace absent;application bytecode remains Java8 compatible.
- PostgreSQL17 and MySQL8.4:33 native contracts each,including ACTUAL pre-rename9f97 JAR refusing journal format4 before claim. The probe discovers the historical class namespace and verifies its code source rather than loading current classes accidentally.
- PostgreSQL17 control store:23 real multi-node CDC checks with MySQL8.4 source(two databases),PostgreSQL17/MySQL8.4 targets,LOCAL journal,Java8,separate original Agent. Includes management/store outages,SQL faults,snapshot faults,review and protocol6 confirmation. Temporary fixtures cleaned with exit0.

本次验证覆盖包名迁移、重新构建及上述限定回归。历史验收归档保留原包名及哈希；更名不会补齐所有厂商/JDK/真实K8s矩阵。协议7事务跳过 API 已有代码，新的跳过界面和完整跨节点跳过链路当时尚未验收，RC-03/RC-08仍保持PARTIAL。此归档不应作为完整跳过功能验收。

Current UI work after this source commit is a separate increment. Full protocol7 upgrade/crash recovery,all target modes,source PostgreSQL/Oracle,realK8s and full runtime matrices are not certified here.
