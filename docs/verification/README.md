# 首版验证基线 / First-release verification baseline

仅保留当前基线索引；开发过程中的重复源码/脚本副本、失败尝试和逐次快照已从工作树移除。Git提交历史保留追溯。操作文档见[中文实施状态](../zh-CN/implementation-status.md)和[English](../en/implementation-status.md)。

The compact [baseline](baseline.json) preserves the exact bounded acceptance provenance. Earlier results are not relabelled as fresh validation of cleanup changes. Current validation status is tracked in baseline.json. No production compatibility or full product matrix is implied.

| SDD | 已验收范围 / Accepted scope | 原验收产物 SHA-256 |
|---|---|---|
| CP-01 | Bounded profile | `e42cd685bcc9fb38a1e0e82819230ac58869a54f40c4417d39cd596337b91d11` |
| CP-07 | Java8 real CDC;fixed Agent;LOCAL/H2 journal;MySQL8.4 source;PostgreSQL17/MySQL8.4 targets;PostgreSQL17/MySQL8.4 control | `aa17cf5b6d9c21e302af818c5f48ff7c46bbb32d3a532ab1fa0491abe43403bf` |
| SE-01 | Bounded profile | `95127453ff4e6b9a6b0f16d80ea1a16c2c5861b4a260226e875bab6bc72c60a3` |
| SE-02 | Bounded profile | `95127453ff4e6b9a6b0f16d80ea1a16c2c5861b4a260226e875bab6bc72c60a3` |
| SE-03 | Bounded profile | `95127453ff4e6b9a6b0f16d80ea1a16c2c5861b4a260226e875bab6bc72c60a3` |
| SE-04 | Bounded profile | `a1ec34fc060b4078cdaa10f357d5933f4df60f5b9616519433c851ddad69fa4a` |
| SE-05 | Bounded profile | `95127453ff4e6b9a6b0f16d80ea1a16c2c5861b4a260226e875bab6bc72c60a3` |
| RC-02 | Bounded profile | `bcaae9309898a33ecfe919d69bfa6c43227647524498f8269682df44942cbcbe` |
| RC-03 | Java8;MySQL8.4 two-database source;original fixed Agent;LOCAL/H2 journal;PostgreSQL17 business skip target;PG17/MySQL8.4 control stores;native PG/MySQL contracts;both pauses retained | `656245c6ed5d8c80c3017ed319e3802d8974f03e69f432a2ec64f0566660ac74` |
| RC-05 | Java8;MySQL8.4 source;original fixed Agent;LOCAL/H2 journal;PostgreSQL17/MySQL8.4 control stores;public real-CDC PostgreSQL confirmation;native PG/MySQL transaction contracts;whole task paused | `9f97b4d98c103e895c9d1262cd19dcd0946b21b6fc0f72fff2267e59c727de00` |

最新清理前的功能档案：Java8、MySQL8.4双库源、原Agent/LOCAL日志、PostgreSQL业务目标，PG/MySQL控制库各26项真实同步检查，PG/MySQL原生合同各34项、96 DOM及发布包51项专项。34项中含现已删除的开发版兼容探针；清理后的原生恢复回归改为32项，不能将两者相加。首次全量回归的租约过期与51项定向重跑情况在baseline.json中如实保留。

The cleaned baseline removes automatic control-mode migration and historical binary tests. Namespace mode remains fixed and mismatches refuse;lease fencing,data integrity and current recovery tests remain. V2/V3 execution consolidation is a separate pending cleanup batch.
