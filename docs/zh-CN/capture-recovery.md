# 采集故障恢复

[English](../en/capture-recovery.md)

当 V3 任务进入 `ERROR` 且失败阶段位于源连接、读日志、解码、快照、发现、DDL 或转换时，管理员可以在故障卡片打开 **检查采集恢复**。固定 Agent 部署可从任一管理入口发起检查，实际读取由任务所属 Agent 执行。检查报告列出可能受影响的订阅、原断点摘要、源准入结果及后续步骤。

```sh
java -jar rivomesh.jar review-source /etc/rivomesh/tasks/pipeline.yml
```

报告校验任务身份、原采集位置和快照依赖。源端只执行准入读取。检查期间若任务代次、断点或配置发生变化，刷新任务后重新检查。

| 观察结果 | 恢复步骤 |
|---|---|
| `SOURCE_LOG_GAP` | 保留 GTID 和可取得的原始 binlog。恢复缺失日志，或由数据库运维恢复持有完整日志的合格主库。检查通过后从原位置恢复。 |
| `POSTGRESQL_REQUIRED_WAL_UNAVAILABLE` | 保留原复制槽、所需 WAL、日志库和 LSN 证据。评估恢复原槽及 WAL 的可行性。无法恢复时，审阅新基线和受影响目标范围。 |
| `SOURCE_CHECKPOINT_AVAILABLE` 且失败阶段为解码、DDL 或快照 | 保留原始事件并处理解码档案、结构变更或读取权限，再重试原断点。源准入通过只证明原断点仍满足恢复条件。 |
| `UNPROVEN_SOURCE_RANGE` | 缺口事务边界和内容待确认。继续使用原日志和断点开展核查。把所有订阅纳入影响范围。 |

### 常见问题

**为什么目标库有数据，采集仍停在原位置？** 目标可能仍保有之前已成功同步的数据。查看任务的采集断点和每个订阅的确认水位。

**什么时候可以使用事务跳过？** 当事件已经形成可审阅的完整目标事务、目标状态明确且符合审批条件时，使用[事务跳过](transaction-skip.md)流程。源端范围隔离和批准排除由 RC-04 的后续工作跟踪。

### MySQL 原生事务边界

受支持的 `SOURCE_DECODE` 故障可查看原断点后的第一个事务。检查从同一 MySQL 实例及同一 binlog 文件读取最多 256 条事件头，要求位移连续直到 `Xid`，展示 GTID、文件范围和涉及表。`nextTransactionHeaders` 的范围为 `BINLOG_HEADERS_ONLY`，应保留原始 binlog 供行内容审阅和审批。轮换、断档、后端变化、未知事件或不完整事务边界都可能导致证据缺失。读取权限沿用 MySQL 复制账号。[MySQL 事件说明](https://dev.mysql.com/doc/refman/8.4/en/show-binlog-events.html) · [实库证据](../verification/README.md#mysql-decode-header-review)
