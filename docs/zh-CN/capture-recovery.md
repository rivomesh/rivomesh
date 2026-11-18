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

采集租约释放后，可验证完整事务字节范围。首批档案覆盖一个不超过 4 MiB 的 CRC32 binlog 行事务。命令通过复制连接读取原日志，逐事件校验 CRC32，核对 GTID、偏移和提交位置。终端输出为摘要和元数据。

```sh
java -jar rivomesh.jar review-source /etc/rivomesh/tasks/pipeline.yml --verify-contents
java -jar rivomesh.jar review-source /etc/rivomesh/tasks/pipeline.yml \
  --retain-contents /var/lib/rivomesh/source-evidence
```

留存前在支持硬链接的文件系统创建操作员专用的 `0700` 目录。每份 `0600` 归档保存审阅信息、断点哈希和原始二进制事件。重复保留相同证据会复用按 SHA 命名的文件。原事件包含业务行值，应按证据保留和备份策略保护该目录。两个命令均保留原采集断点。[实库验收](../verification/README.md#retained-mysql-binlog-contents)

### 从管理界面留存

管理员审阅原生事务边界后，可以选择 **留存原始事务**。分离模式下，持久命令由任务所属 Agent 执行，关联审阅的边界哈希、配置修订和采集错误代次。操作回执提供文件名和归档 SHA-256。二进制事件保存在 Agent 私有的 `capture-evidence/<taskId>` 目录，每个任务的归档预算为 256 MiB。采集断点和故障状态维持原值。使用同一请求重试时，各管理入口返回同一回执。

使用 API 时，为 `POST /api/tasks/{id}/actions` 提供 `X-Request-ID` 及审阅时的值。

```json
{
  "action": "CAPTURE_RETAIN",
  "expectedRevision": "<任务修订>",
  "errorCount": 4,
  "boundaryHash": "<nextTransactionHeaders.boundaryHash>"
}
```

### 批准跳过源事务

留存成功后，在操作回执中选择 **审阅源事务跳过**。核对单笔 GTID、受影响表和全部受影响目标，填写原因并确认影响。跳过后任务保持暂停。

该档案要求同一 binlog 文件内完整提交的行事务，并已留存通过校验的原始字节。受影响的映射表须为已完成快照且具有主键的 InnoDB 表，目标须使用事务型 SQL。无主键表、其他目标协议、未完成快照或无法重新验证的边界，应恢复原源或制定重建方案。MySQL 已清理所需日志时，保留现有归档，并恢复日志或切换到持有所需历史的合格源。

成功后，日志库记录排除回执并把该 GTID 添加到采集断点。各目标确认序号保持原值。执行 **修复后恢复** 可继续投递后续事务，受影响目标的连续应用序号仍止于源缺口。重启 Agent 后可用 **查看源事务缺口** 查询回执。业务接管前应补齐受影响表并核对共同点，历史排除回执仍保留。

通过 API 操作时，将 `CAPTURE_EXCLUDE` 发到同一个 actions 路由，带上 `X-Request-ID`、`expectedRevision` 和计划中对应的 `errorCount`。在 `captureExclusion` 中填写 `boundaryHash`、`archiveSha256`、`planHash`、`reason` 和 `acceptDataLoss: true`。管理员可用 `GET /api/tasks/{id}/capture-exclusions?limit=20` 查询回执，下一页以 `after` 传入返回的 `nextCursor`。
