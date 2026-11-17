# 确认目标已提交的事务

状态：RC-05 限定档案已验收：界面、API、协议6准入、原子 ACK/回执与跨入口恢复。[验收证据](../verification/2026-11-17-managed-commit/README.md)。[SDD](../sdd/001-control-recovery-compatibility/design-checkpoint-recovery.md) · [English](../en/commit-acknowledgement.md)

适用于 MySQL 源、事务型 PostgreSQL/MySQL 目标、完整增量事务已经保留原始和规范化内容的场景。目标只由同步服务写入。其他产品、全量快照故障、尚未形成完整事务的采集/解码故障均不能使用此操作。

## 操作步骤

1. 在任务界面暂停**整个任务**，确认暂停结果中的目标围栏已完成。仅目标处于 PAUSED 不够。暂停会撤销旧 Worker 的 epoch，防止旧进程继续写入。
2. 在失败目标上重新“保存事务审阅证据”，等待操作成功。暂停前的旧证据仍可审阅，但不能用于本次确认。
3. 打开新证据，检查源位置、完整事务、目标和摘要。“目标断点核对”必须显示“目标已提交该事务，日志尚未确认”，且没有目标 epoch 落后提示。
4. 填写至少8个字符的原因，勾选已核对声明，再点击“确认已提交事务”。只有管理员、匹配当前暂停/错误版本的观察才显示该表单。
5. 等待操作成功并查看回执。ACK 仅前进这一个已证明提交的完整事务，目标错误版本加一，任务和目标均保持暂停；业务行不会再写一遍。
6. 根据当前新错误版本检查下一个事务、修复原因，再显式恢复目标和任务。不要复用旧错误版本，也不要把此操作当作跳过未提交事务。

若没有确认按钮，先检查集群开关、整个任务是否暂停、观察是否过期、目标围栏和提交内容是否一致。不要手工更新元数据位点。

## 启用与升级

独立控制器可直接使用。共享任务集群默认关闭人工确认，所有管理节点和 Agent 均需配置：

```yaml
cluster:
  sharedAccess: true
  sharedTasks: true
  recoveryReview: true
  recoveryCommit: true
```

这会使用控制协议6；仅审阅继续使用协议5。升级前正常停止全部原成员，确认记录为 STOPPED；过期或被强杀的 RUNNING 记录不能代替正常停止。保留原 nodeId、执行目录、证据密钥、共享控制库、任务密钥和 PVC，逐个启动升级后的管理节点与原 Agent。协议5或其他旧模式不能加入协议6命名空间；不支持直接降级。

- Linux：在原审阅配置上加入 `recoveryCommit: true`，正常停止/更新后用原目录启动。
- Docker/Compose：在全部原控制器和 Agent 的审阅配置中加入开关，保留原卷；先正常停止再重建容器。
- Helm：使用原各节点 values，再叠加 `deploy/helm/rivomesh/values-recovery-commit.yaml`。保持原 Secret/PVC 和节点身份。
- 手工 Kubernetes YAML：在现有审阅 ConfigMap 的 `cluster` 下加入开关，按正常停止的升级流程重新部署原 StatefulSet/PVC。不能直接滚动混用旧节点。

旧日志升级时，已授权的确认操作可在验证既有日志身份、暂停状态和新证据后，只增加 `rm3_recovery_ack` 回执表。不会重建丢失的 job、订阅、目标断点或业务表。MySQL DDL 有隐式提交，因此建表后会重新获取全部围栏并复查，建表本身不会推进 ACK。已有非事务型元数据不会自动转换，必须先处理配置/存储问题。新表创建需要对应日志 schema 的建表权限；表已存在时不必重新建表。

## API 与回执

使用原任务操作接口及当前任务修订，`X-Request-ID` 在重试时保持不变。Cookie 请求继续要求 CSRF/Origin。

```json
{
  "action": "CONFIRM_COMMIT",
  "expectedRevision": "当前任务修订",
  "subscriptionId": "pg",
  "errorCount": 1,
  "commit": {
    "evidenceId": "11111111-1111-1111-1111-111111111111",
    "evidenceHash": "证据摘要中的64位SHA256",
    "reason": "已核对原事务及目标原生提交结果"
  }
}
```

`POST /api/tasks/{task}/actions` 返回202，轮询该请求的操作状态。成功结果包含 `receipt` 和回执摘要 `hash`；回执记录操作ID、证据摘要、操作人、原因、原/新错误版本、确认序号及目标断点。操作审计不包含事件行值。

## FAQ

**返回超时，是否再点一次？** API 客户端应以原请求ID和完整原参数重试，不能随意更改原因或证据。回执与 ACK 在同一日志事务提交；若已提交，重试读取原回执。界面先刷新查看原操作结果。

**目标断点未前进，能用这个按钮补确认吗？** 不能。必须恰好领先 ACK 一个事务，事务ID和完整目标投影摘要一致。未前进不是业务行不存在的证明，更不是跳过授权。

**为什么必须重新保存证据？** 整体暂停、Worker 重启或错误变化可能改变 epoch/错误版本。旧证据是历史记录，不能替代执行时核对。

**会自动补偿、跳过或恢复吗？** 不会。这里确认的必须是已提交的原生 APPLY；跳过、前镜像补偿、手工补数据和缺口台账是其他尚未完成的功能。

**会丢失其他目标的数据吗？** 只改变选定订阅的 ACK，不改变源 GTID、原日志内容和其他目标的 ACK。整个任务在操作期间暂停；后续恢复仍需分别检查目标和任务状态。

**常见拒绝码？** `FAILURE_ACK_DISABLED` 为开关未启用；`FAILURE_ACK_REQUIRES_PIPELINE_PAUSED` 为整体未暂停；`FAILURE_ACK_TARGET_FENCE_REQUIRED` 为原生围栏未确认；`FAILURE_ACK_COMMIT_NOT_PROVEN` 为提交内容不符；`ERROR_GENERATION_CHANGED`/`FAILURE_CHECKPOINT_JOURNAL_CHANGED` 要求重新核对；`FAILURE_ACK_METADATA_NOT_TRANSACTIONAL` 必须处理日志元数据存储；`FAILURE_ACK_REQUEST_CONFLICT` 禁止复用操作ID提交不同审批内容。
