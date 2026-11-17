# 完整事务跳过与缺口恢复

状态：RC-03首批限定档案已验收，RC-08验证关闭缺口仍未完成。[证据](../verification/2026-11-17-managed-skip/README.md)。已验收Java8、MySQL8.4源、原Agent/LOCAL日志、PostgreSQL业务目标及PG/MySQL控制库；MySQL目标原生合同已通过，其完整公开跳过矩阵及其他产品仍待补验。共享任务集群默认关闭；独立控制器提供管理员入口，仍要求完整证据与显式审批。English: [Transaction skipping](../en/transaction-skip.md)。

## 适用范围

目前限定MySQL源、事务型PostgreSQL/MySQL目标、已经持久保存原始及规范化内容的下一笔完整增量事务。目标只能由同步服务写入；表结构、快照、归属及依赖检查必须通过。不能拆分跨表事务，也不能用于全量快照、采集/解码失败、未知事务边界或其他目标产品。

先处理根本故障。源binlog丢失应恢复日志、切换合格源或重新建立基线；不能用本功能直接改GTID。若目标已提交原事务，应使用[提交确认](commit-acknowledgement.md)，不能标为未应用后跳过。

跳过不执行该事务的业务DML，只对选定目标记录明确的SKIPPED凭据及缺口，并推进其确认进度。源日志及其他目标ACK不改写。该目标以后可能因缺失行、无主键身份或业务依赖而再次失败；继续同步不等于数据一致。

## 界面操作

1. 暂停整个任务并等待目标围栏完成。只暂停某个目标不足以授权操作。
2. 对失败目标重新保存事务审阅证据，打开证据并检查完整原始/规范化事件、目标与位置。旧观察不能替代执行时验证。
3. 核对结果须为目标断点未前进、故障为TARGET_APPLY，且epoch、错误版本及ACK与当前状态相符。不存在跳过按钮时先检查能力开关、状态和证据，不要修改元数据绕过保护。
4. 填写8至1000字符的原因，明确勾选接受该事务的数据缺口，然后提交。仅ADMIN可执行；202只表示接受，需查看最终操作结果。
5. 成功后任务和目标保持暂停。查看“事务恢复与数据缺口”及原计划；自行检查下一事务和恢复条件，再显式恢复，不会自动继续。

原操作失败或超时后，从操作记录的计划入口查看原计划。即使原操作是FAILED，也可能已经保存计划或提交目标；不能以FAILED推断目标未提交。

| 原计划状态/只读检查 | 可以执行的操作 |
|---|---|
| PREPARED，尚未检查 | 仅显式只读检查目标；打开窗口不会自动探测或修改目标 |
| PREPARED，COMMITTED | 恢复已提交缺口的日志确认；不能取消 |
| PREPARED，ABSENT | 重新填写原因并明确接受缺口后继续原计划，或填写原因取消 |
| PREPARED，UNKNOWN | 禁止继续、确认或取消，先处理连接、元数据和恢复单元 |
| COMPLETED | 查看历史凭据和缺口；原操作相同参数重放返回原结果 |
| CANCELLED | 查看取消记录；需要重新审阅新错误版本才能另建计划 |

继续、恢复或取消都保留原计划的审批人/原因；原生回执另记实际执行人，日志决议另记后续恢复人。取消仅适用于已证明目标未提交的计划，不推进ACK，不修复原SQL错误，并使旧错误版本失效。

## 四种部署方式与升级

所有原控制器和Agent配置一致：

```yaml
cluster:
  sharedAccess: true
  sharedTasks: true
  recoveryReview: true
  recoveryCommit: true
  recoverySkip: true
```

其余身份、存储、密钥和角色配置沿用[全局控制](global-control.md)。开关启用控制协议7。升级前正常停止所有原成员，确认记录为STOPPED；租约过期或强杀遗留RUNNING不能代替正常停止。保留原nodeId、Agent目录、证据密钥、任务密钥、控制库和PVC。不能把原私有目录迁到空Agent后只复制游标，也不支持直接降级。

- Linux：在原配置加入开关，正常停止服务、更换JAR后，使用原目录与服务身份启动。入口基础包现为`io.github.rivomesh`。
- Docker/Compose：更新全部原成员挂载的配置，正常停止并重建容器，保留原卷与节点身份；不能执行`down -v`清除业务恢复数据。
- Helm：每个原release保留原values/Secret/PVC，再叠加`deploy/helm/rivomesh/values-recovery-skip.yaml`。必须先正常停止旧成员，不能把此协议升级当普通滚动更新。
- 手工Kubernetes：可参考`deploy/kubernetes/rivomesh-skip-controller-a.yaml`、`rivomesh-skip-controller-b.yaml`和`rivomesh-skip-agent-a.yaml`；既有部署更新原ConfigMap/StatefulSet时保留原名称和卷，不能照示例另建身份替代原Agent。清单验证不等于真实K8s/CSI故障验收。

首次保存跳过意图时，日志数据格式从3单向升级为4，与PREPARED计划在同一事务提交；顶层任务配置仍为`formatVersion: 3`。实际旧版JAR已验证拒绝格式4。待处理计划阻止恢复，必须先继续、确认或安全取消。格式4的计划/缺口表丢失时拒绝重建空历史；应恢复一致的备份单元。

目标原生凭据表与两个可空标记列按受控流程添加；MySQL DDL的隐式提交不代替源日志围栏。需要元数据建表/改表权限。此功能不会自动转换非事务表，不会重建已丢失的目标凭据。

## API

`POST /api/tasks/{task}/actions`使用当前修订和`X-Request-ID`；Cookie写请求还需原有CSRF/Origin。重试同一请求时参数保持完全相同。

```json
{
  "action": "SKIP_TRANSACTION",
  "expectedRevision": "当前任务修订",
  "subscriptionId": "pg",
  "errorCount": 1,
  "skip": {
    "evidenceId": "11111111-1111-1111-1111-111111111111",
    "evidenceHash": "审阅返回的64位摘要",
    "reason": "已审阅完整事务并明确接受该目标的数据缺口",
    "acceptDataLoss": true
  }
}
```

后续操作action为`SKIP_RETRY`、`SKIP_RECOVER`或`SKIP_CANCEL`，skip中改用`planId`和`planHash`，不再提交evidence引用。继续必须`acceptDataLoss: true`；恢复及取消必须明确为`false`。该字段必须是JSON布尔值，字符串、数字、NULL或缺失不代表批准。

- `GET /api/tasks/{task}/recovery-plans/{planId}`：原计划摘要，不读业务行。
- `POST .../recovery-plans/{planId}/inspect`，body为`{}`：有时点的只读原生提交核对，不执行DDL或推进断点。
- `POST /api/tasks/{task}/recovery-plans`：`{"subscriptionId":"pg","after":"","limit":20}`。
- `POST /api/tasks/{task}/gaps`：`{"subscriptionId":"pg","after":"0","limit":20}`。

列表最大100条，游标使用返回的`next`；序号/缺口游标使用字符串，不能转换成JavaScript浮点整数。原始行值仍通过授权事务审阅分页获取，不嵌入计划列表。超长源位置会整项省略并给出摘要，不能把截断GTID当作续传位置。

## FAQ

**目标提交后Agent中断或日志ACK失败？** PREPARED计划会阻止普通恢复；保留同一原计划，在原Agent目录恢复后只读检查。COMMITTED才能恢复缺口确认。重新发起一个无关跳过操作不是恢复方法。

**只读检查UNKNOWN，是否可以强制取消？** 不提供强制取消。目标已提交与未提交的处理不同；连接失败、元数据丢失或备份不一致时不能猜测。

**管理员被停用或换人怎么办？** 待执行命令会重新验证账号修订及ADMIN权限。新的合法管理员可显式重新审批原计划，实际执行人、原审批人和恢复人分别保留。

**缺口会被GC或对账自动消除吗？** 不会。当前计划、原生回执和缺口历史保留；后续正常APPLY可清除当前断点标记，但不删除缺口。完整缺口修复/验证关闭工作流尚未实现，列表保留不等于缺口已修复。

**显示RUNNING是否代表数据完全一致？** 不代表。`hasGaps`和界面缺口警告必须一起看；`gapCountLimited`表示计数有上限。恢复与数据一致是不同状态。

**常见拒绝？** `SKIP_DISABLED`表示开关未开；`SKIP_REQUIRES_PIPELINE_PAUSED`要求整体暂停；`SKIP_RECOVERY_PENDING`要求先处理原计划；`SKIP_CHECKPOINT_RECONCILIATION_REQUIRED`应先核对原生提交；`SKIP_NATIVE_COMMIT_NOT_PROVEN`不能恢复确认；`SKIP_ALREADY_COMMITTED`不能取消；`SKIP_NATIVE_CHECKPOINT_CHANGED`/`SKIP_PLAN_CHANGED`必须重新读取并核实恢复单元。非法请求字段返回400 `REQUEST_INVALID`，不会因此接受命令。
