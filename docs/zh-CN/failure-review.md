# 界面事务证据审阅

[English](../en/failure-review.md) · [故障诊断](failed-transactions.md) · [证据文件与配额](failure-evidence.md)

本页流程已通过[限定档案验收](../verification/2026-11-17-managed-failure-review/README.md)。当前只提供保存和读取历史证据，不执行跳过、补偿、修改GTID或自动恢复。

## 启用与升级

单机管理入口支持管理员审阅。共享全局控制需在所有控制器及Agent配置中显式设置：

```yaml
cluster:
  sharedAccess: true
  sharedTasks: true
  recoveryReview: true
```

这会启用控制协议5；默认关闭时保留原协议3/4及既有控制流程。升级前正常停止旧成员，确认注册生命周期为STOPPED，再在所有成员启用新设置并启动。仅租约过期、直接改数据库状态或等待Pod失联不能代替正常停止。协议5建立后，旧模式拒绝加入；没有自动降级流程。

升级继续使用原nodeId、PVC/私有目录及密钥。以下新示例中的release名称用于新部署；升级时不要把原Agent换成演示名称或空目录，否则不会隐式接管原任务。

| 部署 | 入口 |
|---|---|
| Linux | [控制器](../../config/platform-controller-review.example.yml)、[Agent](../../config/platform-agent-review.example.yml)，各节点独立目录，使用原systemd服务方式 |
| Docker Compose | 原[分离部署](../../deploy/docker-compose-execution.yml)叠加[审阅配置](../../deploy/docker-compose-review.yml)，保留原三个数据卷 |
| Helm | 控制器/Agent values叠加[审阅values](../../deploy/helm/rivomesh/values-recovery-review.yaml)，所有成员使用同一控制库及task-key Secret |
| 原生Kubernetes | [控制器A](../../deploy/kubernetes/rivomesh-review-controller-a.yaml)、[控制器B](../../deploy/kubernetes/rivomesh-review-controller-b.yaml)、[Agent](../../deploy/kubernetes/rivomesh-review-agent-a.yaml)，按环境修改Secret、存储及地址 |

生产浏览器会话继续使用已有HTTPS/Origin配置。Agent仍只提供探针，不增加管理HTTP入口；从控制器访问原Agent的证据。真实Kubernetes/CSI及混合二进制滚动升级不由这些示例直接保证。

## 从失败目标开始

1. 以ADMIN登录，进入任务详情，找到暂停目标，检查故障阶段和错误版本。结构准备/全量快照失败使用对应恢复路径，不当作失败增量事务审阅。
2. 点击“保存事务审阅证据”。操作先持久化审阅ID，再捕获完整队首事务及所需历史结构；202仅表示接受。等待操作成功后，从操作历史点击“查看事务证据”。
3. 首先查看摘要：原任务修订、配置指纹、原owner/目录身份、源身份、GTID/日志位置、事务ID、事件范围、历史表、镜像可用性及摘要。这里的ACK/epoch是捕获时的历史观察，不是当前目标已提交证明。
4. 显式点击“显示原始事件”或“显示规范化事件”。表格按历史列名和源类型显示前后镜像；`SQL NULL`、JSON文本`null`、“未提供”和“不适用”分别展示。可展开原始事件详情查看内部行身份等字段。
5. 使用上一页/下一页读取完整事件。事件页附带同一证据摘要，单事件超限时拒绝，不返回截断镜像。没有已保存规范化内容时，规范化按钮不可用。

普通OPERATOR/VIEWER不能创建或读取此证据，即使知道URL也会拒绝。原始值不进入普通任务状态、操作结果或审计正文。退出登录关闭并清空审阅内容，晚到响应不会重新打开旧会话的数据。

两个控制入口通过同一持久命令及原执行节点查询。重复提交同一请求ID保留同一审阅ID；Agent使用原目录恢复后仍可读取。原日志回收不删除已保存证据。查询原Agent失联或控制库不可用时拒绝/显示不可用，不伪造本机结果。

摘要中的`configurationRevision`记录创建动作绑定的原任务修订，配置指纹另列；它不是配置历史的restore reference。

## API对应关系

创建使用既有动作接口及`X-Request-ID`：

```json
{
  "action": "FAILURE_REVIEW",
  "expectedRevision": "当前任务修订",
  "subscriptionId": "pg",
  "errorCount": 1
}
```

轮询任务操作直到完成，读取`failureEvidenceId`；相同请求重试使用原请求参数，不重新生成请求ID。

- `GET /api/tasks/{task}/failure-evidence/{id}`：管理员摘要。
- `POST /api/tasks/{task}/failure-evidence/{id}/events`：管理员事件页，正文`{"view":"RAW","from":0,"limit":20}`，或`NORMALIZED`。
- Cookie鉴权的POST继续要求CSRF/Origin；Bearer API兼容。错误版本、任务修订、权限变化、证据损坏及超限均明确拒绝。

## 目标断点核对与已提交事务确认

新版本保存事务证据时，会尝试核对 PostgreSQL/MySQL 事务目标的原生断点，结果随该次操作保存，并显示在证据审阅窗口：

- **断点未前进**：原生断点仍等于日志 ACK。不能据此证明目标业务数据不存在。
- **目标已提交、日志未确认**：原生断点恰好领先一个事务，而且事务 ID、目标投影摘要一致。应先恢复日志确认；不能当作未应用事务跳过或再次补偿。
- **核对不可用或拒绝**：显示问题代码。规范化内容缺失、证据与当前 epoch/错误版本不一致、日志已回收、目标不可达或不支持时，原证据仍可审阅，但不会推断提交状态。

这是保存证据时的历史观察；重新保存证据才会重新核对，不会因打开旧证据而查询目标。核对只读，不激活目标、不推进 ACK；目标 epoch 落后会明确提示后续写入需要围栏。旧节点没有返回核对字段时，界面显示不可用，不冒充已核对。符合条件的管理员可使用[已提交事务确认](commit-acknowledgement.md)；跳过和补偿仍未实现。

实现与验收追踪见 [RC-05 设计](../sdd/001-control-recovery-compatibility/design-checkpoint-recovery.md)。只读核对的[限定验收](../verification/2026-11-17-checkpoint-inspection/README.md)已通过；[人工确认限定档案](../verification/2026-11-17-managed-commit/README.md)也已验收；跳过与补偿继续追踪，不扩大 RC-02 历史档案。

## FAQ与当前边界

**保存后任务会恢复吗？** 不会。它不改变业务数据、ACK、GTID或暂停意图。修复原因后另行恢复原事务。

**能直接在审阅窗口修改或跳过吗？** 不能。只读断点核对已交付限定档案；已提交事务可按独立流程确认 ACK；修改/跳过、原生 SKIP/补偿凭据及缺口台账仍待实现。

**为什么管理员也看不到保存按钮？** 共享集群可能尚未启用审阅；旧模式不会接收新命令。先按正常停止/升级流程启用全部成员。

**为什么没有事件或显示错误版本变化？** 目标可能已恢复、错误已变化、没有完整增量队首，或者历史结构缺失。重新确认目标状态，不能直接推进位点解决。

**为什么目录或密钥变化后拒绝？** 证据绑定原执行目录并校验HMAC。备份恢复需同时恢复证据、密钥、权威文件及目录身份；不能用空目录或新密钥冒充旧证据。

**文件是否已加密、会自动清理吗？** 文件采用私有权限和HMAC完整性校验，业务内容不是加密格式；备份、卷和账号访问须按环境保护。当前不提供自动清理、密钥轮换或证据目录管理界面，配额满时保留旧证据并拒绝新增。
