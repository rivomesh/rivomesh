# 全局任务控制：启用、恢复与边界

[English](../en/global-control.md) · [SDD设计](../sdd/001-control-recovery-compatibility/design-global-control.md)

2026-11-17：已接入可选的固定 owner 全局模式，包括公开 HTTP、管理界面、共享配置、持久命令和远程只读查询。默认仍为本机模式。CP-02/03 保持 PARTIAL：旧任务导入、完整业务工作流、容量和故障矩阵尚未全部验收；这不是自动跨节点接管。

## 启用条件

所有节点使用相同的控制库、cluster.id、namespace、sharedAccess=true、sharedTasks=true 和 taskKey；各节点使用不同且稳定的 nodeId，以及独立持久化的任务/数据目录。taskKey 必须是规范 Base64 编码的 32 字节随机密钥，通过受保护环境变量提供。不要放入任务 YAML，不要共用 controller.json。首次启用必须使用没有既有任务的目录；已有本机任务或 YAML 会以 GLOBAL_TASK_IMPORT_REQUIRED 拒绝，目前没有自动导入工具。

[平台示例](../../config/platform-global.example.yml)支持Linux。首次部署按[首版基线](first-release.md)固定全部节点的模式、身份与密钥；控制模式不匹配拒绝，不再自动升级。更换节点ID、目录身份或错误密钥不会回退为本机任务权威。

```sh
java -jar rivomesh-app-0.1.0-SNAPSHOT.jar serve config/platform-global.example.yml
```

数据库账号、令牌、密钥与 nodeId 对应的环境变量由部署环境提供。示例启用 Secure Cookie，浏览器应通过 HTTPS 反向代理访问并配置正确的 publicOrigin。

## Helm 和原生 Kubernetes

[Helm 全局配置](../../deploy/helm/rivomesh/values-global-control.yaml)引用已有 Secret：rivomesh-auth 的 token，以及 rivomesh-control-store 的 username、password、task-key。task-key 是上述 Base64 密钥文本，所有节点一致。为不同入口部署两个 release，每个保留自己的 PVC 和 nodeId：

```sh
helm upgrade --install rivomesh-global-a deploy/helm/rivomesh --namespace rivomesh -f deploy/helm/rivomesh/values-global-control.yaml
helm upgrade --install rivomesh-global-b deploy/helm/rivomesh --namespace rivomesh -f deploy/helm/rivomesh/values-global-control.yaml
```

也可按环境修改后使用原生清单 [A](../../deploy/kubernetes/rivomesh-global-a.yaml)、[B](../../deploy/kubernetes/rivomesh-global-b.yaml)，通过 kubectl apply -f 分别部署。不要把两个独立控制器绑定到同一份私有目录。Helm/清单已有静态一致性和非法配置检查；真实 K8s/CSI、节点故障和滚动升级验收仍待完成。Docker 挂载同样遵守每节点独立持久目录及密钥配置；临时验收脚本不会保留页面。

## 管理界面与配置

登录响应的 taskControlScope=GLOBAL 表示全局模式。任一入口可分页读取授权任务，展示 owner、节点状态、全局修订、Worker 状态、目标/有效配置和命令结果。创建和导入可指定执行节点，也可自动分配。导入会创建一份新任务。节点不可用时状态为 UNKNOWN，不能认为同步进程已停止。

创建、编辑、恢复和动作返回 202，只表示共享存储已接受。配置历史区分 desired/active，只有 owner 成功应用才更新有效版本；失败提案保留，旧有效配置继续有效。配置编辑、历史和保密字段引用可跨入口使用。连接预览、原始差异和修复计划等只读操作通过有期限的 owner 查询通道执行，owner 离线时不能完成。完整过滤、全量日志聚合、真实浏览器视觉/可访问性验收仍开放。

全局 maximumTasks/maximumCommands 与每节点 maxTasks/maxRunningTasks 是不同预算。当前默认全局任务上限 10000、活动命令上限 1000，配置范围见示例/Schema；这只是资源保护参数，不是硬件吞吐承诺。查询请求最多 512KiB、响应 1MiB，有效期 1–60 秒；线程池和保留容量均有界。

全局任务列表支持按精确任务 ID 或执行节点 ID 筛选，也可以同时使用两项。`GET /api/tasks?ownerNodeId=agent-a&taskId=orders&limit=20` 先应用当前授权和筛选条件，再计算总数及下一页。使用 `after` 时保持原筛选条件。在任务详情中选择目标可单独查看该目标的状态、SQL 故障及待确认日志事务。积压数是观察时已采集序号与安全覆盖序号的差值，观察过期时隐藏。消费者完成时间需单独度量。[验收证据](../verification/README.md#global-task-filters-and-destination-backlog)

管理员可在 **诊断记录** 中从 **采集端** 切换到单个目标。接口为 `GET /api/tasks/orders/diagnostics?subscriptionId=warehouse&limit=20`。记录包括目标故障暂停、重试及重试结束的观察时间。采集和目标事件分开保存在执行 Agent 的私有日志目录内。同一任务的目标共用两段各 64 KiB 的滚动文件，频繁故障时早期记录可能轮换。使用 `after` 翻页时保持相同目标。记录已脱敏，只包含事件类型及元数据。重试结束表示 Worker 已观察到恢复，可用目标确认序号及待确认数量判断投递进度。

同一 Agent 的管理入口共用诊断读取额度，每秒 8 页，允许突发读取 16 页。每页最多 25 条。收到 HTTP 429 和 `DIAGNOSTICS_RATE_LIMITED` 时，等待 `Retry-After` 指示的时间，再使用原游标重试。任务状态和控制命令有各自的请求路径。

PostgreSQL 源写入事务型 SQL 目标时，任务状态的 `lastDeliveryTiming` 展示各目标最近一次可核验的提交到确认延迟。它以逻辑复制协议的源提交时间为起点，以 Worker 收到目标提交成功响应为终点，包含通信耗时。源库和 Worker 应同步时钟。终点早于起点时，接口返回 `CLOCK_SKEW`，不提供耗时。两个时间均为 UTC，毫秒数及日志序号使用十进制字符串。[PostgreSQL 逻辑复制消息格式](https://www.postgresql.org/docs/17/protocol-logicalrep-message-formats.html)

每个样本对应一笔已确认事务。任务暂停、重试或有积压时，界面标记为历史样本。首次采样前，以及推进确认序号时没有直接观察到目标提交响应的情况，状态为 `UNKNOWN`。空投递、快照、屏障、跳过和提交结果核对不会产生样本。可根据运行状态、积压数和连续投递序号判断当前进度。Worker 重启或原日志记录已回收后，仍可读取最近确认序号对应的样本。

将 `source.expectedVersion` 固定为 `"8.4"` 或具体的 8.4 补丁版本时，MySQL 8.4 主库的直接写入也会采样。使用未启用写入者分配的本地或外部日志，以及事务型 SQL 目标。起点取自标准 GTID 事件中的微秒字段 `original_commit_timestamp`，并核对源库身份和版本。只有事务由当前主库发起且原始与即时提交时间相同时才采样。中继事务、旧版本 binlog、时间元数据缺失、Group Replication 和启用写入者分配的日志返回 `UNKNOWN`。Group Replication 在事务进入认证阶段前生成原始时间。[MySQL 复制延迟时间戳](https://dev.mysql.com/doc/refman/8.4/en/replication-delayed.html) · [GTID 事件格式](https://dev.mysql.com/doc/dev/mysql-server/8.4.13/classmysql_1_1binlog_1_1event_1_1Gtid__event.html)

### 新任务自动分配

创建向导或导入 YAML 时，可选择 **自动分配执行 Agent**。共享控制库在授权范围内选择在线且有任务容量的 Agent，按已分配任务占 `maxTasks` 的比例排序，并在保存任务的同一事务中预留一个槽。API 可向 `POST /api/tasks` 提供配置、`placement: "AUTO"` 和 `X-Request-ID`。指定 Agent 时填写 `ownerNodeId`。

任务列表显示实际分配节点。用相同请求 ID 和配置重试时，返回第一次的分配。出现 `AGENT_PLACEMENT_UNAVAILABLE` 时，检查当前授权范围内的在线节点和任务槽，确保可选节点都具备该任务所需的驱动、密钥引用和网络访问。运行中任务的自动接管由 Agent 交接流程单独追踪。

### 评估任务迁往另一 Agent

在任务详情选择 **评估 Agent 接管**，从节点目录挑选候选 Agent，再记录评估。窗口展示候选节点的登记状态和回执中的阻断条件。选择 **刷新节点和评估** 可从任一入口读取最新信息。提交结果未知时，**重试同一评估** 会复用原请求 ID 和参数。其他入口已更新评估时，先核对新回执，再决定是否提交新的评估。

管理员可在任一管理入口发送 `POST /api/tasks/{id}/handoff`。请求头填写 `X-Request-ID`，JSON 写入候选 `candidateNodeId` 和任务 `expectedRevision`。通过 `GET /api/tasks/{id}/handoff` 查询最新评估。刷新时使用新的请求 ID，并把当前评估 ID 填入 `expectedAssessment`。原样重试当前请求会返回同一条评估。调用者须有任务和两端 Agent 的授权。

评估绑定任务修订、已生效配置、两端 Agent 注册身份、候选任务容量及来自原 Agent 的采集观察。针对 MySQL 源、PostgreSQL 外部日志和 PostgreSQL/MySQL 目标，检查是否有未结束命令、已完成快照以及各目标追平的共同位点。位点、待处理命令、Agent 注册或容量变化时标为 `STALE`。接口以摘要表示检查点，响应中不含源位点原文、凭据和私有恢复文件。

`EVIDENCE_REQUIRED` 表示管理端检查通过，还需核实候选 Agent 的连接能力及原恢复报告和回执的可达性。`BLOCKED` 会给出 Worker 尚未排空、任务容量耗尽等条件。评估保留原任务归属、Worker、容量和日志状态，返回 `executionAuthorized: false`。

发送 `POST /api/tasks/{id}/handoff/probe`，在 JSON 中写入 `expectedAssessment`。候选 Agent 使用本机连接和密钥环境，核对原日志代次、MySQL GTID 历史，以及 PostgreSQL/MySQL 目标的原生回执。两条只读查询在 30 秒内有效，完成时重新核对任务修订和两端 Agent 注册身份。脱敏结果保存在最新评估中。评估过期或被替换后，需要刷新才能再次核验。

原 Agent 返回私有校验报告、源证据和故障证据目录是否存在，以及保留的操作回执数量。清单保留原 Agent 的资料归属，并返回 `ORIGINAL_PRIVATE_EVIDENCE_CUSTODY_UNPROVEN`。候选核验有效期内，连接阻塞项会清除。结果继续返回 `executionAuthorized: false`。

要核验已完成的校验报告，请在两个 Agent 上挂载同一份受保护的存储，并分别配置 `cluster.handoffEvidenceRoot` 为已存在的绝对路径。挂载目录须使用 POSIX 仅所有者权限。原 Agent 要有写权限，候选 Agent 要有读权限，存储还要支持文件锁和同目录原子改名。既有任务和部署继续使用原报告目录。

使用 Helm 时，为两个 Agent 发行版的 `cluster.handoffEvidenceClaim` 填写同一个已存在的 ReadWriteMany PVC。每个 Agent 的 `persistence.existingClaim` 填各自已初始化的私有状态 PVC。Chart 把共享卷挂载到 `/app/handoff-evidence` 并配置 Rivomesh。两个卷均须事先为 UID/GID `10001` 准备权限，共享卷根目录使用 `0700`。此档案省略 Pod 的 `fsGroup`，由存储端保留文件权限。[Kubernetes 卷权限](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/#configure-volume-permission-and-ownership-change-policy-for-pods)

使用原生 Kubernetes YAML 时，在两个 Agent Deployment 中挂载同一份已有 PVC 到 `/app/handoff-evidence`，各自的状态 PVC 继续挂载到 `/app/data`。此档案省略 `spec.template.spec.securityContext.fsGroup`，在两个 Agent ConfigMap 配置 `cluster.handoffEvidenceRoot: /app/handoff-evidence`。存储权限准备完成后滚动更新 Agent。

原 Agent 在每份已完成报告的文件锁下，按原字节导出 `rows.mv.db` 和 `summary.json`。每次最多处理 32 份报告，报告与历史故障证据共用 64 MiB 数据上限，写完后才替换该原任务的上一份导出。候选 Agent 核对归档摘要、评估 ID、任务修订和两端目录身份。`probe.sharedReportCustody` 返回核验结果、大小和摘要，报告数据保留在受保护的挂载上。原始归档、签名密钥与修复计划由原 Agent 的私有目录保管。核验保留原任务归属，返回 `executionAuthorized: false`。

同一次核验可用 `probe.sharedReceiptCustody` 查看已结束的操作回执。原 Agent 会将已认证的全局命令与本地请求签名、操作者和执行结果核对，只归档匹配的已结束回执。候选 Agent 独立核对共享归档的原字节。该流程覆盖单任务最多 64 条全局命令。回执缺失、已清理、内容不匹配或操作未结束时，保留未核验状态。接口返回数量和摘要。回执导入与执行接管需单独核验。

`probe.sharedFailureEvidenceCustody` 显示最多 64 条历史故障记录的保管核验结果。原 Agent 在存储锁内核对权威记录的任务及节点作用域，并验证每条保留记录的 HMAC 和内容摘要。归档保存权威记录和故障记录的原始字节，签名密钥由原 Agent 保管。候选 Agent 根据已认证的原 Agent 返回值核对归档摘要、评估作用域以及条目数量和格式。权威记录缺失、原始内容变更或存储锁被占用时，原 Agent 拒绝发布本次归档。接口显示数量、大小和摘要。使用历史记录执行恢复仍需原有授权和密钥。

`probe.sharedRawCaptureCustody` 通过独立的私有 ZIP 核验已认证的 MySQL binlog 归档。原 Agent 在归档锁内验证每个文件的 HMAC 回执，并核对准确文件名、大小、摘要和原持有方，然后导出完整原文件与回执。候选 Agent 从已认证的 Agent 通道取得绑定评估范围的 ZIP 摘要，再核对 ZIP 与每个文件的摘要及大小。单任务最多纳入 64 份原始归档，共用独立的 64 MiB 上限。存在未认证的旧文件、回执缺失或内容变更时，源证据核验拒绝，校验报告 ZIP 可继续核验。原 Agent 使用本地私有密钥证明真实性。管理接口只显示核验状态、数量、大小与摘要。该 ZIP 用于核验，写入接管需另行隔离与批准。

两端 Agent 验证原任务已停止、精确切点和闭合回执后，评估接口返回 `preparationEvidenceSha256`。若原 Agent 有校验报告目录，还须完成报告托管核验。管理员可以向 `POST /api/tasks/{task}/handoff/preparation` 提交 `X-Request-ID`，请求体包含 `expectedAssessment`、`expectedRevision` 和 `expectedEvidenceSha256`。同一路径的 `GET` 可查询最新准备记录、到期时间和原、候选 Agent 身份。

准备记录按控制数据库时钟在 15 分钟后到期。到期或解除前，该任务暂停接收新命令和配置修改，候选 Agent 预留一个任务槽和一个 Worker 槽。首版将候选 Agent 的每个已分配任务都计作潜在 Worker，包括已停止的任务。显式和自动分配都计入预留。当前管理员可向 `POST /api/tasks/{task}/handoff/preparation/release` 提交 `X-Request-ID` 和 `expectedPreparation`，立即解除预留并恢复对原任务的操作。相同操作者以相同输入重放已接受请求时，可以读到解除或到期后的记录。准备阶段保留原任务归属和日志写入代次。执行接管还需要通过源端和故障证据核验，并验证数据日志和控制库的写入隔离。

准备记录有效时，管理员可通过 `POST /api/tasks/{task}/handoff/preparation/stage` 暂存已核验的证据。提交 `X-Request-ID`、`expectedPreparation`、`expectedRevision` 和 `expectedEvidenceSha256`。候选 Agent 核对原归档摘要及作用域，在报告锁内验证已完成报告，并将逐文件 SHA-256 写入认证清单。核验后，候选 Agent 在私有状态路径原子发布暂存目录。获授权的请求重放会重新核对已暂存文件。Agent 重新登记、准备记录到期或解除后，需要重新审阅。响应返回数量和 `executionAuthorized: false`。暂存范围是已完成报告和已结束回执。暂存报告使用独立的文件名，常规校验报告读取命令会拒绝打开暂存目录。每个候选 Agent 最多保留 16 个暂存目录，包含中断的暂存。候选 Agent 的任务注册表、原位报告路径及日志写入权保持不变。

对于新建的固定归属 MySQL 任务，如果使用 PostgreSQL 外部日志和 MySQL 或 PostgreSQL 目标，可将 `nodeId` 设置成已登记的原 Agent ID，将 `journal.managedWriter` 设置为 `true`，并将 `journal.writerAssignment` 设置为新增的 UUID。先使用日志库连接配置运行 `java -jar rivomesh.jar init-writer task.yml`，再在指定 Agent 上创建任务。自动分配和普通配置编辑均不能改动该写入绑定。既有未绑定日志需要独立迁移。即使候选配置省略 `managedWriter`，独立 `transfer-writer` 命令也会拒绝移动此日志。

核验通过的准备记录保存原写入分配和该请求的私有后继分配，接口通过 `writerActivation` 返回摘要。控制库和日志库配置同一个 PostgreSQL JDBC 地址，且控制库账号可锁定日志行时，`checkedInControlTransaction` 表示控制事务已核对原日志租约已过期或释放，以及源检查点和所有已确认订阅位置。核对期间，任务和日志写入归属仍由原 Agent 持有。

在控制台打开 **评估 Agent 接管**。证据核验通过后，点击 **预留任务槽和 Worker 槽**。可从任一 Controller 查询同一回执和到期时间。预留有效时，点击 **暂存报告和回执**，将已核验的报告和已办结回执复制到候选 Agent 的私有目录。候选 Agent 核验后，界面显示报告和回执数量。可点击 **复核已暂存的报告和回执** 再次核验。通过任一入口重新打开面板后，**暂存报告和回执** 也会核验同一预留已有的暂存副本。需要继续操作原任务时，先点击 **解除预留**。

## 接受、执行和恢复

PENDING 表示持久排队；DISPATCHED 表示交给固定 owner，调用前保存原本机请求与原修订；ACCEPTED 表示本机已持久接受；SUCCEEDED/FAILED 来自实际操作回执。UNKNOWN 表示证据不足，不能伪装成功，也不能换新修订重新执行。重启先恢复原回执保护及配置权威校验，再允许本机调度。

新启动恢复修正：先按游标读取全部活动命令，包括尚未派发的 STOP/PAUSE。它们在本机持久接受前阻止对应 Worker 恢复旧 RUNNING 意图；旧操作仍可结束，停止命令会优先重试。权限变化导致拒绝后解除阻止；结果不明时继续保留。该修正已通过专项测试和应用构建，真实业务验收结果以候选包归档为准，不追溯覆盖此前候选。

TASK_BUSY/OPERATION_PENDING 保持原请求等待。未确认的本机回执不会按普通 500 条窗口回收。取消有父请求和预留容量：未派发时原子取消，派发不明时先恢复证据，本机接受后才实际取消。取消成功不等于所有目标回滚。

owner 失联期间 STOP 保持等待。必须恢复原节点的私有目录/卷和密钥；空目录不能冒充原 owner。控制器关闭目前仍会停止其 Worker，独立执行生命周期、自动迁移、排空、调度和旧 Worker 隔离属于 CP-04/07。源 MySQL 主库切换续传与控制节点接管是不同故障场景，需要分别验收。

## 运维 FAQ

**如何判断已接受还是已执行？** 查看同一请求 ID 的 deliveryStatus、status、code 和实际 Worker/目标水位。不要把 HTTP 202 当作落库成功。

**刷新页面或更换入口需要重新登录吗？** 共享登录模式通过共享会话恢复身份，仍受 Cookie 的域、Secure、SameSite、有效期和权限撤销规则约束。不同域名需要正确的反向代理和会话配置。

**离线节点能否在 A 上停止？** 可以提交持久 STOP；显示等待/未知，直到原 owner 恢复并提供真实回执。不能仅凭心跳过期宣称已停止。

**直接改共享库的 owner、删除根表或换空目录可以恢复吗？** 不可以。认证元数据、计数、私有目录身份、回执和配置投影均会校验；缺失或篡改拒绝重新初始化。

**控制库中断时可以回退到本机账号吗？** 不会回退；管理面拒绝缺少共享权威的控制。已有数据进程行为依赖其日志/租约/目标协议，不能把管理可用性当作完整数据面隔离证明。

**备份需要包含什么？** 控制库、每节点私有目录、日志/报告/本地 journal、外部 journal（如选用）和原密钥需协调保存。仅恢复游标或旧控制库快照不足以证明一致；自动备份协调及旧备份检测矩阵仍未完成。

**哪些证据已经具备？** 先前候选已有真实 PostgreSQL/MySQL 控制库、两个独立 JVM、HTTPS 共享登录、A 创建 B 任务和离线命令测试；配置专项还有实际 TaskService 加夹具 Worker 验证。新启动修正和真实跨节点业务 CDC 正在补验，不能把这些测试当作全部集群、连接器和硬件性能验收。

**为什么没有常驻预览？** 按用户要求，验收仅启动临时资源并清理自己的容器/卷；其他项目资源不受影响。
