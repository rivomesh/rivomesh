'use strict';
window.RivomeshHandoff=(()=>{
 const bridge=window.RivomeshConfiguration,{api,can,requestId,t}=bridge;
 bridge.register({
  handoffTitle:'评估 Agent 接管',handoffHelp:'评估、预留和暂存期间，任务归原 Agent。完成核验后，可确认归属切换。',
  handoffOriginal:'当前 Agent',handoffOriginalLease:'原 Agent 控制租约',handoffCandidate:'选择候选 Agent',handoffCandidateNode:'候选 Agent',handoffChoose:'请选择 Agent',
  handoffMore:'查看更多 Agent',handoffReload:'刷新节点、评估和预留',handoffAssess:'记录评估',
  handoffReassess:'更新评估',handoffRetry:'重试同一评估',handoffReceipt:'评估回执',
  handoffNone:'该任务暂无评估。',handoffNoCandidate:'当前页没有获授权的候选 Agent。',
  handoffClose:'关闭',handoffCapacity:'任务槽 / Worker 槽',handoffAssigned:'已分配任务 / 任务槽',
  handoffWorkers:'Worker 槽',handoffAt:'评估时间',handoffId:'评估 ID',handoffRevision:'任务修订',handoffCut:'采集检查点摘要',
  handoffStateBlocked:'评估受阻',handoffStateEvidence:'待补证据',handoffStateStale:'评估需更新',
  handoffSaved:'已保存评估。',handoffUnknown:'提交结果未知。可读取最新评估或原样重试。',
  handoffChanged:'其他入口已更新评估。请核对最新回执后再提交。',
  handoffRevisionChanged:'任务修订已变化。请核对当前任务状态后再提交。',
  handoffInvalidReceipt:'收到的评估回执无法核验。',
  handoffUnknownBlocker:'存在未识别的阻断条件。',
  GLOBAL_HANDOFF_REQUIRED:'当前部署未启用集群交接评估。',
  AGENT_NOT_REGISTERED:'该节点尚未完成 Agent 注册。',
  HANDOFF_SAME_AGENT:'请选择与当前执行节点不同的 Agent。',
  CANDIDATE_AGENT_UNAVAILABLE:'候选 Agent 当前不可用。',
  CANDIDATE_TASK_CAPACITY_UNAVAILABLE:'候选 Agent 没有可用的任务槽或 Worker 槽。',
  TASK_CONFIGURATION_NOT_APPLIED:'任务的最新配置尚未生效。',
  HANDOFF_JOURNAL_PROFILE_UNSUPPORTED:'此源库、外部日志及目标组合暂未纳入交接评估。',
  TASK_COMMANDS_UNRESOLVED:'任务还有未确认的管理命令。',
  ORIGINAL_TASK_STATE_UNVERIFIED:'当前 Agent 的任务状态缺少可用观测。',
  ORIGINAL_WORKER_NOT_DRAINED:'当前 Worker 尚未停止。',
  ORIGINAL_CAPTURE_LEASE_UNVERIFIED:'缺少原采集租约已结束的证据。',
  ORIGINAL_COMMON_CUT_UNVERIFIED:'当前日志和目标尚缺可核验的共同位点。',
  CANDIDATE_ACCESS_UNVERIFIED:'尚需核对候选 Agent 的驱动、密钥引用和连接能力。',
  ORIGINAL_RECOVERY_EVIDENCE_UNVERIFIED:'尚需确认候选 Agent 能读取原恢复报告和回执。'
 },{
  handoffTitle:'Assess Agent handoff',handoffHelp:'The current Agent keeps ownership during assessment, reservation and staging. After verification, you can confirm the owner change.',
  handoffOriginal:'Current Agent',handoffOriginalLease:'Original Agent control lease',handoffCandidate:'Choose a candidate Agent',handoffCandidateNode:'Candidate Agent',handoffChoose:'Select an Agent',
  handoffMore:'More Agents',handoffReload:'Refresh nodes, assessment and reservation',handoffAssess:'Record assessment',
  handoffReassess:'Update assessment',handoffRetry:'Retry the same assessment',handoffReceipt:'Assessment receipt',
  handoffNone:'This task has no assessment yet.',handoffNoCandidate:'This page has no eligible Agent within your grants.',
  handoffClose:'Close',handoffCapacity:'Task slots / Worker slots',handoffAssigned:'Assigned tasks / task slots',
  handoffWorkers:'Worker slots',handoffAt:'Assessed at',handoffId:'Assessment ID',handoffRevision:'Task revision',handoffCut:'Capture checkpoint hash',
  handoffStateBlocked:'Blocked',handoffStateEvidence:'Evidence required',handoffStateStale:'Assessment needs updating',
  handoffSaved:'Assessment saved.',handoffUnknown:'The submission outcome is unknown. Read the latest assessment or retry the same request.',
  handoffChanged:'Another management entry updated the assessment. Review the latest receipt before submitting again.',
  handoffRevisionChanged:'The task revision changed. Review the current task before submitting again.',
  handoffInvalidReceipt:'The received assessment could not be verified.',
  handoffUnknownBlocker:'An unrecognized condition blocks the assessment.',
  GLOBAL_HANDOFF_REQUIRED:'Cluster handoff assessment is unavailable in this deployment.',
  AGENT_NOT_REGISTERED:'This node has not completed Agent registration.',
  HANDOFF_SAME_AGENT:'Choose an Agent other than the current owner.',
  CANDIDATE_AGENT_UNAVAILABLE:'The candidate Agent is unavailable.',
  CANDIDATE_TASK_CAPACITY_UNAVAILABLE:'The candidate Agent lacks an available task slot or Worker slot.',
  TASK_CONFIGURATION_NOT_APPLIED:'The latest task configuration is still being applied.',
  HANDOFF_JOURNAL_PROFILE_UNSUPPORTED:'This source, external journal and destination combination is outside the assessed scope.',
  TASK_COMMANDS_UNRESOLVED:'The task has unresolved management commands.',
  ORIGINAL_TASK_STATE_UNVERIFIED:'No usable task observation is available from the current Agent.',
  ORIGINAL_WORKER_NOT_DRAINED:'The current Worker has yet to stop.',
  ORIGINAL_CAPTURE_LEASE_UNVERIFIED:'Evidence that the original capture lease ended is missing.',
  ORIGINAL_COMMON_CUT_UNVERIFIED:'The journal and destinations lack a verified common checkpoint.',
  CANDIDATE_ACCESS_UNVERIFIED:'Verify the candidate Agent’s drivers, secret references and connections.',
  ORIGINAL_RECOVERY_EVIDENCE_UNVERIFIED:'Verify that the candidate Agent can read the original recovery reports and receipts.'
 });

 bridge.register({
  handoffStage:'暂存报告和回执',handoffStageAgain:'复核已暂存的报告和回执',handoffStageRetry:'重试同一暂存请求',
  handoffStageHelp:'在候选 Agent 的私有目录暂存已核验的校验报告和已办结回执。暂存后仍由原 Agent 负责该任务。',
  handoffStageDone:'候选 Agent 已核验暂存文件。',handoffStageReports:'校验报告',handoffStageReceipts:'已办结回执',handoffStageRawCapture:'原始采集档案',handoffAdoptedRawCapture:'只读原始采集档案',handoffStageFailureHistory:'历史故障证据',handoffAdoptedFailureHistory:'只读历史故障证据',handoffStageRawPending:'归属切换后，原始档案仅可作为只读历史查看。',
  handoffStageUnknown:'暂存结果未知。可重试同一请求来核验候选 Agent 上的文件。',
  handoffStageInvalid:'暂存回执与当前准备记录不匹配。请重试核验。',
  HANDOFF_AGENT_CHANGED:'Agent 的登记已变化，请重新评估。',
  HANDOFF_STAGING_BUSY:'候选 Agent 正在处理暂存文件，稍后可重试。',
  HANDOFF_STAGING_CAPACITY:'候选 Agent 的私有暂存目录已达到容量上限。',
  HANDOFF_RAW_STAGING_REQUIRED:'候选 Agent 上缺少经过核验的原始采集档案。请重新暂存。',
  HANDOFF_MANAGED_CONTROL_CUT_REQUIRED:'控制事务无法同时核验日志切点。仍可核验证据或释放预留。',
  HANDOFF_FAILURE_HISTORY_STAGING_REQUIRED:'候选 Agent 上缺少经过核验的历史故障证据。请重新暂存。',
  HANDOFF_STAGING_BYTES_CHANGED:'候选 Agent 上已暂存文件的内容发生变化，未通过核验。',
  HANDOFF_ARCHIVE_BYTES_CHANGED:'原证据归档摘要已变化，请重新核对归档。'
 },{
  handoffStage:'Stage reports and receipts',handoffStageAgain:'Reverify staged reports and receipts',handoffStageRetry:'Retry the same staging request',
  handoffStageHelp:'Stage verified validation reports and closed receipts in the candidate Agent’s private directory. The original Agent keeps ownership.',
  handoffStageDone:'The candidate Agent verified the staged files.',handoffStageReports:'Validation reports',handoffStageReceipts:'Closed receipts',handoffStageRawCapture:'Raw capture archives',handoffAdoptedRawCapture:'Read-only raw capture archives',handoffStageFailureHistory:'Historical failure evidence',handoffAdoptedFailureHistory:'Read-only historical failure evidence',handoffStageRawPending:'After ownership transfer, raw archives are available as read-only history.',
  handoffStageUnknown:'The staging outcome is unknown. Retry the same request to verify the candidate’s files.',
  handoffStageInvalid:'The staging receipt does not match this preparation. Retry to verify.',
  HANDOFF_AGENT_CHANGED:'An Agent registered again. Reassess before staging.',
  HANDOFF_STAGING_BUSY:'The candidate is working with the staging files. Retry shortly.',
  HANDOFF_STAGING_CAPACITY:'The candidate’s private staging directories reached their capacity limit.',
  HANDOFF_RAW_STAGING_REQUIRED:'Restage the verified raw capture archive on the candidate Agent.',
  HANDOFF_MANAGED_CONTROL_CUT_REQUIRED:'The control transaction could not verify the journal cut. Evidence verification and reservation release remain available.',
  HANDOFF_FAILURE_HISTORY_STAGING_REQUIRED:'Restage the verified historical failure evidence on the candidate Agent.',
  HANDOFF_STAGING_BYTES_CHANGED:'The candidate’s staged contents changed and could not be verified.',
  HANDOFF_ARCHIVE_BYTES_CHANGED:'The original evidence archive digest changed. Review the archive again.'
 });

 bridge.register({
  handoffProbe:'核验位点与回执',handoffCheckedAt:'核验时间',handoffProbeExpired:'核验结果已过期，请重新核验。',
  handoffReviewReady:'预留所需证据已核验',handoffOriginEvidence:'原 Agent 回执',
  handoffReports:'校验报告',handoffFailures:'历史失败证据',handoffEvidenceDigest:'准备证据 SHA-256',
  handoffProbeVerified:'已核验',handoffProbeRefused:'核验未通过',handoffProbeSaved:'已更新证据核验结果。',
  handoffReservation:'接管准备预留',handoffPrepared:'预留中',handoffReleased:'已解除',handoffExpired:'已到期',
  handoffReserve:'预留任务槽和 Worker 槽',handoffRelease:'解除预留',
  handoffRetryReserve:'重试同一预留请求',handoffRetryRelease:'重试同一解除请求',
  handoffReservationId:'预留 ID',handoffReservedCapacity:'已预留任务槽 / Worker 槽',handoffExpires:'到期时间',
  handoffReserveImpact:'预留将在 15 分钟后到期。在解除或到期前，本任务暂停接收新命令和配置修改，候选 Agent 保留一个任务槽和一个 Worker 槽。',
  handoffActiveHelp:'预留有效期由控制数据库判断。可刷新查看状态，或解除预留以继续操作原任务。',
  handoffReservationUnknown:'提交结果未知。可刷新查询预留状态，或重试同一请求。',
  handoffReservationInvalid:'收到的预留回执无法核验。',
  handoffReservationSaved:'已收到预留回执。',handoffReservationReleased:'已解除预留，原任务恢复接收操作。',
  HANDOFF_ASSESSMENT_BLOCKED:'当前评估受阻，请先处理评估回执中的问题。',
  HANDOFF_ASSESSMENT_CHANGED:'评估证据或执行环境已变化，请核对最新评估。',
  HANDOFF_PREPARATION_ACTIVE:'该任务已有预留，请先解除或等待到期。',
  HANDOFF_PREPARATION_EVIDENCE_REQUIRED:'当前证据已过期或不足，请重新核验。',
  HANDOFF_PREPARATION_EVIDENCE_CHANGED:'预留证据已变化，请核对最新评估。',
  HANDOFF_PREPARATION_CHANGED:'预留已变化，请核对当前状态。',
  HANDOFF_PREPARATION_REQUEST_REUSED:'请求 ID 已用于其他操作，请查看最新预留后重新提交。',
  HANDOFF_PREPARATION_NOT_FOUND:'当前预留不存在，请刷新核对。',
  HANDOFF_JOURNAL_LEASE_ACTIVE:'原采集租约仍处于活动状态。',
  HANDOFF_CANDIDATE_ACCESS_UNVERIFIED:'候选 Agent 无法核验所需的连接或密钥引用。',
  HANDOFF_CLOSED_RECEIPT_UNPROVEN:'无法核验原 Agent 的已完成操作回执。',
  HANDOFF_ARCHIVE_DISABLED:'该 Agent 未启用共享证据存储。',
  HANDOFF_ARCHIVE_MOUNT_MISSING:'该 Agent 的共享证据目录不可用。',
  HANDOFF_REPORT_ARCHIVE_RESTORE_REQUIRED:'请先还原已归档的校验报告。',
  ORIGINAL_PRIVATE_EVIDENCE_CUSTODY_UNPROVEN:'原 Agent 的私有证据移交仍需核验。'
 },{
  handoffProbe:'Verify checkpoint and receipts',handoffCheckedAt:'Verified at',
  handoffProbeExpired:'Evidence has expired. Run verification again.',
  handoffReviewReady:'Evidence verified for reservation',handoffOriginEvidence:'Original Agent receipts',
  handoffReports:'Validation reports',handoffFailures:'Historical failure evidence',handoffEvidenceDigest:'Preparation evidence SHA-256',
  handoffProbeVerified:'Verified',handoffProbeRefused:'Verification refused',handoffProbeSaved:'Evidence verification updated.',
  handoffReservation:'Handoff preparation reservation',handoffPrepared:'Reserved',handoffReleased:'Released',handoffExpired:'Expired',
  handoffReserve:'Reserve task and Worker slots',handoffRelease:'Release reservation',
  handoffRetryReserve:'Retry the same reservation request',handoffRetryRelease:'Retry the same release request',
  handoffReservationId:'Reservation ID',handoffReservedCapacity:'Reserved task slots / Worker slots',handoffExpires:'Expires at',
  handoffReserveImpact:'The reservation lasts 15 minutes. Until release or expiry, the task pauses new commands and configuration changes, and the candidate holds one task slot and one Worker slot.',
  handoffActiveHelp:'The control database determines the expiry. Refresh to check the status, or release the reservation to continue operating the original task.',
  handoffReservationUnknown:'The submission outcome is unknown. Refresh the reservation status or retry the same request.',
  handoffReservationInvalid:'The received reservation receipt could not be verified.',
  handoffReservationSaved:'Reservation receipt received.',handoffReservationReleased:'Reservation released. The original task accepts operations again.',
  HANDOFF_ASSESSMENT_BLOCKED:'Resolve the blockers in the assessment receipt before checking evidence.',
  HANDOFF_ASSESSMENT_CHANGED:'Assessment evidence or the execution environment changed. Review the latest assessment.',
  HANDOFF_PREPARATION_ACTIVE:'This task already has a reservation. Release it or wait for expiry.',
  HANDOFF_PREPARATION_EVIDENCE_REQUIRED:'Evidence has expired or is incomplete. Verify it again.',
  HANDOFF_PREPARATION_EVIDENCE_CHANGED:'Reservation evidence changed. Review the latest assessment.',
  HANDOFF_PREPARATION_CHANGED:'The reservation changed. Review the current status.',
  HANDOFF_PREPARATION_REQUEST_REUSED:'The request ID belongs to another action. Review the current reservation before submitting again.',
  HANDOFF_PREPARATION_NOT_FOUND:'The reservation no longer exists. Refresh to check its status.',
  HANDOFF_JOURNAL_LEASE_ACTIVE:'The original capture lease is still active.',
  HANDOFF_CANDIDATE_ACCESS_UNVERIFIED:'The candidate Agent could not verify required connections or secret references.',
  HANDOFF_CLOSED_RECEIPT_UNPROVEN:'The original Agent’s closed operation receipts could not be verified.',
  HANDOFF_ARCHIVE_DISABLED:'Shared evidence storage is disabled on this Agent.',
  HANDOFF_ARCHIVE_MOUNT_MISSING:'This Agent’s shared evidence directory is unavailable.',
  HANDOFF_REPORT_ARCHIVE_RESTORE_REQUIRED:'Restore the archived validation report before verification.',
  ORIGINAL_PRIVATE_EVIDENCE_CUSTODY_UNPROVEN:'Custody of the original Agent’s private evidence still needs verification.'
 });

 bridge.register({
  handoffProgress:'查看 Agent 交接状态',handoffTransferReceipt:'交接回执',handoffNoPreparation:'该任务暂无交接准备记录。',
  handoffOriginalFrozen:'原写入者已冻结',handoffOriginalFreezeCancelled:'原写入者冻结已取消',
  handoffOriginalFrozenHelp:'归属保留在原 Agent。原写入者保持停止，普通启动、配置修改和解除预留受阻。',
  handoffOfflineOriginalState:'原 Agent 控制状态',handoffFreezeInventory:'冻结库存摘要',
  handoffOfflineCommit:'切换已冻结任务的归属',handoffOfflineCommitRetry:'重试同一离线切换',
  handoffOfflineCommitHelp:'冻结管理员可切换归属。候选仍停止，隔离目标和启动 Worker 需要另外审批。',
  handoffOfflineConfirm:'我确认原 Agent 已无法使用，批准切换此冻结任务的归属。',
  handoffOfflineCommitted:'离线切换审批 ID',
  HANDOFF_OFFLINE_ORIGINAL_NOT_EXPIRED:'原 Agent 的已登记租约尚未过期。请刷新交接状态。',
  HANDOFF_OFFLINE_ORIGINAL_CHANGED:'原 Agent 的登记绑定已变化。请重新核对冻结回执。',
  HANDOFF_OFFLINE_CANDIDATE_CHANGED:'候选 Agent 的绑定或密封配置已变化。请重新核对冻结回执。',
  HANDOFF_OFFLINE_INVENTORY_UNVERIFIED:'候选的冻结库存无法验证。请核对受保护的暂存副本。',
  HANDOFF_OFFLINE_COMMIT_REQUEST_REUSED:'此冻结已由另一个离线请求消费。请刷新交接状态。',
  handoffFreezeRequest:'冻结请求 ID',handoffFreezeEpoch:'冻结日志纪元',
  handoffCommitted:'归属已提交',handoffProjected:'候选配置已安装',handoffEvidenceAdopted:'证据已接收',
  handoffCommittedHelp:'任务归属已变更。候选 Agent 正在等待安装已审阅的任务配置。',
  handoffProjectedHelp:'候选 Agent 已保存停止状态的任务配置。接收报告和回执后会更新交接状态。',
  handoffEvidenceAdoptedHelp:'候选 Agent 已接收经核验的报告和已办结回执。交接回执记录的任务状态为停止。',
  handoffFencePending:'等待目标隔离',handoffTargetsFenced:'目标已隔离',
  handoffFencePendingHelp:'候选 Agent 正在核验并提高目标端纪元。Worker 保持停止。',
  handoffTargetsFencedHelp:'目标端已拒绝旧纪元写入。候选 Worker 保持停止。',handoffFencedCount:'已隔离的目标数',
  handoffFence:'隔离 SQL 目标',handoffFenceHelp:'隔离会提高 SQL 目标的写入纪元。候选 Worker 保持停止。',
  handoffFenceRetry:'重试同一隔离审批',handoffFenceUnknown:'审批结果未知。请重试同一审批或刷新状态。',
  handoffStartPending:'等待启动授权',handoffWorkerAdmitted:'已授权启动 Worker',handoffWorkerProjected:'已保存启动意图',
  handoffStartPendingHelp:'候选 Agent 正在复核源端断点和目标隔离。请刷新查看进度。',
  handoffWorkerAdmittedHelp:'共享日志已开放给候选 Worker。候选 Agent 正在保存启动意图。',
  handoffWorkerProjectedHelp:'候选 Agent 已保存从交接断点续传的启动意图。实际运行状态见任务详情。',
  handoffStartWorker:'启动候选 Worker',handoffStartWorkerHelp:'批准候选 Worker 从交接断点继续同步。',
  handoffStartRetry:'重试同一启动审批',handoffStartReapprove:'重新审批启动',
  handoffFenceReapprove:'重新审批目标隔离',handoffReapprovalHelp:'授权已变化。请重新审批当前交接。',handoffPendingProblem:'最近一次核验受阻',
  HANDOFF_TARGET_FENCE_CHANGED:'目标检查点和已签名的隔离回执不一致。请核对目标后刷新。',
  HANDOFF_TARGET_CUT_CHANGED:'目标断点已变化，任务继续停驻。',
  HANDOFF_WORKER_JOURNAL_NOT_PARKED:'共享日志的停驻状态已变化，请核对当前交接记录。',
  handoffCommitOwner:'确认切换归属',handoffCommitRetry:'重试同一归属切换',
  handoffCommitOwnerHelp:'切换后由候选 Agent 保存停止状态的任务，并接收校验报告和回执。下一步仍须隔离 SQL 目标。',
  handoffCommitUnknown:'归属切换结果待确认。可用同一请求重试或刷新交接状态。',
  HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED:'当前任务还有未接收的源或故障证据，需要先完成证据接管。',
  HANDOFF_EXECUTION_REQUEST_CHANGED:'已有归属切换请求。刷新交接状态后重试原请求。',
  HANDOFF_TARGET_FENCE_REQUIRED:'启动前须完成所有 SQL 目标的隔离。',
  HANDOFF_WORKER_START_REQUEST_CHANGED:'已有启动审批。请刷新交接状态。',
  handoffCommittedRevision:'已提交任务修订',handoffCommittedConfig:'已提交配置版本',handoffJournalEpoch:'已提交日志代次',
  handoffFormerOwner:'原 Agent',handoffCurrentOwner:'当前归属 Agent',handoffTransferId:'交接准备 ID',handoffRefreshProgress:'刷新交接状态'
 },{
  handoffProgress:'View Agent handoff status',handoffTransferReceipt:'Handoff receipt',handoffNoPreparation:'This task has no handoff preparation.',
  handoffOriginalFrozen:'Original writer frozen',handoffOriginalFreezeCancelled:'Original writer freeze cancelled',
  handoffOriginalFrozenHelp:'The original Agent keeps ownership and stays stopped. A freeze blocks ordinary start, configuration changes and reservation release.',
  handoffOfflineOriginalState:'Original Agent control state',handoffFreezeInventory:'Frozen inventory digest',
  handoffOfflineCommit:'Transfer frozen task ownership',handoffOfflineCommitRetry:'Retry the same offline transfer',
  handoffOfflineCommitHelp:'The administrator who froze the task can transfer ownership. The candidate stays stopped. Target fencing and Worker start require separate approvals.',
  handoffOfflineConfirm:'I confirm that the original Agent is unavailable and approve transferring this frozen task.',
  handoffOfflineCommitted:'Offline transfer approval ID',
  HANDOFF_OFFLINE_ORIGINAL_NOT_EXPIRED:'The registered original Agent lease is still valid. Refresh the handoff status.',
  HANDOFF_OFFLINE_ORIGINAL_CHANGED:'The original Agent registration changed. Review the frozen preparation.',
  HANDOFF_OFFLINE_CANDIDATE_CHANGED:'The candidate Agent binding or sealed configuration changed. Review the frozen preparation.',
  HANDOFF_OFFLINE_INVENTORY_UNVERIFIED:'The candidate could not verify the frozen inventory. Check its protected staged copy.',
  HANDOFF_OFFLINE_COMMIT_REQUEST_REUSED:'A different offline request already consumed the freeze. Refresh the handoff status.',
  handoffFreezeRequest:'Freeze request ID',handoffFreezeEpoch:'Frozen journal epoch',
  handoffCommitted:'Ownership committed',handoffProjected:'Candidate configuration installed',handoffEvidenceAdopted:'Evidence received',
  handoffCommittedHelp:'Task ownership has changed. The candidate Agent is waiting to install the reviewed configuration.',
  handoffProjectedHelp:'The candidate Agent saved the task configuration in a stopped state. Handoff status will update when it receives the reports and receipts.',
  handoffEvidenceAdoptedHelp:'The candidate Agent received the verified reports and closed receipts. The handoff receipt records the task as stopped.',
  handoffFencePending:'Target fencing pending',handoffTargetsFenced:'Targets fenced',
  handoffFencePendingHelp:'The candidate is verifying and advancing the target epochs. The Worker remains stopped.',
  handoffTargetsFencedHelp:'The targets reject writes from the previous epoch. The candidate Worker remains stopped.',handoffFencedCount:'Fenced targets',
  handoffFence:'Fence SQL targets',handoffFenceHelp:'Fencing advances the SQL target writer epochs. The candidate Worker remains stopped.',
  handoffFenceRetry:'Retry the same fence approval',handoffFenceUnknown:'Approval outcome unknown. Retry the same approval or refresh the status.',
  handoffStartPending:'Worker start pending',handoffWorkerAdmitted:'Worker start authorized',handoffWorkerProjected:'Start intent saved',
  handoffStartPendingHelp:'The candidate is verifying the source cut and native target fences. Refresh to view progress.',
  handoffWorkerAdmittedHelp:'The journal admits the candidate Worker. The candidate Agent is saving the start intent.',
  handoffWorkerProjectedHelp:'The candidate Agent saved the intent to resume from the handoff cut. See the task view for the actual Worker state.',
  handoffStartWorker:'Start candidate Worker',handoffStartWorkerHelp:'Approve the candidate Worker to resume synchronization from the handoff cut.',
  handoffStartRetry:'Retry the same start approval',handoffStartReapprove:'Approve Worker start again',
  handoffFenceReapprove:'Approve SQL fencing again',handoffReapprovalHelp:'Authorization changed. Approve this handoff step again.',handoffPendingProblem:'Latest verification blocker',
  HANDOFF_TARGET_FENCE_CHANGED:'A target checkpoint differs from the signed fence receipt. Verify the target and refresh.',
  HANDOFF_TARGET_CUT_CHANGED:'The target cut changed. The task remains parked.',
  HANDOFF_WORKER_JOURNAL_NOT_PARKED:'The shared journal parking state changed. Review the current handoff.',
  handoffCommitOwner:'Commit ownership transfer',handoffCommitRetry:'Retry the same ownership transfer',
  handoffCommitOwnerHelp:'The candidate will install the task in a stopped state and receive validation reports and receipts. Fence the SQL targets in the next step.',
  handoffCommitUnknown:'The ownership transfer outcome is pending. Retry the same request or refresh the handoff status.',
  HANDOFF_RECOVERY_EVIDENCE_ADMISSION_REQUIRED:'This task has source or failure evidence still awaiting adoption.',
  HANDOFF_EXECUTION_REQUEST_CHANGED:'An ownership transfer is already assigned. Refresh the status and retry the original request.',
  HANDOFF_TARGET_FENCE_REQUIRED:'Fence every SQL destination before starting the Worker.',
  HANDOFF_WORKER_START_REQUEST_CHANGED:'A start approval already exists. Refresh the handoff status.',
  handoffCommittedRevision:'Committed task revision',handoffCommittedConfig:'Committed configuration version',handoffJournalEpoch:'Committed journal epoch',
  handoffFormerOwner:'Original Agent',handoffCurrentOwner:'Current owner Agent',handoffTransferId:'Preparation ID',handoffRefreshProgress:'Refresh handoff status'
 });

 bridge.register({
  handoffOfflineTitle:'离线接管预检',handoffOfflineBlocked:'离线接管受阻',
  handoffOfflineAuthority:'任务与已审查配置',handoffOfflineCut:'历史排空切点',
  handoffOfflineCustody:'历史报告和结案回执',handoffOfflineMatch:'匹配',handoffOfflineChanged:'已变化',
  handoffOfflineRecorded:'已记录',handoffOfflineMissing:'未记录',
  HANDOFF_OFFLINE_DURABLE_FREEZE_REQUIRED:'缺少已持久化的原写入者冻结证明。',
  HANDOFF_OFFLINE_COMPLETE_ARCHIVE_REQUIRED:'缺少可独立访问的完整恢复归档。',
  HANDOFF_OFFLINE_REVIEW_CHANGED:'任务修订或配置已变化，原评估不再匹配。',
  HANDOFF_OFFLINE_STOPPED_CUT_UNRECORDED:'评估中缺少已排空的切点。',
  HANDOFF_OFFLINE_REPORT_RECEIPT_CUSTODY_UNRECORDED:'评估中缺少经核验的报告和结案回执。'
 },{
  handoffOfflineTitle:'Offline handoff preflight',handoffOfflineBlocked:'Offline handoff blocked',
  handoffOfflineAuthority:'Task and reviewed configuration',handoffOfflineCut:'Historical drained checkpoint',
  handoffOfflineCustody:'Historical reports and closed receipts',handoffOfflineMatch:'Matches',handoffOfflineChanged:'Changed',
  handoffOfflineRecorded:'Recorded',handoffOfflineMissing:'Missing',
  HANDOFF_OFFLINE_DURABLE_FREEZE_REQUIRED:'A durable freeze for the original writer is missing.',
  HANDOFF_OFFLINE_COMPLETE_ARCHIVE_REQUIRED:'A complete, independently accessible recovery archive is missing.',
  HANDOFF_OFFLINE_REVIEW_CHANGED:'The task revision or configuration changed since this assessment.',
  HANDOFF_OFFLINE_STOPPED_CUT_UNRECORDED:'The assessment lacks a drained checkpoint.',
  HANDOFF_OFFLINE_REPORT_RECEIPT_CUSTODY_UNRECORDED:'The assessment lacks verified reports and closed receipts.'
 });

 const nodeId=/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
 const revisionId=/^[0-9a-f-]{36}$/;
 const requestPattern=/^[A-Za-z0-9_-]{8,128}$/;
 const sha256=/^[0-9a-f]{64}$/;
 const stateLabels={BLOCKED:'handoffStateBlocked',EVIDENCE_REQUIRED:'handoffStateEvidence',STALE:'handoffStateStale'};
 const offlineControlStates=['STALE','STOPPED','UNKNOWN'];
 const preparationLabels={PREPARED:'handoffPrepared',RELEASED:'handoffReleased',EXPIRED:'handoffExpired',
  ORIGINAL_FROZEN:'handoffOriginalFrozen',FREEZE_CANCELLED:'handoffOriginalFreezeCancelled'};
 const transferLabels={COMMITTED:'handoffCommitted',PROJECTED:'handoffProjected',EVIDENCE_ADOPTED:'handoffEvidenceAdopted',FENCE_PENDING:'handoffFencePending',TARGETS_FENCED:'handoffTargetsFenced',
  START_PENDING:'handoffStartPending',WORKER_ADMITTED:'handoffWorkerAdmitted',WORKER_PROJECTED:'handoffWorkerProjected'};
 const startStates=['START_PENDING','WORKER_ADMITTED','WORKER_PROJECTED'];
 function workerAdmitted(state){return state==='WORKER_ADMITTED'||state==='WORKER_PROJECTED';}
 let view=null;

 function node(tag,value='',className){
  const element=document.createElement(tag);element.textContent=value;
  if(className)element.className=className;
  return element;
 }
 function nodeGrant(id){
  const scope=bridge.session()?.nodeScope;
  return typeof id==='string'&&nodeId.test(id)&&(!Array.isArray(scope)||scope.includes(id));
 }
 function eligible(task){
  const session=bridge.session();
  const visibleTask=task?.formatVersion===3||task?.formatVersion==null&&task?.statusAvailable===false;
  return visibleTask&&session?.taskControlScope==='GLOBAL'&&session.role==='ADMIN'
   &&can('TASK_CONFIG')&&nodeGrant(task.ownerNodeId);
 }
 function current(v){
  const task=bridge.task(v.taskId);
  return view===v&&eligible(task)&&task.ownerNodeId===v.owner&&bridge.session()?.id===v.actor;
 }
 function close(restore=false){
  if(!view)return;
  const {dialog,taskId}=view;view=null;
  dialog.close();dialog.remove();
  if(restore){
   const launcher=document.querySelector('#task-detail [data-handoff-task]');
   if(launcher?.dataset.handoffTask===taskId)launcher.focus({preventScroll:true});
  }
 }
 function endpoint(v){return '/tasks/'+encodeURIComponent(v.taskId)+'/handoff';}
 function preparationEndpoint(v){return endpoint(v)+'/preparation';}
 function postOptions(body,id){
  return {method:'POST',headers:{'Content-Type':'application/json',...(id?{'X-Request-ID':id}:{})},body};
 }
 function freshAssessment(v){
  const receipt=v.receipt;
  return receipt&&receipt.state!=='STALE'&&receipt.taskRevision===bridge.task(v.taskId)?.revision
   &&receipt.originalNodeId===v.owner;
 }
 function reviewReady(v){
  return freshAssessment(v)&&v.receipt.state==='EVIDENCE_REQUIRED'&&v.receipt.probe?.current===true
   &&sha256.test(v.receipt.preparationEvidenceSha256||'');
 }
 function transferred(preparation){return Object.hasOwn(transferLabels,preparation?.state);}
 function preparationScope(v,preparation){
  if(!nodeGrant(preparation.originalNodeId)||!nodeGrant(preparation.candidateNodeId)
   ||preparation.originalNodeId===preparation.candidateNodeId)return false;
  if(!transferred(preparation))return preparation.originalNodeId===v.owner;
  const task=bridge.task(v.taskId);
  return preparation.candidateNodeId===v.owner
   &&(preparation.committedRevision===task?.revision||workerAdmitted(preparation.state))
   &&preparation.committedConfigurationVersion===task?.configurationVersion;
 }
 function readOnly(v){
  return bridge.task(v.taskId)?.formatVersion!==3||transferred(v.preparation)
   ||v.preparation?.state==='ORIGINAL_FROZEN'||v.unverifiedTransfer;
 }
 function offlineReadback(v){
  const task=bridge.task(v.taskId);
  return task?.statusAvailable===false&&offlineControlStates.includes(task.ownerControlState)
   &&!transferred(v.preparation)&&v.preparation?.state!=='ORIGINAL_FROZEN'&&!v.unverifiedTransfer;
 }
 function offlineReceipt(v,receipt){
  return receipt?.originalNodeId===v.owner&&offlineControlStates.includes(receipt.originalControlState)
   &&['STALE','BLOCKED'].includes(receipt.state);
 }
 function evidenceCount(value){return Number.isSafeInteger(value)&&value>0&&value<=64;}
 function validOfflineCommit(preparation){
  const approval=preparation.offlineOwnerCommit;
  if(approval==null)return true;
  return transferred(preparation)&&requestPattern.test(approval.requestId||'')
   &&requestPattern.test(approval.freezeRequestId||'')&&approval.requestId===preparation.commitRequestId
   &&approval.requestId!==preparation.id&&approval.freezeRequestId!==preparation.id
   &&approval.requestId!==approval.freezeRequestId&&Number.isFinite(Date.parse(approval.committedAt))
   &&Date.parse(approval.committedAt)>=Date.parse(preparation.createdAt);
 }
 function validTransfer(preparation){
  if(!validOfflineCommit(preparation)||!revisionId.test(preparation.committedRevision||'')||preparation.committedRevision===preparation.taskRevision
   ||!revisionId.test(preparation.committedConfigurationVersion||'')||typeof preparation.activatedJournalEpoch!=='string'
   ||!/^[1-9][0-9]{0,18}$/.test(preparation.activatedJournalEpoch))return false;
  const evidence=preparation.adoptedEvidence;
  const fencing=['EVIDENCE_ADOPTED','FENCE_PENDING'].includes(preparation.state);
  const starting=['TARGETS_FENCED','START_PENDING'].includes(preparation.state);
  if(preparation.targetFenceAvailable!=null&&(!fencing||preparation.targetFenceAvailable!==true))return false;
  if(preparation.workerStartAvailable!=null&&(!starting||preparation.workerStartAvailable!==true))return false;
  const problem=preparation.pendingProblem;
  if(problem!=null&&(!['START_PENDING','FENCE_PENDING'].includes(preparation.state)
   ||typeof problem.code!=='string'||!/^([A-Z][A-Z0-9_]{2,119})$/.test(problem.code)
   ||!Number.isFinite(Date.parse(problem.observedAt))||Date.parse(problem.observedAt)<Date.parse(preparation.createdAt)))return false;
  const start=preparation.workerStart,fence=preparation.targetFence;
  if(startStates.includes(preparation.state)){
   if(!requestPattern.test(start?.requestId||'')||start.requestId===fence?.requestId)return false;
  }else if(start!=null)return false;
  if(['FENCE_PENDING','TARGETS_FENCED',...startStates].includes(preparation.state)){
   if(!requestPattern.test(fence?.requestId||'')||!Number.isSafeInteger(fence.destinationsFenced)
    ||(preparation.state==='FENCE_PENDING'?fence.destinationsFenced!==0:fence.destinationsFenced<1||fence.destinationsFenced>8))return false;
  }else if(fence!=null)return false;
  return ['EVIDENCE_ADOPTED','FENCE_PENDING','TARGETS_FENCED',...startStates].includes(preparation.state)?evidence!=null
    &&Number.isSafeInteger(evidence.validationReports)&&evidence.validationReports>=0
    &&Number.isSafeInteger(evidence.closedReceipts)&&evidence.closedReceipts>0
    &&(evidence.rawCaptures==null||evidenceCount(evidence.rawCaptures))
    &&(evidence.failureRecords==null||evidenceCount(evidence.failureRecords))
    :evidence==null;
 }
 function nextHandoffAction(v){
  const plan=v.preparation;
  if(!v.preparationLoaded||!plan||!preparationScope(v,plan))return null;
  const task=bridge.task(v.taskId);
  if(plan.state==='ORIGINAL_FROZEN'&&validOriginalFreeze(plan)&&plan.writerActivation?.checkedInControlTransaction===true
   &&task?.statusAvailable===false&&task.ownerControlState==='STALE'&&task.revision===plan.taskRevision)
   return 'commit-offline-owner';
  if(plan.state==='PREPARED'&&!readOnly(v)&&plan.writerActivation?.checkedInControlTransaction===true
   &&v.stageAttempt?.counts
   &&sameStagePreparation(plan,v.stageAttempt.plan))return 'commit-owner';
  if(['EVIDENCE_ADOPTED','FENCE_PENDING'].includes(plan.state)&&plan.targetFenceAvailable===true)return 'fence-targets';
  if(['TARGETS_FENCED','START_PENDING'].includes(plan.state)&&plan.workerStartAvailable===true)return 'start-worker';
  return null;
 }
 function activeReservation(v){return v.preparation?.state==='PREPARED';}
 function stagePreparation(v){
  return activeReservation(v)&&v.preparation.taskRevision===bridge.task(v.taskId)?.revision
   &&sha256.test(v.preparation.custodySha256||'')?v.preparation:null;
 }
 function sameStagePreparation(plan,binding){
  return plan?.id===binding.id&&plan.taskRevision===binding.taskRevision
   &&plan.candidateNodeId===binding.candidateNodeId&&plan.evidenceSha256===binding.evidenceSha256
   &&plan.custodySha256===binding.custodySha256;
 }
 function timestamp(value){
  const time=node('time',new Date(value).toLocaleString(document.documentElement.lang,{dateStyle:'medium',timeStyle:'long'}));
  time.dateTime=value;
  return time;
 }
 function refusal(code){
  return typeof code==='string'&&/^[A-Z][A-Z0-9_]{2,119}$/.test(code)?bridge.explain(code):t('handoffUnknownBlocker');
 }

 function verified(v,response,attempt){
  if(response?.taskId!==v.taskId||!requestPattern.test(response.id||'')||!Object.hasOwn(stateLabels,response.state)
    ||!revisionId.test(response.taskRevision||'')||!Number.isFinite(Date.parse(response.assessedAt))
    ||response.executionAuthorized!==false||!nodeGrant(response.originalNodeId)||!nodeGrant(response.candidateNodeId)
    ||!Array.isArray(response.blockers)||response.blockers.length>12
    ||attempt&&(response.id!==attempt.id||response.candidateNodeId!==attempt.candidate
                  ||response.originalNodeId!==v.owner||response.taskRevision!==attempt.revision)){
   throw new Error(t('handoffInvalidReceipt'));
  }
  return response;
 }
 function validOriginalFreeze(preparation){
  const freeze=preparation.originalFreeze;
  if(!['ORIGINAL_FROZEN','FREEZE_CANCELLED'].includes(preparation.state))return freeze==null;
  return typeof freeze?.requestId==='string'&&requestPattern.test(freeze.requestId)&&freeze.requestId!==preparation.id&&freeze.requestId!==preparation.releaseId
   &&sha256.test(freeze.manifestSha256||'')&&typeof freeze.journalEpoch==='string'&&/^[1-9][0-9]{0,18}$/.test(freeze.journalEpoch)
   &&Number.isFinite(Date.parse(freeze.frozenAt))&&Date.parse(freeze.frozenAt)>=Date.parse(preparation.createdAt)
   &&Date.parse(freeze.frozenAt)<Date.parse(preparation.expiresAt);
 }
 function verifiedPreparation(v,response,attempt){
  const state=response?.state;
  const active=state==='PREPARED',transferredState=transferred(response);
  if(response?.taskId!==v.taskId||!preparationScope(v,response)||!requestPattern.test(response.id||'')
    ||!requestPattern.test(response.assessmentId||'')||!revisionId.test(response.taskRevision||'')
    ||!(Object.hasOwn(preparationLabels,state)||transferredState&&validTransfer(response))
    ||response.executionAuthorized!==workerAdmitted(state)
    ||response.commitRequestId!=null&&(!requestPattern.test(response.commitRequestId)||!(active||transferredState))
    ||response.commitBlocker!=null&&(!active||response.writerActivation?.checkedInControlTransaction!==false
     ||response.commitBlocker!=='HANDOFF_MANAGED_CONTROL_CUT_REQUIRED')
    ||response.reservedTaskSlots!==Number(active)||response.reservedWorkerSlots!==Number(active)
    ||!sha256.test(response.evidenceSha256||'')||!Number.isFinite(Date.parse(response.createdAt))
    ||!(Date.parse(response.expiresAt)>Date.parse(response.createdAt))
    ||(['RELEASED','FREEZE_CANCELLED'].includes(state)?!requestPattern.test(response.releaseId||''):response.releaseId!=null)
    ||!validOriginalFreeze(response)||!transferredState&&response.offlineOwnerCommit!=null
    ||attempt&&!matchesPreparation(response,attempt)){
   throw new Error(t('handoffReservationInvalid'));
  }
  return response;
 }
 function matchesPreparation(receipt,attempt){
  return receipt&&receipt.id===attempt.preparationId&&receipt.assessmentId===attempt.assessmentId
   &&receipt.taskRevision===attempt.revision&&receipt.candidateNodeId===attempt.candidate
   &&receipt.evidenceSha256===attempt.evidence
   &&(!attempt.release||receipt.state==='RELEASED'&&receipt.releaseId===attempt.id);
 }
 function renderProbe(v,probe){
  if(!probe||!Number.isFinite(Date.parse(probe.checkedAt)))return;
  const facts=node('dl');
  const checked=node('dd');checked.append(timestamp(probe.checkedAt));
  facts.append(node('dt',t('handoffCheckedAt')),checked);
  const original=probe.originalEvidence;
  for(const [label,result] of [
   ['handoffCandidateNode',probe.candidate],
   ['handoffOriginEvidence',probe.sharedReceiptCustody||original?.closedReceipts||original],
   ['handoffReports',probe.sharedReportCustody||original?.shareableReports],
   ['handoffFailures',probe.sharedFailureEvidenceCustody||original?.historicalFailureEvidence]
  ]){
   if(result?.state==='VERIFIED'||result?.state==='REFUSED'){
    const value=result.state==='VERIFIED'?t('handoffProbeVerified'):refusal(result.code);
    facts.append(node('dt',t(label)),node('dd',value));
   }
  }
  if(!probe.current)v.result.append(node('p',t('handoffProbeExpired'),'callout warning'));
  v.result.append(facts);
 }
 function renderOfflinePreflight(v,receipt){
  const preflight=receipt.offlinePreflight;
  if(!offlineReceipt(v,receipt)||preflight?.state!=='BLOCKED'
   ||typeof preflight.authorityMatchesReview!=='boolean'||typeof preflight.historicalDrainedCut!=='boolean'
   ||typeof preflight.historicalReportsAndReceipts!=='boolean'||!Array.isArray(preflight.blockers)
   ||preflight.blockers.length<1||preflight.blockers.length>5)return;
  const section=node('section'),heading=node('h4',t('handoffOfflineTitle')),facts=node('dl'),blockers=node('ul');
  section.dataset.handoffOfflinePreflight='';
  heading.id='handoff-offline-preflight-title';section.setAttribute('aria-labelledby',heading.id);
  for(const [label,value] of [
   ['handoffOfflineAuthority',preflight.authorityMatchesReview?'handoffOfflineMatch':'handoffOfflineChanged'],
   ['handoffOfflineCut',preflight.historicalDrainedCut?'handoffOfflineRecorded':'handoffOfflineMissing'],
   ['handoffOfflineCustody',preflight.historicalReportsAndReceipts?'handoffOfflineRecorded':'handoffOfflineMissing']
  ])facts.append(node('dt',t(label)),node('dd',t(value)));
  for(const code of preflight.blockers)blockers.append(node('li',refusal(code)));
  section.append(heading,node('p',t('handoffOfflineBlocked'),'callout warning'),facts,blockers);
  v.result.append(section);
 }
 function renderReceipt(v){
  const receipt=v.receipt,revision=bridge.task(v.taskId)?.revision,locale=document.documentElement.lang;
  const stale=receipt&&(receipt.state==='STALE'||receipt.taskRevision!==revision||receipt.originalNodeId!==v.owner);
  if(v.drawn&&v.drawn.receipt===receipt&&v.drawn.stale===stale&&v.drawn.loaded===v.loaded&&v.drawn.locale===locale)return;
  v.drawn={receipt,stale,loaded:v.loaded,locale};
  v.result.replaceChildren();
  if(!receipt){
   if(v.loaded)v.result.append(node('p',t('handoffNone'),'subtle'));
   return;
  }
  const status=stale?'STALE':receipt.state;
  const ready=reviewReady(v),heading=node('h3',t('handoffReceipt'));
  const badge=node('span',t(ready?'handoffReviewReady':stateLabels[status]),'badge '+(ready?'good':'warn'));
  const facts=node('dl');
  for(const [label,value] of [
   ['handoffOriginal',receipt.originalNodeId],['handoffCandidateNode',receipt.candidateNodeId+' · '+t(receipt.candidateState||'UNKNOWN')],
   ...(['HEALTHY','STALE','STOPPED','UNKNOWN'].includes(receipt.originalControlState)
    ?[['handoffOriginalLease',t(receipt.originalControlState)]]:[]),
   ['handoffAssigned',receipt.candidateAssignedTasks+' / '+receipt.candidateTaskCapacity],
   ['handoffWorkers',receipt.candidateWorkerCapacity],
   ['handoffAt',new Date(receipt.assessedAt).toLocaleString()]
  ])facts.append(node('dt',t(label)),node('dd',String(value)));
  const blockers=node('ul');
  for(const code of receipt.blockers){
   const label=typeof code==='string'?t(code):'';
   blockers.append(node('li',typeof label==='string'&&label&&label!==code?label:t('handoffUnknownBlocker')));
  }
  const details=node('details'),summary=node('summary',t('handoffId'));
  details.append(summary,node('code',receipt.id),node('p',t('handoffRevision')+' '+receipt.taskRevision));
  if(receipt.originalCut?.checkpointHash)details.append(node('p',t('handoffCut')),node('code',receipt.originalCut.checkpointHash));
  v.result.append(heading,badge,facts);
  if(blockers.childElementCount)v.result.append(blockers);
  v.result.append(details);
  renderProbe(v,receipt.probe);
  renderOfflinePreflight(v,receipt);
 }
 function renderPreparation(v,readonly){
  const preparation=v.preparation,proof=!readonly&&reviewReady(v)?v.receipt.preparationEvidenceSha256:null;
  const empty=readonly&&v.preparationLoaded&&!preparation,locale=document.documentElement.lang;
  const ownerControlState=preparation?.state==='ORIGINAL_FROZEN'?bridge.task(v.taskId)?.ownerControlState||'UNKNOWN':null;
  if(v.drawnPreparation?.receipt===preparation&&v.drawnPreparation?.proof===proof
   &&v.drawnPreparation?.empty===empty&&v.drawnPreparation?.locale===locale
   &&v.drawnPreparation?.ownerControlState===ownerControlState)return;
  v.drawnPreparation={receipt:preparation,proof,empty,locale,ownerControlState};
  v.preparationResult.replaceChildren();
  if(empty)v.preparationResult.append(node('p',t('handoffNoPreparation'),'subtle'));
  if(!preparation&&!proof)return;
  if(transferred(preparation)){
   renderTransfer(v,preparation);
   return;
  }
  v.preparationResult.append(node('h3',t('handoffReservation')));
  if(preparation){
   const active=activeReservation(v),facts=node('dl');
   v.preparationResult.append(node('span',t(preparationLabels[preparation.state]),'badge '+(active?'good':'neutral')));
   for(const [label,value] of [
    ['handoffOriginal',preparation.originalNodeId],['handoffCandidateNode',preparation.candidateNodeId],
    ['handoffReservedCapacity',preparation.reservedTaskSlots+' / '+preparation.reservedWorkerSlots],
    ['handoffExpires',timestamp(preparation.expiresAt)]
   ]){
    const item=node('dd');item.append(value);facts.append(node('dt',t(label)),item);
   }
   const details=node('details');
   details.append(node('summary',t('handoffReservationId')),node('code',preparation.id));
   v.preparationResult.append(facts,details);
   const freeze=preparation.originalFreeze;
   if(freeze){
    facts.append(node('dt',t('handoffFreezeEpoch')),node('dd',freeze.journalEpoch));
    details.append(node('p',t('handoffFreezeRequest')),node('code',freeze.requestId),
     node('p',t('handoffFreezeInventory')),node('code',freeze.manifestSha256));
    if(preparation.state==='ORIGINAL_FROZEN'){
     facts.append(node('dt',t('handoffOfflineOriginalState')),node('dd',t(ownerControlState)));
     v.preparationResult.append(node('p',t('handoffOriginalFrozenHelp'),'callout warning'));
    }
   }
   if(active)v.preparationResult.append(node('p',t('handoffActiveHelp'),'subtle'));
   if(active&&preparation.commitBlocker==='HANDOFF_MANAGED_CONTROL_CUT_REQUIRED')
    v.preparationResult.append(node('p',t(preparation.commitBlocker),'callout warning'));
  }
  if(proof&&!activeReservation(v)){
   const review=node('dl'),digest=node('details');
   for(const [label,value] of [
    ['handoffOriginal',v.receipt.originalNodeId],['handoffCandidateNode',v.receipt.candidateNodeId]
   ])review.append(node('dt',t(label)),node('dd',value));
   digest.append(node('summary',t('handoffEvidenceDigest')),node('code',proof));
   v.preparationResult.append(review,digest,node('p',t('handoffReserveImpact'),'callout warning'));
  }
 }
 function renderTransfer(v,preparation){
  const phase=transferLabels[preparation.state],facts=node('dl');
  const values=[
   ['handoffFormerOwner',preparation.originalNodeId],['handoffCurrentOwner',preparation.candidateNodeId],
   ['handoffJournalEpoch',preparation.activatedJournalEpoch]
  ];
  if(preparation.adoptedEvidence)values.push(
   ['handoffStageReports',preparation.adoptedEvidence.validationReports],
   ['handoffStageReceipts',preparation.adoptedEvidence.closedReceipts]);
  if(preparation.adoptedEvidence?.rawCaptures)values.push(['handoffAdoptedRawCapture',preparation.adoptedEvidence.rawCaptures]);
  if(preparation.adoptedEvidence?.failureRecords)values.push(['handoffAdoptedFailureHistory',preparation.adoptedEvidence.failureRecords]);
  if(preparation.targetFence)values.push(['handoffFencedCount',preparation.targetFence.destinationsFenced]);
  if(preparation.offlineOwnerCommit)values.push(['handoffOfflineCommitted',preparation.offlineOwnerCommit.requestId]);
  for(const [label,value] of values)facts.append(node('dt',t(label)),node('dd',value));
  const details=node('details');
  details.append(node('summary',t('handoffTransferId')),node('code',preparation.id),
   node('p',t('handoffCommittedRevision')),node('code',preparation.committedRevision),
   node('p',t('handoffCommittedConfig')),node('code',preparation.committedConfigurationVersion));
  v.preparationResult.append(node('h3',t('handoffTransferReceipt')),node('span',t(phase),'badge good'),
   node('p',t(phase+'Help'),'callout'));
  if(preparation.pendingProblem){
   const issue=preparation.pendingProblem,problem=node('div','','callout warning');
   problem.append(node('strong',t('handoffPendingProblem')),node('p',refusal(issue.code)),timestamp(issue.observedAt));
   v.preparationResult.append(problem);
  }
  v.preparationResult.append(facts,details);
 }
 function renderStaging(v,busy,readonly){
  const plan=stagePreparation(v);
  if(v.stageAttempt&&!sameStagePreparation(plan,v.stageAttempt.plan))v.stageAttempt=null;
  const attempted=v.stageAttempt;
  v.stagePanel.hidden=readonly||!plan;
  v.stage.disabled=busy||readonly||!!v.preparationAttempt||!v.preparationLoaded||!plan;
  v.stage.textContent=t(attempted?.uncertain?'handoffStageRetry':attempted?.counts?'handoffStageAgain':'handoffStage');
  const counts=attempted?.counts;
  const status=counts?t('handoffStageDone')+' '+t('handoffStageReports')+' '+counts.reports+' · '
   +t('handoffStageReceipts')+' '+counts.closedReceipts
   +(counts.rawCaptures?' · '+t('handoffStageRawCapture')+' '+counts.rawCaptures+' · '+t('handoffStageRawPending'):'')
   +(counts.failureRecords?' · '+t('handoffStageFailureHistory')+' '+counts.failureRecords:''):'';
  if(v.stageStatus.textContent!==status)v.stageStatus.textContent=status;
 }
 function ownershipChange(action){return action==='commit-owner'||action==='commit-offline-owner';}
 function update(v){
  if(!current(v)){
   if(view===v){
    const task=bridge.task(v.taskId);
    if(ownershipChange(v.fenceAttempt?.action)&&task?.ownerNodeId===v.fenceAttempt.candidate
      &&bridge.session()?.id===v.actor&&eligible(task))open(task);
    else close();
   }
   return;
  }
  const busy=v.posting||v.reading||v.probing||v.preparationReading||v.preparationPosting||v.staging||v.fencing;
  v.dialog.setAttribute('aria-busy',String(Boolean(busy||v.loadingNodes)));
  for(const option of [...v.select.options])if(option.value&&!nodeGrant(option.value)){
   v.nodes.delete(option.value);option.remove();
  }
  if(v.receipt&&(!nodeGrant(v.receipt.originalNodeId)||!nodeGrant(v.receipt.candidateNodeId))){
   v.receipt=null;v.loaded=false;
  }
  if(v.attempt&&!nodeGrant(v.attempt.candidate))v.attempt=null;
  if(v.preparation&&!preparationScope(v,v.preparation)){
   v.preparation=null;v.preparationLoaded=false;
  }
  if(v.preparationAttempt&&!nodeGrant(v.preparationAttempt.candidate))v.preparationAttempt=null;
  const active=activeReservation(v),attempt=v.preparationAttempt,readonly=readOnly(v);
  const assessmentLocked=readonly||active||!!attempt;
  v.header.textContent=t(readonly?'handoffProgress':'handoffTitle');
  for(const entry of [v.help,v.original,v.field,v.candidateMessage,v.submit])entry.hidden=readonly;
  v.result.hidden=readonly&&!(offlineReadback(v)&&offlineReceipt(v,v.receipt));
  v.reload.textContent=t(readonly?'handoffRefreshProgress':'handoffReload');
  v.select.disabled=busy||v.loadingNodes||assessmentLocked||!!v.attempt||!v.nodes.size;
  v.more.hidden=readonly||!v.next;
  v.more.disabled=busy||v.loadingNodes||assessmentLocked||!!v.attempt;
  v.reload.disabled=busy||v.loadingNodes;
  v.submit.textContent=t(v.attempt?'handoffRetry':v.receipt?'handoffReassess':'handoffAssess');
  v.submit.disabled=busy||v.loadingNodes||assessmentLocked||(!v.attempt&&(!v.loaded||!v.nodes.has(v.select.value)
    ||!revisionId.test(bridge.task(v.taskId)?.revision||'')));
  v.probe.hidden=readonly||active||v.receipt?.state!=='EVIDENCE_REQUIRED'||!sha256.test(v.receipt.originalCut?.checkpointHash||'');
  v.probe.disabled=busy||assessmentLocked||Boolean(v.attempt)||!freshAssessment(v);
  const reserveShown=!readonly&&(attempt?!attempt.release:reviewReady(v)&&!active);
  const releaseShown=!readonly&&(attempt?attempt.release:active);
  const focused=document.activeElement;
  v.reserve.hidden=!reserveShown;v.release.hidden=!releaseShown;
  v.reserve.textContent=t(attempt&&!attempt.release?'handoffRetryReserve':'handoffReserve');
  v.release.textContent=t(attempt?.release?'handoffRetryRelease':'handoffRelease');
  v.reserve.disabled=busy||readonly||Boolean(v.attempt)||!attempt&&!reviewReady(v);
  v.release.disabled=busy||readonly||!attempt&&!v.preparationLoaded;
  v.preparationPanel.hidden=!v.preparation&&!reserveShown&&!releaseShown&&!(readonly&&v.preparationLoaded);
  renderReceipt(v);
  renderPreparation(v,readonly);
  renderStaging(v,busy,readonly);
  const offer=nextHandoffAction(v);
  const labels={
   'fence-targets':['handoffFence','handoffFenceRetry','handoffFenceHelp'],
   'start-worker':['handoffStartWorker','handoffStartRetry','handoffStartWorkerHelp'],
   'commit-owner':['handoffCommitOwner','handoffCommitRetry','handoffCommitOwnerHelp'],
   'commit-offline-owner':['handoffOfflineCommit','handoffOfflineCommitRetry','handoffOfflineCommitHelp']
  };
  const [label,retry,help]=labels[offer]||labels['fence-targets'];
  const offline=offer==='commit-offline-owner';
  const frozen=offline?v.preparation.originalFreeze:null;
  const freezeReview=frozen?[v.preparation.id,v.preparation.evidenceSha256,frozen.requestId,frozen.manifestSha256].join(':'):null;
  if(freezeReview!==v.offlineFreezeReview){
   v.offlineFreezeReview=freezeReview;v.offlineConfirmation.checked=false;
   if(offline&&v.fenceAttempt?.action===offer)v.fenceAttempt=null;
  }
  v.offlineConfirmField.hidden=!offline;
  v.offlineConfirmation.disabled=!offline||busy||v.fenceAttempt?.action===offer;
  v.offlineConfirmationText.textContent=t('handoffOfflineConfirm');
  v.fencePanel.hidden=!offer;v.fence.disabled=busy||!offer||offline&&!v.offlineConfirmation.checked;
  v.fence.dataset.handoffAction=offer||'';
  const pending=v.preparation?.state;
  const reapprove=offer==='start-worker'&&pending==='START_PENDING'?'handoffStartReapprove'
   :offer==='fence-targets'&&pending==='FENCE_PENDING'?'handoffFenceReapprove':label;
  v.fence.textContent=t(v.fenceAttempt?.action===offer||offer==='commit-owner'&&v.preparation?.commitRequestId?retry:reapprove);
  v.actionHelp.textContent=t(reapprove===label?help:'handoffReapprovalHelp');
  if(!offer&&focused===v.fence&&!v.reload.disabled)v.reload.focus({preventScroll:true});
  if((focused===v.reserve&&!reserveShown)||(focused===v.release&&!releaseShown)){
   const next=releaseShown?v.release:reserveShown?v.reserve:v.reload;
   if(!next.disabled)next.focus({preventScroll:true});
  }
 }

 async function read(v){
  v.reading=true;update(v);
  try{
   let receipt;
   try{receipt=verified(v,await api(endpoint(v)));}
   catch(error){if(error.code!=='HANDOFF_ASSESSMENT_NOT_FOUND')throw error;receipt=null;}
   if(!current(v))return;
   v.receipt=receipt;v.loaded=true;
   if(v.attempt&&receipt?.id!==v.attempt.previous)v.attempt=null;
  }catch(error){
   if(current(v)){
    v.receipt=null;v.loaded=false;v.feedback.textContent=error.message;
    if(error.status===403||error.code==='TASK_NOT_FOUND')v.attempt=null;
   }
  }finally{
   if(current(v)){v.reading=false;update(v);}
  }
 }
 async function readPreparation(v){
  v.preparationReading=true;update(v);
  try{
   let value;
   try{
    const response=await api(preparationEndpoint(v));
    if(!current(v))return;
    v.unverifiedTransfer=transferred(response)||response?.state==='ORIGINAL_FROZEN';
    value=verifiedPreparation(v,response);
   }catch(error){if(error.code!=='HANDOFF_PREPARATION_NOT_FOUND')throw error;value=null;}
   if(!current(v))return;
   v.unverifiedTransfer=false;v.preparation=value;v.preparationLoaded=true;
   if(v.fenceAttempt){
    const attempted=v.fenceAttempt;
    const previousApproval=value?.state==='START_PENDING'&&value.workerStartAvailable===true?value.workerStart?.requestId
     :value?.state==='FENCE_PENDING'&&value.targetFenceAvailable===true?value.targetFence?.requestId:null;
    if(attempted.action!==nextHandoffAction(v)||attempted.preparation!==value?.id||attempted.id===previousApproval)
     v.fenceAttempt=null;
   }
   if(transferred(value)){
    v.attempt=null;v.preparationAttempt=null;
   }else if(v.preparationAttempt&&matchesPreparation(value,v.preparationAttempt)){
    v.preparationAttempt=null;v.feedback.textContent=t('handoffReservationSaved');
   }
  }catch(error){
   if(!current(v))return;
   v.preparationLoaded=false;v.feedback.textContent=error.message;
   if(v.unverifiedTransfer||error.code==='FORBIDDEN'||error.code==='TASK_NOT_FOUND'){
    v.preparation=null;v.preparationAttempt=null;
   }
  }finally{if(current(v)){v.preparationReading=false;update(v);}}
 }
 async function refreshDialog(v){
  v.feedback.textContent='';
  if(ownershipChange(v.fenceAttempt?.action)||v.preparation?.state==='ORIGINAL_FROZEN')await bridge.refresh();
  if(!current(v))return;
  await readPreparation(v);
  if(!current(v))return;
  if(!readOnly(v))await Promise.all([read(v),candidates(v)]);
  else if(offlineReadback(v))await read(v);
 }
 async function reconcile(v,error){
  if(error.code==='REVISION_CHANGED')await bridge.refresh();
  if(!current(v))return;
  if(['FORBIDDEN','TASK_NOT_FOUND'].includes(error.code)){
   v.receipt=null;v.loaded=false;v.preparation=null;v.preparationLoaded=false;
   return;
  }
  await Promise.all([read(v),readPreparation(v)]);
 }
 async function probe(v){
  if(!current(v)||v.probe.hidden||v.probe.disabled)return;
  const reviewed=v.receipt;
  v.probing=true;v.feedback.textContent='';update(v);
  try{
   const data=await api(endpoint(v)+'/probe',postOptions(JSON.stringify({expectedAssessment:reviewed.id})));
   if(!current(v))return;
   v.receipt=verified(v,data,{id:reviewed.id,candidate:reviewed.candidateNodeId,revision:reviewed.taskRevision});
   v.feedback.textContent=t('handoffProbeSaved');
  }catch(error){
   if(!current(v))return;
   v.feedback.textContent=error.message;
   await reconcile(v,error);
  }finally{if(current(v)){v.probing=false;update(v);}}
 }
 async function mutatePreparation(v,release){
  const control=release?v.release:v.reserve;
  if(!current(v)||control.hidden||control.disabled)return;
  if(!v.preparationAttempt){
   const selected=release?v.preparation:v.receipt;
   const id=requestId(),revision=selected.taskRevision;
   const evidence=release?selected.evidenceSha256:selected.preparationEvidenceSha256;
   const body=release?{expectedPreparation:selected.id}
    :{expectedAssessment:selected.id,expectedRevision:revision,expectedEvidenceSha256:evidence};
   v.preparationAttempt={id,release,revision,evidence,body:JSON.stringify(body),candidate:selected.candidateNodeId,
    assessmentId:release?selected.assessmentId:selected.id,preparationId:release?selected.id:id};
  }
  const attempt=v.preparationAttempt;
  v.preparationPosting=true;v.feedback.textContent='';update(v);
  try{
   const response=await api(preparationEndpoint(v)+(release?'/release':''),postOptions(attempt.body,attempt.id));
   if(!current(v))return;
   v.preparation=verifiedPreparation(v,response,attempt);v.preparationLoaded=true;v.preparationAttempt=null;
   v.feedback.textContent=t(release?'handoffReservationReleased':'handoffReservationSaved');
   if(release)await read(v);
  }catch(error){
   if(!current(v))return;
   if(!(error.status>=400&&error.status<500)||error.status===408||error.status===429){
    v.feedback.textContent=t('handoffReservationUnknown');
    return;
   }
   v.preparationAttempt=null;
   v.feedback.textContent=error.message;
   await reconcile(v,error);
  }finally{if(current(v)){v.preparationPosting=false;update(v);}}
 }
 async function submitHandoffAction(v){
  const action=nextHandoffAction(v);
  if(!current(v)||v.fence.disabled||!action)return;
  const plan=v.preparation,committing=ownershipChange(action),offline=action==='commit-offline-owner';
  const input={expectedPreparation:plan.id,expectedRevision:committing?plan.taskRevision:plan.committedRevision};
  if(committing)input.expectedEvidenceSha256=plan.evidenceSha256;
  if(offline){
   input.expectedInventorySha256=plan.originalFreeze.manifestSha256;
   input.confirmOriginalUnavailable=true;
  }
  const attempt=v.fenceAttempt??={action,preparation:plan.id,candidate:plan.candidateNodeId,
   id:action==='commit-owner'&&plan.commitRequestId||requestId(),body:JSON.stringify(input)};
  v.fencing=true;v.feedback.textContent='';update(v);
  try{
   const reply=await api(preparationEndpoint(v)+'/'+action,postOptions(attempt.body,attempt.id));
   if(view!==v||v.fenceAttempt!==attempt)return;
   if(!current(v)){update(v);return;}
   if(committing){
    if(!transferred(reply)||!validTransfer(reply)||reply.executionAuthorized!==workerAdmitted(reply.state)
      ||reply.commitRequestId!==attempt.id||reply.id!==plan.id||reply.taskId!==v.taskId
      ||reply.taskRevision!==plan.taskRevision||reply.evidenceSha256!==plan.evidenceSha256
      ||reply.originalNodeId!==v.owner||reply.candidateNodeId!==attempt.candidate||!nodeGrant(attempt.candidate)
      ||(offline?reply.offlineOwnerCommit?.freezeRequestId!==plan.originalFreeze.requestId
        ||reply.activatedJournalEpoch!==plan.originalFreeze.journalEpoch:reply.offlineOwnerCommit!=null))
     throw new Error(t('handoffReservationInvalid'));
    await bridge.refresh();
    if(current(v))v.feedback.textContent=t('handoffCommitUnknown');
    return;
   }
   const response=verifiedPreparation(v,reply);
   const starting=action==='start-worker';
   const states=starting?startStates:['FENCE_PENDING','TARGETS_FENCED'];
   const approval=starting?response.workerStart:response.targetFence;
   if(response.id!==attempt.preparation||!states.includes(response.state)||approval?.requestId!==attempt.id)
    throw new Error(t('handoffReservationInvalid'));
   v.preparation=response;v.fenceAttempt=null;
  }catch(error){
   if(!current(v)||v.fenceAttempt!==attempt)return;
   const uncertain=!(error.status>=400&&error.status<500)||[408,429].includes(error.status)
    ||committing&&['OWNER_QUERY_TIMEOUT','OWNER_QUERY_EXPIRED','OWNER_QUERY_UNAVAILABLE'].includes(error.code);
   if(uncertain){
    v.feedback.textContent=t(committing?'handoffCommitUnknown':'handoffFenceUnknown');return;
   }
   v.fenceAttempt=null;
   if(offline)v.offlineConfirmation.checked=false;
   v.feedback.textContent=offline&&error.code&&t(error.code)!==error.code?t(error.code):error.message;
   await reconcile(v,error);
   if(current(v))await readPreparation(v);
  }finally{if(current(v)){v.fencing=false;update(v);}}
 }
 async function stageEvidence(v){
  if(!current(v)||v.stage.disabled)return;
  const plan=stagePreparation(v);
  if(!plan)return;
  const attempt=v.stageAttempt??={plan,id:requestId()};
  const body=JSON.stringify({expectedPreparation:plan.id,expectedRevision:plan.taskRevision,
   expectedEvidenceSha256:plan.evidenceSha256});
  v.staging=true;v.feedback.textContent='';update(v);
  try{
   const result=await api(preparationEndpoint(v)+'/stage',postOptions(body,attempt.id));
   if(!current(v)||v.stageAttempt!==attempt)return;
   const raw=result?.rawCapture;
   const rawValid=raw==null||raw.state==='STAGED'&&raw.executionAuthorized===false
     &&raw.preparationId===plan.id&&raw.candidateNodeId===plan.candidateNodeId&&raw.evidenceSha256===plan.evidenceSha256
     &&sha256.test(raw.archiveSha256||'')&&raw.archiveSha256!==result.archiveSha256&&sha256.test(raw.stagedSealSha256||'')
     &&Number.isSafeInteger(raw.archiveBytes)&&raw.archiveBytes>0&&Number.isSafeInteger(raw.rawCaptures)&&raw.rawCaptures>0;
   const failures=result?.failureRecords;
   if(!rawValid||(failures!=null&&!evidenceCount(failures))||result?.state!=='STAGED'||result.executionAuthorized!==false||result.preparationId!==plan.id
     ||result.taskRevision!==plan.taskRevision||result.candidateNodeId!==plan.candidateNodeId
     ||result.evidenceSha256!==plan.evidenceSha256||result.archiveSha256!==plan.custodySha256
     ||!Number.isSafeInteger(result.reports)||result.reports<0
     ||!Number.isSafeInteger(result.closedReceipts)||result.closedReceipts<1){
    attempt.counts=null;attempt.uncertain=true;v.feedback.textContent=t('handoffStageInvalid');
    return;
   }
   attempt.counts={reports:result.reports,closedReceipts:result.closedReceipts,rawCaptures:raw?.rawCaptures,failureRecords:failures};attempt.uncertain=false;
  }catch(error){
   if(!current(v)||v.stageAttempt!==attempt)return;
   attempt.counts=null;
   attempt.uncertain=!(error.status>=400&&error.status<500)||[408,429].includes(error.status)
    ||['OWNER_QUERY_TIMEOUT','OWNER_QUERY_EXPIRED','HANDOFF_STAGING_TIMEOUT'].includes(error.code);
   v.feedback.textContent=attempt.uncertain?t('handoffStageUnknown'):error.message;
   if(['HANDOFF_PREPARATION_CHANGED','HANDOFF_PREPARATION_EVIDENCE_CHANGED','HANDOFF_AGENT_CHANGED',
    'REVISION_CHANGED','FORBIDDEN','TASK_NOT_FOUND'].includes(error.code))await reconcile(v,error);
  }finally{if(current(v)){v.staging=false;update(v);}}
 }
 async function candidates(v,after=''){
  v.loadingNodes=true;update(v);
  try{
   const page=await api('/nodes?limit=20'+(after?'&after='+encodeURIComponent(after):''));
   if(!current(v))return;
   const available=after?v.nodes:new Map();
   for(const candidate of page.nodes||[]){
    if(candidate.id!==v.owner&&nodeGrant(candidate.id)&&['AGENT','CONTROLLER_WORKER'].includes(candidate.role))
     available.set(candidate.id,candidate);
   }
   const selected=v.select.value;
   v.nodes=available;
   v.next=nodeId.test(page.next||'')&&page.next!==after?page.next:null;
   const placeholder=node('option',t('handoffChoose'));placeholder.value='';
   v.select.replaceChildren(placeholder);
   for(const candidate of available.values()){
    const caption=candidate.id+' · '+t(candidate.state||'UNKNOWN')+' · '+t('handoffCapacity')+' '
     +candidate.taskCapacity+' / '+candidate.runningCapacity;
    const option=node('option',caption);option.value=candidate.id;v.select.append(option);
   }
   if(available.has(selected))v.select.value=selected;
   v.candidateMessage.textContent=available.size?'':t('handoffNoCandidate');
  }catch(error){if(current(v))v.candidateMessage.textContent=error.message;}
  finally{if(current(v)){v.loadingNodes=false;update(v);}}
 }
 async function submit(v){
  if(!current(v)||v.submit.disabled)return;
  if(!v.attempt){
   const revision=bridge.task(v.taskId).revision,candidate=v.select.value,previous=v.receipt?.id;
   v.attempt={id:requestId(),candidate,revision,previous,
    body:JSON.stringify({candidateNodeId:candidate,expectedRevision:revision,...(previous?{expectedAssessment:previous}:{})})};
  }
  const attempt=v.attempt;
  v.posting=true;v.feedback.textContent='';update(v);
  try{
   const response=await api(endpoint(v),postOptions(attempt.body,attempt.id));
   if(!current(v))return;
   v.receipt=verified(v,response,attempt);v.attempt=null;v.loaded=true;v.feedback.textContent=t('handoffSaved');
  }catch(error){
   if(!current(v))return;
   if(!(error.status>=400&&error.status<500)||error.status===408||error.status===429){
    v.feedback.textContent=t('handoffUnknown');return;
   }
   v.attempt=null;
   if(error.code==='HANDOFF_ASSESSMENT_CHANGED'||error.code==='REVISION_CHANGED'||error.code==='HANDOFF_REQUEST_REUSED'){
    v.feedback.textContent=t(error.code==='REVISION_CHANGED'?'handoffRevisionChanged':'handoffChanged');
    if(error.code==='REVISION_CHANGED')await bridge.refresh();
    if(current(v))await Promise.all([read(v),readPreparation(v)]);
   }else if(error.code==='HANDOFF_PREPARATION_ACTIVE'){
    v.feedback.textContent=error.message;
    await readPreparation(v);
   }else{
    v.feedback.textContent=error.message;
    if(error.code==='FORBIDDEN'||error.code==='TASK_NOT_FOUND'){v.receipt=null;v.loaded=false;}
   }
  }finally{if(current(v)){v.posting=false;update(v);}}
 }

 function open(task){
  if(!eligible(task))return;
  close();
  const dialog=node('dialog','','handoff-dialog failure-review-dialog'),heading=node('h2',t('handoffTitle')),
   form=node('form'),original=node('p',t('handoffOriginal')+' '+task.ownerNodeId),
   field=node('label'),select=node('select'),candidateMessage=node('p','','subtle'),
   feedback=node('p','','subtle'),result=node('section'),actions=node('div','','dialog-actions'),
   more=node('button',t('handoffMore'),'quiet'),reload=node('button',t('handoffReload'),'quiet'),
   submitButton=node('button',t('handoffAssess'),'primary'),exit=node('button',t('handoffClose'),'quiet'),
   probeButton=node('button',t('handoffProbe')),reserve=node('button',t('handoffReserve'),'primary'),
   release=node('button',t('handoffRelease')),preparationPanel=node('section','','recovery-retry'),
   preparationResult=node('div'),stagePanel=node('div'),stageStatus=node('p','','subtle'),
   stageButton=node('button',t('handoffStage')),help=node('p',t('handoffHelp'),'callout'),
   fencePanel=node('div'),fence=node('button',t('handoffFence'),'primary'),actionHelp=node('p','','subtle'),
   offlineConfirmField=node('label','','handoff-confirmation'),offlineConfirmation=node('input'),offlineConfirmationText=node('span');
  const v={dialog,taskId:task.id,owner:task.ownerNodeId,actor:bridge.session().id,select,candidateMessage,
   feedback,result,more,reload,submit:submitButton,probe:probeButton,reserve,release,preparationPanel,preparationResult,
   stage:stageButton,stagePanel,stageStatus,fencePanel,fence,actionHelp,offlineConfirmField,offlineConfirmation,
   offlineConfirmationText,header:heading,help,original,field,nodes:new Map()};
  view=v;
  heading.id='handoff-title';dialog.setAttribute('aria-labelledby',heading.id);
  select.name='candidateNodeId';field.append(node('span',t('handoffCandidate')),select);
  feedback.setAttribute('role','status');result.setAttribute('aria-live','polite');
  preparationResult.setAttribute('aria-live','polite');
  for(const button of [more,reload,exit,probeButton,reserve,release,stageButton,fence])button.type='button';
  submitButton.type='submit';submitButton.dataset.handoffAssess='';
  reload.dataset.handoffReload='';more.dataset.handoffMore='';result.dataset.handoffReceipt='';
  probeButton.dataset.handoffProbe='';reserve.dataset.handoffPrepare='';release.dataset.handoffRelease='';
  preparationPanel.dataset.handoffPreparation='';feedback.dataset.handoffFeedback='';
  stageButton.dataset.handoffStage='';stageStatus.dataset.handoffStageStatus='';stageStatus.setAttribute('role','status');
  exit.onclick=()=>close(true);dialog.addEventListener('cancel',event=>{event.preventDefault();close(true);});
  select.onchange=()=>update(v);
  more.onclick=()=>candidates(v,v.next);
  reload.onclick=()=>refreshDialog(v);
  form.onsubmit=event=>{event.preventDefault();submit(v);};
  probeButton.onclick=()=>probe(v);reserve.onclick=()=>mutatePreparation(v,false);
  release.onclick=()=>mutatePreparation(v,true);
  stageButton.onclick=()=>stageEvidence(v);
  fence.dataset.handoffFence='';fence.onclick=()=>submitHandoffAction(v);
  actions.append(exit,more,reload,submitButton);
  form.append(field,candidateMessage,actions);
  stagePanel.append(node('p',t('handoffStageHelp'),'subtle'),stageButton,stageStatus);
  offlineConfirmation.type='checkbox';offlineConfirmation.dataset.handoffOfflineConfirm='';
  offlineConfirmation.onchange=()=>update(v);
  offlineConfirmField.append(offlineConfirmation,offlineConfirmationText);
  fencePanel.append(actionHelp,offlineConfirmField,fence);
  preparationPanel.append(preparationResult,stagePanel,reserve,release,fencePanel);
  dialog.append(heading,node('p',task.id,'subtle'),help,original,form,feedback,
   result,probeButton,preparationPanel);
  document.body.append(dialog);update(v);dialog.showModal();
  refreshDialog(v);
 }
 function button(task){
  if(!eligible(task))return null;
  const control=node('button',t(task.formatVersion===3?'handoffTitle':'handoffProgress'),'quiet');control.dataset.handoffTask=task.id;
  control.onclick=()=>open(bridge.task(task.id));return control;
 }
 function rememberFocus(root,taskId){
  const before=document.activeElement;
  if(!taskId||before?.dataset.handoffTask!==taskId||!root.contains(before))return;
  return()=>{
   const next=root.querySelector('[data-handoff-task]');
   if(!before.isConnected&&document.activeElement===document.body&&next?.dataset.handoffTask===taskId)next.focus({preventScroll:true});
  };
 }
 return{button,rememberFocus,sync(){if(view)update(view);},close};
})();
