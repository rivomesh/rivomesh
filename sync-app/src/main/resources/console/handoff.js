'use strict';
window.RivomeshHandoff=(()=>{
 const bridge=window.RivomeshConfiguration,{api,can,requestId,t}=bridge;
 bridge.register({
  handoffTitle:'评估 Agent 接管',handoffHelp:'评估和预留期间，任务始终归原 Agent。此界面仅提供接管准备。',
  handoffOriginal:'当前 Agent',handoffCandidate:'选择候选 Agent',handoffCandidateNode:'候选 Agent',handoffChoose:'请选择 Agent',
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
  handoffTitle:'Assess Agent handoff',handoffHelp:'The current Agent keeps ownership throughout assessment and reservation. Use this panel to prepare a handoff.',
  handoffOriginal:'Current Agent',handoffCandidate:'Choose a candidate Agent',handoffCandidateNode:'Candidate Agent',handoffChoose:'Select an Agent',
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
  handoffStageDone:'候选 Agent 已核验暂存文件。',handoffStageReports:'校验报告',handoffStageReceipts:'已办结回执',
  handoffStageUnknown:'暂存结果未知。可重试同一请求来核验候选 Agent 上的文件。',
  handoffStageInvalid:'暂存回执与当前准备记录不匹配。请重试核验。',
  HANDOFF_AGENT_CHANGED:'Agent 的登记已变化，请重新评估。',
  HANDOFF_STAGING_BUSY:'候选 Agent 正在处理暂存文件，稍后可重试。',
  HANDOFF_STAGING_CAPACITY:'候选 Agent 的私有暂存目录已达到容量上限。',
  HANDOFF_STAGING_BYTES_CHANGED:'候选 Agent 上已暂存文件的内容发生变化，未通过核验。',
  HANDOFF_ARCHIVE_BYTES_CHANGED:'原证据归档摘要已变化，请重新核对归档。'
 },{
  handoffStage:'Stage reports and receipts',handoffStageAgain:'Reverify staged reports and receipts',handoffStageRetry:'Retry the same staging request',
  handoffStageHelp:'Stage verified validation reports and closed receipts in the candidate Agent’s private directory. The original Agent keeps ownership.',
  handoffStageDone:'The candidate Agent verified the staged files.',handoffStageReports:'Validation reports',handoffStageReceipts:'Closed receipts',
  handoffStageUnknown:'The staging outcome is unknown. Retry the same request to verify the candidate’s files.',
  handoffStageInvalid:'The staging receipt does not match this preparation. Retry to verify.',
  HANDOFF_AGENT_CHANGED:'An Agent registered again. Reassess before staging.',
  HANDOFF_STAGING_BUSY:'The candidate is working with the staging files. Retry shortly.',
  HANDOFF_STAGING_CAPACITY:'The candidate’s private staging directories reached their capacity limit.',
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
  handoffCommitted:'归属已提交',handoffProjected:'候选配置已安装',handoffEvidenceAdopted:'证据已接收',
  handoffCommittedHelp:'任务归属已变更。候选 Agent 正在等待安装已审阅的任务配置。',
  handoffProjectedHelp:'候选 Agent 已保存停止状态的任务配置。接收报告和回执后会更新交接状态。',
  handoffEvidenceAdoptedHelp:'候选 Agent 已接收经核验的报告和已办结回执。交接回执记录的任务状态为停止。',
  handoffFencePending:'等待目标隔离',handoffTargetsFenced:'目标已隔离',
  handoffFencePendingHelp:'候选 Agent 正在核验并提高目标端纪元。Worker 保持停止。',
  handoffTargetsFencedHelp:'目标端已拒绝旧纪元写入。候选 Worker 保持停止。',handoffFencedCount:'已隔离的目标数',
  handoffCommittedRevision:'已提交任务修订',handoffCommittedConfig:'已提交配置版本',handoffJournalEpoch:'已提交日志代次',
  handoffFormerOwner:'原 Agent',handoffCurrentOwner:'当前归属 Agent',handoffTransferId:'交接准备 ID',handoffRefreshProgress:'刷新交接状态'
 },{
  handoffProgress:'View Agent handoff status',handoffTransferReceipt:'Handoff receipt',handoffNoPreparation:'This task has no handoff preparation.',
  handoffCommitted:'Ownership committed',handoffProjected:'Candidate configuration installed',handoffEvidenceAdopted:'Evidence received',
  handoffCommittedHelp:'Task ownership has changed. The candidate Agent is waiting to install the reviewed configuration.',
  handoffProjectedHelp:'The candidate Agent saved the task configuration in a stopped state. Handoff status will update when it receives the reports and receipts.',
  handoffEvidenceAdoptedHelp:'The candidate Agent received the verified reports and closed receipts. The handoff receipt records the task as stopped.',
  handoffFencePending:'Target fencing pending',handoffTargetsFenced:'Targets fenced',
  handoffFencePendingHelp:'The candidate is verifying and advancing the target epochs. The Worker remains stopped.',
  handoffTargetsFencedHelp:'The targets reject writes from the previous epoch. The candidate Worker remains stopped.',handoffFencedCount:'Fenced targets',
  handoffCommittedRevision:'Committed task revision',handoffCommittedConfig:'Committed configuration version',handoffJournalEpoch:'Committed journal epoch',
  handoffFormerOwner:'Original Agent',handoffCurrentOwner:'Current owner Agent',handoffTransferId:'Preparation ID',handoffRefreshProgress:'Refresh handoff status'
 });

 const nodeId=/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
 const revisionId=/^[0-9a-f-]{36}$/;
 const requestPattern=/^[A-Za-z0-9_-]{8,128}$/;
 const sha256=/^[0-9a-f]{64}$/;
 const stateLabels={BLOCKED:'handoffStateBlocked',EVIDENCE_REQUIRED:'handoffStateEvidence',STALE:'handoffStateStale'};
 const preparationLabels={PREPARED:'handoffPrepared',RELEASED:'handoffReleased',EXPIRED:'handoffExpired'};
 const transferLabels={COMMITTED:'handoffCommitted',PROJECTED:'handoffProjected',EVIDENCE_ADOPTED:'handoffEvidenceAdopted',FENCE_PENDING:'handoffFencePending',TARGETS_FENCED:'handoffTargetsFenced'};
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
  return preparation.candidateNodeId===v.owner&&preparation.committedRevision===task?.revision
   &&preparation.committedConfigurationVersion===task?.configurationVersion;
 }
 function readOnly(v){
  return bridge.task(v.taskId)?.formatVersion!==3||transferred(v.preparation)||v.unverifiedTransfer;
 }
 function validTransfer(preparation){
  if(!revisionId.test(preparation.committedRevision||'')||preparation.committedRevision===preparation.taskRevision
   ||!revisionId.test(preparation.committedConfigurationVersion||'')||typeof preparation.activatedJournalEpoch!=='string'
   ||!/^[1-9][0-9]{0,18}$/.test(preparation.activatedJournalEpoch))return false;
  const evidence=preparation.adoptedEvidence;
  const fence=preparation.targetFence;
  if(['FENCE_PENDING','TARGETS_FENCED'].includes(preparation.state)){
   if(!requestPattern.test(fence?.requestId||'')||!Number.isSafeInteger(fence.destinationsFenced)
    ||(preparation.state==='FENCE_PENDING'?fence.destinationsFenced!==0:fence.destinationsFenced<1||fence.destinationsFenced>8))return false;
  }else if(fence!=null)return false;
  return ['EVIDENCE_ADOPTED','FENCE_PENDING','TARGETS_FENCED'].includes(preparation.state)?evidence!=null
    &&Number.isSafeInteger(evidence.validationReports)&&evidence.validationReports>=0
    &&Number.isSafeInteger(evidence.closedReceipts)&&evidence.closedReceipts>0:evidence==null;
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
 function verifiedPreparation(v,response,attempt){
  const state=response?.state;
  const active=state==='PREPARED',transferredState=transferred(response);
  if(response?.taskId!==v.taskId||!preparationScope(v,response)||!requestPattern.test(response.id||'')
    ||!requestPattern.test(response.assessmentId||'')||!revisionId.test(response.taskRevision||'')
    ||!(Object.hasOwn(preparationLabels,state)||transferredState&&validTransfer(response))||response.executionAuthorized!==false
    ||response.reservedTaskSlots!==Number(active)||response.reservedWorkerSlots!==Number(active)
    ||!sha256.test(response.evidenceSha256||'')||!Number.isFinite(Date.parse(response.createdAt))
    ||!(Date.parse(response.expiresAt)>Date.parse(response.createdAt))
    ||(state==='RELEASED'?!requestPattern.test(response.releaseId||''):response.releaseId!=null)
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
 function renderReceipt(v){
  const receipt=v.receipt,revision=bridge.task(v.taskId)?.revision;
  const stale=receipt&&(receipt.state==='STALE'||receipt.taskRevision!==revision||receipt.originalNodeId!==v.owner);
  if(v.drawn&&v.drawn.receipt===receipt&&v.drawn.stale===stale&&v.drawn.loaded===v.loaded)return;
  v.drawn={receipt,stale,loaded:v.loaded};
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
 }
 function renderPreparation(v,readonly){
  const preparation=v.preparation,proof=!readonly&&reviewReady(v)?v.receipt.preparationEvidenceSha256:null;
  const empty=readonly&&v.preparationLoaded&&!preparation;
  if(v.drawnPreparation?.receipt===preparation&&v.drawnPreparation?.proof===proof&&v.drawnPreparation?.empty===empty)return;
  v.drawnPreparation={receipt:preparation,proof,empty};
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
   if(active)v.preparationResult.append(node('p',t('handoffActiveHelp'),'subtle'));
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
  if(preparation.targetFence)values.push(['handoffFencedCount',preparation.targetFence.destinationsFenced]);
  for(const [label,value] of values)facts.append(node('dt',t(label)),node('dd',value));
  const details=node('details');
  details.append(node('summary',t('handoffTransferId')),node('code',preparation.id),
   node('p',t('handoffCommittedRevision')),node('code',preparation.committedRevision),
   node('p',t('handoffCommittedConfig')),node('code',preparation.committedConfigurationVersion));
  v.preparationResult.append(node('h3',t('handoffTransferReceipt')),node('span',t(phase),'badge good'),
   node('p',t(phase+'Help'),'callout'),facts,details);
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
   +t('handoffStageReceipts')+' '+counts.closedReceipts:'';
  if(v.stageStatus.textContent!==status)v.stageStatus.textContent=status;
 }
 function update(v){
  if(!current(v)){if(view===v)close();return;}
  const busy=v.posting||v.reading||v.probing||v.preparationReading||v.preparationPosting||v.staging;
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
  for(const entry of [v.help,v.original,v.field,v.candidateMessage,v.submit,v.result])entry.hidden=readonly;
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
    v.unverifiedTransfer=transferred(response);
    value=verifiedPreparation(v,response);
   }catch(error){if(error.code!=='HANDOFF_PREPARATION_NOT_FOUND')throw error;value=null;}
   if(!current(v))return;
   v.unverifiedTransfer=false;v.preparation=value;v.preparationLoaded=true;
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
  await readPreparation(v);
  if(current(v)&&!readOnly(v))await Promise.all([read(v),candidates(v)]);
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
   if(result?.state!=='STAGED'||result.executionAuthorized!==false||result.preparationId!==plan.id
     ||result.taskRevision!==plan.taskRevision||result.candidateNodeId!==plan.candidateNodeId
     ||result.evidenceSha256!==plan.evidenceSha256||result.archiveSha256!==plan.custodySha256
     ||!Number.isSafeInteger(result.reports)||result.reports<0
     ||!Number.isSafeInteger(result.closedReceipts)||result.closedReceipts<1){
    attempt.counts=null;attempt.uncertain=true;v.feedback.textContent=t('handoffStageInvalid');
    return;
   }
   attempt.counts={reports:result.reports,closedReceipts:result.closedReceipts};attempt.uncertain=false;
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
   stageButton=node('button',t('handoffStage')),help=node('p',t('handoffHelp'),'callout');
  const v={dialog,taskId:task.id,owner:task.ownerNodeId,actor:bridge.session().id,select,candidateMessage,
   feedback,result,more,reload,submit:submitButton,probe:probeButton,reserve,release,preparationPanel,preparationResult,
   stage:stageButton,stagePanel,stageStatus,header:heading,help,original,field,nodes:new Map()};
  view=v;
  heading.id='handoff-title';dialog.setAttribute('aria-labelledby',heading.id);
  select.name='candidateNodeId';field.append(node('span',t('handoffCandidate')),select);
  feedback.setAttribute('role','status');result.setAttribute('aria-live','polite');
  preparationResult.setAttribute('aria-live','polite');
  for(const button of [more,reload,exit,probeButton,reserve,release,stageButton])button.type='button';
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
  actions.append(exit,more,reload,submitButton);
  form.append(field,candidateMessage,actions);
  stagePanel.append(node('p',t('handoffStageHelp'),'subtle'),stageButton,stageStatus);
  preparationPanel.append(preparationResult,stagePanel,reserve,release);
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
