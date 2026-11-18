'use strict';
window.RivomeshHandoff=(()=>{
 const bridge=window.RivomeshConfiguration,{api,can,requestId,t}=bridge;
 bridge.register({
  handoffTitle:'评估 Agent 接管',handoffHelp:'评估只保存检查记录。当前 Agent 继续负责该任务。',
  handoffOriginal:'当前 Agent',handoffCandidate:'选择候选 Agent',handoffCandidateNode:'候选 Agent',handoffChoose:'请选择 Agent',
  handoffMore:'查看更多 Agent',handoffReload:'刷新节点和评估',handoffAssess:'记录评估',
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
  handoffTitle:'Assess Agent handoff',handoffHelp:'This saves an assessment. The current Agent continues to own the task.',
  handoffOriginal:'Current Agent',handoffCandidate:'Choose a candidate Agent',handoffCandidateNode:'Candidate Agent',handoffChoose:'Select an Agent',
  handoffMore:'More Agents',handoffReload:'Refresh nodes and assessment',handoffAssess:'Record assessment',
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

 const nodeId=/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
 const revisionId=/^[0-9a-f-]{36}$/;
 const requestPattern=/^[A-Za-z0-9_-]{8,128}$/;
 const stateLabels={BLOCKED:'handoffStateBlocked',EVIDENCE_REQUIRED:'handoffStateEvidence',STALE:'handoffStateStale'};
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
  return task?.formatVersion===3&&session?.taskControlScope==='GLOBAL'&&session.role==='ADMIN'
   &&can('TASK_CONFIG')&&nodeGrant(task.ownerNodeId);
 }
 function current(v){
  const task=bridge.task(v.taskId);
  return view===v&&eligible(task)&&task.ownerNodeId===v.owner&&bridge.session()?.id===v.actor;
 }
 function close(){
  if(!view)return;
  const dialog=view.dialog;view=null;
  dialog.close();dialog.remove();
 }
 function endpoint(v){return '/tasks/'+encodeURIComponent(v.taskId)+'/handoff';}

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
  const heading=node('h3',t('handoffReceipt')),badge=node('span',t(stateLabels[status]),'badge warn');
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
  v.result.append(heading,badge,facts,blockers,details);
 }
 function update(v){
  if(!current(v)){if(view===v)close();return;}
  const busy=v.posting||v.reading;
  v.dialog.setAttribute('aria-busy',String(Boolean(busy||v.loadingNodes)));
  for(const option of [...v.select.options])if(option.value&&!nodeGrant(option.value)){
   v.nodes.delete(option.value);option.remove();
  }
  if(v.receipt&&(!nodeGrant(v.receipt.originalNodeId)||!nodeGrant(v.receipt.candidateNodeId))){
   v.receipt=null;v.loaded=false;
  }
  if(v.attempt&&!nodeGrant(v.attempt.candidate))v.attempt=null;
  v.select.disabled=busy||v.loadingNodes||!!v.attempt||!v.nodes.size;
  v.more.hidden=!v.next;
  v.more.disabled=busy||v.loadingNodes||!!v.attempt;
  v.reload.disabled=busy||v.loadingNodes;
  v.submit.textContent=t(v.attempt?'handoffRetry':v.receipt?'handoffReassess':'handoffAssess');
  v.submit.disabled=busy||v.loadingNodes||(!v.attempt&&(!v.loaded||!v.nodes.has(v.select.value)
    ||!revisionId.test(bridge.task(v.taskId)?.revision||'')));
  renderReceipt(v);
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
   const response=await api(endpoint(v),{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':attempt.id},body:attempt.body});
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
    if(current(v))await read(v);
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
   submitButton=node('button',t('handoffAssess'),'primary'),exit=node('button',t('handoffClose'),'quiet');
  const v={dialog,taskId:task.id,owner:task.ownerNodeId,actor:bridge.session().id,select,candidateMessage,
   feedback,result,more,reload,submit:submitButton,nodes:new Map()};
  view=v;
  heading.id='handoff-title';dialog.setAttribute('aria-labelledby',heading.id);
  select.name='candidateNodeId';field.append(node('span',t('handoffCandidate')),select);
  feedback.setAttribute('role','status');result.setAttribute('aria-live','polite');
  for(const button of [more,reload,exit])button.type='button';
  submitButton.type='submit';submitButton.dataset.handoffAssess='';
  reload.dataset.handoffReload='';more.dataset.handoffMore='';result.dataset.handoffReceipt='';
  exit.onclick=close;dialog.addEventListener('cancel',event=>{event.preventDefault();close();});
  select.onchange=()=>update(v);
  more.onclick=()=>candidates(v,v.next);
  reload.onclick=()=>{v.feedback.textContent='';read(v);candidates(v);};
  form.onsubmit=event=>{event.preventDefault();submit(v);};
  actions.append(exit,more,reload,submitButton);
  form.append(field,candidateMessage,actions,feedback);
  dialog.append(heading,node('p',task.id,'subtle'),node('p',t('handoffHelp'),'callout'),original,form,result);
  document.body.append(dialog);dialog.showModal();
  read(v);candidates(v);
 }
 function button(task){
  if(!eligible(task))return null;
  const control=node('button',t('handoffTitle'),'quiet');control.dataset.handoffTask=task.id;
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
