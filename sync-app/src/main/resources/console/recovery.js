'use strict';
window.RivomeshRecovery=(()=>{
 const bridge=window.RivomeshConfiguration,{api,can,t,notify,refresh,requestId}=bridge;
 let dialog=null,ticket=0;
 const text=(tag,value,cls)=>{const e=document.createElement(tag);e.textContent=value;if(cls)e.className=cls;return e;};
 const label=(tag,key)=>{const e=text(tag,t(key));e.dataset.recoveryLabel=key;return e;};
 const active=()=>Boolean(bridge.session()?.recoverySkip&&can('TASK_CONFIG'));
 const currentTask=task=>bridge.task(task.id)||task;
 const paused=task=>task.desired==='PAUSED'&&task.status?.run_state==='PAUSED'&&task.statusAvailable!==false;
 const base=task=>'/tasks/'+encodeURIComponent(task.id);
 function close(){ticket++;if(dialog){dialog.replaceChildren();dialog.remove();dialog=null;}}
 function translate(){document.querySelectorAll('[data-recovery-label]').forEach(e=>e.textContent=t(e.dataset.recoveryLabel));}
 function open(title){close();const generation=++ticket,d=document.createElement('dialog');dialog=d;d.className='failure-review-dialog recovery-dialog';const exit=label('button','recoveryClose');exit.onclick=close;d.addEventListener('cancel',event=>{event.preventDefault();close();});const body=text('div');d.append(label('h2',title),exit,body);document.body.append(d);d.showModal();return{d,body,current:()=>generation===ticket&&dialog===d&&d.isConnected&&active()};}
 async function submit(task,action,sub,errorGeneration,skip,current,onAccepted){
  if(!current()||!active())return;const latest=currentTask(task);if(!paused(latest)){notify(t('SKIP_REQUIRES_PIPELINE_PAUSED'));return;}
  await api(base(task)+'/actions',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':requestId()},body:JSON.stringify({action,expectedRevision:latest.revision,subscriptionId:sub,errorCount:errorGeneration,skip})});
  if(!current())return;onAccepted?.();close();notify(t('accepted'));await refresh();
 }
 function approval(root,task,options){
  const reason=document.createElement('textarea');reason.rows=3;reason.maxLength=1000;const reasonLabel=text('label');reasonLabel.append(label('span','recoveryReason'),reason);root.append(reasonLabel);
  let accepted=null;if(options.loss){accepted=document.createElement('input');accepted.type='checkbox';const declaration=text('label');declaration.append(accepted,label('span','recoveryLossApproval'));root.append(declaration);}
  const button=label('button',options.button),message=text('p','','error');button.dataset.recoveryAction=options.action;button.disabled=true;root.append(button,message);let pending=false;
  const update=()=>button.disabled=pending||!active()||reason.value.trim().length<8||Boolean(accepted&&!accepted.checked);reason.oninput=update;if(accepted)accepted.onchange=update;
  button.onclick=async()=>{if(button.disabled)return;pending=true;update();try{await submit(task,options.action,options.target,options.errorGeneration,{...options.scope,reason:reason.value,acceptDataLoss:Boolean(accepted?.checked)},options.current,options.onAccepted);}catch(error){if(options.current())message.textContent=error.message;}finally{pending=false;if(options.current())update();}};
 }
 function renderSkipApproval(root,task,id,hash,summary,observation,onAccepted){
  if(!active()||observation?.verdict!=='CHECKPOINT_NOT_ADVANCED'||summary.failurePhase!=='TARGET_APPLY')return;
  const sub=task.status?.subscriptions?.find(s=>s.id===observation.subscriptionId);
  if(!paused(task)||task.status.epoch!==observation.journalEpoch||sub?.state!=='PAUSED'||sub?.errorGeneration!==observation.errorGeneration||sub?.acknowledged!==observation.acknowledged||observation.targetFenceRequired){root.append(label('p','recoveryPauseFresh'));return;}
  const section=text('section','','recovery-skip-approval');section.append(label('h3','recoverySkipTitle'),label('p','recoverySkipWarning'));
  approval(section,task,{action:'SKIP_TRANSACTION',button:'recoveryExecuteSkip',loss:true,target:observation.subscriptionId,errorGeneration:observation.errorGeneration,scope:{evidenceId:id,evidenceHash:hash},current:()=>section.isConnected&&active(),onAccepted});root.append(section);
 }
 async function openPlan(task,id){
  if(!active())return;const view=open('recoveryPlanTitle');view.body.textContent=t('loading');
  const load=async native=>{
   view.body.textContent=t('loading');try{
    const path=base(task)+'/recovery-plans/'+encodeURIComponent(id),plan=await api(path+(native?'/inspect':''),native?{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}:undefined);if(!view.current())return;
    if(plan.planId!==id||!/^[0-9a-f]{64}$/.test(plan.planHash||''))throw new Error(t('SKIP_PLAN_CHANGED'));
    view.body.replaceChildren(label('p','recoveryHistorical'),label('p','recoveryState_'+plan.state),text('pre',JSON.stringify(plan,null,2)));
    if(plan.evidenceId&&bridge.reviewEvidence){const evidence=label('button','viewFailureEvidence');evidence.onclick=()=>{close();bridge.reviewEvidence(currentTask(task),plan.evidenceId);};view.body.append(evidence);}
    if(plan.state!=='PREPARED'){view.body.append(label('p',plan.state==='COMPLETED'?'recoveryGapRetained':'recoveryCancelledHelp'));return;}
    const inspect=label('button','recoveryInspect');inspect.onclick=()=>load(true);view.body.append(inspect);
    if(!paused(currentTask(task))){view.body.append(label('p','recoveryPauseFresh'));return;}
    if(!plan.nativeProof)return;
    if(plan.nativeProof.state==='UNKNOWN'){view.body.append(label('p','recoveryUnknown'));return;}
    const scope={planId:plan.planId,planHash:plan.planHash};view.body.append(label('p','recoveryLeavesPaused'));
    if(plan.nativeProof.state==='COMMITTED')approval(view.body,task,{action:'SKIP_RECOVER',button:'recoveryConfirmGap',loss:false,target:plan.target,errorGeneration:plan.errorGeneration,scope,current:view.current});
    if(plan.nativeProof.state==='ABSENT'){
     const retry=text('section','','recovery-retry'),cancel=text('section','','recovery-cancel');retry.append(label('h3','recoveryRetryTitle'));cancel.append(label('h3','recoveryCancelTitle'),label('p','recoveryCancelledHelp'));
     approval(retry,task,{action:'SKIP_RETRY',button:'recoveryRetry',loss:true,target:plan.target,errorGeneration:plan.errorGeneration,scope,current:view.current});approval(cancel,task,{action:'SKIP_CANCEL',button:'recoveryCancel',loss:false,target:plan.target,errorGeneration:plan.errorGeneration,scope,current:view.current});view.body.append(retry,cancel);
    }
   }catch(error){if(view.current())view.body.textContent=error.message;}
  };await load(false);
 }
 async function openPage(task,target,gaps){
  if(!active())return;const view=open(gaps?'recoveryGapsTitle':'recoveryPlansTitle');let after=gaps?'0':'',next=null,pages=[];
  const content=text('div'),previous=label('button','recoveryPrevious'),more=label('button','recoveryNext');view.body.append(label('p','recoveryHistorical'),content,previous,more);
  const load=async cursor=>{previous.disabled=true;more.disabled=true;content.textContent=t('loading');try{
   const response=await api(base(task)+(gaps?'/gaps':'/recovery-plans'),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subscriptionId:target,after:cursor,limit:20})});if(!view.current())return;
   after=cursor;next=response.next;content.replaceChildren();const rows=response.items||[];if(!rows.length)content.append(label('p','recoveryEmpty'));
   for(const row of rows){const card=text('article','','subscription-card');card.append(text('strong',String(row.sequence||'')+' · '+String(row.planId||'')),text('p',row.outcome==='SKIPPED'?t('recoverySkipped'):t('recoveryState_'+row.state)));const button=label('button','recoveryViewPlan');button.onclick=()=>openPlan(currentTask(task),row.planId);card.append(button);content.append(card);}
  }catch(error){if(view.current())content.textContent=error.message;}finally{if(view.current()){previous.disabled=!pages.length;more.disabled=next==null;}}};
  previous.onclick=()=>{if(pages.length)load(pages.pop());};more.onclick=()=>{if(next!=null){pages.push(after);load(String(next));}};await load(after);
 }
 function render(root,task){
  if(task.formatVersion!==3)return;
  if(task.status?.hasGaps){const warning=text('section','','callout warning data-gap-warning');warning.append(label('strong','recoverySkipped'),label('p','recoveryGapWarning'));root.append(warning);}
  if(!active())return;const section=text('section','','recovery-ledger');section.append(label('h2','recoveryLedgerTitle'),label('p','recoveryLedgerHelp'));
  const select=document.createElement('select');select.setAttribute('aria-label',t('subscriptions'));for(const destination of task.destinations||task.status?.subscriptions||[]){const option=text('option',destination.id);option.value=destination.id;select.append(option);}
  const plans=label('button','recoveryPlansTitle'),gaps=label('button','recoveryGapsTitle');plans.disabled=gaps.disabled=!select.options.length;plans.onclick=()=>openPage(currentTask(task),select.value,false);gaps.onclick=()=>openPage(currentTask(task),select.value,true);section.append(select,plans,gaps);
  for(const op of (task.operations||[]).slice(-8).reverse())if(op.skipPlanId){const button=text('button',t('recoveryViewPlan')+' · '+op.skipPlanId,'quiet');button.onclick=()=>openPlan(currentTask(task),op.skipPlanId);section.append(button);}root.append(section);
 }
 bridge.register({recoveryPrevious:'上一页记录',recoveryNext:'下一页记录',recoveryClose:'关闭恢复窗口',recoveryReason:'操作原因（至少8个字符）',recoveryLossApproval:'我已审阅完整事务，接受跳过所产生的数据缺口。',recoverySkipTitle:'按完整事务跳过',recoverySkipWarning:'此操作不会写入该事务的业务数据，会形成持久数据缺口。只影响所选目标；任务和目标保持暂停。',recoveryExecuteSkip:'接受缺口并跳过此事务',recoveryPauseFresh:'请暂停整个任务、确认目标围栏，并使用匹配当前错误版本的新证据。',recoveryPlanTitle:'跳过计划与恢复',recoveryHistorical:'这是历史记录或有时间点的观察。提交操作时会重新检查权限、事务和断点。',recoveryInspect:'检查目标提交状态（只读）',recoveryUnknown:'目前无法证明目标是否已经提交。已禁止继续或取消；请检查连接、元数据及备份。',recoveryLeavesPaused:'这些操作不会自动恢复同步。',recoveryConfirmGap:'恢复已提交的缺口确认',recoveryRetryTitle:'继续原跳过计划',recoveryRetry:'重新审批并继续跳过',recoveryCancelTitle:'取消未提交计划',recoveryCancel:'取消跳过计划',recoveryCancelledHelp:'取消不会修复原 SQL 错误，不推进 ACK；任务仍暂停，旧错误版本失效。',recoveryGapRetained:'事务已明确跳过，缺口记录继续保留，不代表数据一致。',recoveryGapsTitle:'查看数据缺口',recoveryPlansTitle:'查看跳过计划',recoveryEmpty:'没有符合条件的记录。',recoveryViewPlan:'查看/处理计划',recoverySkipped:'存在已跳过的数据',recoveryGapWarning:'当前确认进度包含跳过事务，不代表目标与源数据一致。请检查缺口及后续对账结果。',recoveryLedgerTitle:'事务恢复与数据缺口',recoveryLedgerHelp:'先查看原计划，再只读检查目标提交状态。已提交的恢复确认，未提交的可重新审批继续或取消。',recoveryState_PREPARED:'准备完成，等待执行或恢复',recoveryState_COMPLETED:'已完成，数据缺口已记录',recoveryState_CANCELLED:'计划已取消',skip_transaction:'事务跳过',skip_retry:'继续跳过计划',skip_recover:'恢复缺口确认',skip_cancel:'取消跳过计划',SKIP_DISABLED:'此集群尚未启用受控事务跳过。',SKIP_REQUIRES_PIPELINE_PAUSED:'请先暂停整个任务。',SKIP_COMPLETED:'跳过已确认，数据缺口已记录；任务仍暂停。',SKIP_RECOVERED:'缺口确认已恢复，任务仍暂停。',SKIP_CANCELLED:'计划已取消，原故障仍需处理。',SKIP_RECOVERY_PENDING:'存在尚未解决的跳过计划，请先恢复、继续或取消。',SKIP_PLAN_CHANGED:'计划摘要或引用已变化，请重新读取。',SKIP_PLAN_NOT_FOUND:'尚无该计划。原操作可能在准备计划前已被拒绝。',SKIP_PLAN_CANCELLED:'原跳过计划已取消，请保存新证据。',SKIP_ALREADY_COMMITTED:'目标已经提交跳过，不能取消；请恢复缺口确认。',SKIP_CHECKPOINT_RECONCILIATION_REQUIRED:'请先核对目标断点；已提交事务应走确认流程。',SKIP_NATIVE_COMMIT_NOT_PROVEN:'尚未证明目标已提交，不能执行恢复确认。',SKIP_NATIVE_CHECKPOINT_CHANGED:'目标断点与计划不符，未继续执行。',SKIP_DATA_LOSS_APPROVAL_REQUIRED:'需要明确接受本事务的数据缺口。'},
 {recoveryPrevious:'Previous records',recoveryNext:'Next records',recoveryClose:'Close recovery',recoveryReason:'Operation reason (at least 8 characters)',recoveryLossApproval:'I reviewed the complete transaction and accept the resulting data gap.',recoverySkipTitle:'Skip a complete transaction',recoverySkipWarning:'Business changes from this transaction will not be applied. Record an explicit gap for this destination;keep task and subscriber paused.',recoveryExecuteSkip:'Accept gap and skip transaction',recoveryPauseFresh:'Pause the whole task,confirm destination fencing,and use fresh evidence matching the current failure.',recoveryPlanTitle:'Skip plan and recovery',recoveryHistorical:'Historical record or point-in-time observation. Permissions,transaction and checkpoints are rechecked when executing.',recoveryInspect:'Inspect native commit (read-only)',recoveryUnknown:'Native commit cannot currently be proven. Continue/cancel are disabled;check connectivity,metadata and backups.',recoveryLeavesPaused:'These operations never resume replication automatically.',recoveryConfirmGap:'Recover committed gap acknowledgment',recoveryRetryTitle:'Continue original skip plan',recoveryRetry:'Reapprove and continue skipping',recoveryCancelTitle:'Cancel uncommitted plan',recoveryCancel:'Cancel skip plan',recoveryCancelledHelp:'Cancellation does not repair the SQL fault or advance ACK. Keep the task paused;the previous error generation becomes invalid.',recoveryGapRetained:'Transaction explicitly skipped. Its durable gap remains;this is not data equality.',recoveryGapsTitle:'View data gaps',recoveryPlansTitle:'View skip plans',recoveryEmpty:'No matching records.',recoveryViewPlan:'View/resolve plan',recoverySkipped:'Skipped data exists',recoveryGapWarning:'Acknowledged progress includes skipped transactions and does not imply source/destination equality. Review gaps and subsequent validation.',recoveryLedgerTitle:'Transaction recovery and data gaps',recoveryLedgerHelp:'Open the original plan and inspect the native commit. Recover committed skips;reapprove or cancel uncommitted plans.',recoveryState_PREPARED:'Prepared;execution or recovery pending',recoveryState_COMPLETED:'Completed;gap retained',recoveryState_CANCELLED:'Plan cancelled',skip_transaction:'Skip transaction',skip_retry:'Continue skip plan',skip_recover:'Recover gap acknowledgment',skip_cancel:'Cancel skip plan',SKIP_DISABLED:'Controlled transaction skipping is not enabled for this cluster.',SKIP_REQUIRES_PIPELINE_PAUSED:'Pause the whole task first.',SKIP_COMPLETED:'Skip confirmed;gap retained and task remains paused.',SKIP_RECOVERED:'Gap acknowledgment recovered;task remains paused.',SKIP_CANCELLED:'Plan cancelled;the original fault still needs handling.',SKIP_RECOVERY_PENDING:'Resolve,continue or cancel the pending skip plan before resuming.',SKIP_PLAN_CHANGED:'Plan hash or reference changed;reload it.',SKIP_PLAN_NOT_FOUND:'No such plan exists;the original operation may have refused before preparation.',SKIP_PLAN_CANCELLED:'Original skip plan cancelled;retain fresh evidence.',SKIP_ALREADY_COMMITTED:'Native skip already committed;recover acknowledgment instead of cancelling.',SKIP_CHECKPOINT_RECONCILIATION_REQUIRED:'Compare native checkpoints first;proven commits need acknowledgment reconciliation.',SKIP_NATIVE_COMMIT_NOT_PROVEN:'Native commit is not proven;acknowledgment recovery refused.',SKIP_NATIVE_CHECKPOINT_CHANGED:'Native checkpoint differs from the plan;execution refused.',SKIP_DATA_LOSS_APPROVAL_REQUIRED:'Explicitly accept this transaction data gap.'});
 return{render,renderSkipApproval,openPlan,close,translate};
})();
