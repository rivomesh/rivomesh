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
 function close(){
  ticket++;
  if(!dialog)return;
  const closed=dialog;dialog=null;closed.close();closed.replaceChildren();closed.remove();
 }
 function translate(){document.querySelectorAll('[data-recovery-label]').forEach(e=>e.textContent=t(e.dataset.recoveryLabel));}
 function open(title,permitted=active){
  close();
  const generation=++ticket,d=document.createElement('dialog'),heading=label('h2',title);
  dialog=d;d.className='failure-review-dialog recovery-dialog';
  heading.id='recovery-dialog-title';d.setAttribute('aria-labelledby',heading.id);
  const exit=label('button','recoveryClose');exit.onclick=close;
  d.addEventListener('cancel',event=>{event.preventDefault();close();});
  const body=text('div');d.append(heading,exit,body);document.body.append(d);d.showModal();
  return{d,body,current:()=>generation===ticket&&dialog===d&&d.isConnected&&permitted()};
 }
 const receiptId=/^[A-Za-z0-9_-]{8,128}$/;
 const receiptActions=new Set([
  'VALIDATE','VALIDATE_RETRY','VALIDATE_CANCEL','REPAIR_PLAN','REPAIR_EXECUTE','REPAIR_CANCEL',
  'FAILURE_REVIEW','REVIEW_KAFKA_RECEIPTS','CONFIRM_COMMIT','SKIP_TRANSACTION','SKIP_RETRY','SKIP_RECOVER','SKIP_CANCEL',
  'GAP_VERIFY_PLAN','GAP_VERIFY_EXECUTE','GAP_VERIFY_CANCEL','COMPENSATION_PLAN','COMPENSATION_EXECUTE',
  'CORRECTION_EXECUTE','RECOVERY_CANCEL','CAPTURE_RETAIN','CAPTURE_EXCLUDE','VALIDATION_RETENTION_REVIEW',
  'VALIDATION_ARCHIVE_COPY','VALIDATION_ARCHIVE_RESTORE','VALIDATION_ARCHIVE_RECLAIM'
 ]);
 const receiptAllowed=task=>!!task&&(task.formatVersion===3||task.formatVersion==null&&!!task.ownerNodeId)
  &&bridge.session()?.taskControlScope==='GLOBAL'&&can('TASK_CONFIG');
 const receiptFields={reportId:'receiptReport',repairPlanId:'receiptRepairPlan',failureEvidenceId:'receiptEvidence',
  skipPlanId:'receiptSkipPlan',gapPlanId:'receiptGapPlan',archiveId:'receiptArchive'};
 const sequence=/^(0|[1-9][0-9]{0,18})$/,digest=/^[0-9a-f]{64}$/;
 function kafkaReceipt(receipt){
  const proof=receipt.kafkaReview;
  if(receipt.action!=='REVIEW_KAFKA_RECEIPTS'||receipt.status!=='SUCCEEDED')return null;
  if(proof?.status!=='PRECEDING_KAFKA_RECEIPTS_VERIFIED'||proof.target!==receipt.subscriptionId
   ||![proof.epoch,proof.errorGeneration,proof.acknowledgedSequence,proof.failedSequence].every(value=>typeof value==='string'&&sequence.test(value))
   ||!Number.isFinite(Date.parse(proof.observedAt))||!digest.test(proof.observedTopicIdentitySha256||'')
   ||!Array.isArray(proof.nativeReceipts)||!proof.nativeReceipts.length||proof.nativeReceipts.length>256
   ||!proof.nativeReceipts.every(item=>item&&/^[A-Za-z][A-Za-z0-9_]{0,62}$/.test(item.namespace||'')
    &&typeof item.offset==='string'&&sequence.test(item.offset)&&digest.test(item.payloadSha256||'')))
   throw new Error(t('receiptMismatch'));
  const section=text('section'),facts=text('dl'),offsets=text('ul');
  for(const [key,value] of [
   ['kafkaReceiptObservedAt',new Date(proof.observedAt).toLocaleString()],['kafkaReceiptEpoch',proof.epoch],
   ['kafkaReceiptGeneration',proof.errorGeneration],['kafkaReceiptAcknowledged',proof.acknowledgedSequence],
   ['kafkaReceiptFailed',proof.failedSequence],['kafkaReceiptTopic',proof.observedTopicIdentitySha256]
  ])facts.append(label('dt',key),text('dd',value));
  for(const item of proof.nativeReceipts)offsets.append(text('li',item.namespace+' · '+t('kafkaReceiptOffset')+' '+item.offset+' · '+item.payloadSha256));
  section.append(label('h3','kafkaReceiptTitle'),facts,offsets);return section;
 }
 function kafkaNoticePreview(task,receipt,current){
  const scope=()=>bridge.task(task.id);
  const eligible=latest=>receiptAllowed(latest)&&paused(latest)&&bridge.session()?.recoveryReview
   &&latest.destinations?.some(item=>item.id===receipt.subscriptionId&&item.kafkaNoticePlanningAvailable===true);
  const proof=receipt.kafkaReview;
  if(!proof?.originalFailure||!eligible(scope()))return null;
  const section=text('section'),button=label('button','kafkaNoticePreview'),result=text('div');
  section.dataset.kafkaNoticePreview='';result.setAttribute('aria-live','polite');
  button.onclick=async()=>{
   const activeTask=scope();
   if(!current()||!eligible(activeTask)||button.disabled)return;
   button.disabled=true;result.replaceChildren();
   try{
    const data=await api(base(task)+'/kafka-notice-plan',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':requestId()},
     body:JSON.stringify({expectedRevision:activeTask.revision,predecessorRequestId:receipt.id})});
    if(!current()||!section.isConnected||!eligible(scope())||scope().revision!==activeTask.revision)return;
    const plan=data?.plan,topic=plan?.controlTopic,failure=plan?.failure;
    if(data?.status!=='KAFKA_NOTICE_PLAN_REVIEWED'||data?.executionAuthorized!==false
     ||data?.publicationBlockCode!=='KAFKA_NOTICE_GLOBAL_OWNERSHIP_UNVERIFIED'
     ||plan?.task?.taskId!==task.id||plan?.predecessorRequestId!==receipt.id
     ||topic?.targetId!==receipt.subscriptionId||failure?.failedSequence!==proof.failedSequence
     ||!digest.test(data.approvalSha256||'')||!digest.test(plan.payloadSha256||'')
     ||!/^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$/.test(topic.topicName||'')||!/^[A-Za-z0-9_-]{22}$/.test(topic.topicId||''))
     throw new Error(t('receiptMismatch'));
    const details=text('dl');
    for(const [key,value] of [['kafkaNoticeControlTopic',topic.topicName],['kafkaNoticeTopicId',topic.topicId],
     ['kafkaReceiptFailed',failure.failedSequence],['kafkaNoticePayloadSha',plan.payloadSha256],['kafkaNoticePlanSha',data.approvalSha256]])
     details.append(label('dt',key),text('dd',value));
    result.append(label('p','KAFKA_NOTICE_GLOBAL_OWNERSHIP_UNVERIFIED'),details);
    const coverage=data.catalogCoverage;
    const coverageCodes={
     KAFKA_NOTICE_CATALOG_SINGLE_TASK_VERIFIED:'VERIFIED_SINGLETON',
     KAFKA_NOTICE_CATALOG_RESTORABLE_ROUTES_VERIFIED:'VERIFIED_AT_REVIEW',
     KAFKA_NOTICE_CATALOG_PHYSICAL_TOPIC_COLLISION:'CONFLICT',
     KAFKA_NOTICE_CATALOG_NATIVE_IDENTITY_MISSING:'INCOMPLETE',
     KAFKA_NOTICE_CATALOG_RETAINED_REVISION_MISSING:'INCOMPLETE',
     KAFKA_NOTICE_CATALOG_MANAGED_TARGETS_MISSING:'INCOMPLETE',
     KAFKA_NOTICE_CATALOG_REVIEW_LIMIT:'INCOMPLETE'
    };
    if(coverage&&Object.hasOwn(coverageCodes,coverage.code)&&coverageCodes[coverage.code]===coverage.status)
     result.append(label('p',coverage.code));
    const writer=data.writerEntitlement;
    if(writer?.status==='VERIFIED_AT_REVIEW'&&writer.code==='KAFKA_NOTICE_WRITER_EFFECTIVE_PERMISSIONS_VERIFIED')
     result.append(label('p','kafkaNoticeWriterVerified'));
    if(writer?.status==='BLOCKED'&&/^KAFKA_NOTICE_WRITER_[A-Z_]{1,64}$/.test(writer.code||''))
     result.append(label('p','kafkaNoticeWriterBlocked'),text('code',writer.code));
    if(digest.test(data.approvalScopeSha256||'')&&Array.isArray(data.approvalBlockers)
     &&data.approvalBlockers.includes('KAFKA_NOTICE_HISTORICAL_OWNERSHIP_UNVERIFIED')){
     const approvalId=requestId(),approve=label('button','kafkaNoticeApprove'),approvalResult=text('div');
     approvalResult.setAttribute('aria-live','polite');
     approve.onclick=async()=>{
      if(!current()||!eligible(scope())||scope().revision!==activeTask.revision||approve.disabled)return;
      approve.disabled=true;approvalResult.replaceChildren();
      try{
       const recorded=await api(base(task)+'/kafka-notice-approvals',{method:'POST',
        headers:{'Content-Type':'application/json','X-Request-ID':approvalId},
        body:JSON.stringify({expectedRevision:activeTask.revision,predecessorRequestId:receipt.id,
         approvalScopeSha256:data.approvalScopeSha256})});
       if(!current()||!approvalResult.isConnected)return;
       if(recorded?.id!==approvalId||recorded?.taskId!==task.id||recorded?.status!=='RECORDED_BLOCKED'
        ||recorded.executionAuthorized!==false||recorded.approvalScopeSha256!==data.approvalScopeSha256)
        throw new Error(t('receiptMismatch'));
       approvalResult.append(label('p','kafkaNoticeApprovalRecorded'),text('code',approvalId));
      }catch(error){
       if(current()&&approvalResult.isConnected)approvalResult.append(text('p',error.message,'error'));
       approve.disabled=false;
      }
     };
     result.append(label('p','kafkaNoticeApprovalNote'),approve,approvalResult);
    }
   }catch(error){if(current()&&section.isConnected)result.append(text('p',error.message,'error'));}
   finally{button.disabled=false;}
  };
  section.append(button,result);return section;
 }
 function receiptAction(task,operation){
  if(!receiptAllowed(task)||!receiptActions.has(operation.action)||!receiptId.test(operation.id||''))return null;
  const button=label('button','receiptView');button.className='quiet';
  button.dataset.recoveryReceiptTask=task.id;button.dataset.recoveryReceiptRequest=operation.id;
  button.onclick=()=>openReceipt(task,operation.id);return button;
 }
 function rememberReceiptFocus(root,taskId){
  const before=document.activeElement,key=before?.dataset.recoveryReceiptRequest;
  if(!taskId||before?.dataset.recoveryReceiptTask!==taskId||!root.contains(before))return;
  return()=>{
   if(before.isConnected||document.activeElement!==document.body)return;
   const matches=[...root.querySelectorAll('[data-recovery-receipt-task]')].filter(element=>
    element.dataset.recoveryReceiptTask===taskId&&element.dataset.recoveryReceiptRequest===key);
   if(matches.length===1)matches[0].focus({preventScroll:true});
  };
 }
 function openReceipt(task,known=''){
  const permitted=()=>receiptAllowed(bridge.task(task.id));
  if(!permitted())return;
  const dialog=open('receiptTitle',permitted),form=text('form'),field=text('label'),input=document.createElement('input'),
   button=label('button','receiptFind'),feedback=text('p','','error'),result=text('section');
  input.name='requestId';input.maxLength=128;input.required=true;input.autocomplete='off';input.value=known;
  feedback.setAttribute('role','alert');result.setAttribute('aria-live','polite');result.dataset.recoveryReceiptResult='';
  button.type='submit';field.append(label('span','receiptId'),input);form.append(field,button,feedback);
  dialog.body.append(label('p','receiptHelp'),form,result);
  const load=async()=>{
   if(!dialog.current()||button.disabled)return;
   const id=input.value.trim();
   result.replaceChildren();feedback.textContent='';
   if(!receiptId.test(id)){feedback.textContent=t('receiptInvalid');return;}
   button.disabled=input.disabled=true;
   try{
    const receipt=await api(base(task)+'/recovery-receipts/'+encodeURIComponent(id));
    if(!dialog.current())return;
    if(receipt?.kind!=='CONTROL_OPERATION'||receipt.id!==id||receipt.taskId!==task.id||!receiptActions.has(receipt.action))
     throw new Error(t('receiptMismatch'));
    const nativeEvidence=kafkaReceipt(receipt);
    const facts=text('dl'),values=[
     ['receiptId',id],['receiptAction',t(receipt.action.toLowerCase())],
     ['ownerNode',receipt.ownerNodeId],['receiptActor',receipt.actorId],['receiptTarget',receipt.subscriptionId],
     ['receiptTime',Number.isFinite(Date.parse(receipt.createdAt))?new Date(receipt.createdAt).toLocaleString():null],
     ['receiptParent',receipt.parentRequestId]
    ];
    for(const [field,key] of Object.entries(receiptFields))
     if(receiptId.test(receipt.references?.[field]||''))values.push([key,receipt.references[field]]);
    for(const [key,value] of values)if(typeof value==='string'&&value)facts.append(label('dt',key),text('dd',value));
    result.append(text('p',t(receipt.status),'badge'));
    if(typeof receipt.code==='string'&&/^[A-Z0-9_]{1,120}$/.test(receipt.code))result.append(text('p',bridge.explain(receipt.code)));
    result.append(facts);
    if(nativeEvidence){
     result.append(nativeEvidence);
     const notice=kafkaNoticePreview(task,receipt,dialog.current);
     if(notice)result.append(notice);
    }
   }catch(error){if(dialog.current())feedback.textContent=error.message;}
   finally{if(dialog.current())button.disabled=input.disabled=false;}
  };
  form.onsubmit=event=>{event.preventDefault();load();};
  if(known)load();else input.focus();
 }
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
   for(const row of rows){const card=text('article','','subscription-card');card.append(text('strong',String(row.sequence||'')+' · '+String(row.planId||'')),text('p',row.state==='VERIFIED_AT_COMMON_CUT'?t('gapVerifiedAtCut'):row.outcome==='SKIPPED'?t('gapOpen'):t('recoveryState_'+row.state)));const button=label('button','recoveryViewPlan');button.onclick=()=>openPlan(currentTask(task),row.planId);card.append(button);if(row.verificationId){const proof=label('button','gapVerificationTitle');proof.onclick=()=>openVerificationPlan(currentTask(task),row.verificationId);card.append(proof);}content.append(card);}
  }catch(error){if(view.current())content.textContent=error.message;}finally{if(view.current()){previous.disabled=!pages.length;more.disabled=next==null;}}};
  previous.onclick=()=>{if(pages.length)load(pages.pop());};more.onclick=()=>{if(next!=null){pages.push(after);load(String(next));}};await load(after);
 }
 async function submitRecovery(task,action,target,generation,options,current,attempt={},field="gap"){
  if(!current()||!active())return;
  if(!attempt.body){const latest=currentTask(task);if(!['GAP_VERIFY_CANCEL','RECOVERY_CANCEL'].includes(action)&&!paused(latest))throw new Error(t('SKIP_REQUIRES_PIPELINE_PAUSED'));attempt.id=requestId();attempt.body=JSON.stringify({action,expectedRevision:latest.revision,subscriptionId:target,errorCount:generation,...(options?{[field]:options}:{})});}
  await api(base(task)+'/actions',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':attempt.id},body:attempt.body});
  if(!current())return;close();notify(t('accepted'));await refresh();
 }
 function renderCompensationPreview(root,task,id,hash,summary,observation,onAccepted){
  if(!active()||!paused(task)||observation?.verdict!=='CHECKPOINT_NOT_ADVANCED'||observation.targetFenceRequired||summary.failurePhase!=='TARGET_APPLY')return;
  const sub=task.status?.subscriptions?.find(item=>item.id===observation.subscriptionId);
  const destination=task.destinations?.find(item=>item.id===observation.subscriptionId);
  if(!['MYSQL','POSTGRESQL'].includes(destination?.product)||task.status.epoch!==observation.journalEpoch||sub?.state!=='PAUSED'||sub.errorGeneration!==observation.errorGeneration||sub.acknowledged!==observation.acknowledged)return;
  const section=text('section','','compensation-preview'),button=label('button','compensationPreview'),message=text('p','','error'),attempt={};
  button.dataset.recoveryAction='COMPENSATION_PLAN';
  button.onclick=async()=>{button.disabled=true;const current=()=>section.isConnected&&active();
   try{await submitRecovery(task,'COMPENSATION_PLAN',observation.subscriptionId,observation.errorGeneration,{evidenceId:id,evidenceHash:hash},current,attempt,'compensation');if(current())onAccepted?.();}
   catch(error){if(current())message.textContent=error.message;}
   finally{if(current())button.disabled=false;}
  };
  section.append(label('p','compensationHelp'),button,message);root.append(section);
 }
 function openCompensation(task,plan){
  const view=open('compensationPreview'),table=text('table'),head=text('tr');
  for(const key of ['gapTable','compensationIdentities','compensationDeletes','compensationInserts'])head.append(label('th',key));
  table.append(head);
  for(const row of Object.values(plan.tables||{})){
   const line=text('tr');
   for(const value of [row.database+'.'+row.table,row.identities,row.deletes,row.inserts])line.append(text('td',String(value)));
   table.append(line);
  }
  view.body.append(label('p','compensationHelp'),text('p',plan.subscriptionId+' · '+plan.sequence),table,text('p',plan.hash));
  if(!paused(currentTask(task)))return;
  const reason=document.createElement('textarea'),reasonLabel=text('label'),check=document.createElement('input'),confirm=text('label'),button=label('button','compensationExecute'),message=text('p','','error'),attempt={};
  reason.rows=3;reason.maxLength=1000;reasonLabel.append(label('span','recoveryReason'),reason);
  check.type='checkbox';confirm.append(check,label('span','compensationApproval'));button.dataset.recoveryAction='COMPENSATION_EXECUTE';
  let sending=false;
  const update=()=>{button.disabled=sending||!active()||(!attempt.body&&(!check.checked||reason.value.trim().length<8));};
  reason.oninput=check.onchange=update;update();
  button.onclick=async()=>{if(button.disabled)return;sending=true;update();
   try{const submission=submitRecovery(task,'COMPENSATION_EXECUTE',plan.subscriptionId,plan.errorGeneration,{evidenceId:plan.evidenceId,evidenceHash:plan.evidenceHash,approvedHash:plan.hash,reason:reason.value},view.current,attempt,'compensation');reason.disabled=check.disabled=Boolean(attempt.body);await submission;}
   catch(error){if(view.current())message.textContent=error.message;}
   finally{sending=false;if(view.current())update();}
  };
  view.body.append(reasonLabel,confirm,button,message);
 }
 function renderCorrection(root,task,id,hash,summary,observation,onOpen){
  if(!active()||!paused(task)||observation?.verdict!=='CHECKPOINT_NOT_ADVANCED'
      ||observation.targetFenceRequired||summary.failurePhase!=='TARGET_APPLY')return;
  const sub=task.status?.subscriptions?.find(item=>item.id===observation.subscriptionId);
  const destination=task.destinations?.find(item=>item.id===observation.subscriptionId);
  if(!['MYSQL','POSTGRESQL'].includes(destination?.product)||task.status.epoch!==observation.journalEpoch
      ||sub?.state!=='PAUSED'||sub.errorGeneration!==observation.errorGeneration||sub.acknowledged!==observation.acknowledged)return;
  const button=label('button','correctionOpen');
  button.onclick=()=>{onOpen?.();openCorrection(task,id,hash,observation);};
  root.append(button);
 }
 async function openCorrection(task,id,hash,observation){
  const view=open('correctionOpen'),scope={evidenceId:id,evidenceHash:hash};
  const target=observation.subscriptionId,generation=observation.errorGeneration;
  const query=(route,revision,correction)=>api(base(task)+'/corrections/'+route,{
   method:'POST',headers:{'Content-Type':'application/json'},
   body:JSON.stringify({expectedRevision:revision,subscriptionId:target,errorCount:generation,correction})
  });
  view.body.textContent=t('loading');
  let schema;
  try {
   schema=await query('schema',currentTask(task).revision,scope);
   if(!view.current())return;
  } catch(error){
   if(view.current())view.body.textContent=error.message;
   return;
  }
  const edits=[],tables=new Map((schema.tables||[]).map(table=>[table.id,table]));
  const mode=document.createElement('select'),editor=text('div'),review=text('div'),message=text('p','','error');
  const title=text('p',target),preview=label('button','correctionPreview');
  let editVersion=0,loading=false,read=()=>null;
  const invalidate=()=>{editVersion++;review.replaceChildren();message.textContent='';};
  const option=(select,value,title)=>{
   const item=text('option',title);item.value=value;select.append(item);
  };
  option(mode,'form',t('correctionForm'));option(mode,'import',t('correctionImport'));
  mode.setAttribute('aria-label',t('correctionMode'));
  mode.onchange=()=>{invalidate();renderMode();};
  view.body.replaceChildren(title,label('p','correctionHelp'),mode,editor,preview,message,review);
  function renderMode(){
   editor.replaceChildren();
   if(mode.value==='import'){
    const content=document.createElement('textarea'),file=document.createElement('input'),field=text('label');
    file.type='file';file.accept='.json,application/json';file.setAttribute('aria-label',t('correctionImportFile'));
    content.rows=7;content.maxLength=131072;content.oninput=()=>{importRead++;invalidate();};
    field.append(label('span','correctionImportText'),content);
    let importRead=0;
    file.onchange=async()=>{
     const request=++importRead;
     const picked=file.files?.[0];
     if(!picked)return;
     content.value='';invalidate();
     if(picked.size>131072){message.textContent=t('CORRECTION_BATCH_LIMIT');return;}
     try{const value=await picked.text();if(view.current()&&content.isConnected&&request===importRead){content.value=value;invalidate();}}
     catch(error){if(view.current()&&content.isConnected)message.textContent=error.message;}
    };
    editor.append(file,field);
    read=()=>({importJson:content.value});
    return;
   }
   const select=document.createElement('select'),action=document.createElement('select');
   select.setAttribute('aria-label',t('gapTable'));action.setAttribute('aria-label',t('correctionAction'));
   for(const table of tables.values())option(select,table.id,table.name);
   const fields=text('div'),list=text('ul'),add=label('button','correctionAdd');
   editor.append(select,action,fields,add,label('h3','correctionBatch'),list);
   const listRows=()=>{
    list.replaceChildren();
    edits.forEach((edit,index)=>{
     const row=text('li'),remove=label('button','correctionRemove');
     row.append(text('span',tables.get(edit.tableId).name+' · '+t('correction_'+edit.action)),remove);
     remove.onclick=()=>{edits.splice(index,1);invalidate();listRows();};
     list.append(row);
    });
   };
   const renderFields=()=>{
    invalidate();fields.replaceChildren();add.disabled=true;
    const table=tables.get(select.value);
    if(!table||!action.value)return;
    const type=action.value,keyFields=[],valueFields=[];
    let rowId=null;
    if(!table.keys.length){
     rowId=document.createElement('input');rowId.maxLength=64;
     const field=text('label');field.append(label('span','correctionRowId'),rowId);fields.append(field);
    }
    const addField=(column,key)=>{
     const name=text('span',column.name+' · '+column.type+(column.generated?' · '+t('correctionGenerated'):''));
     const row=text('label'),field=document.createElement('input'),state=document.createElement('select');
     const optional=type==='UPDATE'&&!key;
     if(optional)option(state,'omit',t('correctionUnchanged'));
     option(state,'value',t('correctionValue'));
     if(!key&&column.nullable)option(state,'null',t('correctionNull'));
     field.type='text';
     field.setAttribute('aria-label',column.name);
     state.setAttribute('aria-label',column.name+' '+t('correctionValue'));
     field.disabled=state.value!=='value';
     state.onchange=()=>{field.disabled=state.value!=='value';};
     row.append(name,state,field);fields.append(row);
     (key?keyFields:valueFields).push({column,field,state});
    };
    if(type!=='INSERT')for(const column of table.columns)if(table.keys.includes(column.name))addField(column,true);
    if(type!=='DELETE')for(const column of table.columns)
     if(type==='INSERT'||!table.keys.includes(column.name))addField(column,false);
    const values=group=>Object.fromEntries(group.filter(item=>item.state.value!=='omit').map(item=>[
     item.column.name,item.state.value==='null'?null:item.field.value
    ]));
    add.disabled=false;
    add.onclick=()=>{
     if(edits.length>=50){message.textContent=t('CORRECTION_BATCH_LIMIT');return;}
     const edit={tableId:table.id,action:type,key:values(keyFields),values:values(valueFields)};
     if(rowId){
      if(!/^[0-9a-f]{64}$/.test(rowId.value)){message.textContent=t('CORRECTION_ROW_ID_INVALID');return;}
      edit.rowId=rowId.value;
     }
     if(type==='UPDATE'&&!Object.keys(edit.values).length){message.textContent=t('CORRECTION_VALUES_INVALID');return;}
     edits.push(edit);invalidate();listRows();
    };
   };
   select.onchange=()=>{
    action.replaceChildren();
    const table=tables.get(select.value);
    if(table?.keys.length)option(action,'INSERT',t('correction_INSERT'));
    for(const item of ['UPDATE','DELETE'])option(action,item,t('correction_'+item));
    renderFields();
   };
   action.onchange=renderFields;
   select.onchange();listRows();
   read=()=>({batch:{edits:edits.slice()}});
  }
  renderMode();
  preview.onclick=async()=>{
   if(loading||!view.current())return;
   const latest=currentTask(task);
   if(!paused(latest)){message.textContent=t('SKIP_REQUIRES_PIPELINE_PAUSED');return;}
   const payload=read();
   if(!payload?.importJson&&!payload?.batch?.edits.length){message.textContent=t('CORRECTION_BATCH_REQUIRED');return;}
   const revision=latest.revision,version=editVersion;
   loading=preview.disabled=true;review.replaceChildren();message.textContent=t('loading');
   try{
    const plan=await query('preview',revision,{...scope,...payload});
    if(!view.current()||version!==editVersion)return;
    if(plan?.code!=='CORRECTION_PLAN_READY'||plan.evidenceId!==id||plan.evidenceHash!==hash
        ||plan.subscriptionId!==target||plan.errorGeneration!==generation||!/^[0-9a-f]{64}$/.test(plan.hash||'')
        ||!Array.isArray(plan.rows))throw new Error(t('CORRECTION_PLAN_CHANGED'));
    const changed=plan.rows.filter(row=>row.changed).length;
    message.textContent='';
    review.append(label('h3','correctionPreview'),text('p',changed+' / '+plan.rows.length+' · '+t('correctionChangedRows')));
    for(const row of plan.rows){
     const card=text('article','','subscription-card');
     card.append(text('strong',row.table+' · '+t('correction_'+row.action)));
     if(row.rowId)card.append(text('code',row.rowId));
     card.append(label('span','correctionBefore'),text('pre',JSON.stringify(row.before,null,2)),
      label('span','correctionAfter'),text('pre',JSON.stringify(row.after,null,2)));
     review.append(card);
    }
    const reason=document.createElement('textarea'),reasonLabel=text('label'),check=document.createElement('input');
    reason.rows=3;reason.maxLength=1000;reasonLabel.append(label('span','recoveryReason'),reason);
    check.type='checkbox';const confirm=text('label');confirm.append(check,label('span','correctionApproval'));
    const apply=label('button','correctionApply'),attempt={};
    apply.dataset.recoveryAction='CORRECTION_EXECUTE';
    let sending=false;
    const update=()=>{apply.disabled=sending||(!attempt.body&&(!check.checked||reason.value.trim().length<8));};
    check.onchange=reason.oninput=update;update();
    const current=()=>view.current()&&apply.isConnected&&version===editVersion;
    apply.onclick=async()=>{
     if(apply.disabled||!current())return;
     if(!attempt.body&&currentTask(task).revision!==revision){invalidate();message.textContent=t('CORRECTION_PLAN_CHANGED');return;}
     sending=true;update();
     try{
      await submitRecovery(task,'CORRECTION_EXECUTE',target,generation,
       {...scope,...payload,approvedHash:plan.hash,reason:reason.value},current,attempt,'correction');
     }catch(error){if(current())message.textContent=error.message;}
     finally{if(current()){sending=false;check.disabled=reason.disabled=Boolean(attempt.body);update();}}
    };
    review.append(reasonLabel,confirm,apply);
   }catch(error){if(view.current()&&version===editVersion)message.textContent=error.message;}
   finally{if(view.current()){loading=false;preview.disabled=false;}}
  };
 }
 async function openVerificationPlan(task,id){
  if(!active())return;const view=open('gapVerificationTitle');view.body.textContent=t('loading');
  try{
   const response=await api(base(task)+'/gap-verifications/'+encodeURIComponent(id));if(!view.current())return;
   if(response.id!==id||!/^[0-9a-f]{64}$/.test(response.hash||'')||response.plan?.id!==id)throw new Error(t('GAP_VERIFICATION_PLAN_CHANGED'));
   const plan=response.plan;view.body.replaceChildren(label('p','gapVerificationHistorical'),text('p',id+' · '+response.hash),text('p',plan.subscriptionId+' · '+plan.reportId),text('p',String(plan.epoch)+' / '+String(response.sequence??plan.sequence)),text('p',plan.createdBy+' · '+plan.reason),text('p',String(Object.keys(plan.gaps||{}).length)+' / '+String(Object.keys(plan.tables||{}).length)+' · '+String(plan.maximumMillis/1000)+'s'));
   const rows=Object.entries(plan.tables||{}),container=text('div'),previous=label('button','recoveryPrevious'),next=label('button','recoveryNext');let page=0;
   const renderTables=()=>{container.replaceChildren();const table=text('table'),head=text('tr');for(const key of ['gapTable','gapRows','gapSourceRoot'])head.append(label('th',key));table.append(head);for(const [id,row] of rows.slice(page*50,page*50+50)){const line=text('tr');line.append(text('td',row.database+'.'+row.name),text('td',String(row.rows)),text('td',row.sourceRoot));line.dataset.gapTable=id;table.append(line);}container.append(table);previous.disabled=page===0;next.disabled=(page+1)*50>=rows.length;};previous.onclick=()=>{page--;renderTables();};next.onclick=()=>{page++;renderTables();};view.body.append(container,previous,next);renderTables();
   if(response.receipt){view.body.append(label('strong','gapVerifiedAtCut'),text('pre',JSON.stringify(response.receipt,null,2)));return;}
   if(response.state!=='PREPARED'||!paused(currentTask(task))){view.body.append(label('p','recoveryPauseFresh'));return;}
   const reason=document.createElement('textarea');reason.rows=3;reason.maxLength=1000;const reasonLabel=text('label');reasonLabel.append(label('span','recoveryReason'),reason);
   const check=document.createElement('input');check.type='checkbox';const confirm=text('label');confirm.append(check,label('span','gapVerificationApproval'));const execute=label('button','gapVerifyExecute'),message=text('p','','error');execute.dataset.recoveryAction='GAP_VERIFY_EXECUTE';let pending=false;const attempt={};
   const update=()=>{execute.disabled=pending||!active()||(!attempt.body&&(!check.checked||reason.value.trim().length<8));};reason.oninput=check.onchange=update;update();
   execute.onclick=async()=>{if(execute.disabled)return;pending=true;update();try{const operation=submitRecovery(task,'GAP_VERIFY_EXECUTE',plan.subscriptionId,plan.errorGeneration,{planId:id,approvedHash:response.hash,reason:reason.value},view.current,attempt);reason.disabled=check.disabled=Boolean(attempt.body);await operation;}catch(error){if(view.current())message.textContent=error.message;}finally{pending=false;if(view.current())update();}};
   view.body.append(reasonLabel,confirm,execute,message,label('p','recoveryLeavesPaused'));
  }catch(error){if(view.current())view.body.textContent=error.message;}
 }
 async function openVerificationScope(task,report,target){
  if(!active())return;const view=open('gapVerificationScope'),selected=new Set(),attempt={};let after='0',next=null,pages=[],pending=false;
  const content=text('div'),previous=label('button','recoveryPrevious'),more=label('button','recoveryNext'),reason=document.createElement('textarea'),seconds=document.createElement('input'),create=label('button','gapVerifyPlan'),message=text('p','','error');reason.rows=3;reason.maxLength=1000;seconds.type='number';seconds.min=1;seconds.max=86400;seconds.value='300';create.dataset.recoveryAction='GAP_VERIFY_PLAN';
  const reasonLabel=text('label');reasonLabel.append(label('span','recoveryReason'),reason);const limitLabel=text('label');limitLabel.append(label('span','gapMaximumSeconds'),seconds);
  const update=()=>create.disabled=pending||!active()||(!attempt.body&&(!selected.size||selected.size>100||reason.value.trim().length<8||!Number.isInteger(Number(seconds.value))||Number(seconds.value)<1||Number(seconds.value)>86400));reason.oninput=seconds.oninput=update;
  view.body.append(label('p','gapVerificationHistorical'),text('p',target+' · '+report.id),content,previous,more,reasonLabel,limitLabel,create,message);update();
  const load=async cursor=>{previous.disabled=more.disabled=true;content.textContent=t('loading');try{const response=await api(base(task)+'/gaps',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subscriptionId:target,after:cursor,limit:20})});if(!view.current())return;after=cursor;next=response.next;content.replaceChildren();if(!response.items?.length)content.append(label('p','recoveryEmpty'));for(const row of response.items||[]){const line=text('label');if(row.state==='OPEN'){const check=document.createElement('input');check.type='checkbox';check.dataset.gapSelection=row.planId;check.checked=selected.has(row.planId);check.disabled=Boolean(attempt.body);check.onchange=()=>{if(check.checked&&selected.size>=100){check.checked=false;notify(t('gapScopeLimit'));return;}check.checked?selected.add(row.planId):selected.delete(row.planId);update();};line.append(check);}line.append(text('span',String(row.sequence)+' · '+row.planId+' · '+t(row.state==='VERIFIED_AT_COMMON_CUT'?'gapVerifiedAtCut':'gapOpen')));content.append(line);}}catch(error){if(view.current())content.textContent=error.message;}finally{if(view.current()){previous.disabled=!pages.length;more.disabled=next==null;update();}}};
  previous.onclick=()=>{if(pages.length)load(pages.pop());};more.onclick=()=>{if(next!=null){pages.push(after);load(String(next));}};
  create.onclick=async()=>{if(create.disabled)return;pending=true;update();try{const operation=submitRecovery(task,'GAP_VERIFY_PLAN',target,(currentTask(task).status?.errorGeneration??currentTask(task).status?.error_count),{reportId:report.id,gaps:[...selected].sort(),reason:reason.value,maximumMillis:Number(seconds.value)*1000},view.current,attempt);reason.disabled=seconds.disabled=Boolean(attempt.body);for(const input of content.querySelectorAll('input'))input.disabled=Boolean(attempt.body);await operation;}catch(error){if(view.current())message.textContent=error.message;}finally{pending=false;if(view.current())update();}};await load(after);
 }
 function renderVerificationFromReport(root,task,report){
  if(!active()||report.state!=='COMPLETE'||report.code!=='VALIDATION_MATCH')return;
  const ids=[...new Set(Object.values(report.results||{}).map(row=>row.target))].filter(id=>(task.destinations||[]).some(t=>t.id===id&&['MYSQL','POSTGRESQL'].includes(t.product)));if(!ids.length)return;
  const section=text('section','','gap-verification-from-report'),select=document.createElement('select');select.setAttribute('aria-label',t('subscriptions'));for(const id of ids){const option=text('option',id);option.value=id;select.append(option);}
  const button=label('button','gapVerificationScope');button.disabled=!paused(task);button.onclick=()=>openVerificationScope(currentTask(task),report,select.value);section.append(label('p','gapVerificationHistorical'),select,button);root.append(section);
 }
 function render(root,task){
  if(receiptAllowed(task)){
   const lookup=label('button','receiptFind');lookup.className='quiet';lookup.dataset.recoveryReceiptLookup='';
   lookup.dataset.recoveryReceiptTask=task.id;
   lookup.onclick=()=>openReceipt(task);root.append(lookup);
  }
  if(task.formatVersion!==3)return;
  if(task.status?.hasGaps){const verified=task.status.hasOpenGaps===false;const warning=text('section','','callout '+(verified?'info':'warning')+' data-gap-warning');warning.append(label('strong',verified?'gapVerifiedHistory':'recoverySkipped'),label('p',verified?'gapVerificationHistorical':'recoveryGapWarning'));if(task.status.openGapCount!=null)warning.append(text('p',t('gapOpen')+': '+task.status.openGapCount+' · '+t('gapVerifiedAtCut')+': '+task.status.verifiedGapCount+(task.status.gapCountLimited?' +':'')));root.append(warning);}
  if(!active())return;const section=text('section','','recovery-ledger');section.append(label('h2','recoveryLedgerTitle'),label('p','recoveryLedgerHelp'));
  for(const op of (task.operations||[]).slice(-8).reverse()){
   if(op.action!=='COMPENSATION_PLAN'||op.status!=='SUCCEEDED'||op.result?.code!=='COMPENSATION_PLAN_READY'||!/^[0-9a-f]{64}$/.test(op.result.hash||''))continue;
   const review=label('button','compensationReview');review.onclick=()=>openCompensation(currentTask(task),op.result);section.append(review);
  }
  for(const op of task.operations||[]){
   if(!['COMPENSATION_PLAN','COMPENSATION_EXECUTE','CORRECTION_EXECUTE'].includes(op.action)
     ||!['PENDING','ACCEPTED','DISPATCHED','UNKNOWN'].includes(op.status))continue;
   const requested=op.cancelRequested||(task.operations||[]).some(item=>item.action==='RECOVERY_CANCEL'
      &&item.parentRequestId===op.id&&!['FAILED','REJECTED'].includes(item.status));
   const cancel=label('button',requested?'cancelRecoveryPending':'cancelRecoveryRequest'),attempt={};
   cancel.dataset.recoveryAction='RECOVERY_CANCEL';cancel.disabled=Boolean(requested);
   cancel.setAttribute('aria-label',cancel.textContent+' · '+op.id);
   cancel.onclick=async()=>{
    cancel.disabled=true;
    try{await submitRecovery(task,'RECOVERY_CANCEL',null,-1,op.id,()=>active(),attempt,'parentRequestId');}
    catch(error){cancel.disabled=false;notify(error.message);}
   };
   section.append(cancel);
  }
  const select=document.createElement('select');select.setAttribute('aria-label',t('subscriptions'));for(const destination of task.destinations||task.status?.subscriptions||[]){const option=text('option',destination.id);option.value=destination.id;select.append(option);}
  const plans=label('button','recoveryPlansTitle'),gaps=label('button','recoveryGapsTitle');plans.disabled=gaps.disabled=!select.options.length;plans.onclick=()=>openPage(currentTask(task),select.value,false);gaps.onclick=()=>openPage(currentTask(task),select.value,true);section.append(select,plans,gaps);
  for(const op of (task.operations||[]).slice(-8).reverse())if(op.skipPlanId){const button=text('button',t('recoveryViewPlan')+' · '+op.skipPlanId,'quiet');button.onclick=()=>openPlan(currentTask(task),op.skipPlanId);section.append(button);}for(const op of (task.operations||[]).slice(-8).reverse())if(op.gapPlanId){const button=text('button',t('gapVerificationTitle')+' · '+op.gapPlanId,'quiet');button.onclick=()=>openVerificationPlan(currentTask(task),op.gapPlanId);section.append(button);if(op.status==='PENDING'){const cancel=label('button','gapCancel');cancel.disabled=op.cancelRequested;cancel.onclick=async()=>{try{await submitRecovery(currentTask(task),'GAP_VERIFY_CANCEL',null,-1,null,()=>active());}catch(error){notify(error.message);}};section.append(cancel);}}root.append(section);
 }
 bridge.register({
  cancelRecoveryRequest:'请求取消恢复操作',cancelRecoveryPending:'正在确认取消结果',recovery_cancel:'取消恢复操作',
  RECOVERY_CANCEL_REQUESTED:'已提交取消请求。请查看原操作的结果。',
  RECOVERY_CANCEL_RESOLVING:'正在核对目标库提交回执。任务继续保持暂停。',
  RECOVERY_CANCELLED:'原请求已取消。任务继续保持暂停。',
  PARENT_ALREADY_TERMINAL:'原请求已经结束，请查阅其结果。',
  CANCELLED_BEFORE_DISPATCH:'原请求已在下发前取消。'
 },{
  cancelRecoveryRequest:'Request recovery cancellation',cancelRecoveryPending:'Resolving cancellation',recovery_cancel:'Cancel recovery',
  RECOVERY_CANCEL_REQUESTED:'Cancellation requested. Check the original operation for the outcome.',
  RECOVERY_CANCEL_RESOLVING:'Checking the target commit receipt. The task remains paused.',
  RECOVERY_CANCELLED:'The original request was cancelled. The task remains paused.',
  PARENT_ALREADY_TERMINAL:'The original request has finished. Inspect its outcome.',
  CANCELLED_BEFORE_DISPATCH:'The original request was cancelled before dispatch.'
 });
 bridge.register({"gapVerificationTitle":"缺口核验计划与凭据","gapVerificationScope":"选择缺口并生成核验计划","gapVerificationHistorical":"仅核验指定历史共同点；必须重新扫描目标数据。目标仅由同步服务写入。核验不代表当前实时一致，完成后仍暂停。","gapVerificationApproval":"我已审阅缺口和表范围，并确认目标仅由同步服务写入。","gapVerifyExecute":"确认范围并重新核验","gapVerifyPlan":"生成核验计划","gapMaximumSeconds":"核验最长秒数","gapScopeLimit":"一次最多选择100条缺口。","gapVerifiedAtCut":"已在指定共同点核验","gapOpen":"未核验缺口","gapVerifiedHistory":"存在已核验的跳过历史","gapTable":"数据表","gapRows":"源行数","gapSourceRoot":"源内容摘要","gapCancel":"请求取消核验","gap_verify_plan":"准备缺口核验","gap_verify_execute":"执行缺口核验","gap_verify_cancel":"取消缺口核验","GAP_VERIFICATION_PLAN_READY":"核验计划已生成，请审阅后确认。","GAP_VERIFICATION_COMPLETED":"所选缺口已在指定共同点核验；历史记录保留，任务仍暂停。","GAP_LIVE_DATA_DIFFERENCES":"目标数据仍有差异，未写入核验成功记录。","GAP_VERIFICATION_PLAN_CHANGED":"核验计划已变化或损坏，请重新读取。","GAP_VERIFICATION_CANCEL_REQUESTED":"已请求取消，请查询计划凭据确认最终结果。","GAP_VERIFICATION_CANCELLED_CHECK_RECEIPT":"核验已中断或取消；提交可能已完成，请查询原计划凭据，不能据此断言已回滚。","GAP_ALREADY_VERIFIED":"此缺口已有核验凭据，请查看历史记录。",recoveryPrevious:'上一页记录',recoveryNext:'下一页记录',recoveryClose:'关闭恢复窗口',recoveryReason:'操作原因（至少8个字符）',recoveryLossApproval:'我已审阅完整事务，接受跳过所产生的数据缺口。',recoverySkipTitle:'按完整事务跳过',recoverySkipWarning:'此操作不会写入该事务的业务数据，会形成持久数据缺口。只影响所选目标；任务和目标保持暂停。',recoveryExecuteSkip:'接受缺口并跳过此事务',recoveryPauseFresh:'请暂停整个任务、确认目标围栏，并使用匹配当前错误版本的新证据。',recoveryPlanTitle:'跳过计划与恢复',recoveryHistorical:'这是历史记录或有时间点的观察。提交操作时会重新检查权限、事务和断点。',recoveryInspect:'检查目标提交状态（只读）',recoveryUnknown:'目前无法证明目标是否已经提交。已禁止继续或取消；请检查连接、元数据及备份。',recoveryLeavesPaused:'这些操作不会自动恢复同步。',recoveryConfirmGap:'恢复已提交的缺口确认',recoveryRetryTitle:'继续原跳过计划',recoveryRetry:'重新审批并继续跳过',recoveryCancelTitle:'取消未提交计划',recoveryCancel:'取消跳过计划',recoveryCancelledHelp:'取消不会修复原 SQL 错误，不推进 ACK；任务仍暂停，旧错误版本失效。',recoveryGapRetained:'事务已明确跳过，缺口记录继续保留，不代表数据一致。',recoveryGapsTitle:'查看数据缺口',recoveryPlansTitle:'查看跳过计划',recoveryEmpty:'没有符合条件的记录。',recoveryViewPlan:'查看/处理计划',recoverySkipped:'存在已跳过的数据',recoveryGapWarning:'当前确认进度包含跳过事务，不代表目标与源数据一致。请检查缺口及后续对账结果。',recoveryLedgerTitle:'事务恢复与数据缺口',recoveryLedgerHelp:'先查看原计划，再只读检查目标提交状态。已提交的恢复确认，未提交的可重新审批继续或取消。',recoveryState_PREPARED:'准备完成，等待执行或恢复',recoveryState_COMPLETED:'已完成，数据缺口已记录',recoveryState_CANCELLED:'计划已取消',skip_transaction:'事务跳过',skip_retry:'继续跳过计划',skip_recover:'恢复缺口确认',skip_cancel:'取消跳过计划',SKIP_DISABLED:'此集群尚未启用受控事务跳过。',SKIP_REQUIRES_PIPELINE_PAUSED:'请先暂停整个任务。',SKIP_COMPLETED:'跳过已确认，数据缺口已记录；任务仍暂停。',SKIP_RECOVERED:'缺口确认已恢复，任务仍暂停。',SKIP_CANCELLED:'计划已取消，原故障仍需处理。',SKIP_RECOVERY_PENDING:'存在尚未解决的跳过计划，请先恢复、继续或取消。',SKIP_PLAN_CHANGED:'计划摘要或引用已变化，请重新读取。',SKIP_PLAN_NOT_FOUND:'尚无该计划。原操作可能在准备计划前已被拒绝。',SKIP_PLAN_CANCELLED:'原跳过计划已取消，请保存新证据。',SKIP_ALREADY_COMMITTED:'目标已经提交跳过，不能取消；请恢复缺口确认。',SKIP_CHECKPOINT_RECONCILIATION_REQUIRED:'请先核对目标断点；已提交事务应走确认流程。',SKIP_NATIVE_COMMIT_NOT_PROVEN:'尚未证明目标已提交，不能执行恢复确认。',SKIP_NATIVE_CHECKPOINT_CHANGED:'目标断点与计划不符，未继续执行。',SKIP_DATA_LOSS_APPROVAL_REQUIRED:'需要明确接受本事务的数据缺口。'},
 {"gapVerificationTitle":"Gap verification plan and receipt","gapVerificationScope":"Select gaps and prepare verification","gapVerificationHistorical":"Verify the specified historical cut with a fresh target scan. Only the sync service may write the target. This is not current live equality;keep the task paused.","gapVerificationApproval":"I reviewed the gaps and table scope and confirm that only the sync service writes the target.","gapVerifyExecute":"Approve scope and verify again","gapVerifyPlan":"Prepare verification plan","gapMaximumSeconds":"Maximum verification seconds","gapScopeLimit":"Select at most 100 gaps per plan.","gapVerifiedAtCut":"Verified at the specified common cut","gapOpen":"Unverified gaps","gapVerifiedHistory":"Verified skip history exists","gapTable":"Table","gapRows":"Source rows","gapSourceRoot":"Source content root","gapCancel":"Request verification cancellation","gap_verify_plan":"Prepare gap verification","gap_verify_execute":"Verify gaps","gap_verify_cancel":"Cancel gap verification","GAP_VERIFICATION_PLAN_READY":"Plan prepared;review it before confirming.","GAP_VERIFICATION_COMPLETED":"Selected gaps verified at the specified cut;history retained and task paused.","GAP_LIVE_DATA_DIFFERENCES":"Live target differences remain;no successful verification was recorded.","GAP_VERIFICATION_PLAN_CHANGED":"Verification plan changed or is damaged;reload it.","GAP_VERIFICATION_CANCEL_REQUESTED":"Cancellation requested;inspect the plan receipt for the final outcome.","GAP_VERIFICATION_CANCELLED_CHECK_RECEIPT":"Verification interrupted or cancelled;commit may already have completed. Inspect the original receipt;rollback is not implied.","GAP_ALREADY_VERIFIED":"This gap already has a verification receipt;inspect its history.",recoveryPrevious:'Previous records',recoveryNext:'Next records',recoveryClose:'Close recovery',recoveryReason:'Operation reason (at least 8 characters)',recoveryLossApproval:'I reviewed the complete transaction and accept the resulting data gap.',recoverySkipTitle:'Skip a complete transaction',recoverySkipWarning:'Business changes from this transaction will not be applied. Record an explicit gap for this destination;keep task and subscriber paused.',recoveryExecuteSkip:'Accept gap and skip transaction',recoveryPauseFresh:'Pause the whole task,confirm destination fencing,and use fresh evidence matching the current failure.',recoveryPlanTitle:'Skip plan and recovery',recoveryHistorical:'Historical record or point-in-time observation. Permissions,transaction and checkpoints are rechecked when executing.',recoveryInspect:'Inspect native commit (read-only)',recoveryUnknown:'Native commit cannot currently be proven. Continue/cancel are disabled;check connectivity,metadata and backups.',recoveryLeavesPaused:'These operations never resume replication automatically.',recoveryConfirmGap:'Recover committed gap acknowledgment',recoveryRetryTitle:'Continue original skip plan',recoveryRetry:'Reapprove and continue skipping',recoveryCancelTitle:'Cancel uncommitted plan',recoveryCancel:'Cancel skip plan',recoveryCancelledHelp:'Cancellation does not repair the SQL fault or advance ACK. Keep the task paused;the previous error generation becomes invalid.',recoveryGapRetained:'Transaction explicitly skipped. Its durable gap remains;this is not data equality.',recoveryGapsTitle:'View data gaps',recoveryPlansTitle:'View skip plans',recoveryEmpty:'No matching records.',recoveryViewPlan:'View/resolve plan',recoverySkipped:'Skipped data exists',recoveryGapWarning:'Acknowledged progress includes skipped transactions and does not imply source/destination equality. Review gaps and subsequent validation.',recoveryLedgerTitle:'Transaction recovery and data gaps',recoveryLedgerHelp:'Open the original plan and inspect the native commit. Recover committed skips;reapprove or cancel uncommitted plans.',recoveryState_PREPARED:'Prepared;execution or recovery pending',recoveryState_COMPLETED:'Completed;gap retained',recoveryState_CANCELLED:'Plan cancelled',skip_transaction:'Skip transaction',skip_retry:'Continue skip plan',skip_recover:'Recover gap acknowledgment',skip_cancel:'Cancel skip plan',SKIP_DISABLED:'Controlled transaction skipping is not enabled for this cluster.',SKIP_REQUIRES_PIPELINE_PAUSED:'Pause the whole task first.',SKIP_COMPLETED:'Skip confirmed;gap retained and task remains paused.',SKIP_RECOVERED:'Gap acknowledgment recovered;task remains paused.',SKIP_CANCELLED:'Plan cancelled;the original fault still needs handling.',SKIP_RECOVERY_PENDING:'Resolve,continue or cancel the pending skip plan before resuming.',SKIP_PLAN_CHANGED:'Plan hash or reference changed;reload it.',SKIP_PLAN_NOT_FOUND:'No such plan exists;the original operation may have refused before preparation.',SKIP_PLAN_CANCELLED:'Original skip plan cancelled;retain fresh evidence.',SKIP_ALREADY_COMMITTED:'Native skip already committed;recover acknowledgment instead of cancelling.',SKIP_CHECKPOINT_RECONCILIATION_REQUIRED:'Compare native checkpoints first;proven commits need acknowledgment reconciliation.',SKIP_NATIVE_COMMIT_NOT_PROVEN:'Native commit is not proven;acknowledgment recovery refused.',SKIP_NATIVE_CHECKPOINT_CHANGED:'Native checkpoint differs from the plan;execution refused.',SKIP_DATA_LOSS_APPROVAL_REQUIRED:'Explicitly accept this transaction data gap.'});
 bridge.register({"compensationPreview": "生成事务补偿预览", "compensationReview": "审阅事务补偿计划", "compensationHelp": "补偿只还原受影响行的事务前状态。确认后任务保持暂停，请检查结果再恢复原事务。", "compensationIdentities": "受影响行标识", "compensationDeletes": "需删除", "compensationInserts": "需写入", "compensationExecute": "确认并执行补偿", "compensationApproval": "我已审阅目标和行变更范围。", "compensation_plan": "事务补偿预览", "compensation_execute": "执行事务补偿", "COMPENSATION_PLAN_READY": "补偿预览已生成。", "COMPENSATION_COMPLETED": "补偿已提交，任务保持暂停。", "COMPENSATION_PLAN_CHANGED": "目标数据或断点已变化，请重新预览。", "COMPENSATION_CHECKPOINT_RECONCILIATION_REQUIRED": "请先确认目标库是否已提交原事务。"},{"compensationPreview": "Generate compensation preview", "compensationReview": "Review compensation plan", "compensationHelp": "Compensation restores affected rows to their pre-transaction state. The task stays paused for review before replay.", "compensationIdentities": "Affected identities", "compensationDeletes": "Rows to delete", "compensationInserts": "Rows to insert", "compensationExecute": "Approve and apply compensation", "compensationApproval": "I reviewed the target and row changes.", "compensation_plan": "Compensation preview", "compensation_execute": "Apply compensation", "COMPENSATION_PLAN_READY": "Compensation preview ready.", "COMPENSATION_COMPLETED": "Compensation committed. The task remains paused.", "COMPENSATION_PLAN_CHANGED": "Target rows or checkpoints changed. Generate a new preview.", "COMPENSATION_CHECKPOINT_RECONCILIATION_REQUIRED": "Reconcile the original target commit first."});
 bridge.register({"correctionOpen": "手工补数据", "correctionHelp": "选择数据表和行操作。加入批次后预览前后值，确认后任务仍会保持暂停。", "correctionForm": "逐行录入", "correctionImport": "导入 JSON", "correctionMode": "录入方式", "correctionImportFile": "选择 JSON 文件", "correctionImportText": "批次 JSON", "correctionAction": "行操作", "correctionAdd": "加入批次", "correctionBatch": "待预览行", "correctionRemove": "移除", "correctionRowId": "无主键行 ROW_ID", "correctionGenerated": "物化的生成值", "correctionUnchanged": "保留当前值", "correctionValue": "输入值", "correctionNull": "SQL NULL", "correction_INSERT": "新增", "correction_UPDATE": "修改", "correction_DELETE": "删除", "correctionPreview": "预览手工修正", "correctionChangedRows": "行将变化", "correctionBefore": "修改前", "correctionAfter": "修改后", "correctionApproval": "我已审阅前后值和受影响行。", "correctionApply": "确认并提交手工修正", "correction_execute": "手工修正", "CORRECTION_COMPLETED": "手工修正已提交，任务保持暂停。", "CORRECTION_PLAN_CHANGED": "目标数据或任务已变化，请重新预览。", "CORRECTION_BATCH_REQUIRED": "请加入待修正行或导入 JSON。", "CORRECTION_BATCH_LIMIT": "一次最多 50 行，JSON 不超过 128 KiB。", "CORRECTION_ROW_ID_INVALID": "请输入证据中的 64 位 ROW_ID。", "CORRECTION_VALUES_INVALID": "请至少选择一个字段填写。", "CORRECTION_IMPORT_INVALID": "JSON 无法解析，请检查格式和重复字段。", "CORRECTION_TYPED_TEXT_REQUIRED": "请用字符串传值，SQL NULL 使用 JSON null。", "CORRECTION_ROW_ALREADY_EXISTS": "要新增的行已存在，请检查目标键。", "CORRECTION_ROW_MISSING": "目标行已不存在，请检查行标识。", "CORRECTION_NULL_NOT_ALLOWED": "该字段不允许 SQL NULL。", "CORRECTION_TYPED_VALUE_INVALID": "字段值不符合类型或精度要求。"},{"correctionOpen": "Correct target rows", "correctionHelp": "Choose a table and row operation. Add edits to the batch, review before and after values and confirm. The task stays paused.", "correctionForm": "Enter rows", "correctionImport": "Import JSON", "correctionMode": "Input mode", "correctionImportFile": "Choose JSON file", "correctionImportText": "Batch JSON", "correctionAction": "Row action", "correctionAdd": "Add to batch", "correctionBatch": "Rows to preview", "correctionRemove": "Remove", "correctionRowId": "Keyless row ID", "correctionGenerated": "Materialized generated value", "correctionUnchanged": "Keep current value", "correctionValue": "Value", "correctionNull": "SQL NULL", "correction_INSERT": "Insert", "correction_UPDATE": "Update", "correction_DELETE": "Delete", "correctionPreview": "Preview corrections", "correctionChangedRows": "rows will change", "correctionBefore": "Before", "correctionAfter": "After", "correctionApproval": "I reviewed the before and after values and affected rows.", "correctionApply": "Approve and apply corrections", "correction_execute": "Manual correction", "CORRECTION_COMPLETED": "Corrections committed. The task remains paused.", "CORRECTION_PLAN_CHANGED": "Target data or task state changed. Preview again.", "CORRECTION_BATCH_REQUIRED": "Add rows or import a JSON batch.", "CORRECTION_BATCH_LIMIT": "A batch can contain up to 50 rows and 128 KiB of JSON.", "CORRECTION_ROW_ID_INVALID": "Enter the 64-character ROW_ID from evidence.", "CORRECTION_VALUES_INVALID": "Select at least one field to update.", "CORRECTION_IMPORT_INVALID": "Invalid import JSON. Check the format and duplicate fields.", "CORRECTION_TYPED_TEXT_REQUIRED": "Use JSON strings for values and JSON null for SQL NULL.", "CORRECTION_ROW_ALREADY_EXISTS": "The insert identity already exists. Check its key.", "CORRECTION_ROW_MISSING": "The requested row is absent. Check its identity.", "CORRECTION_NULL_NOT_ALLOWED": "This field cannot be SQL NULL.", "CORRECTION_TYPED_VALUE_INVALID": "A value does not fit its type or precision."});
 bridge.register({
  receiptTitle:'恢复操作回执',receiptFind:'按请求 ID 查询回执',receiptView:'查看回执',
  receiptHelp:'回执展示控制面记录的操作状态。可查询已离开近期列表的请求。',
  receiptId:'管理请求 ID',receiptAction:'操作',receiptActor:'发起人',receiptTarget:'目标',receiptTime:'提交时间',receiptParent:'关联请求',
  receiptReport:'校验报告',receiptRepairPlan:'修复计划',receiptEvidence:'事务证据',receiptSkipPlan:'跳过计划',
  receiptGapPlan:'缺口核验计划',receiptArchive:'报告归档',receiptInvalid:'请输入有效的管理请求 ID。',receiptMismatch:'返回的回执与当前请求不符。',
  review_kafka_receipts:'核对 Kafka 回执',kafkaReceiptTitle:'前一笔已确认事务的 Kafka 回执',
  kafkaReceiptObservedAt:'核对时间',kafkaReceiptEpoch:'任务代次',kafkaReceiptGeneration:'目标错误代次',
  kafkaReceiptAcknowledged:'已确认事务序号',kafkaReceiptFailed:'失败事务序号',kafkaReceiptTopic:'Topic 身份摘要',kafkaReceiptOffset:'位点',
  kafkaNoticePreview:'核对 Kafka 通知计划',kafkaNoticeControlTopic:'专用控制 Topic',kafkaNoticeTopicId:'Topic ID',
  kafkaNoticePayloadSha:'通知内容摘要',kafkaNoticePlanSha:'计划摘要',
  KAFKA_NOTICE_GLOBAL_OWNERSHIP_UNVERIFIED:'发布受阻。专用 Topic 的全局归属尚未确认。',
  kafkaNoticeWriterVerified:'已核对独立 Kafka 写入身份在这两个 Topic 上的有效权限。',
  kafkaNoticeWriterBlocked:'独立 Kafka 写入身份的权限核对未通过。',
  kafkaNoticeApprove:'登记条件审批',
  kafkaNoticeApprovalNote:'历史物理归属仍待核对。登记审批仅保存本次核对范围，通知不会发送。',
  kafkaNoticeApprovalRecorded:'审批已登记，通知仍未获准发送。',
  KAFKA_NOTICE_CATALOG_SINGLE_TASK_VERIFIED:'已核对唯一任务及其唯一配置版本中的 Kafka 目标。',
  KAFKA_NOTICE_CATALOG_RESTORABLE_ROUTES_VERIFIED:'已核对生效和可回退配置当前指向的 Kafka Topic。',
  KAFKA_NOTICE_CATALOG_REVIEW_LIMIT:'管理目录超出这次只读核对的范围。',
  KAFKA_NOTICE_CATALOG_PHYSICAL_TOPIC_COLLISION:'业务 Topic 与专用通知 Topic 指向同一物理资源。',
  KAFKA_NOTICE_CATALOG_NATIVE_IDENTITY_MISSING:'所属节点未提供 Kafka 原生 Topic 标识。',
  KAFKA_NOTICE_CATALOG_RETAINED_REVISION_MISSING:'历史配置仍需补齐物理 Topic 归属证据。',
  KAFKA_NOTICE_CATALOG_MANAGED_TARGETS_MISSING:'其他受管任务或 Kafka 目标仍需补齐物理 Topic 归属证据。',
  KAFKA_NOTICE_PREDECESSOR_REFRESH_REQUIRED:'请在当前任务配置下重新核对 Kafka 回执。',
  PRECEDING_KAFKA_RECEIPTS_VERIFIED:'已核对前一笔 Kafka 回执。',
  KAFKA_RECOVERY_PUBLICATION_UNCERTAIN:'发现后续事务发布记录，无法确认当前投递状态。',
  KAFKA_RECOVERY_PRECEDING_RECEIPT_UNAVAILABLE:'找不到前一笔已确认事务的完整 Kafka 回执。',
  KAFKA_RECOVERY_REQUIRES_PIPELINE_PAUSED:'请先暂停整条同步任务。',
  RECOVERY_RECEIPT_NOT_FOUND:'当前任务下没有对应的恢复操作回执。'
 },{
  receiptTitle:'Recovery operation receipt',receiptFind:'Find receipt by request ID',receiptView:'View receipt',
  receiptHelp:'A receipt shows the operation state recorded by the control plane. Earlier requests can be found after they leave recent history.',
  receiptId:'Management request ID',receiptAction:'Operation',receiptActor:'Requested by',receiptTarget:'Destination',receiptTime:'Submitted at',
  receiptParent:'Related request',receiptReport:'Validation report',receiptRepairPlan:'Repair plan',receiptEvidence:'Transaction evidence',
  receiptSkipPlan:'Skip plan',receiptGapPlan:'Gap verification plan',receiptArchive:'Report archive',receiptInvalid:'Enter a valid management request ID.',
  receiptMismatch:'The returned receipt does not match the current request.',
  review_kafka_receipts:'Review Kafka receipts',kafkaReceiptTitle:'Kafka receipts for the preceding acknowledged transaction',
  kafkaReceiptObservedAt:'Reviewed at',kafkaReceiptEpoch:'Pipeline epoch',kafkaReceiptGeneration:'Destination error generation',
  kafkaReceiptAcknowledged:'Acknowledged sequence',kafkaReceiptFailed:'Failed sequence',kafkaReceiptTopic:'Topic identity digest',kafkaReceiptOffset:'Offset',
  kafkaNoticePreview:'Review Kafka notice plan',kafkaNoticeControlTopic:'Dedicated control topic',kafkaNoticeTopicId:'Topic ID',
  kafkaNoticePayloadSha:'Notice payload digest',kafkaNoticePlanSha:'Plan digest',
  KAFKA_NOTICE_GLOBAL_OWNERSHIP_UNVERIFIED:'Publication is blocked until dedicated topic ownership is verified globally.',
  kafkaNoticeWriterVerified:'Verified the effective permissions of the dedicated Kafka writer on these two topics.',
  kafkaNoticeWriterBlocked:'The effective permissions of the dedicated Kafka writer could not be verified.',
  kafkaNoticeApprove:'Record conditional approval',
  kafkaNoticeApprovalNote:'Historical topic ownership needs verification. Recording approval saves the reviewed scope and keeps publication blocked.',
  kafkaNoticeApprovalRecorded:'Approval recorded. Publication is still blocked.',
  KAFKA_NOTICE_CATALOG_SINGLE_TASK_VERIFIED:'Verified the Kafka destination in the sole managed task and its only configuration version.',
  KAFKA_NOTICE_CATALOG_RESTORABLE_ROUTES_VERIFIED:'Verified where active and restorable Kafka configurations point at review time.',
  KAFKA_NOTICE_CATALOG_REVIEW_LIMIT:'The managed catalog exceeds the scope of this read-only review.',
  KAFKA_NOTICE_CATALOG_PHYSICAL_TOPIC_COLLISION:'The business topic and dedicated control topic identify the same physical resource.',
  KAFKA_NOTICE_CATALOG_NATIVE_IDENTITY_MISSING:'The task owner did not provide native Kafka topic identity evidence.',
  KAFKA_NOTICE_CATALOG_RETAINED_REVISION_MISSING:'Historical configurations still require physical topic evidence.',
  KAFKA_NOTICE_CATALOG_MANAGED_TARGETS_MISSING:'Other managed tasks or Kafka destinations still require physical topic evidence.',
  KAFKA_NOTICE_PREDECESSOR_REFRESH_REQUIRED:'Review the Kafka receipts again using the current task configuration.',
  PRECEDING_KAFKA_RECEIPTS_VERIFIED:'Verified the preceding Kafka receipts.',
  KAFKA_RECOVERY_PUBLICATION_UNCERTAIN:'A later publication is visible and its delivery state is uncertain.',
  KAFKA_RECOVERY_PRECEDING_RECEIPT_UNAVAILABLE:'Complete Kafka receipts for the preceding acknowledged transaction are unavailable.',
  KAFKA_RECOVERY_REQUIRES_PIPELINE_PAUSED:'Pause the entire pipeline first.',
  RECOVERY_RECEIPT_NOT_FOUND:'No corresponding recovery operation receipt exists for this task.'
 });
 return{render,renderSkipApproval,renderCompensationPreview,renderCorrection,openPlan,openVerificationPlan,openVerificationScope,
  renderVerificationFromReport,receiptAction,rememberReceiptFocus,close,translate};
})();
