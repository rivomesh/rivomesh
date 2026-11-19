import {test} from 'node:test';
import assert from 'node:assert/strict';

export function registerHandoffTests({setup,flush,login}){
 const revision='a1234567-aaaa-4abc-8abc-0123456789ab';
 const newRevision='b1234567-aaaa-4abc-8abc-0123456789ab';
 const committedConfiguration='c1234567-aaaa-4abc-8abc-0123456789ab';
 const route='/api/tasks/handoff-task/handoff';
 const preparationRoute=route+'/preparation';
 const digest='a'.repeat(64);
 const archiveDigest='4'.repeat(64);
 const expiration='2026-11-18T09:15:00Z';
 const reply=(data,status=200)=>({ok:status<400,status,json:async()=>data});
 const error=(code,status)=>reply({code},status);
 const agent=(id,role='AGENT',state='HEALTHY')=>({id,role,state,taskCapacity:2,runningCapacity:1});
 const snapshot=(id,taskRevision=revision,state='EVIDENCE_REQUIRED')=>({
  taskId:'handoff-task',id,taskRevision,state,originalNodeId:'original-agent',candidateNodeId:'target-agent',
  candidateState:'HEALTHY',candidateAssignedTasks:0,candidateTaskCapacity:2,candidateWorkerCapacity:1,
  assessedAt:'2026-11-18T09:00:00Z',executionAuthorized:false,
  blockers:[...(state==='BLOCKED'?['ORIGINAL_WORKER_NOT_DRAINED']:[]),'CANDIDATE_ACCESS_UNVERIFIED','ORIGINAL_RECOVERY_EVIDENCE_UNVERIFIED']
 });
 function reviewed(id='fresh-assessment',current=true){
  return {...snapshot(id),originalCut:{checkpointHash:'7'.repeat(64)},probe:{
   checkedAt:'2026-11-18T09:00:10Z',current,candidate:{state:'VERIFIED'},
   sharedReceiptCustody:{state:'VERIFIED'},sharedReportCustody:{state:'VERIFIED'}
  },preparationEvidenceSha256:digest};
 }
 function preparation(id,assessmentId='fresh-assessment',state='PREPARED',releaseId=null){
  const slots=state==='PREPARED'?1:0;
  return {id,taskId:'handoff-task',state,assessmentId,taskRevision:revision,originalNodeId:'original-agent',
   candidateNodeId:'target-agent',executionAuthorized:false,evidenceSha256:digest,custodySha256:archiveDigest,createdAt:'2026-11-18T09:00:00Z',
   expiresAt:expiration,reservedTaskSlots:slots,reservedWorkerSlots:slots,...(releaseId?{releaseId}:{})};
 }
 function fixture({role='ADMIN',scope=['original-agent','target-agent','offline-agent'],mode='GLOBAL',
  read,submit,probe,readPreparation,prepare,release,stage,fence,start,commit,taskState}={}){
  const task={id:'handoff-task',revision,ownerNodeId:'original-agent',formatVersion:3,
   workerState:'STOPPED',desired:'STOPPED',status:{},operations:[],destinations:[],...taskState};
  const ui=setup([task],role,{authenticated:false,taskControlScope:mode,nodeRole:'CONTROLLER',nodeIds:scope}),fallback=ui.w.fetch;
  const requests=[],reserves=[],releases=[],probes=[],stages=[],fences=[],starts=[],commits=[];
  ui.w.fetch=async(path,options={})=>{
   if(path.startsWith('/api/nodes?')){
    if(path.includes('after='))return reply({nodes:[agent('target-agent'),agent('offline-agent','AGENT','STALE')],next:null});
    return reply({nodes:[agent('original-agent'),agent('hidden-agent'),agent('controller','CONTROLLER')],next:'node-cursor'});
   }
   if(path===route){
    if(options.method!=='POST')return read?read():error('HANDOFF_ASSESSMENT_NOT_FOUND',404);
    requests.push(options);
    return submit?submit(options):reply(snapshot(options.headers['X-Request-ID'],JSON.parse(options.body).expectedRevision));
   }
   if(path===route+'/probe'){
    probes.push(options);
    assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
    return probe(options);
   }
   if(path===preparationRoute){
    if(options.method!=='POST')return readPreparation?readPreparation():error('HANDOFF_PREPARATION_NOT_FOUND',404);
    reserves.push(options);
    assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
    return prepare(options);
   }
   if(path===preparationRoute+'/release'){
    releases.push(options);
    assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
    return release(options);
   }
   if(path===preparationRoute+'/fence-targets'){
    fences.push(options);
    assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
    return fence(options);
   }
   if(path===preparationRoute+'/commit-owner'){
    commits.push(options);
    assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
    return commit(options);
   }
   if(path===preparationRoute+'/start-worker'){
    starts.push(options);
    assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
    return start(options);
   }
   if(path===preparationRoute+'/stage'){
    stages.push(options);
    assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
    return stage(options);
   }
   return fallback(path,options);
  };
  return{...ui,task,requests,reserves,releases,probes,stages,fences,starts,commits};
 }
 const launcher=ui=>ui.d.querySelector('[data-handoff-task]');
 const dialog=ui=>ui.d.querySelector('.handoff-dialog');
 const field=ui=>dialog(ui).querySelector('select');
 const action=ui=>dialog(ui).querySelector('[data-handoff-assess]');
 const receipt=ui=>dialog(ui).querySelector('[data-handoff-receipt]');
 const reservation=ui=>dialog(ui).querySelector('[data-handoff-preparation]');
 const button=(ui,action)=>dialog(ui).querySelector('[data-handoff-'+action+']');
 const staged=ui=>dialog(ui).querySelector('[data-handoff-stage-status]');
 const feedback=ui=>dialog(ui).querySelector('[data-handoff-feedback]').textContent;
 async function open(ui){await login(ui);launcher(ui).click();await flush();}
 async function choices(ui){dialog(ui).querySelector('[data-handoff-more]').click();await flush();}
 function choose(ui,id='target-agent'){const select=field(ui);select.value=id;select.dispatchEvent(new ui.w.Event('change'));}
 async function post(ui){dialog(ui).querySelector('form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();}

 test('handoff assessment offers authorized candidates and retries an uncertain submission with the same latest revision request',async()=>{
  let attempts=0;
  const ui=fixture({submit:options=>{
   if(++attempts===1)throw new TypeError('disconnected');
   return reply(snapshot(options.headers['X-Request-ID'],JSON.parse(options.body).expectedRevision,'BLOCKED'));
  }});
  try{
   await open(ui);
   assert.equal(ui.d.getElementById(dialog(ui).getAttribute('aria-labelledby')).textContent,'评估 Agent 接管');
   assert.match(receipt(ui).textContent,/暂无评估/);
   assert.deepEqual([...field(ui).options].map(o=>o.value),['']);
   await choices(ui);
   assert.deepEqual([...field(ui).options].map(o=>o.value),['','target-agent','offline-agent']);
   assert.ok(action(ui).disabled);
   await post(ui);assert.equal(ui.requests.length,0);
   choose(ui);ui.task.revision=newRevision;ui.w.RivomeshConfiguration.render();
   await post(ui);
   assert.match(action(ui).textContent,/重试同一评估/);assert.ok(field(ui).disabled);
   assert.match(dialog(ui).textContent,/提交结果未知/);
   await post(ui);
   const [first,retry]=ui.requests;
   assert.deepEqual(JSON.parse(first.body),{candidateNodeId:'target-agent',expectedRevision:newRevision});
   assert.equal(retry.body,first.body);
   assert.equal(retry.headers['X-Request-ID'],first.headers['X-Request-ID']);
   assert.equal(retry.headers['X-CSRF-Token'],'test-csrf');
   assert.match(receipt(ui).textContent,/评估受阻.*当前 Worker 尚未停止/s);
   assert.ok(receipt(ui).textContent.includes('已分配任务 / 任务槽0 / 2'));
   assert.match(receipt(ui).querySelector('details').textContent,new RegExp(first.headers['X-Request-ID']));
   assert.equal(ui.task.ownerNodeId,'original-agent');
   assert.ok(!dialog(ui).textContent.includes('切换执行节点'));
  }finally{ui.dom.window.close();}
 });

 test('handoff verifies bound evidence, reviews reservation impact, reserves capacity and releases the original task',async()=>{
  let latest={...snapshot('fresh-assessment'),originalCut:{checkpointHash:'7'.repeat(64)}};
  let planned,checks=0;
  const ui=fixture({read:()=>reply(latest),probe:()=>{
   latest=reviewed();
   if(++checks===1){
    delete latest.preparationEvidenceSha256;
    latest.probe={current:true,checkedAt:latest.probe.checkedAt,candidate:{state:'VERIFIED'},
     originalEvidence:{shareableReports:{state:'REFUSED',code:'HANDOFF_REPORT_ARCHIVE_RESTORE_REQUIRED'}}};
   }
   return reply(latest);
  },prepare:options=>{
   planned=preparation(options.headers['X-Request-ID']);
   return reply(planned);
  },release:options=>reply({...preparation(planned.id,'fresh-assessment','RELEASED',options.headers['X-Request-ID'])})});
  try{
   await open(ui);
   assert.ok(button(ui,'prepare').hidden);assert.equal(button(ui,'probe').type,'button');
   button(ui,'probe').click();await flush();
   assert.deepEqual(JSON.parse(ui.probes[0].body),{expectedAssessment:'fresh-assessment'});
   assert.match(receipt(ui).textContent,/请先还原已归档的校验报告/);
   assert.ok(button(ui,'prepare').hidden);
   button(ui,'probe').click();await flush();
   assert.match(receipt(ui).textContent,/预留所需证据已核验/);
   assert.match(reservation(ui).textContent,/original-agent.*target-agent.*本任务暂停接收新命令/s);
   assert.equal(reservation(ui).querySelector('details code').textContent,digest);
   assert.ok(!button(ui,'prepare').disabled);assert.ok(button(ui,'release').hidden);
   button(ui,'prepare').focus();button(ui,'prepare').click();await flush();
   assert.deepEqual(JSON.parse(ui.reserves[0].body),{expectedAssessment:'fresh-assessment',
    expectedRevision:revision,expectedEvidenceSha256:digest});
   assert.match(reservation(ui).textContent,/预留中.*已预留任务槽 [/] Worker 槽1 [/] 1/s);
   assert.equal(reservation(ui).querySelector('time').dateTime,expiration);
   assert.ok(action(ui).disabled&&button(ui,'prepare').hidden&&button(ui,'probe').hidden);
   assert.equal(ui.d.activeElement,button(ui,'release'));
   button(ui,'release').click();await flush();
   assert.deepEqual(JSON.parse(ui.releases[0].body),{expectedPreparation:planned.id});
   assert.notEqual(ui.reserves[0].headers['X-Request-ID'],ui.releases[0].headers['X-Request-ID']);
   assert.match(reservation(ui).textContent,/已解除.*已预留任务槽 [/] Worker 槽0 [/] 0/s);
   assert.ok(button(ui,'release').hidden);assert.match(feedback(ui),/原任务恢复接收操作/);
   assert.equal(ui.task.ownerNodeId,'original-agent');assert.equal(ui.task.revision,revision);
  }finally{ui.dom.window.close();}
 });

 test('handoff reuses an uncertain mutation and can resolve an acknowledged reservation through refresh',async()=>{
  let planned,reserveCount=0,releaseCount=0;
  const ui=fixture({read:()=>reply(reviewed()),readPreparation:()=>planned?reply(planned):error('HANDOFF_PREPARATION_NOT_FOUND',404),
   prepare:options=>{
    if(++reserveCount===1){planned=preparation(options.headers['X-Request-ID']);throw new TypeError('disconnected');}
    throw new Error('Refresh must not submit a second reservation');
   },release:options=>{
    if(++releaseCount===1)throw new TypeError('disconnected');
    return reply(preparation(planned.id,'fresh-assessment','RELEASED',options.headers['X-Request-ID']));
   }});
  try{
   await open(ui);
   button(ui,'prepare').click();await flush();
   assert.match(feedback(ui),/提交结果未知/);
   assert.match(button(ui,'prepare').textContent,/重试同一预留请求/);
   assert.ok(action(ui).disabled&&button(ui,'probe').disabled&&button(ui,'release').hidden);
   button(ui,'reload').click();await flush();
   assert.equal(ui.reserves.length,1);
   assert.match(reservation(ui).textContent,/预留中/);
   button(ui,'release').click();await flush();
   assert.match(button(ui,'release').textContent,/重试同一解除请求/);
   button(ui,'release').click();await flush();
   assert.equal(ui.releases[1].body,ui.releases[0].body);
   assert.equal(ui.releases[1].headers['X-Request-ID'],ui.releases[0].headers['X-Request-ID']);
   assert.match(reservation(ui).textContent,/已解除/);
  }finally{ui.dom.window.close();}
 });

 test('handoff reads database expiry and asks for fresh evidence after a reserve refusal',async()=>{
  let latest=reviewed(),planned=preparation('earlier-reservation','fresh-assessment','EXPIRED');
  const ui=fixture({read:()=>reply(latest),readPreparation:()=>reply(planned),
   prepare:()=>{latest=reviewed('fresh-assessment',false);return error('HANDOFF_PREPARATION_EVIDENCE_REQUIRED',409);}});
  try{
   await open(ui);
   assert.match(reservation(ui).textContent,/已到期/);
   assert.equal(reservation(ui).querySelector('time').dateTime,expiration);
   assert.ok(button(ui,'release').hidden);
   button(ui,'prepare').click();await flush();
   assert.match(feedback(ui),/证据已过期或不足/);
   assert.match(receipt(ui).textContent,/核验结果已过期/);
   assert.ok(button(ui,'prepare').hidden);assert.ok(!button(ui,'probe').disabled);
   assert.equal(ui.reserves.length,1);
  }finally{ui.dom.window.close();}
 });

 test('handoff rejects a reservation receipt for different evidence or an executing task',async()=>{
  for(const invalid of [{evidenceSha256:'b'.repeat(64)},{executionAuthorized:true},{candidateNodeId:'hidden-agent'}]){
   const ui=fixture({read:()=>reply(reviewed()),prepare:options=>
    reply({...preparation(options.headers['X-Request-ID']),...invalid})});
   try{
    await open(ui);button(ui,'prepare').click();await flush();
    assert.match(feedback(ui),/提交结果未知/);
    assert.ok(button(ui,'release').hidden);
    assert.ok(!reservation(ui).textContent.includes('预留中'));
   }finally{ui.dom.window.close();}
  }
 });

 function stagingReply(plan,overrides={}){
  return reply({state:'STAGED',preparationId:plan.id,taskRevision:plan.taskRevision,candidateNodeId:plan.candidateNodeId,
   evidenceSha256:plan.evidenceSha256,archiveSha256:plan.custodySha256,reports:2,closedReceipts:5,
   executionAuthorized:false,privatePath:'/private/staged/report.json',...overrides});
 }

 test('handoff explicitly stages exact active evidence, shows masked counts and reverifies with the same request',async()=>{
  const plan=preparation('staging-preparation');
  let pending;
  const ui=fixture({read:()=>reply(reviewed()),readPreparation:()=>reply({...plan}),
   stage:()=>new Promise(resolve=>{pending=resolve;})});
  try{
   await open(ui);
   assert.equal(ui.stages.length,0);
   assert.ok(!button(ui,'stage').parentElement.hidden&&!button(ui,'stage').disabled);
   assert.equal(staged(ui).textContent,'');
   button(ui,'stage').click();await flush();
   assert.ok(button(ui,'release').disabled&&button(ui,'stage').disabled);
   assert.deepEqual(JSON.parse(ui.stages[0].body),{expectedPreparation:plan.id,expectedRevision:revision,expectedEvidenceSha256:digest});
   pending(stagingReply(plan));await flush();
   assert.match(staged(ui).textContent,/候选 Agent 已核验暂存文件。.*校验报告 2.*已办结回执 5/);
   assert.doesNotMatch(reservation(ui).textContent,/private\/staged/);
   assert.match(button(ui,'stage').textContent,/复核已暂存/);
   button(ui,'reload').click();await flush();
   assert.equal(ui.stages.length,1);
   assert.match(staged(ui).textContent,/候选 Agent 已核验暂存文件/);
   button(ui,'stage').click();await flush();pending(stagingReply(plan));await flush();
   assert.equal(ui.stages[1].headers['X-Request-ID'],ui.stages[0].headers['X-Request-ID']);
   assert.equal(ui.stages[1].body,ui.stages[0].body);
   assert.equal(ui.task.ownerNodeId,plan.originalNodeId);
   assert.equal(ui.task.revision,revision);
  }finally{ui.dom.window.close();}
 });

 test('handoff staging retries uncertainty and never promotes a mismatched staging receipt',async()=>{
  const plan=preparation('staging-preparation');
  let attempts=0;
  const ui=fixture({read:()=>reply(reviewed()),readPreparation:()=>reply(plan),stage:()=>{
   if(++attempts===1)return error('OWNER_QUERY_TIMEOUT',409);
   if(attempts===2)return stagingReply(plan,{archiveSha256:'0'.repeat(64)});
   if(attempts===3)return stagingReply(plan,{executionAuthorized:true});
   if(attempts===4)return stagingReply(plan,{candidateNodeId:'offline-agent'});
   return stagingReply(plan);
  }});
  try{
   await open(ui);
   for(const phrase of [/暂存结果未知/,/暂存回执与当前准备记录不匹配/,/暂存回执与当前准备记录不匹配/,/暂存回执与当前准备记录不匹配/]){
    button(ui,'stage').click();await flush();
    assert.match(feedback(ui),phrase);
    assert.match(button(ui,'stage').textContent,/重试同一暂存请求/);
    assert.equal(staged(ui).textContent,'');
   }
   button(ui,'stage').click();await flush();
   assert.match(staged(ui).textContent,/已核验暂存文件/);
   assert.equal(new Set(ui.stages.map(stage=>stage.headers['X-Request-ID'])).size,1);
   assert.equal(new Set(ui.stages.map(stage=>stage.body)).size,1);
  }finally{ui.dom.window.close();}
 });

 test('handoff shows verified raw capture counts and refuses evidence from a different preparation',async()=>{
  const plan=preparation('staging-preparation');
  const raw={state:'STAGED',executionAuthorized:false,preparationId:plan.id,candidateNodeId:plan.candidateNodeId,
   evidenceSha256:plan.evidenceSha256,archiveSha256:'a'.repeat(64),stagedSealSha256:'6'.repeat(64),archiveBytes:4096,rawCaptures:1};
  let result={...raw,preparationId:'unrelated-stage'};
  const ui=fixture({read:()=>reply(reviewed()),readPreparation:()=>reply(plan),
   stage:()=>stagingReply(plan,{rawCapture:result})});
  try{
   await open(ui);button(ui,'stage').click();await flush();
   assert.match(feedback(ui),/暂存回执与当前准备记录不匹配/);
   assert.equal(staged(ui).textContent,'');
   result=raw;
   button(ui,'stage').click();await flush();
   assert.match(staged(ui).textContent,/校验报告 2.*已办结回执 5.*原始采集档案 1/);
   assert.doesNotMatch(staged(ui).textContent,/6{20}|a{20}|4096/);
   assert.equal(ui.stages[0].headers['X-Request-ID'],ui.stages[1].headers['X-Request-ID']);
  }finally{ui.dom.window.close();}
 });

 test('handoff forgets the staged receipt when the reservation expires or scope changes',async()=>{
  let plan=preparation('staging-preparation');
  const ui=fixture({read:()=>reply(reviewed()),readPreparation:()=>reply(plan),stage:()=>stagingReply(plan)});
  try{
   await open(ui);button(ui,'stage').click();await flush();
   assert.match(staged(ui).textContent,/已核验暂存文件/);
   plan=preparation('staging-preparation','fresh-assessment','EXPIRED');
   button(ui,'reload').click();await flush();
   assert.ok(button(ui,'stage').parentElement.hidden);
   assert.equal(staged(ui).textContent,'');
   plan={...preparation('other-preparation'),custodySha256:null};
   button(ui,'reload').click();await flush();
   assert.ok(button(ui,'stage').parentElement.hidden);
   plan=preparation('other-preparation');
   button(ui,'reload').click();await flush();
   assert.ok(!button(ui,'stage').parentElement.hidden);
   assert.match(button(ui,'stage').textContent,/^暂存报告和回执$/);
   ui.task.revision=newRevision;ui.w.RivomeshConfiguration.render();
   assert.ok(button(ui,'stage').parentElement.hidden);
   assert.equal(ui.stages.length,1);
  }finally{ui.dom.window.close();}
 });

 const transferredTask={ownerNodeId:'target-agent',revision:newRevision,configurationVersion:committedConfiguration};
 function completed(state,overrides={}){
  return {...preparation('completed-handoff','fresh-assessment',state),activatedJournalEpoch:'9007199254740993',
   committedRevision:newRevision,committedConfigurationVersion:committedConfiguration,
   ...(state==='EVIDENCE_ADOPTED'?{adoptedEvidence:{validationReports:2,closedReceipts:5}}:{}),...overrides};
 }
 function assertReadOnly(ui){
  for(const name of ['assess','probe','prepare','release','stage']){
   const control=button(ui,name);
   assert.ok(control.hidden||control.closest('[hidden]'),name+' is hidden');
   assert.ok(control.disabled,name+' is disabled');
   control.click();
  }
  assert.equal(ui.requests.length+ui.probes.length+ui.reserves.length+ui.releases.length+ui.stages.length,0);
 }

 test('handoff can commit the reviewed owner after staging and resume the bound approval',async()=>{
  let plan={...preparation('completed-handoff'),writerActivation:{checkedInControlTransaction:true}};
  let calls=0;
  const ui=fixture({read:()=>reply(reviewed()),readPreparation:()=>reply(plan),stage:()=>stagingReply(plan),commit:options=>{
   const commitRequestId=options.headers['X-Request-ID'];
   if(++calls===1){
    plan={...plan,commitRequestId};
    return error('OWNER_QUERY_TIMEOUT',409);
   }
   const accepted=completed('COMMITTED',{commitRequestId});
   plan=accepted;
   Object.assign(ui.task,transferredTask);
   return reply(accepted);
  }});
  try{
   await open(ui);
   assert.ok(button(ui,'fence').disabled);
   button(ui,'stage').click();await flush();
   assert.equal(button(ui,'fence').dataset.handoffAction,'commit-owner');
   assert.match(button(ui,'fence').textContent,/确认切换归属/);
   button(ui,'fence').click();await flush();
   assert.match(feedback(ui),/归属切换结果待确认/);
   assert.equal(ui.task.ownerNodeId,'original-agent');
   button(ui,'reload').click();await flush();
   assert.match(button(ui,'fence').textContent,/重试同一归属切换/);
   button(ui,'fence').click();await flush();
   assert.equal(ui.commits.length,2);
   assert.deepEqual(ui.commits[0],ui.commits[1]);
   assert.deepEqual(JSON.parse(ui.commits[0].body),{expectedPreparation:'completed-handoff',
    expectedRevision:revision,expectedEvidenceSha256:digest});
   assert.equal(ui.task.ownerNodeId,'target-agent');
   if(!dialog(ui))await open(ui);
   assert.match(reservation(ui).textContent,/归属已提交/);
   assert.ok(button(ui,'stage').disabled&&button(ui,'fence').disabled);
   assert.equal(ui.starts.length+ui.fences.length,0);
  }finally{ui.dom.window.close();}
 });

 test('handoff shows ownership commit before the candidate publishes a local task observation',async()=>{
  const ui=fixture({taskState:{...transferredTask,formatVersion:undefined,statusAvailable:false,
   workerState:'UNKNOWN',desired:'UNKNOWN'},read:()=>assert.fail('Read-only dialog does not load an assessment'),
   readPreparation:()=>reply(completed('COMMITTED'))});
  try{
   await open(ui);
   assert.match(launcher(ui).textContent,/查看 Agent 交接状态/);
   assert.match(dialog(ui).querySelector('h2').textContent,/查看 Agent 交接状态/);
   assert.match(reservation(ui).textContent,/归属已提交.*原 Agentoriginal-agent.*当前归属 Agenttarget-agent/s);
   assert.match(reservation(ui).textContent,/已提交日志代次9007199254740993/);
   assert.doesNotMatch(reservation(ui).textContent,/到期时间|已接收|已安装/);
   assertReadOnly(ui);
  }finally{ui.dom.window.close();}
 });

 test('handoff refreshes signed projection and evidence adoption as read-only masked status',async()=>{
  let state='COMMITTED';
  const ui=fixture({taskState:transferredTask,readPreparation:()=>reply(completed(state,{
   privatePath:'/private/original',
   adoptedEvidence:state==='EVIDENCE_ADOPTED'?{validationReports:2,closedReceipts:5,privatePath:'/private/reports'}:null
  }))});
  try{
   await open(ui);assert.match(reservation(ui).textContent,/归属已提交/);
   state='PROJECTED';button(ui,'reload').click();await flush();
   assert.match(reservation(ui).textContent,/候选配置已安装.*停止状态/s);
   assert.doesNotMatch(reservation(ui).textContent,/证据已接收/);
   state='EVIDENCE_ADOPTED';button(ui,'reload').click();await flush();
   assert.match(reservation(ui).textContent,/证据已接收.*校验报告2.*已办结回执5/s);
   assert.match(reservation(ui).textContent,new RegExp(committedConfiguration));
   assert.doesNotMatch(reservation(ui).textContent,/private|activeConfigurationVersion|executionAuthorized/);
   await post(ui);assertReadOnly(ui);
  }finally{ui.dom.window.close();}
 });

 test('handoff approves the exact parked target fence and retries an uncertain request without starting the Worker',async()=>{
  let plan=completed('EVIDENCE_ADOPTED',{targetFenceAvailable:true}),attempts=0;
  const ui=fixture({taskState:transferredTask,readPreparation:()=>reply(plan),fence:options=>{
   if(++attempts===1)throw new TypeError('disconnected');
   plan=completed('FENCE_PENDING',{adoptedEvidence:{validationReports:2,closedReceipts:5},
    targetFence:{requestId:options.headers['X-Request-ID'],destinationsFenced:0}});
   return reply(plan,202);
  }});
  try{
   await open(ui);assert.equal(button(ui,'fence').disabled,false);
   assert.match(button(ui,'fence').textContent,/隔离 SQL 目标/);
   button(ui,'fence').click();await flush();
   assert.match(feedback(ui),/审批结果未知/);
   assert.match(button(ui,'fence').textContent,/重试同一隔离审批/);
   button(ui,'fence').click();await flush();
   assert.equal(ui.fences.length,2);
   const [first,retry]=ui.fences;
   assert.deepEqual(JSON.parse(first.body),{expectedPreparation:'completed-handoff',expectedRevision:newRevision});
   assert.equal(retry.body,first.body);
   assert.equal(retry.headers['X-Request-ID'],first.headers['X-Request-ID']);
   assert.match(reservation(ui).textContent,/等待目标隔离.*Worker 保持停止/s);
   assert.ok(button(ui,'fence').closest('[hidden]'));assert.ok(button(ui,'fence').disabled);
   assertReadOnly(ui);
  }finally{ui.dom.window.close();}
 });

 test('handoff refreshes target fencing and preserves the parked read-only controls',async()=>{
  let plan=completed('FENCE_PENDING',{adoptedEvidence:{validationReports:2,closedReceipts:5},
   targetFence:{requestId:'approved-fence',destinationsFenced:0}});
  const ui=fixture({taskState:transferredTask,readPreparation:()=>reply(plan)});
  try{
   await open(ui);assert.match(reservation(ui).textContent,/等待目标隔离.*已隔离的目标数0/s);assertReadOnly(ui);
   plan={...plan,state:'TARGETS_FENCED',targetFence:{...plan.targetFence,destinationsFenced:2}};
   button(ui,'reload').click();await flush();
   assert.match(reservation(ui).textContent,/目标已隔离.*保持停止.*校验报告2.*已隔离的目标数2/s);assertReadOnly(ui);
   plan={...plan,targetFence:{...plan.targetFence,destinationsFenced:9}};
   button(ui,'reload').click();await flush();
   assert.match(feedback(ui),/回执无法核验/);assert.ok(reservation(ui).hidden);
  }finally{ui.dom.window.close();}
 });

 test('handoff starts the candidate once and keeps the receipt visible after later task revisions',async()=>{
  const targetFence={requestId:'approved-fence',destinationsFenced:2};
  let plan=completed('TARGETS_FENCED',{workerStartAvailable:true,targetFence,
   adoptedEvidence:{validationReports:2,closedReceipts:5}});
  let calls=0;
  const ui=fixture({taskState:transferredTask,readPreparation:()=>reply(plan),start:options=>{
   if(++calls===1)throw new TypeError('disconnected');
   plan={...plan,state:'START_PENDING',workerStartAvailable:undefined,workerStart:{requestId:options.headers['X-Request-ID']}};
   return reply(plan,202);
  }});
  try{
   await open(ui);
   const start=button(ui,'fence');
   assert.equal(start.dataset.handoffAction,'start-worker');
   assert.match(start.textContent,/启动候选 Worker/);
   start.click();await flush();
   assert.match(start.textContent,/重试同一启动审批/);
   start.click();await flush();
   assert.equal(ui.starts.length,2);assert.equal(ui.fences.length,0);
   assert.deepEqual(ui.starts[0],ui.starts[1]);
   assert.deepEqual(JSON.parse(ui.starts[1].body),{expectedPreparation:'completed-handoff',expectedRevision:newRevision});
   assert.match(reservation(ui).textContent,/等待启动授权.*复核源端断点/s);
   assert.ok(start.disabled);
   plan={...plan,state:'WORKER_ADMITTED',executionAuthorized:true};
   button(ui,'reload').click();await flush();
   assert.match(reservation(ui).textContent,/已授权启动 Worker.*正在保存启动意图/s);
   ui.task.revision='00123456-0000-0000-0000-000000000000';
   plan={...plan,state:'WORKER_PROJECTED'};
   button(ui,'reload').click();await flush();
   assert.match(reservation(ui).textContent,/已保存启动意图.*实际运行状态见任务详情/s);
   assertReadOnly(ui);
   plan={...plan,workerStart:{requestId:targetFence.requestId}};
   button(ui,'reload').click();await flush();
   assert.match(feedback(ui),/回执无法核验/);assert.ok(reservation(ui).hidden);
  }finally{ui.dom.window.close();}
 });

 test('handoff shows a masked start blocker and uses a new request only when reapproval is authorized',async()=>{
  let plan=completed('START_PENDING',{targetFence:{requestId:'approved-fence',destinationsFenced:2},
   workerStart:{requestId:'prior-start-approval'},adoptedEvidence:{validationReports:2,closedReceipts:5},
   pendingProblem:{code:'HANDOFF_TARGET_FENCE_CHANGED',observedAt:'2026-11-18T09:05:00Z',secret:'private-data'}});
  const ui=fixture({taskState:transferredTask,readPreparation:()=>reply(plan),start:options=>{
   plan={...plan,workerStart:{requestId:options.headers['X-Request-ID']},workerStartAvailable:undefined,pendingProblem:undefined};
   return reply(plan,202);
  }});
  try{
   await open(ui);
   assert.match(reservation(ui).textContent,/最近一次核验受阻.*目标检查点和已签名的隔离回执不一致/s);
   assert.doesNotMatch(reservation(ui).textContent,/private-data/);
   assert.equal(reservation(ui).querySelector('time').dateTime,'2026-11-18T09:05:00Z');
   assert.ok(button(ui,'fence').disabled);
   plan={...plan,workerStartAvailable:true,pendingProblem:{...plan.pendingProblem,code:'AUTHORIZATION_CHANGED'}};
   button(ui,'reload').click();await flush();assert.match(button(ui,'fence').textContent,/重新审批启动/);
   button(ui,'fence').click();await flush();
   assert.equal(ui.starts.length,1);
   assert.notEqual(ui.starts[0].headers['X-Request-ID'],'prior-start-approval');
   assert.doesNotMatch(reservation(ui).textContent,/最近一次核验受阻/);
   assert.ok(button(ui,'fence').disabled);
  }finally{ui.dom.window.close();}
 });

 test('handoff refuses transferred status with wrong authority, adoption shape or missing original grant',async()=>{
  for(const invalid of [
   {committedRevision:revision},{committedConfigurationVersion:revision},{candidateNodeId:'offline-agent'},
   {originalNodeId:'hidden-agent'},{executionAuthorized:true},{adoptedEvidence:{validationReports:2,closedReceipts:0}},
   {activatedJournalEpoch:9007199254740992}
  ]){
   const ui=fixture({taskState:transferredTask,readPreparation:()=>reply(completed('EVIDENCE_ADOPTED',invalid))});
   try{
    await open(ui);
    assert.match(feedback(ui),/回执无法核验/);
    assert.ok(reservation(ui).hidden);
    assert.doesNotMatch(reservation(ui).textContent,/completed-handoff|hidden-agent/);
    assertReadOnly(ui);
   }finally{ui.dom.window.close();}
  }
  const ui=fixture({scope:['target-agent'],taskState:transferredTask,
   readPreparation:()=>error('FORBIDDEN',403)});
  try{
   await open(ui);
   assert.ok(reservation(ui).hidden);
   assert.doesNotMatch(dialog(ui).textContent,/original-agent/);
  }finally{ui.dom.window.close();}
 });

 test('handoff replacement reads a concurrent receipt and requires another deliberate submit with its ID',async()=>{
  let latest=snapshot('first-assessment'),attempts=0;
  const ui=fixture({read:()=>reply(latest),submit:options=>{
   if(++attempts===1){latest=snapshot('other-controller-assessment');return error('HANDOFF_ASSESSMENT_CHANGED',409);}
   const body=JSON.parse(options.body);latest=snapshot(options.headers['X-Request-ID'],body.expectedRevision);
   return reply(latest);
  }});
  try{
   await open(ui);await choices(ui);choose(ui);
   assert.match(receipt(ui).textContent,/待补证据/);
   await post(ui);
   assert.equal(ui.requests.length,1);
   assert.equal(JSON.parse(ui.requests[0].body).expectedAssessment,'first-assessment');
   assert.match(dialog(ui).textContent,/其他入口已更新评估/);
   assert.match(receipt(ui).textContent,/other-controller-assessment/);
   ui.task.revision=newRevision;ui.w.RivomeshConfiguration.render();
   assert.match(receipt(ui).textContent,/评估需更新/);
   await post(ui);
   assert.equal(ui.requests.length,2);
   assert.notEqual(ui.requests[1].headers['X-Request-ID'],ui.requests[0].headers['X-Request-ID']);
   assert.deepEqual(JSON.parse(ui.requests[1].body),{candidateNodeId:'target-agent',expectedRevision:newRevision,
    expectedAssessment:'other-controller-assessment'});
   assert.match(receipt(ui).textContent,/待补证据/);
  }finally{ui.dom.window.close();}
 });

 test('handoff control is hidden outside global administrator or current owner scope',async()=>{
  for(const options of [{role:'VIEWER'},{role:'OPERATOR'},{mode:'LOCAL'},{scope:['target-agent']}]){
   const ui=fixture(options);
   try{await login(ui);assert.equal(launcher(ui),null);}
   finally{ui.dom.window.close();}
  }
 });

 test('handoff distinguishes a missing assessment from task loss and refuses an unbound or executing receipt',async()=>{
  for(const read of [
   ()=>error('TASK_NOT_FOUND',404),
   ()=>reply({...snapshot('foreign-receipt'),taskId:'another-task'}),
   ()=>reply({...snapshot('executing-receipt'),executionAuthorized:true})
  ]){
   const ui=fixture({read});
   try{
    await open(ui);await choices(ui);choose(ui);
    assert.ok(action(ui).disabled);assert.equal(receipt(ui).textContent,'');
    assert.match(dialog(ui).textContent,/任务不存在|无法核验/);
    await post(ui);assert.equal(ui.requests.length,0);
   }finally{ui.dom.window.close();}
  }
 });
}
