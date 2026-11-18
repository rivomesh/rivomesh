import {test} from 'node:test';
import assert from 'node:assert/strict';

export function registerHandoffTests({setup,flush,login}){
 const revision='a1234567-aaaa-4abc-8abc-0123456789ab';
 const newRevision='b1234567-aaaa-4abc-8abc-0123456789ab';
 const route='/api/tasks/handoff-task/handoff';
 const preparationRoute=route+'/preparation';
 const digest='a'.repeat(64);
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
   candidateNodeId:'target-agent',executionAuthorized:false,evidenceSha256:digest,createdAt:'2026-11-18T09:00:00Z',
   expiresAt:expiration,reservedTaskSlots:slots,reservedWorkerSlots:slots,...(releaseId?{releaseId}:{})};
 }
 function fixture({role='ADMIN',scope=['original-agent','target-agent','offline-agent'],mode='GLOBAL',
  read,submit,probe,readPreparation,prepare,release}={}){
  const task={id:'handoff-task',revision,ownerNodeId:'original-agent',formatVersion:3,
   workerState:'STOPPED',desired:'STOPPED',status:{},operations:[],destinations:[]};
  const ui=setup([task],role,{authenticated:false,taskControlScope:mode,nodeRole:'CONTROLLER',nodeIds:scope}),fallback=ui.w.fetch;
  const requests=[],reserves=[],releases=[],probes=[];
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
   return fallback(path,options);
  };
  return{...ui,task,requests,reserves,releases,probes};
 }
 const launcher=ui=>ui.d.querySelector('[data-handoff-task]');
 const dialog=ui=>ui.d.querySelector('.handoff-dialog');
 const field=ui=>dialog(ui).querySelector('select');
 const action=ui=>dialog(ui).querySelector('[data-handoff-assess]');
 const receipt=ui=>dialog(ui).querySelector('[data-handoff-receipt]');
 const reservation=ui=>dialog(ui).querySelector('[data-handoff-preparation]');
 const button=(ui,action)=>dialog(ui).querySelector('[data-handoff-'+action+']');
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
