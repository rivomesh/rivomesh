import {test} from 'node:test';
import assert from 'node:assert/strict';

export function registerHandoffTests({setup,flush,login}){
 const revision='a1234567-aaaa-4abc-8abc-0123456789ab';
 const newRevision='b1234567-aaaa-4abc-8abc-0123456789ab';
 const route='/api/tasks/handoff-task/handoff';
 const reply=(data,status=200)=>({ok:status<400,status,json:async()=>data});
 const error=(code,status)=>reply({code},status);
 const agent=(id,role='AGENT',state='HEALTHY')=>({id,role,state,taskCapacity:2,runningCapacity:1});
 const snapshot=(id,taskRevision=revision,state='EVIDENCE_REQUIRED')=>({
  taskId:'handoff-task',id,taskRevision,state,originalNodeId:'original-agent',candidateNodeId:'target-agent',
  candidateState:'HEALTHY',candidateAssignedTasks:0,candidateTaskCapacity:2,candidateWorkerCapacity:1,
  assessedAt:'2026-11-18T09:00:00Z',executionAuthorized:false,
  blockers:[...(state==='BLOCKED'?['ORIGINAL_WORKER_NOT_DRAINED']:[]),'CANDIDATE_ACCESS_UNVERIFIED','ORIGINAL_RECOVERY_EVIDENCE_UNVERIFIED']
 });
 function fixture({role='ADMIN',scope=['original-agent','target-agent','offline-agent'],mode='GLOBAL',read,submit}={}){
  const task={id:'handoff-task',revision,ownerNodeId:'original-agent',formatVersion:3,
   workerState:'STOPPED',desired:'STOPPED',status:{},operations:[],destinations:[]};
  const ui=setup([task],role,{authenticated:false,taskControlScope:mode,nodeRole:'CONTROLLER',nodeIds:scope}),fallback=ui.w.fetch;
  const requests=[];
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
   return fallback(path,options);
  };
  return{...ui,task,requests};
 }
 const launcher=ui=>ui.d.querySelector('[data-handoff-task]');
 const dialog=ui=>ui.d.querySelector('.handoff-dialog');
 const field=ui=>dialog(ui).querySelector('select');
 const action=ui=>dialog(ui).querySelector('[data-handoff-assess]');
 const receipt=ui=>dialog(ui).querySelector('[data-handoff-receipt]');
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
