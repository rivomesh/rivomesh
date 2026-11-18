import {test} from 'node:test';
import assert from 'node:assert/strict';

export function registerRecoveryTests({setup,flush,login}){
 function fixture({role='ADMIN',enabled=true,nativeState='ABSENT',phase='TARGET_APPLY',stale=false}={}){
  const id='11111111-1111-1111-1111-111111111111',hash='a'.repeat(64),planId='original_skip_1234',planHash='b'.repeat(64);
  const observation={evidenceId:id,evidenceHash:hash,verdict:'CHECKPOINT_NOT_ADVANCED',journalEpoch:4,errorGeneration:2,acknowledged:1,subscriptionId:'pg',targetFenceRequired:false};
  const task={id:'skip-job',revision:'paused-revision',formatVersion:3,desired:'PAUSED',status:{run_state:'PAUSED',epoch:4,subscriptions:[{id:'pg',state:'PAUSED',errorGeneration:stale?3:2,acknowledged:1}]},destinations:[{id:'pg',product:'POSTGRESQL'}],operations:[{id:'review-request',action:'FAILURE_REVIEW',status:'SUCCEEDED',failureEvidenceId:id,result:{checkpointObservation:observation}},{id:'failed-skip',action:'SKIP_TRANSACTION',status:'FAILED',skipPlanId:planId}]};
  const ui=setup([task],role,{authenticated:false,recoveryReview:true,recoveryCommit:true,recoverySkip:enabled}),fallback=ui.w.fetch;
  ui.w.fetch=async(path,options={})=>{
   let data;if(path.includes('/failure-evidence/'))data={id,hash,failurePhase:phase,subscriptionId:'pg',errorGeneration:2,rawEvents:1};
   else if(path.includes('/recovery-plans/'+planId)){ui.calls.push({path,...options});data={planId,planHash,state:'PREPARED',target:'pg',errorGeneration:2,evidenceId:id,reason:'<svg onload=alert(1)>',...(path.endsWith('/inspect')?{nativeProof:{state:nativeState}}:{})};}
   else return fallback(path,options);
   return{ok:true,status:200,json:async()=>data};
  };return{...ui,task,id,hash,planId,planHash};
 }
 const clickLabel=(ui,key,root=ui.d)=>{const button=root.querySelector('[data-recovery-label="'+key+'"]');assert.ok(button,key);button.click();};
 const enterReason=(ui,root,value='Reviewed transaction and destination')=>{const input=root.querySelector('textarea');assert.ok(input);input.value=value;input.dispatchEvent(new ui.w.Event('input'));};
 async function openReview(ui){await login(ui);[...ui.d.querySelectorAll('.history button')].find(b=>b.textContent==='查看事务证据').click();await flush();}
 async function inspect(ui){await login(ui);await ui.w.RivomeshRecovery.openPlan(ui.task,ui.planId);clickLabel(ui,'recoveryInspect');await flush();}

 test('skip approval requires explicit loss acceptance and reason,uses the current revision,and never resumes',async()=>{
  const ui=fixture();try{
   await openReview(ui);const panel=ui.d.querySelector('.recovery-skip-approval'),button=panel.querySelector('[data-recovery-action="SKIP_TRANSACTION"]');assert.ok(button.disabled);enterReason(ui,panel);assert.ok(button.disabled);const checkbox=panel.querySelector('input[type=checkbox]');checkbox.checked=true;checkbox.dispatchEvent(new ui.w.Event('change'));assert.equal(button.disabled,false);ui.task.revision='latest-paused-revision';button.click();await flush();
   const actions=ui.calls.filter(c=>c.path.endsWith('/actions'));assert.equal(actions.length,1);assert.deepEqual(JSON.parse(actions[0].body),{action:'SKIP_TRANSACTION',expectedRevision:'latest-paused-revision',subscriptionId:'pg',errorCount:2,skip:{evidenceId:ui.id,evidenceHash:ui.hash,reason:'Reviewed transaction and destination',acceptDataLoss:true}});assert.ok(actions[0].headers['X-Request-ID']);assert.equal(ui.d.querySelector('.failure-review-dialog'),null);
  }finally{ui.dom.window.close();}
 });
 test('skip approval is hidden for disabled capabilities,snapshot faults and stale failure generations',async()=>{
  for(const options of [{enabled:false},{phase:'TARGET_SNAPSHOT'},{stale:true}]){const ui=fixture(options);try{await openReview(ui);assert.equal(ui.d.querySelector('[data-recovery-action="SKIP_TRANSACTION"]'),null);assert.equal(ui.calls.filter(c=>c.path.endsWith('/actions')).length,0);}finally{ui.dom.window.close();}}
 });
 test('opening a pending plan never probes or mutates the destination automatically;committed proof offers only recovery',async()=>{
  const ui=fixture({nativeState:'COMMITTED'});try{
   await login(ui);await ui.w.RivomeshRecovery.openPlan(ui.task,ui.planId);assert.equal(ui.calls.filter(c=>c.path.endsWith('/inspect')).length,0);assert.equal(ui.d.querySelector('[data-recovery-action]'),null);clickLabel(ui,'recoveryInspect');await flush();const dialog=ui.d.querySelector('.recovery-dialog');assert.equal(dialog.querySelectorAll('[data-recovery-action]').length,1);const button=dialog.querySelector('[data-recovery-action="SKIP_RECOVER"]');assert.ok(button);assert.equal(dialog.querySelector('input[type=checkbox]'),null);enterReason(ui,dialog);button.click();await flush();const body=JSON.parse(ui.calls.find(c=>c.path.endsWith('/actions')).body);assert.equal(body.action,'SKIP_RECOVER');assert.deepEqual(body.skip,{planId:ui.planId,planHash:ui.planHash,reason:'Reviewed transaction and destination',acceptDataLoss:false});
  }finally{ui.dom.window.close();}
 });
 test('absence proof offers separately authorized retry or cancellation and binds the original plan hash',async()=>{
  for(const action of ['SKIP_RETRY','SKIP_CANCEL']){const ui=fixture();try{
   await inspect(ui);const panel=ui.d.querySelector(action==='SKIP_RETRY'?'.recovery-retry':'.recovery-cancel'),button=panel.querySelector('[data-recovery-action]');enterReason(ui,panel);if(action==='SKIP_RETRY'){assert.ok(button.disabled);const check=panel.querySelector('input');check.checked=true;check.dispatchEvent(new ui.w.Event('change'));}else assert.equal(panel.querySelector('input'),null);assert.equal(button.disabled,false);button.click();await flush();const actions=ui.calls.filter(c=>c.path.endsWith('/actions'));assert.equal(actions.length,1);assert.deepEqual(JSON.parse(actions[0].body).skip,{planId:ui.planId,planHash:ui.planHash,reason:'Reviewed transaction and destination',acceptDataLoss:action==='SKIP_RETRY'});assert.equal(JSON.parse(actions[0].body).action,action);
  }finally{ui.dom.window.close();}}
 });
 test('unknown native proof never offers mutation buttons and historical plan values are escaped',async()=>{
  const ui=fixture({nativeState:'UNKNOWN'});try{await inspect(ui);const dialog=ui.d.querySelector('.recovery-dialog');assert.match(dialog.textContent,/无法证明目标是否已经提交/);assert.match(dialog.textContent,/<svg onload/);assert.equal(dialog.querySelectorAll('svg,script,[data-recovery-action]').length,0);}finally{ui.dom.window.close();}
 });
 test('gap warnings are visible to readers even when skip capability is disabled',async()=>{
  for(const role of ['VIEWER','OPERATOR']){const ui=fixture({role,enabled:false});ui.task.status.hasGaps=true;try{await login(ui);assert.match(ui.d.querySelector('.data-gap-warning').textContent,/不代表目标与源数据一致/);assert.equal(ui.d.querySelector('.recovery-ledger'),null);assert.equal(ui.d.querySelector('[data-recovery-action]'),null);}finally{ui.dom.window.close();}}
 });
 test('gap pages are bounded and preserve cursors beyond JavaScript integer precision',async()=>{
  const ui=fixture(),fallback=ui.w.fetch,cursor='9007199254740993';ui.w.fetch=async(path,options={})=>{if(!path.endsWith('/gaps'))return fallback(path,options);ui.calls.push({path,...options});const request=JSON.parse(options.body);return{ok:true,status:200,json:async()=>({items:[{sequence:cursor,planId:ui.planId,outcome:'SKIPPED'}],next:request.after==='0'?cursor:null})};};
  try{await login(ui);clickLabel(ui,'recoveryGapsTitle');await flush();assert.match(ui.d.querySelector('.recovery-dialog').textContent,new RegExp(cursor));clickLabel(ui,'recoveryNext',ui.d.querySelector('.recovery-dialog'));await flush();const pages=ui.calls.filter(c=>c.path.endsWith('/gaps')).map(c=>JSON.parse(c.body));assert.deepEqual(pages,[{subscriptionId:'pg',after:'0',limit:20},{subscriptionId:'pg',after:cursor,limit:20}]);}finally{ui.dom.window.close();}
 });
 test('logout removes recovery dialogs and discards a late native proof response',async()=>{
  const ui=fixture(),fallback=ui.w.fetch;let release;ui.w.fetch=(path,options={})=>path.endsWith('/inspect')?new Promise(resolve=>{release=()=>resolve({ok:true,status:200,json:async()=>({planId:ui.planId,planHash:ui.planHash,state:'PREPARED',target:'pg',errorGeneration:2,nativeProof:{state:'COMMITTED'}})});}):fallback(path,options);
  try{await login(ui);await ui.w.RivomeshRecovery.openPlan(ui.task,ui.planId);clickLabel(ui,'recoveryInspect');await flush();assert.ok(release);ui.d.getElementById('logout').click();await flush();release();await flush();assert.equal(ui.d.querySelector('.recovery-dialog'),null);assert.equal(ui.d.querySelector('[data-recovery-action]'),null);assert.equal(ui.calls.filter(c=>c.path.endsWith('/actions')).length,0);}finally{ui.dom.window.close();}
 });
 test('language switching updates skip approval inside an already open evidence dialog',async()=>{
  const ui=fixture();try{await openReview(ui);assert.match(ui.d.querySelector('.recovery-skip-approval').textContent,/按完整事务跳过/);ui.d.getElementById('language').click();assert.match(ui.d.querySelector('.recovery-skip-approval').textContent,/Skip a complete transaction/);assert.match(ui.d.querySelector('.recovery-skip-approval').textContent,/accept the resulting data gap/);}finally{ui.dom.window.close();}
 });
 test('a source decode failure gives source guidance without destination skip controls',async()=>{
  const ui=fixture();
  ui.task.operations=[];
  ui.task.status.run_state='ERROR';
  ui.task.status.error_stage='SOURCE_DECODE';
  ui.task.status.error_code='BINLOG_EVENT_DECODE_FAILED';
  ui.task.status.subscriptions[0].state='RUNNING';
  try{
   await login(ui);
   const diagnostic=ui.d.querySelector('.pipeline-failure');
   assert.match(diagnostic.textContent,/解码 binlog/);
   assert.match(diagnostic.textContent,/行镜像/);
   assert.ok(!ui.d.querySelector('[data-recovery-action="SKIP_TRANSACTION"]'));
  }finally{ui.dom.window.close();}
 });
 test('cross-node cancellation binds the request and hides unavailable or completed controls',async()=>{
  for(const options of [{},{role:'OPERATOR'},{enabled:false}]){
   const ui=fixture(options);
   const original={id:'correction_original_123',action:'CORRECTION_EXECUTE',status:'ACCEPTED'};
   ui.task.operations.push(original,{id:'completed_plan_123',action:'COMPENSATION_PLAN',status:'SUCCEEDED'});
   try{
    await login(ui);
    const selector='[data-recovery-action="RECOVERY_CANCEL"]',button=ui.d.querySelector(selector);
    if(options.role||options.enabled===false){assert.equal(button,null);continue;}
    assert.equal(ui.d.querySelectorAll(selector).length,1);
    ui.task.revision='current-shared-revision';
    button.click();await flush();
    const [call]=ui.calls.filter(value=>value.path.endsWith('/actions'));
    assert.ok(call.headers['X-Request-ID']);
    assert.deepEqual(JSON.parse(call.body),{action:'RECOVERY_CANCEL',expectedRevision:'current-shared-revision',
     subscriptionId:null,errorCount:-1,parentRequestId:original.id});
    ui.task.operations.push({id:'cancellation_123',action:'RECOVERY_CANCEL',status:'PENDING',parentRequestId:original.id});
    await ui.w.RivomeshConfiguration.refresh();
    assert.ok(ui.d.querySelector(selector).disabled);
   }finally{ui.dom.window.close();}
  }
 });
 test('compensation preview and approval bind failure evidence and current task revision',async()=>{
  const ui=fixture();try{
   await openReview(ui);
   const button=ui.d.querySelector('[data-recovery-action="COMPENSATION_PLAN"]');
   assert.ok(button);button.click();await flush();
   const first=ui.calls.find(call=>call.path.endsWith('/actions'));
   assert.deepEqual(JSON.parse(first.body).compensation,{evidenceId:ui.id,evidenceHash:ui.hash});
   ui.task.operations.push({action:'COMPENSATION_PLAN',status:'SUCCEEDED',result:{code:'COMPENSATION_PLAN_READY',hash:ui.planHash,evidenceId:ui.id,evidenceHash:ui.hash,subscriptionId:'pg',errorGeneration:2,sequence:'9007199254740993',tables:{one:{database:'sales',table:'<img src=x>',identities:1,deletes:1,inserts:0}}}});
   await ui.w.RivomeshConfiguration.refresh();
   clickLabel(ui,'compensationReview');
   const dialog=ui.d.querySelector('.recovery-dialog');
   assert.equal(dialog.querySelector('img'),null);assert.match(dialog.textContent,/9007199254740993/);
   const apply=dialog.querySelector('[data-recovery-action="COMPENSATION_EXECUTE"]');
   enterReason(ui,dialog);assert.equal(apply.disabled,true);
   const check=dialog.querySelector('input[type=checkbox]');
   check.checked=true;check.dispatchEvent(new ui.w.Event('change'));
   ui.task.revision='reviewed-new-revision';
   apply.click();await flush();
   const requests=ui.calls.filter(call=>call.path.endsWith('/actions')).map(call=>JSON.parse(call.body));
   assert.equal(requests.length,2);
   assert.deepEqual(requests[1],{action:'COMPENSATION_EXECUTE',expectedRevision:'reviewed-new-revision',subscriptionId:'pg',errorCount:2,compensation:{evidenceId:ui.id,evidenceHash:ui.hash,approvedHash:ui.planHash,reason:'Reviewed transaction and destination'}});
  }finally{ui.dom.window.close();}
 });

 function correctionFixture(){
  const ui=fixture(),fallback=ui.w.fetch,tableId='a'.repeat(64);
  ui.w.fetch=async(path,options={})=>{
   if(!path.includes('/corrections/'))return fallback(path,options);
   ui.calls.push({path,...options});
   const data=path.endsWith('/schema')?{tables:[
    {id:tableId,name:'sales.items',keys:['id'],columns:[
     {name:'id',type:'UINT64',nullable:false},{name:'amount',type:'DECIMAL',nullable:true}
    ]},
    {id:'b'.repeat(64),name:'sales.duplicates',keys:[],columns:[{name:'amount',type:'DECIMAL',nullable:true}]}
   ]}:{code:'CORRECTION_PLAN_READY',evidenceId:ui.id,evidenceHash:ui.hash,subscriptionId:'pg',errorGeneration:2,hash:ui.planHash,
    rows:[{table:'sales.items',action:'INSERT',before:null,after:{id:'9007199254740993',amount:'<img src=x>'},changed:true}]};
   return {ok:true,status:200,json:async()=>data};
  };
  return {...ui,tableId};
 }
 test('correction form preserves exact textual IDs and NULL through preview and approved execution',async()=>{
  const ui=correctionFixture();
  try{
   await openReview(ui);clickLabel(ui,'correctionOpen');await flush();
   const panel=ui.d.querySelector('.recovery-dialog');
   panel.querySelector('input[aria-label="id"]').value='9007199254740993';
   const state=[...panel.querySelectorAll('select')].find(select=>select.getAttribute('aria-label')?.startsWith('amount '));
   state.value='null';state.dispatchEvent(new ui.w.Event('change'));
   clickLabel(ui,'correctionAdd',panel);clickLabel(ui,'correctionPreview',panel);await flush();
   const edit={tableId:ui.tableId,action:'INSERT',key:{},values:{id:'9007199254740993',amount:null}};
   const preview=ui.calls.find(call=>call.path.endsWith('/corrections/preview'));
   assert.deepEqual(JSON.parse(preview.body).correction.batch.edits,[edit]);
   assert.match(panel.textContent,/9007199254740993/);
   assert.match(panel.textContent,/<img src=x>/);
   assert.equal(panel.querySelector('img'),null);
   const apply=panel.querySelector('[data-recovery-action="CORRECTION_EXECUTE"]');
   enterReason(ui,panel);assert.ok(apply.disabled);
   const check=panel.querySelector('input[type=checkbox]');check.checked=true;check.dispatchEvent(new ui.w.Event('change'));
   apply.click();await flush();
   const body=JSON.parse(ui.calls.find(call=>call.path.endsWith('/actions')).body);
   assert.deepEqual(body.correction,{evidenceId:ui.id,evidenceHash:ui.hash,batch:{edits:[edit]},approvedHash:ui.planHash,reason:'Reviewed transaction and destination'});
   assert.equal(body.action,'CORRECTION_EXECUTE');
   assert.equal(body.expectedRevision,'paused-revision');
  }finally{ui.dom.window.close();}
 });
 test('correction import sends the original JSON and an edited draft invalidates stale approval',async()=>{
  const ui=correctionFixture();
  try{
   await openReview(ui);clickLabel(ui,'correctionOpen');await flush();
   const panel=ui.d.querySelector('.recovery-dialog'),mode=panel.querySelector('select');
   mode.value='import';mode.dispatchEvent(new ui.w.Event('change'));
   const input=panel.querySelector('textarea'),raw='{"edits":[{"tableId":"'+ui.tableId+'","action":"DELETE","key":{"id":"9007199254740993"}}]}';
   input.value=raw;input.dispatchEvent(new ui.w.Event('input'));
   clickLabel(ui,'correctionPreview',panel);await flush();
   assert.equal(JSON.parse(ui.calls.find(call=>call.path.endsWith('/corrections/preview')).body).correction.importJson,raw);
   assert.ok(panel.querySelector('[data-recovery-action="CORRECTION_EXECUTE"]'));
   input.value+=' ';input.dispatchEvent(new ui.w.Event('input'));
   assert.equal(panel.querySelector('[data-recovery-action="CORRECTION_EXECUTE"]'),null);
   assert.equal(ui.calls.filter(call=>call.path.endsWith('/actions')).length,0);
  }finally{ui.dom.window.close();}
 });
 test('keyless form requires an occurrence and shows only update and delete',async()=>{
  const ui=correctionFixture();
  try{
   await openReview(ui);clickLabel(ui,'correctionOpen');await flush();
   const panel=ui.d.querySelector('.recovery-dialog'),[,table,action]=panel.querySelectorAll('select');
   table.value='b'.repeat(64);table.dispatchEvent(new ui.w.Event('change'));
   assert.deepEqual([...action.options].map(option=>option.value),['UPDATE','DELETE']);
   clickLabel(ui,'correctionAdd',panel);
   assert.match(panel.textContent,/64 位 ROW_ID/);
  }finally{ui.dom.window.close();}
 });

}
