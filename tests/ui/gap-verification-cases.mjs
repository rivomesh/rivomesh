import {test} from 'node:test';
import assert from 'node:assert/strict';
export function registerGapVerificationTests({setup,flush,login}){
 function fixture({role='ADMIN',enabled=true,verified=false}={}){
  const reportId='11111111-1111-1111-1111-111111111111',id='gap_plan_12345678',hash='b'.repeat(64),table='c'.repeat(64),gap='original_skip_1234';
  const task={id:'gap-job',revision:'paused-revision',formatVersion:3,desired:'PAUSED',status:{run_state:'PAUSED',epoch:5,errorGeneration:4,hasGaps:true,hasOpenGaps:!verified,openGapCount:verified?0:1,verifiedGapCount:verified?1:0,subscriptions:[{id:'pg',state:'PAUSED',errorGeneration:2}]},destinations:[{id:'pg',product:'POSTGRESQL'}],operations:[]};
  const ui=setup([task],role,{authenticated:false,recoverySkip:enabled}),fallback=ui.w.fetch,report={id:reportId,state:'COMPLETE',code:'VALIDATION_MATCH',results:{['pg/'+table]:{target:'pg',tableId:table}}};
  const response={id,hash,state:verified?'VERIFIED_AT_COMMON_CUT':'PREPARED',sequence:'9007199254740993',plan:{id,subscriptionId:'pg',reportId,epoch:5,sequence:1,errorGeneration:4,createdBy:'planner',reason:'<svg onload=alert(1)>',maximumMillis:300000,gaps:{[gap]:{}},tables:{[table]:{database:'sales',name:'<img src=x>',rows:10,sourceRoot:'d'.repeat(64)}}},receipt:verified?{outcome:'VERIFIED_AT_COMMON_CUT',verifiedAt:'2026-11-17T14:00:00Z',executedBy:'approver'}:null};
  ui.w.fetch=async(path,options={})=>{
   if(path.includes('/gap-verifications/')){ui.calls.push({path,...options});return{ok:true,status:200,json:async()=>response};}
   if(path.endsWith('/gaps')){ui.calls.push({path,...options});return{ok:true,status:200,json:async()=>({items:[{planId:gap,sequence:'9007199254740993',state:'OPEN'},{planId:'already_closed_skip',sequence:'9007199254740994',state:'VERIFIED_AT_COMMON_CUT',verificationId:id}],next:null})};}
   return fallback(path,options);
  };return{...ui,task,report,id,hash,gap,response};
 }
 const reason=(ui,root)=>{const input=root.querySelector('textarea');input.value='Reviewed metadata and target-exclusive writing';input.dispatchEvent(new ui.w.Event('input'));};
 test('gap planning requires selected open gaps and reason and preserves large sequence text',async()=>{
  const ui=fixture();try{await login(ui);await ui.w.RivomeshRecovery.openVerificationScope(ui.task,ui.report,'pg');const d=ui.d.querySelector('.recovery-dialog'),button=d.querySelector('[data-recovery-action="GAP_VERIFY_PLAN"]');assert.ok(button.disabled);assert.match(d.textContent,/9007199254740993/);assert.equal(d.querySelectorAll('[data-gap-selection]').length,1);reason(ui,d);assert.ok(button.disabled);const check=d.querySelector('[data-gap-selection]');check.checked=true;check.dispatchEvent(new ui.w.Event('change'));assert.equal(button.disabled,false);ui.task.revision='latest-revision';button.click();await flush();const call=ui.calls.find(c=>c.path.endsWith('/actions'));assert.deepEqual(JSON.parse(call.body),{action:'GAP_VERIFY_PLAN',expectedRevision:'latest-revision',subscriptionId:'pg',errorCount:4,gap:{reportId:ui.report.id,gaps:[ui.gap],reason:'Reviewed metadata and target-exclusive writing',maximumMillis:300000}});assert.ok(call.headers['X-Request-ID']);}finally{ui.dom.window.close();}
 });
 test('gap execution requires approved hash and explicit review and keeps unsafe names as text',async()=>{
  const ui=fixture();try{await login(ui);await ui.w.RivomeshRecovery.openVerificationPlan(ui.task,ui.id);const d=ui.d.querySelector('.recovery-dialog');assert.match(d.textContent,/9007199254740993/);assert.equal(d.querySelectorAll('svg,img').length,0);assert.match(d.textContent,/<img src=x>/);assert.equal(ui.calls.filter(c=>c.path.endsWith('/actions')).length,0);const button=d.querySelector('[data-recovery-action="GAP_VERIFY_EXECUTE"]');reason(ui,d);assert.ok(button.disabled);const check=d.querySelector('input[type=checkbox]');check.checked=true;check.dispatchEvent(new ui.w.Event('change'));button.click();await flush();const actions=ui.calls.filter(c=>c.path.endsWith('/actions'));assert.equal(actions.length,1);assert.deepEqual(JSON.parse(actions[0].body).gap,{planId:ui.id,approvedHash:ui.hash,reason:'Reviewed metadata and target-exclusive writing'});assert.equal(JSON.parse(actions[0].body).action,'GAP_VERIFY_EXECUTE');}finally{ui.dom.window.close();}
 });
 test('verified history has a distinct warning and receipt review offers no execution controls',async()=>{
  const ui=fixture({verified:true});try{await login(ui);assert.match(ui.d.querySelector('.data-gap-warning').textContent,/已核验的跳过历史/);await ui.w.RivomeshRecovery.openVerificationPlan(ui.task,ui.id);assert.match(ui.d.querySelector('.recovery-dialog').textContent,/approver/);assert.equal(ui.d.querySelector('[data-recovery-action]'),null);}finally{ui.dom.window.close();}
 });
 test('only complete matching reports and authorized supported targets expose verification',async()=>{
  for(const options of [{role:'VIEWER'},{enabled:false},{}]){const ui=fixture(options);try{await login(ui);const root=ui.d.createElement('div');ui.w.RivomeshRecovery.renderVerificationFromReport(root,ui.task,ui.report);assert.equal(root.querySelectorAll('button').length,options.role==='VIEWER'||options.enabled===false?0:1);root.replaceChildren();ui.w.RivomeshRecovery.renderVerificationFromReport(root,ui.task,{...ui.report,code:'VALIDATION_DIFFERENCES'});assert.equal(root.querySelectorAll('button').length,0);}finally{ui.dom.window.close();}}
 });
 test('uncertain submission retries the identical request and revision without changing approval',async()=>{
  const ui=fixture(),fallback=ui.w.fetch;let tries=0;ui.w.fetch=async(path,options={})=>{if(path.endsWith('/actions')){ui.calls.push({path,...options});if(++tries===1)throw new Error('response lost');return{ok:true,status:202,json:async()=>ui.task};}return fallback(path,options);};
  try{await login(ui);await ui.w.RivomeshRecovery.openVerificationPlan(ui.task,ui.id);const d=ui.d.querySelector('.recovery-dialog');reason(ui,d);const check=d.querySelector('input[type=checkbox]');check.checked=true;check.dispatchEvent(new ui.w.Event('change'));const button=d.querySelector('[data-recovery-action]');button.click();await flush();assert.match(d.textContent,/response lost/);assert.ok(d.querySelector('textarea').disabled);ui.task.revision='changed-after-acceptance';button.click();await flush();const calls=ui.calls.filter(c=>c.path.endsWith('/actions'));assert.equal(calls.length,2);assert.equal(calls[0].body,calls[1].body);assert.equal(calls[0].headers['X-Request-ID'],calls[1].headers['X-Request-ID']);}finally{ui.dom.window.close();}
 });
 test('logout discards a late verification-plan response and removes approvals',async()=>{
  const ui=fixture(),fallback=ui.w.fetch;let release;ui.w.fetch=(path,options={})=>path.includes('/gap-verifications/')?new Promise(resolve=>{release=()=>resolve({ok:true,status:200,json:async()=>ui.response});}):fallback(path,options);
  try{await login(ui);const reading=ui.w.RivomeshRecovery.openVerificationPlan(ui.task,ui.id);await flush();ui.d.getElementById('logout').click();await flush();release();await reading;assert.equal(ui.d.querySelector('.recovery-dialog'),null);assert.equal(ui.d.querySelector('[data-recovery-action]'),null);}finally{ui.dom.window.close();}
 });
 test('progress keeps exact coverage separate from the applied prefix and broker completion',async()=>{
  const ui=fixture();ui.task.status.captured_sequence='9007199254740995';ui.task.status.subscriptions=[{id:'pg',state:'RUNNING',safeCoveredSequence:'9007199254740993',contiguousDeliveredSequence:'9',firstSkippedSequence:'10',progressSemantics:'SQL_COMMITTED'},{id:'mq',state:'RUNNING',safeCoveredSequence:'9007199254740994',contiguousDeliveredSequence:'9007199254740994',firstSkippedSequence:null,progressSemantics:'BROKER_ACCEPTED'}];
  try{await login(ui);const cards=ui.d.querySelectorAll('.subscription-list .subscription-card');assert.match(cards[0].textContent,/安全覆盖序号.*9007199254740993/);assert.match(cards[0].textContent,/连续已应用序号.*9/);assert.match(cards[0].textContent,/首次显式跳过序号.*10/);assert.match(cards[1].textContent,/不代表下游消费者已完成处理/);}finally{ui.dom.window.close();}
 });

}
