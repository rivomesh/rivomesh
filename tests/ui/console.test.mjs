import {registerGapVerificationTests} from './gap-verification-cases.mjs';
import {registerHandoffTests} from './handoff-cases.mjs';
import {registerRecoveryTests} from './recovery-cases.mjs';
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {JSDOM} from 'jsdom';

const root=new URL('../../sync-app/src/main/resources/console/',import.meta.url);
const html=await readFile(new URL('index.html',root),'utf8');
const script=await readFile(new URL('app.js',root),'utf8');
const configurationScript=await readFile(new URL('configuration-editor.js',root),'utf8');
const auditScript=await readFile(new URL('audit.js',root),'utf8');
const accountsScript=await readFile(new URL('accounts.js',root),'utf8');
const validationScript=await readFile(new URL('validation.js',root),'utf8');
const recoveryScript=await readFile(new URL('recovery.js',root),'utf8');
const handoffScript=await readFile(new URL('handoff.js',root),'utf8');
const token='test-console-token-at-least-24-bytes';
const flush=async()=>{for(let i=0;i<4;i++)await new Promise(setImmediate);};
function setup(initial=[],role='ADMIN',browser={authenticated:false}){
  const dom=new JSDOM(html,{url:'http://localhost:8080/',runScripts:'outside-only'}),w=dom.window;
  const calls=[],authCalls=[],tasks=[...initial];
  w.localStorage.setItem('rivomesh-language','zh');
  w.setInterval=()=>0;
  browser.channels??=[];
  w.BroadcastChannel=class{
    constructor(){browser.channels.push(this);}
    postMessage(data){for(const peer of browser.channels)if(peer!==this)peer.onmessage?.({data});}
    close(){browser.channels=browser.channels.filter(peer=>peer!==this);}
  };
  w.HTMLDialogElement.prototype.showModal=function(){this.setAttribute('open','');};
  w.HTMLDialogElement.prototype.close=function(){this.removeAttribute('open');};
  w.fetch=async(path,options={})=>{
    if(path==='/auth/oidc/available'){
      assert.equal(options.credentials,'omit');
      return{ok:true,status:200,json:async()=>({enabled:browser.oidc===true})};
    }
    const authRoute=path==='/api/session'||path==='/api/session/logout';
    (authRoute?authCalls:calls).push({path,...options});
    const denied=()=>({ok:false,status:401,json:async()=>({code:'UNAUTHORIZED'})});
    if(path==='/api/session'&&options.method==='POST'){
      if(options.headers.Authorization!=='Bearer '+(browser.token||token))return denied();
      browser.authenticated=true;
    }else if(!browser.authenticated)return denied();
    if(options.headers.Authorization&&path!=='/api/session')throw new Error('Long-lived credential was reused');
    if(options.method&&options.method!=='GET'&&!(path==='/api/session'&&options.method==='POST'))assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
    if(path==='/api/session/logout'){browser.authenticated=false;return{ok:true,status:200,json:async()=>({status:'SIGNED_OUT'})};}
    let data;
    if(path==='/api/session')data={id:'test-user',role,recoveryReview:Boolean(browser.recoveryReview),recoveryCommit:Boolean(browser.recoveryCommit),recoverySkip:Boolean(browser.recoverySkip),authentication:'COOKIE',csrfToken:'test-csrf',nodeId:'node-a',nodeRole:browser.nodeRole||'CONTROLLER_WORKER',taskControlScope:browser.taskControlScope||'LOCAL',nodeScope:browser.nodeIds??null,permissions:['TASK_READ',...(role!=='VIEWER'?['TASK_CONTROL']:[]),...(role==='ADMIN'?['TASK_CREATE','TASK_CONFIG','AUDIT_READ','ACCOUNT_ADMIN']:[])]};
    else if(path==='/api/capabilities')data=browser.capabilities??[{id:'MYSQL',role:'SOURCE',status:'AVAILABLE',note:''},{id:'PULSAR',role:'SINK',status:'PLANNED',note:''}];
    else if(options.method==='POST'&&path==='/api/tasks'){
      const config=JSON.parse(options.body).config;
      const task={id:config.jobId||config.pipelineId,revision:'revision-1',formatVersion:config.formatVersion||2,desired:'STOPPED',workerState:'STOPPED',sourceDatabase:config.source.database||config.databases.join(', '),sourceCluster:config.source.clusterId,targetSchema:config.target?.schema||config.targets.length+' targets',status:{},statusAvailable:true,observedAt:null,operations:[]};tasks.push(task);data=task;
    }else if(options.method==='POST')data=tasks[0];
    else data={tasks,invalidConfigs:[]};
    return{ok:true,status:200,json:async()=>data};
  };
  w.eval(script);w.eval(configurationScript);w.eval(auditScript);w.eval(accountsScript);w.eval(validationScript);w.eval(recoveryScript);w.eval(handoffScript);return{dom,w,d:w.document,calls,authCalls,browser};
}
async function login(ui){await flush();ui.d.getElementById('token').value=token;ui.d.getElementById('login-form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();}
registerHandoffTests({setup,flush,login});
function detailFixture(role='ADMIN'){
 const task={id:'details',revision:'revision',formatVersion:3,desired:'PAUSED',workerState:'PAUSED',status:{run_state:'PAUSED'},destinations:[],operations:[{id:'check',action:'VALIDATE',status:'SUCCEEDED',code:'VALIDATION_DIFFERENCES',validation:{reportId:'report'}}]},ui=setup([task],role),fallback=ui.w.fetch;
 ui.w.fetch=async(path,options={})=>{
  let data;
  if(path==='/api/tasks/details/validations/report')data={state:'COMPLETE',code:'VALIDATION_DIFFERENCES',sourceComplete:true,tables:[{id:'table',database:'db',name:'<img src=x>'}],results:{table:{target:'pg',tableId:'table',sourceRows:2,targetRows:2,missing:1,extra:1,changed:1}}};
  else if(path.endsWith('/differences')){ui.calls.push({path,...options});const body=JSON.parse(options.body);data={target:'pg',tableId:'table',columns:[{name:'note',type:'STRING'},{name:'optional',type:'STRING'}],items:body.after?[{identityHash:'c'.repeat(64),kind:'EXTRA'}]:[{identityHash:'a'.repeat(64),kind:'CHANGED'},{identityHash:'b'.repeat(64),kind:'MISSING'}],next:body.after?null:'b'.repeat(64)};}
  else if(path.endsWith('/values')){ui.calls.push({path,...options});const body=JSON.parse(options.body);data={columns:[{name:'<svg onload=alert(1)>',type:'STRING'},{name:'optional',type:'STRING'}],values:{expected:[{present:true,value:'before'},{present:true,value:null}],actual:body.identityHash.startsWith('b')?null:[{present:true,value:'<img src=x onerror=alert(1)>'},{present:true,value:'SQL NULL'}]}};}
  else return fallback(path,options);
  return{ok:true,status:200,json:async()=>data};
 };return ui;
}
test('admin reveals raw differences as text, distinguishes NULL, and sends page cursors',async()=>{
 const ui=detailFixture();try{
  await login(ui);ui.d.querySelector('[data-validation-report]').click();await flush();ui.d.querySelector('[data-validation-differences]').click();await flush();ui.d.querySelector('[data-validation-values]').click();await flush();
  const section=ui.d.querySelector('.validation-differences');assert.match(section.textContent,/<img src=x onerror=alert\(1\)>/);assert.equal(section.querySelectorAll('img,svg').length,0);assert.equal([...section.querySelectorAll('.badge')].filter(node=>node.textContent==='SQL NULL').length,1);assert.ok([...section.querySelectorAll('pre')].some(node=>node.textContent==='SQL NULL'));
  section.querySelectorAll('[data-validation-values]')[1].click();await flush();assert.match(ui.d.querySelector('.validation-differences').textContent,/不存在该行/);
  const next=[...ui.d.querySelectorAll('.validation-differences > button')].find(button=>button.textContent==='下一页');next.click();await flush();const pages=ui.calls.filter(call=>call.path.endsWith('/differences'));assert.equal(JSON.parse(pages[1].body).after,'b'.repeat(64));assert.ok(pages[1].headers['X-Request-ID']);assert.equal(ui.w.sessionStorage.length,0);
 }finally{ui.dom.window.close();}
});
test('summary readers do not receive raw-detail buttons',async()=>{
 for(const role of ['VIEWER','OPERATOR']){const ui=detailFixture(role);try{await login(ui);ui.d.querySelector('[data-validation-report]').click();await flush();assert.match(ui.d.getElementById('task-detail').textContent,/db\.<img src=x>/);assert.equal(ui.d.querySelector('[data-validation-differences]'),null);assert.equal(ui.d.querySelector('[data-validation-values]'),null);}finally{ui.dom.window.close();}}
});

test('historical validation reports can be discovered after their operations leave the task history',async()=>{
 const task={id:'archive',formatVersion:3,workerState:'STOPPED',operations:[],status:{},destinations:[]};
 const ui=setup([task],'VIEWER'),fetch=ui.w.fetch;
 const first={id:'archived-first',createdAt:'2026-11-17T07:00:00Z'};
 const second={id:'archived-second',createdAt:'2026-11-16T07:00:00Z'};
 ui.w.fetch=async(path,options)=>{
  if(path.startsWith('/api/tasks/archive/validations?')){
   const next=path.includes('&after=');
   return{ok:true,status:200,json:async()=>({reports:[next?second:first],nextCursor:next?null:'older-cursor'})};
  }
  if(path==='/api/tasks/archive/validations/'+second.id)
   return{ok:true,status:200,json:async()=>({id:second.id,state:'COMPLETE',code:'VALIDATION_MATCH',results:{}})};
  return fetch(path,options);
 };
 try{
  await login(ui);
  ui.d.querySelector('[data-validation-catalog]').click();await flush();
  assert.ok(ui.d.querySelector('[data-validation-report="'+first.id+'"]'));
  const next=[...ui.d.querySelectorAll('.validation-catalog-actions button')].find(button=>button.textContent==='下一页');
  next.click();await flush();
  ui.d.querySelector('[data-validation-report="'+second.id+'"]').click();await flush();
  assert.match(ui.d.querySelector('.validation-panel').textContent,/archived-second.*数据匹配/);
  assert.equal(ui.d.querySelector('[data-validation-values]'),null);
 }finally{ui.dom.window.close();}
});

test('opening a validation report by exact ID uses its scoped endpoint and preserves the prior directory page',async()=>{
 const task={id:'archive',formatVersion:3,workerState:'STOPPED',operations:[],status:{},destinations:[]};
 const ui=setup([task],'VIEWER'),fetch=ui.w.fetch,requests=[];
 const reportId='a4500000-1223-4abc-8abc-0123456789ab',olderId='a4500000-1223-4abc-8abc-0123456789ac';
 ui.w.fetch=async(path,options)=>{
  if(path.startsWith('/api/tasks/archive/validations?')){
   const older=path.includes('&after=');
   return{ok:true,status:200,json:async()=>({reports:[{id:older?olderId:'latest',createdAt:'2026-11-16T07:00:00Z'}],nextCursor:older?null:'older'})};
  }
  if(path.startsWith('/api/tasks/archive/validations/')){
   requests.push(path);
   return{ok:true,status:200,json:async()=>({id:reportId,state:'COMPLETE',code:'VALIDATION_MATCH',results:{}})};
  }
  return fetch(path,options);
 };
 try{
  await login(ui);ui.d.querySelector('[data-validation-catalog]').click();await flush();
  const button=name=>[...ui.d.querySelectorAll('.validation-catalog-actions button')].find(item=>item.textContent===name);
  button('下一页').click();await flush();
  assert.ok(ui.d.querySelector('[data-validation-report="'+olderId+'"]'));
  const search=ui.d.querySelector('[data-validation-search]');
  search.focus();ui.w.RivomeshConfiguration.render();
  assert(!search.isConnected);
  assert.equal(ui.d.activeElement,ui.d.querySelector('[data-validation-search]'));
  const logout=ui.d.getElementById('logout');
  logout.focus();ui.w.RivomeshConfiguration.render();
  assert.equal(ui.d.activeElement,logout);
  ui.d.querySelector('[data-validation-search]').click();
  const dialog=ui.d.querySelector('.validation-report-search'),input=dialog.querySelector('[name="reportId"]');
  const submit=()=>dialog.querySelector('form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));
  assert.equal(ui.d.activeElement,input);
  assert.equal(ui.d.getElementById(dialog.getAttribute('aria-labelledby')).textContent,'按报告 ID 打开');
  input.value='../other-task';submit();await flush();
  assert.equal(requests.length,0);assert.match(dialog.querySelector('[role="alert"]').textContent,/有效的报告 ID/);
  input.value=olderId;submit();await flush();
  assert.match(dialog.querySelector('[role="alert"]').textContent,/报告 ID 与输入不符/);
  assert.equal(ui.d.querySelector('[data-validation-report="'+reportId+'"]'),null);
  input.value=' '+reportId.toUpperCase()+' ';submit();await flush();
  assert.deepEqual(requests,[olderId,reportId].map(id=>'/api/tasks/archive/validations/'+id));
  assert.equal(ui.d.querySelector('.validation-report-search'),null);
  const card=ui.d.querySelector('[data-validation-report="'+reportId+'"]').closest('article');
  assert.equal(card.querySelector('strong').textContent,'校验报告');
  assert.equal(card.querySelector('details:not([open]) code').textContent,reportId);
  assert.match(card.textContent,/数据匹配/);assert.equal(card.querySelector('[data-validation-values]'),null);
  const disclosure=card.querySelector('details'),identifier=disclosure.querySelector('summary');
  disclosure.open=true;disclosure.dispatchEvent(new ui.w.Event('toggle'));identifier.focus();
  ui.w.RivomeshConfiguration.render();
  assert.equal(ui.d.activeElement.closest('details[open]').querySelector('code').textContent,reportId);
  button('返回历史目录').click();assert.ok(ui.d.querySelector('[data-validation-report="'+olderId+'"]'));
  button('上一页').click();await flush();assert.ok(ui.d.querySelector('[data-validation-report="latest"]'));
 }finally{ui.dom.window.close();}
});

test('the report storage view is read only and reports protection without showing unknown filenames',async()=>{
 const reportId='a4500000-1223-4abc-8abc-0123456789ab';
 const task={id:'report-storage',formatVersion:3,workerState:'STOPPED',operations:[],status:{},destinations:[]};
 const ui=setup([task]),fallback=ui.w.fetch;
 const calls=[];
 ui.w.fetch=async(path,options)=>{
  const base='/api/tasks/report-storage/validations';
  if(path.startsWith(base+'?'))return{ok:true,status:200,json:async()=>({reports:[{id:reportId}],nextCursor:null})};
  if(path===base+'/'+reportId+'/storage'){
   calls.push({path,options});
   return{ok:true,status:200,json:async()=>({id:reportId,activity:'PROTECTED_ACTIVE',observedAt:'2026-11-18T09:00:00Z',
    files:{'rows.mv.db':8192,'summary.json':1024,'private-source-name':1000}})};
  }
  return fallback(path,options);
 };
 try{
  await login(ui);
  ui.d.querySelector('[data-validation-catalog]').click();await flush();
  ui.d.querySelector('[data-validation-storage="'+reportId+'"]').click();await flush();
  const dialog=ui.d.querySelector('.validation-storage-dialog');
  assert.equal(ui.d.getElementById(dialog.getAttribute('aria-labelledby')).textContent,'报告存储检查');
  assert.match(dialog.textContent,/报告受保护/);
  assert.match(dialog.textContent,/8,192 字节/);
  assert.ok(!dialog.textContent.includes('private-source-name'));
  assert.equal(dialog.querySelectorAll('button').length,1);
  assert.equal(calls.length,1);assert.equal(calls[0].options.method,undefined);
 }finally{ui.dom.window.close();}
});

test('recovery receipt lookup reads earlier requests for the active task and withholds approval payloads',async()=>{
 const task={id:'recovery-archive',formatVersion:3,workerState:'STOPPED',operations:[],status:{},destinations:[]};
 const ui=setup([task],'ADMIN',{authenticated:false,taskControlScope:'GLOBAL',recoverySkip:false}),fallback=ui.w.fetch;
 const paths=[],id='original_correction_request';
 let mismatch=true;
 ui.w.fetch=async(path,options)=>{
  const endpoint='/api/tasks/recovery-archive/recovery-receipts/'+id;
  if(path!==endpoint)return fallback(path,options);
  paths.push(path);
  return{ok:true,status:200,json:async()=>({kind:'CONTROL_OPERATION',taskId:mismatch?'another-task':task.id,id,
   action:'CORRECTION_EXECUTE',status:'SUCCEEDED',code:'CORRECTION_COMPLETED',createdAt:'2026-11-18T09:00:00Z',
   ownerNodeId:'agent-a',actorId:'admin',subscriptionId:'warehouse',references:{failureEvidenceId:'evidence-id-001',reason:'private-reference'},
   correction:{importJson:'private values'},reason:'private reason',result:{rows:['private-row']}})};
 };
 try{
  await login(ui);
  ui.d.querySelector('[data-recovery-receipt-lookup]').click();
  const dialog=ui.d.querySelector('.recovery-dialog'),input=dialog.querySelector('[name=requestId]');
  const submit=()=>dialog.querySelector('form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));
  assert.equal(ui.d.activeElement,input);
  input.value='../other-task';submit();await flush();
  assert.equal(paths.length,0);assert.match(dialog.querySelector('[role=alert]').textContent,/有效的管理请求 ID/);
  input.value=id;submit();await flush();
  assert.match(dialog.querySelector('[role=alert]').textContent,/返回的回执与当前请求不符/);
  mismatch=false;submit();await flush();
  const result=dialog.querySelector('[data-recovery-receipt-result]');
  assert.match(result.textContent,/原始|手工修正/);
  assert.match(result.textContent,/agent-a.*warehouse.*evidence-id-001/s);
  assert.ok(!result.textContent.includes('private'));
  assert.equal(paths.length,2);
  dialog.dispatchEvent(new ui.w.Event('cancel',{cancelable:true}));
  assert(!dialog.isConnected);
 }finally{ui.dom.window.close();}
});

test('Kafka review submits a paused target and opens only the verified native receipt fields',async()=>{
 const action='REVIEW_KAFKA_RECEIPTS',id='verified_kafka_review',code='PRECEDING_KAFKA_RECEIPTS_VERIFIED';
 const task={id:'kafka-review',revision:'task-revision',formatVersion:3,workerState:'PAUSED',desired:'PAUSED',statusAvailable:true,
  status:{run_state:'PAUSED',subscriptions:[{id:'kafka',state:'PAUSED',errorGeneration:17,
   failurePhase:'TARGET_APPLY',acknowledged:6,failedSequence:7}]},destinations:[{id:'kafka',product:'KAFKA',mappings:{}}],
  operations:[{id,action,code,status:'SUCCEEDED'}]};
 const session={authenticated:false,taskControlScope:'GLOBAL',recoveryReview:true};
 const ui=setup([task],'ADMIN',session),fallback=ui.w.fetch;
 const submit='/api/tasks/'+task.id+'/actions',lookup='/api/tasks/'+task.id+'/recovery-receipts/'+id;
 let mismatchedTarget=true;
 ui.w.fetch=async(path,options)=>{
  if(path!==lookup)return fallback(path,options);
  return{ok:true,status:200,json:async()=>({kind:'CONTROL_OPERATION',taskId:task.id,id,action,code,status:'SUCCEEDED',
   subscriptionId:'kafka',kafkaReview:{status:code,target:mismatchedTarget?'other':'kafka',observedAt:'2026-11-18T09:00:00Z',
    epoch:'3',errorGeneration:'17',acknowledgedSequence:'6',failedSequence:'7',observedTopicIdentitySha256:'a'.repeat(64),
    nativeReceipts:[{namespace:'orders',offset:'31',payloadSha256:'b'.repeat(64),body:'hidden raw bytes'}]}})};
 };
 try{
  await login(ui);
  ui.d.querySelector('[data-kafka-receipt-review="kafka"]').click();await flush();
  const posted=ui.calls.find(call=>call.path===submit);
  assert.ok(posted.headers['X-Request-ID']);
  assert.deepEqual(JSON.parse(posted.body),{action,expectedRevision:'task-revision',errorCount:17,subscriptionId:'kafka'});
  ui.d.querySelector('[data-recovery-receipt-request="'+id+'"]').click();await flush();
  const dialog=ui.d.querySelector('.recovery-dialog');
  assert.match(dialog.querySelector('[role=alert]').textContent,/回执与当前请求不符/);
  mismatchedTarget=false;
  dialog.querySelector('form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();
  const result=dialog.querySelector('[data-recovery-receipt-result]');
  assert.match(result.textContent,/orders · 位点 31/);
  assert.ok(result.textContent.includes('b'.repeat(64)));
  assert.ok(!result.textContent.includes('hidden raw bytes'));
 }finally{ui.dom.window.close();}
 for(const [role,model] of [['OPERATOR',task],['ADMIN',{...task,desired:'RUNNING'}]]){
  const blocked=setup([model],role,{authenticated:false,taskControlScope:'GLOBAL',recoveryReview:true});
  try{await login(blocked);assert.equal(blocked.d.querySelector('[data-kafka-receipt-review]'),null);}
  finally{blocked.dom.window.close();}
 }
});

test('Kafka notice preview is bound to the verified receipt and displays publication blocked',async()=>{
 const id='verified_notice_review',target='kafka',code='PRECEDING_KAFKA_RECEIPTS_VERIFIED';
 const version='4d8d04a0-d50d-400f-baa6-223864edbeb5',topicId='cKtKfC5KQGq-qM1RDCb_3w';
 const task={id:'notice-preview',revision:'task-revision',formatVersion:3,workerState:'PAUSED',desired:'PAUSED',statusAvailable:true,
  status:{run_state:'PAUSED'},destinations:[{id:target,product:'KAFKA',kafkaNoticePlanningAvailable:true}],
  operations:[{id,action:'REVIEW_KAFKA_RECEIPTS',code,status:'SUCCEEDED'}]};
 const ui=setup([task],'ADMIN',{authenticated:false,taskControlScope:'GLOBAL',recoveryReview:true}),fallback=ui.w.fetch;
 const base='/api/tasks/'+task.id,requests=[];
 let publishing=true;
 let coverage={status:'INCOMPLETE',code:'KAFKA_NOTICE_CATALOG_RETAINED_REVISION_MISSING'};
 let approvalScope=null;
 const approvalRequests=[];
 const preflightRequests=[];
 let preflightAuthorizes=true,fixedHistory=false;
 const dispatches=[];
 ui.w.fetch=async(path,options)=>{
  if(path===base+'/recovery-receipts/'+id)return{ok:true,status:200,json:async()=>({kind:'CONTROL_OPERATION',taskId:task.id,
   id,action:'REVIEW_KAFKA_RECEIPTS',status:'SUCCEEDED',code,subscriptionId:target,kafkaReview:{status:code,target,
    observedAt:'2026-11-18T09:00:00Z',epoch:'3',errorGeneration:'17',acknowledgedSequence:'6',failedSequence:'7',
    observedTopicIdentitySha256:'a'.repeat(64),originalFailure:{journalGeneration:'retained'},
    nativeReceipts:[{namespace:'orders',offset:'31',payloadSha256:'b'.repeat(64)}]}})};
  if(path===base+'/kafka-notice-approvals'){
   assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
   const request={id:options.headers['X-Request-ID'],body:JSON.parse(options.body)};
   approvalRequests.push(request);
   if(approvalRequests.length===1)return{ok:false,status:503,json:async()=>({code:'CONTROL_STORE_UNAVAILABLE'})};
   return{ok:true,status:200,json:async()=>({id:request.id,taskId:task.id,status:fixedHistory?'RECORDED':'RECORDED_BLOCKED',
    executionAuthorized:false,approvalScopeSha256:approvalScope})};
  }
  if(path===base+'/kafka-notice-approvals/'+approvalRequests.at(-1)?.id+'/preflight'){
   assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
   preflightRequests.push(JSON.parse(options.body));
   return{ok:true,status:200,json:async()=>({status:fixedHistory?'SENDER_READY':'SENDER_BLOCKED',executionAuthorized:preflightAuthorizes,
    approvalRequestId:approvalRequests.at(-1).id,approvalScopeSha256:approvalScope,controlTopicId:topicId,payloadSha256:'d'.repeat(64),
    task:{taskId:task.id,revision:task.revision,configurationVersion:version},failure:{failedSequence:'7',targetId:target},
    precedingPhysicalReceipt:{status:'VERIFIED',receiptSha256:'f'.repeat(64),signature:'private display fixture'},
    blockers:fixedHistory?[]:['KAFKA_NOTICE_HISTORICAL_OWNERSHIP_UNVERIFIED','KAFKA_NOTICE_TOPIC_ID_PRODUCE_REQUIRED']})};
  }
  if(path===base+'/kafka-notice-approvals/'+approvalRequests.at(-1)?.id+'/dispatch'){
   assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
   const sent={id:options.headers['X-Request-ID'],body:JSON.parse(options.body)};
   dispatches.push(sent);
   if(dispatches.length===1)return{ok:false,status:503,json:async()=>({code:'CONTROL_STORE_UNAVAILABLE'})};
   return{ok:true,status:200,json:async()=>({kind:'KAFKA_NOTICE_DISPATCH',id:sent.id,taskId:task.id,status:'PENDING',
    approvalRequestId:approvalRequests.at(-1).id,approvalScopeSha256:approvalScope})};
  }
  if(path!==base+'/kafka-notice-plan')return fallback(path,options);
  assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
  requests.push(JSON.parse(options.body));
  return{ok:true,status:200,json:async()=>({status:'KAFKA_NOTICE_PLAN_REVIEWED',executionAuthorized:publishing,
   publicationBlockCode:fixedHistory?'KAFKA_NOTICE_APPROVAL_REQUIRED':'KAFKA_NOTICE_GLOBAL_OWNERSHIP_UNVERIFIED',approvalSha256:'c'.repeat(64),
   catalogCoverage:coverage,
   approvalScopeSha256:approvalScope,approvalBlockers:fixedHistory?[]:['KAFKA_NOTICE_HISTORICAL_OWNERSHIP_UNVERIFIED'],
   writerEntitlement:{status:'VERIFIED_AT_REVIEW',code:'KAFKA_NOTICE_WRITER_EFFECTIVE_PERMISSIONS_VERIFIED'},
   plan:{task:{taskId:task.id,revision:task.revision,configurationVersion:version},predecessorRequestId:id,payloadSha256:'d'.repeat(64),failure:{failedSequence:'7'},
    controlTopic:{targetId:target,topicName:'control.events',topicId},credential:'not sent to the DOM'}})};
 };
 try{
  await login(ui);ui.d.querySelector('[data-recovery-receipt-request="'+id+'"]').click();await flush();
  const section=ui.d.querySelector('[data-kafka-notice-preview]'),button=section.querySelector('button');
  button.click();await flush();
  assert.match(section.textContent,/回执与当前请求不符/);
  publishing=false;button.click();await flush();
  assert.deepEqual(requests,[{expectedRevision:'task-revision',predecessorRequestId:id},{expectedRevision:'task-revision',predecessorRequestId:id}]);
  assert.match(section.textContent,/发布受阻.*control.events/s);
  assert.match(section.textContent,/历史配置仍需补齐物理 Topic 归属证据/);
  assert.match(section.textContent,/已核对独立 Kafka 写入身份/);
  assert.ok(section.textContent.includes('d'.repeat(64)));
  assert.ok(!section.textContent.includes('not sent to the DOM'));
  assert.equal(section.querySelectorAll('button').length,1);
  coverage={status:'VERIFIED_AT_REVIEW',code:'KAFKA_NOTICE_CATALOG_RESTORABLE_ROUTES_VERIFIED'};
  button.click();await flush();
  assert.match(section.textContent,/已核对生效和可回退配置当前指向的 Kafka Topic/);
  assert.match(section.textContent,/发布受阻/);
  assert.equal(section.querySelectorAll('button').length,1);
  approvalScope='e'.repeat(64);
  button.click();await flush();
  const approve=section.querySelectorAll('button')[1];
  assert.match(approve.textContent,/登记条件审批/);
  approve.click();await flush();
  assert.ok(!approve.disabled);
  approve.click();await flush();
  assert.equal(approvalRequests.length,2);
  assert.deepEqual(approvalRequests[0],approvalRequests[1]);
  assert.deepEqual(approvalRequests[0].body,{expectedRevision:'task-revision',predecessorRequestId:id,
   approvalScopeSha256:approvalScope});
  assert.match(section.textContent,/审批已登记，通知仍未获准发送/);
  assert.ok(approve.disabled);
  const preflight=section.querySelector('[data-kafka-notice-preflight]'),check=preflight.querySelector('button');
  check.click();await flush();
  assert.match(preflight.textContent,/回执与当前请求不符/);
  preflightAuthorizes=false;check.click();await flush();
  assert.match(preflight.textContent,/已核实本次故障的确切前序 ACK/);
  assert.match(preflight.textContent,/历史归属仍待核实，通知继续受阻/);
  assert.match(preflight.textContent,/发送端必须按已审批的物理 Topic UUID 写入/);
  assert.ok(!preflight.textContent.includes('private display fixture'));
  assert.equal(preflight.querySelectorAll('button').length,1);
  assert.deepEqual(preflightRequests,Array(2).fill({expectedRevision:task.revision,approvalScopeSha256:approvalScope}));
  assert.deepEqual(dispatches,[]);

  fixedHistory=true;approvalScope='f'.repeat(64);coverage.fixedHistorySha256='a'.repeat(64);
  button.click();await flush();
  const fixedApproval=section.querySelectorAll('button')[1];
  assert.equal(fixedApproval.textContent,'登记审批');
  fixedApproval.click();await flush();
  const verified=section.querySelector('[data-kafka-notice-preflight]');
  assert.equal(verified.querySelectorAll('button').length,1);
  verified.querySelector('button').click();await flush();
  const send=verified.querySelectorAll('button')[1];
  assert.equal(send.textContent,'发送缺口通知');
  send.click();await flush();
  assert.ok(!send.disabled);
  send.click();await flush();
  assert.deepEqual(dispatches[0],dispatches[1]);
  assert.deepEqual(dispatches[0].body,{expectedRevision:task.revision,approvalScopeSha256:approvalScope});
  assert.ok(send.disabled);
 }finally{ui.dom.window.close();}
});

for(const product of ['DORIS','REDIS_STREAMS','NATS_JETSTREAM','PULSAR','RABBITMQ']){
test(product+' destination receipt is scoped to a paused target and displays its exact digest',async()=>{
 const task={id:'analytics-receipt',revision:'current-revision',formatVersion:3,desired:'PAUSED',workerState:'PAUSED',
  statusAvailable:true,status:{run_state:'PAUSED',subscriptions:[
   {id:'dw',state:'PAUSED',errorGeneration:3,failurePhase:'TARGET_APPLY',acknowledged:7},
   {id:'pg',state:'PAUSED',errorGeneration:3,failurePhase:'TARGET_APPLY',acknowledged:8}]},
  destinations:[{id:'dw',product,rabbitReceiptReviewAvailable:product==='RABBITMQ'},{id:'pg',product:'RABBITMQ'}],operations:[]};
 const ui=setup([task],'ADMIN',{authenticated:false,taskControlScope:'GLOBAL',recoveryReview:true}),fallback=ui.w.fetch;
 const requests=[];
 ui.w.fetch=async(path,options)=>{
  if(path!=='/api/tasks/'+task.id+'/destination-receipt')return fallback(path,options);
  assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
  requests.push(JSON.parse(options.body));
  return {ok:true,status:200,json:async()=>({status:'PRECEDING_RECEIPT_VERIFIED',target:'dw',product,
   errorGeneration:'3',acknowledgedSequence:'7',nativeReceiptSha256:'e'.repeat(64)})};
 };
 try{
  await login(ui);
  const buttons=ui.d.querySelectorAll('[data-materialized-receipt-review]');
  assert.equal(buttons.length,1);
  assert.equal(buttons[0].dataset.materializedReceiptReview,'dw');
  buttons[0].click();await flush();
  assert.deepEqual(requests,[{expectedRevision:task.revision,subscriptionId:'dw',errorGeneration:3}]);
  assert.match(buttons[0].parentElement.textContent,/已核对故障前最近一笔已确认回执/);
  assert.ok(buttons[0].parentElement.textContent.includes('e'.repeat(64)));
 }finally{ui.dom.window.close();}
});
}

test('Kafka physical receipt is read-only and displays only evidence for the current configuration and ACK',async()=>{
 const id='physical_receipt_review',target='kafka',code='PRECEDING_KAFKA_RECEIPTS_VERIFIED';
 const version='f438d9a1-02ed-41c8-9c55-27eb804bd980';
 const task={id:'physical-receipt',revision:'task-revision',activeConfigurationVersion:version,formatVersion:3,
  workerState:'PAUSED',desired:'PAUSED',statusAvailable:true,status:{run_state:'PAUSED'},
  destinations:[{id:target,product:'KAFKA'}],operations:[{id,action:'REVIEW_KAFKA_RECEIPTS',code,status:'SUCCEEDED'}]};
 const ui=setup([task],'ADMIN',{authenticated:false,taskControlScope:'GLOBAL',recoveryReview:true}),fallback=ui.w.fetch;
 const base='/api/tasks/'+task.id,requests=[];
 const exact={targetId:target,configurationVersion:version,acknowledgedSequence:'6'};
 let reply={...exact,status:'MISSING',code:'KAFKA_PHYSICAL_RECEIPT_UNVERIFIED'};
 ui.w.fetch=async(path,options)=>{
  if(path===base+'/recovery-receipts/'+id)return{ok:true,status:200,json:async()=>({kind:'CONTROL_OPERATION',taskId:task.id,
   id,action:'REVIEW_KAFKA_RECEIPTS',status:'SUCCEEDED',code,subscriptionId:target,kafkaReview:{status:code,target,
    observedAt:'2026-11-18T09:00:00Z',epoch:'3',errorGeneration:'17',acknowledgedSequence:'6',failedSequence:'7',
    observedTopicIdentitySha256:'a'.repeat(64),originalFailure:{journalGeneration:'retained'},
    nativeReceipts:[{namespace:'orders',offset:'31',payloadSha256:'b'.repeat(64)}]}})};
  if(path!==base+'/kafka-physical-receipt')return fallback(path,options);
  assert.equal(options.headers['X-CSRF-Token'],'test-csrf');
  requests.push(JSON.parse(options.body));
  return{ok:true,status:200,json:async()=>reply};
 };
 try{
  await login(ui);ui.d.querySelector('[data-recovery-receipt-request="'+id+'"]').click();await flush();
  const section=ui.d.querySelector('[data-kafka-physical-receipt]'),button=section.querySelector('button');
  assert.equal(ui.d.querySelector('[data-kafka-notice-preview]'),null);
  button.click();await flush();
  assert.match(section.textContent,/缺少可核实的物理收据/);
  reply={status:'UNREACHABLE',code:'OWNER_QUERY_UNAVAILABLE'};
  button.click();await flush();
  assert.match(section.textContent,/暂时无法读取物理收据/);
  reply={...exact,status:'VERIFIED',code:'KAFKA_PHYSICAL_RECEIPT_VERIFIED',configurationVersion:'older',
   receiptSha256:'d'.repeat(64),publishedAt:'2026-11-18T09:00:00Z',acknowledgedAt:'2026-11-18T09:00:01Z',
   signature:'private data should never render'};
  button.click();await flush();
  assert.match(section.textContent,/回执与当前请求不符/);
  reply.configurationVersion=version;
  button.click();await flush();
  assert.match(section.textContent,/已核实本次故障的确切前序 ACK/);
  assert.ok(section.textContent.includes('d'.repeat(64)));
  assert.ok(!section.textContent.includes('private data should never render'));
  assert.equal(section.querySelectorAll('button').length,1);
  assert.deepEqual(requests,Array(4).fill({expectedRevision:task.revision,predecessorRequestId:id}));
 }finally{ui.dom.window.close();}
});

test('retention review shows the existing hold, requires approval, and retries the identical request',async()=>{
 const reportId='a4500000-1223-4abc-8abc-0123456789ab';
 const task={id:'retention-review',revision:'task-revision',formatVersion:3,workerState:'STOPPED',status:{},destinations:[],
  operations:[{id:'validation_report_request',action:'VALIDATE',status:'SUCCEEDED',validation:{reportId}}]};
 const ui=setup([task]),fallback=ui.w.fetch,calls=[],base='/api/tasks/'+task.id+'/validations/'+reportId;
 ui.w.fetch=async(path,options)=>{
  if(path===base+'/storage')return{ok:true,status:200,json:async()=>({id:reportId,activity:'ACTIVITY_UNKNOWN',observedAt:'2026-11-18T09:00:00Z',
   retentionMinimumDays:30,retention:{reportId,requestId:'previous_review',minimumDays:60,actorId:'admin-a',
    minimumRetainUntil:'2027-01-17T09:00:00Z'},files:{'rows.mv.db':8192}})};
  if(path!==base+'/retention')return fallback(path,options);
  calls.push(options);
  if(calls.length===1)throw new Error('Temporary network error');
  return{ok:true,status:202,json:async()=>task};
 };
 try{
  await login(ui);
  ui.d.querySelector('[data-validation-storage]').click();await flush();
  const dialog=ui.d.querySelector('.validation-storage-dialog'),form=dialog.querySelector('form'),
   approval=form.querySelector('[name=approved]'),days=form.querySelector('[name=minimumDays]');
  assert.match(dialog.textContent,/admin-a/);assert.equal(days.min,'30');assert.equal(days.value,'60');
  const submit=()=>form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));
  submit();await flush();assert.equal(calls.length,0);
  days.value='90';approval.checked=true;submit();await flush();
  assert.equal(calls.length,1);
  assert.deepEqual(JSON.parse(calls[0].body),{minimumDays:90,approved:true,expectedRevision:'task-revision',expectedReviewId:'previous_review'});
  assert.equal(calls[0].headers['X-CSRF-Token'],'test-csrf');
  submit();await flush();assert.equal(calls.length,2);
  assert.equal(calls[1].body,calls[0].body);
  assert.equal(calls[1].headers['X-Request-ID'],calls[0].headers['X-Request-ID']);
  assert.ok(dialog.textContent.includes(calls[0].headers['X-Request-ID']));
 }finally{ui.dom.window.close();}
});

test('a Global viewer cannot open recovery or report-storage lookup controls',async()=>{
 const reportId='a4500000-1223-4abc-8abc-0123456789ab';
 const task={id:'restricted-receipt',formatVersion:3,workerState:'STOPPED',status:{},destinations:[],
  operations:[{id:'request_validated_001',action:'VALIDATE',status:'SUCCEEDED',validation:{reportId}}]};
 const ui=setup([task],'VIEWER',{authenticated:false,taskControlScope:'GLOBAL'});
 try{
  await login(ui);
  assert.ok(ui.d.querySelector('[data-validation-report]'));
  assert.equal(ui.d.querySelector('[data-validation-storage],[data-recovery-receipt-lookup],[data-recovery-receipt-request]'),null);
 }finally{ui.dom.window.close();}
});

test('capture recovery shows native header evidence as text and still requires retained event contents',async()=>{
 const task={id:'headers',formatVersion:3,workerState:'STOPPED',desired:'RUNNING',operations:[],destinations:[],
  status:{run_state:'ERROR',error_stage:'SOURCE_DECODE',error_code:'BINLOG_SCHEMA_WIDTH_CHANGED'}};
 const ui=setup([task]),fallback=ui.w.fetch;
 ui.w.fetch=async(path,options)=>{
  if(path!=='/api/tasks/headers/capture-recovery')return fallback(path,options);
  return {ok:true,status:200,json:async()=>({sourceAdmission:'SOURCE_CHECKPOINT_AVAILABLE',affectedSubscriptions:['warehouse'],
   actions:['PRESERVE_SOURCE_LOGS','REPAIR_CAPTURE_CAUSE'],nextTransactionHeaders:{scope:'BINLOG_HEADERS_ONLY',transactionId:'source-gtid',
    logFile:'mysql-bin.000042',startOffset:'1024',endOffset:'2048',tables:['sales.<img src=x>']}})};
 };
 try{
  await login(ui);ui.d.querySelector('.pipeline-failure button').click();await flush();
  const dialog=ui.d.querySelector('dialog.failure-review-dialog');
  assert.match(dialog.textContent,/原生 binlog 事务边界/);assert.match(dialog.textContent,/原始事件内容/);
  assert.match(dialog.textContent,/mysql-bin.000042.*1024–2048/);assert.match(dialog.textContent,/sales.<img src=x>/);
  assert.equal(dialog.querySelector('img'),null);assert.equal(dialog.querySelectorAll('button').length,1);
 }finally{ui.dom.window.close();}
});

test('admin explicitly retains current source bytes with a generation-bound durable request',async()=>{
 const task={id:'headers',revision:'source-revision',formatVersion:3,workerState:'STOPPED',desired:'RUNNING',captureLeaseState:'EXPIRED',operations:[],destinations:[],
  status:{run_state:'ERROR',error_stage:'SOURCE_DECODE',error_code:'BINLOG_SCHEMA_WIDTH_CHANGED'}};
 const ui=setup([task]),fallback=ui.w.fetch;
 ui.w.fetch=async(path,options)=>{
  if(path!=='/api/tasks/headers/capture-recovery')return fallback(path,options);
  return {ok:true,status:200,json:async()=>({sourceAdmission:'SOURCE_CHECKPOINT_AVAILABLE',affectedSubscriptions:['warehouse'],errorGeneration:4,
   actions:['PRESERVE_SOURCE_LOGS','REPAIR_CAPTURE_CAUSE'],nextTransactionHeaders:{scope:'BINLOG_HEADERS_ONLY',transactionId:'source-gtid',
    logFile:'mysql-bin.000042',startOffset:'1024',endOffset:'2048',tables:['sales.items'],boundaryHash:'a'.repeat(64)}})};
 };
 try{
  await login(ui);ui.d.querySelector('.pipeline-failure button').click();await flush();
  const retain=[...ui.d.querySelectorAll('dialog button')].find(button=>button.textContent==='留存原始事务');assert.ok(retain);
  assert.equal(ui.calls.filter(call=>call.path.endsWith('/actions')).length,0);
  retain.click();await flush();
  const sent=ui.calls.find(call=>call.path.endsWith('/actions'));assert.ok(sent.headers['X-Request-ID']);
  assert.deepEqual(JSON.parse(sent.body),{action:'CAPTURE_RETAIN',expectedRevision:'source-revision',errorCount:4,boundaryHash:'a'.repeat(64)});
  assert.equal(ui.d.querySelector('dialog.failure-review-dialog'),null);
 }finally{ui.dom.window.close();}
});

test('source exclusion requires the reviewed artifact and affirmative operator approval',async()=>{
 const plan={transactionId:'original-gtid',errorGeneration:4,boundaryHash:'a'.repeat(64),
  archiveSha256:'b'.repeat(64),planHash:'c'.repeat(64),affectedSubscriptions:{warehouse:['sales.items']}};
 const task={id:'exclusion',revision:'task-revision',formatVersion:3,statusAvailable:true,workerState:'STOPPED',desired:'RUNNING',
  operations:[{id:'retention_0001',action:'CAPTURE_RETAIN',status:'SUCCEEDED',result:{exclusionPlan:plan}}],
  status:{run_state:'ERROR',error_count:4,error_stage:'SOURCE_DECODE'}};
 const ui=setup([task]);
 try{
  await login(ui);
  [...ui.d.querySelectorAll('.history button')].find(button=>button.textContent==='审阅源事务跳过').click();
  const dialog=ui.d.querySelector('dialog.failure-review-dialog');
  assert.match(dialog.textContent,/original-gtid.*warehouse · sales.items/);
  const submit=[...dialog.querySelectorAll('button')].find(button=>button.textContent==='批准跳过并保持暂停');
  const reason=dialog.querySelector('textarea'),approval=dialog.querySelector('input[type=checkbox]');
  assert.equal(submit.disabled,true);
  reason.value='confirmed original transaction';
  reason.dispatchEvent(new ui.w.Event('input'));
  assert.equal(submit.disabled,true);
  approval.checked=true;
  approval.dispatchEvent(new ui.w.Event('change'));
  assert.equal(submit.disabled,false);
  submit.click();await flush();
  const call=ui.calls.find(call=>call.path.endsWith('/actions'));
  assert.ok(call.headers['X-Request-ID']);
  assert.deepEqual(JSON.parse(call.body),{action:'CAPTURE_EXCLUDE',expectedRevision:'task-revision',errorCount:4,
   captureExclusion:{planHash:plan.planHash,archiveSha256:plan.archiveSha256,boundaryHash:plan.boundaryHash,
    reason:reason.value,acceptDataLoss:true}});
 }finally{ui.dom.window.close();}
});

test('capture recovery explains a source gap without offering a target skip',async()=>{
 const task={id:'capture',formatVersion:3,workerState:'STOPPED',desired:'RUNNING',operations:[],destinations:[],
  status:{run_state:'ERROR',error_stage:'SOURCE_CONNECT',error_code:'SOURCE_LOG_GAP'}};
 for(const role of ['ADMIN','OPERATOR','VIEWER']){
  const ui=setup([task],role),fallback=ui.w.fetch;
  ui.w.fetch=async(path,options)=>{
   if(path!=='/api/tasks/capture/capture-recovery')return fallback(path,options);
   ui.calls.push({path,...options});
   return {ok:true,status:200,json:async()=>({sourceAdmission:'SOURCE_LOG_GAP',
    affectedSubscriptions:['warehouse','backup'],actions:['PRESERVE_SOURCE_LOGS','RESTORE_LOGS_OR_SELECT_ELIGIBLE_SOURCE','REVIEW_NEW_BASELINE']})};
  };
  try{
   await login(ui);
   const review=ui.d.querySelector('.pipeline-failure button');
   if(role!=='ADMIN'){assert.equal(review,null);continue;}
   review.click();await flush();
   const dialog=ui.d.querySelector('dialog.failure-review-dialog');
   assert.match(dialog.textContent,/尚未证明缺口/);
   assert.match(dialog.textContent,/warehouse, backup/);
   assert.match(dialog.textContent,/恢复所需日志/);
   assert.equal(dialog.querySelectorAll('button').length,1);
   assert.equal(ui.calls.find(call=>call.path.endsWith('/capture-recovery')).method,undefined);
   ui.d.getElementById('logout').click();await flush();
   assert.equal(ui.d.querySelector('dialog.failure-review-dialog'),null);
  }finally{ui.dom.window.close();}
 }
});

for(const [product,scope] of [
 ['MySQL', 'CURRENT_MYSQL_CATALOG_VS_JOURNAL'],
 ['PostgreSQL', 'CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL'],
 ['Oracle PDB', 'CURRENT_ORACLE_PDB_CATALOG_VS_JOURNAL']]){
test(`the ${product} DDL review shows advisory recorded and current schema differences`,async()=>{
 const task={id:'ddl-review',formatVersion:3,workerState:'STOPPED',desired:'RUNNING',operations:[],destinations:[],
  status:{run_state:'ERROR',error_stage:'DDL',error_code:'DDL_PLAN_REQUIRED'}};
 const ui=setup([task]),fallback=ui.w.fetch;
 ui.w.fetch=async(path,options)=>{
  if(path!=='/api/tasks/ddl-review/capture-recovery')return fallback(path,options);
  ui.calls.push({path,...options});
  const blockedArchiveEvent=product==='Oracle PDB'?{
   state:'OBSERVED',fingerprint:'a'.repeat(64),
   event:{eventScn:'110',archiveStartScn:'80',archiveEndScnExclusive:'120',recordSetId:'0x000005.00000010.0010',
    statementSequence:'0',kind:'USER_DDL',decodedSqlSha256:'b'.repeat(64)}
  }:undefined;
  return {ok:true,status:200,json:async()=>({sourceAdmission:'SOURCE_CHECKPOINT_AVAILABLE',affectedSubscriptions:['warehouse'],blockedArchiveEvent,
   actions:['REPAIR_CAPTURE_CAUSE'],schemaDrift:{scope,state:'CHANGES_VISIBLE',
    changedTables:1,detailsLimited:false,tables:[{database:'sales',table:'orders',change:'TABLE_CHANGED',identityChanged:true,
     columnOrderChanged:true,indexesChanged:true,columns:{added:['planned_field','<img src=x onerror=alert(1)>'],removed:['old_field'],modified:[],
      addedCount:2,removedCount:1,modifiedCount:0}}]}})};
 };
 try{
  await login(ui);
  ui.d.querySelector('.pipeline-failure button').click();await flush();
  const dialog=ui.d.querySelector('dialog.failure-review-dialog');
  assert.match(dialog.textContent,new RegExp(product+' 结构差异'));
  assert.match(dialog.textContent,/当前目录可能包含后续 DDL/);
  assert.match(dialog.textContent,/sales.orders/);
  assert.match(dialog.textContent,/planned_field/);
  assert.match(dialog.textContent,/old_field/);
  assert.match(dialog.textContent,/行身份键已变化/);
  assert.match(dialog.textContent,/已有列的相对顺序已变化/);
  assert.match(dialog.textContent,/源目录索引定义已变化/);
  if(product==='Oracle PDB'){
   assert.match(dialog.textContent,/Oracle 归档故障指纹/);
   assert.match(dialog.textContent,/SCN 110/);
   assert.match(dialog.textContent,/\[80, 120\)/);
   assert.match(dialog.textContent,new RegExp('a'.repeat(64)));
  }
  assert.equal(dialog.querySelector('img'),null);
  assert.equal(dialog.querySelectorAll('button').length,1);
  assert.equal(ui.calls.find(call=>call.path.endsWith('/capture-recovery')).method,undefined);
 }finally{ui.dom.window.close();}
});
}

test('a running worker and an expired capture lease remain separate in the console',async()=>{
 const task={id:'lease',formatVersion:3,workerState:'RUNNING',desired:'RUNNING',operations:[],destinations:[],
  status:{run_state:'RUNNING'},captureLeaseState:'EXPIRED',statusAvailable:true};
 const ui=setup([task]);
 try{
  await login(ui);
  const grid=ui.d.querySelector('.status-grid');
  const lease=[...grid.querySelectorAll('div')].find(section=>section.firstChild.textContent==='采集租约');
  assert.equal(lease.lastChild.textContent,'已过期');
  assert.match(grid.textContent,/运行中/);
 }finally{ui.dom.window.close();}
});

test('capture diagnostics pages are restricted to administrators and close on sign-out',async()=>{
 const task={id:'diagnose',formatVersion:3,workerState:'RUNNING',desired:'RUNNING',operations:[],destinations:[],
  status:{run_state:'RUNNING'}};
 for(const role of ['ADMIN','VIEWER']){
  const ui=setup([task],role),fallback=ui.w.fetch;
  const now='2026-11-18T04:00:00Z';
  ui.w.fetch=async(path,options)=>{
   if(!path.startsWith('/api/tasks/diagnose/diagnostics?'))return fallback(path,options);
   ui.calls.push({path,...options});
   const second=path.endsWith('&after=first');
   return {ok:true,status:200,json:async()=>({ownerNodeId:'agent-a',
    events:[{at:now,event:second?'CAPTURE_READY':'CAPTURE_FAILURE',phase:second?null:'SOURCE_CONNECT'}],
    nextCursor:second?null:'first'})};
  };
  try{
   await login(ui);
   const button=ui.d.querySelector('[data-worker-diagnostics]');
   if(role==='VIEWER'){assert.equal(button,null);continue;}
   button.click();await flush();
   const dialog=ui.d.querySelector('.worker-diagnostics');
   assert.match(dialog.textContent,/agent-a/);
   assert.match(dialog.textContent,/采集失败 · 源库连接/);
   dialog.querySelector('section button').click();await flush();
   assert.match(dialog.textContent,/采集已启动/);
   ui.d.getElementById('logout').click();await flush();
   assert.equal(ui.d.querySelector('.worker-diagnostics'),null);
  }finally{ui.dom.window.close();}
 }
});

test('switching diagnostic destination drops old responses and keeps the target on older pages',async()=>{
 const task={id:'diagnose',formatVersion:3,workerState:'RUNNING',desired:'RUNNING',operations:[],
  destinations:[{id:'pg'},{id:'mysql'}],status:{run_state:'RUNNING'}};
 const ui=setup([task]),fallback=ui.w.fetch,now='2026-11-18T04:00:00Z';
 let finishPostgres;
 const reply=(event,nextCursor=null)=>({ok:true,status:200,json:async()=>({
  ownerNodeId:'agent-a',events:[{at:now,event}],nextCursor})});
 ui.w.fetch=async(path,options)=>{
  if(!path.startsWith('/api/tasks/diagnose/diagnostics?'))return fallback(path,options);
  ui.calls.push({path,...options});
  if(path.includes('subscriptionId=pg'))return new Promise(resolve=>{finishPostgres=resolve;});
  if(path.includes('subscriptionId=mysql'))return path.includes('after=older')
   ?reply('TARGET_RETRYING'):reply('TARGET_RETRY_CLEARED','older');
  return reply('CAPTURE_READY');
 };
 try{
  await login(ui);ui.d.querySelector('[data-worker-diagnostics]').click();await flush();
  const dialog=ui.d.querySelector('.worker-diagnostics'),scope=dialog.querySelector('select');
  scope.value='pg';scope.dispatchEvent(new ui.w.Event('change'));await flush();
  scope.value='mysql';scope.dispatchEvent(new ui.w.Event('change'));await flush();
  finishPostgres(reply('TARGET_PAUSED'));await flush();
  assert.match(dialog.textContent,/目标重试已结束/);
  assert.doesNotMatch(dialog.textContent,/目标因故障暂停/);
  dialog.querySelector('section button').click();await flush();
  assert.ok(ui.calls.some(call=>call.path.endsWith('&subscriptionId=mysql&after=older')));
  assert.match(dialog.textContent,/目标正在重试/);
 }finally{ui.dom.window.close();}
});

test('last delivery delay shows uncertainty and outage history without suggesting health',async()=>{
 const observed={state:'OBSERVED',millis:'42',sequence:'7',sourceCommittedAt:'2026-11-18T04:00:00Z',
  targetCommitObservedAt:'2026-11-18T04:00:00.042Z'};
 const subscription=(id,lastDeliveryTiming,changes={})=>({id,state:'RUNNING',pendingTransactions:'0',
  progressSemantics:'SQL_COMMITTED',lastDeliveryTiming,...changes});
 const task={id:'delivery-clock',formatVersion:3,sourceType:'POSTGRESQL',workerState:'RUNNING',
  desired:'RUNNING',operations:[],statusAvailable:true,destinations:[],status:{run_state:'RUNNING',subscriptions:[
   subscription('active',observed),subscription('paused',observed,{state:'PAUSED',pendingTransactions:'1'}),
   subscription('future',{state:'CLOCK_SKEW'}),subscription('missing',{state:'UNKNOWN'})
  ]}};
 const ui=setup([task],'VIEWER');
 try{
  await login(ui);
  const labels=[...ui.d.querySelectorAll('[data-delivery-timing]')].map(node=>node.textContent);
  assert.match(labels[0],/上次提交至目标确认 · 42 毫秒 · 事务序号 7/);
  assert.match(labels[1],/历史确认样本 · 42 毫秒/);
  assert.match(labels[2],/请检查时间同步/);
  assert.match(labels[3],/暂无可核验样本/);
  task.statusAvailable=false;
  ui.d.querySelector('.task-card').click();await flush();
  assert.equal(ui.d.querySelector('[data-delivery-timing]'),null);
 }finally{ui.dom.window.close();}
});

test('validation submits explicit SQL targets and budgets without including messaging targets',async()=>{
 const task={id:'validate',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING'},operations:[],destinations:[{id:'pg',product:'POSTGRESQL',consistency:'TRANSACTIONAL'},{id:'mq',product:'KAFKA',consistency:'AT_LEAST_ONCE_EVENTS'}]};const ui=setup([task]);try{
  await login(ui);assert.equal(ui.d.querySelectorAll('[data-validation-target]').length,1);const read=ui.d.querySelector('[data-validation-limit="read"]');read.value='7200';read.dispatchEvent(new ui.w.Event('input'));ui.d.querySelector('[data-validation-action="start"]').click();await flush();
  const call=ui.calls.find(call=>call.method==='POST'&&JSON.parse(call.body).action==='VALIDATE');assert.ok(call);const body=JSON.parse(call.body);assert.deepEqual(body.validation.targets,['pg']);assert.equal(body.validation.sourceMillis,7200000);assert.equal(body.validation.limits.maximumRowBytes,4194304);assert.equal(body.expectedRevision,'revision');assert.ok(call.headers['X-Request-ID']);
 }finally{ui.dom.window.close();}
});
test('viewer and operator can see validation history but cannot start or cancel jobs',async()=>{
 const task={id:'validate',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING'},operations:[{id:'scan',action:'VALIDATE',status:'PENDING',validation:{reportId:'report-id'}}],destinations:[{id:'pg',product:'POSTGRESQL',consistency:'TRANSACTIONAL'}]};
 for(const role of ['VIEWER','OPERATOR']){const ui=setup([task],role);try{await login(ui);assert.equal(ui.d.querySelector('[data-validation-action="start"]'),null);assert.equal(ui.d.querySelector('[data-validation-action="cancel"]'),null);assert.match(ui.d.getElementById('task-detail').textContent,/report-id/);}finally{ui.dom.window.close();}}
});
test('validation cancellation carries the current revision and is separate from resuming',async()=>{
 const task={id:'validate',revision:'current-revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'PAUSED'},operations:[{id:'scan',action:'VALIDATE',status:'PENDING',validation:{reportId:'report-id'}}],destinations:[]};const ui=setup([task]);try{
  await login(ui);assert.equal(ui.d.querySelector('[data-validation-action="start"]').disabled,true);ui.d.querySelector('[data-validation-action="cancel"]').click();await flush();const call=ui.calls.find(call=>call.method==='POST');assert.equal(JSON.parse(call.body).action,'VALIDATE_CANCEL');assert.equal(JSON.parse(call.body).expectedRevision,'current-revision');
 }finally{ui.dom.window.close();}
});

test('login keeps the token out of persistent browser storage and renders availability honestly',async()=>{
  const ui=setup();try{
    await flush();assert.equal(ui.d.getElementById('login-dialog').open,true);await login(ui);
    assert.equal(ui.d.getElementById('login-dialog').open,false);
    assert.equal(ui.d.getElementById('token').value,'');
    assert.equal(ui.w.localStorage.length,1);assert.equal(ui.w.sessionStorage.length,0);
    ui.d.getElementById('adapters-nav').click();assert.match(ui.d.getElementById('adapter-list').textContent,/PULSAR.*规划中/);
    assert.equal(ui.d.querySelectorAll('#adapter-list button').length,0);
    ui.d.getElementById('language').click();assert.match(ui.d.getElementById('adapter-list').textContent,/Planned/);
    assert.equal(ui.d.getElementById('page-title').textContent,'Adapter capabilities');
  }finally{ui.dom.window.close();}
});

test('target capabilities distinguish reviewed SQL recovery, manual rebuild and unsupported products',async()=>{
  const capabilities=[
    {id:'POSTGRESQL',role:'SINK',status:'AVAILABLE',transactionSkip:'REVIEWED_LOCAL_TRANSACTION',transactionCompensation:'REVIEWED_BEFORE_IMAGES',rebuildMode:'MANUAL_NEW_BASELINE'},
    {id:'KAFKA',role:'SINK',status:'PARTIALLY_VERIFIED',transactionSkip:'UNAVAILABLE',transactionCompensation:'UNAVAILABLE',rebuildMode:'MANUAL_NEW_BASELINE'},
    {id:'GBASE_8A',role:'SINK',status:'PLANNED',transactionSkip:'UNAVAILABLE',transactionCompensation:'UNAVAILABLE',rebuildMode:'UNAVAILABLE'},
    {id:'MYSQL',role:'SOURCE',status:'AVAILABLE'}
  ];
  const ui=setup([],'ADMIN',{capabilities});
  try{
    await login(ui);ui.d.getElementById('adapters-nav').click();
    const card=id=>[...ui.d.querySelectorAll('#adapter-list article')].find(row=>row.querySelector('strong').textContent===id);
    assert.match(card('POSTGRESQL').textContent,/跳过事务 · 需审阅的本地事务.*前镜像补偿 · 使用已审阅的前镜像/);
    assert.match(card('KAFKA').textContent,/跳过事务 · 不可用.*前镜像补偿 · 不可用.*重建 · 人工规划新基线/);
    assert.match(card('GBASE_8A').textContent,/重建 · 不可用/);
    assert.doesNotMatch(card('MYSQL').textContent,/跳过事务|前镜像补偿|重建/);
    ui.d.getElementById('language').click();
    assert.match(card('KAFKA').textContent,/Skip transaction · Unavailable.*Rebuild · Plan a new baseline manually/);
    assert.equal(ui.d.querySelectorAll('#adapter-list button').length,0);
  }finally{ui.dom.window.close();}
});

test('viewer and operator accounts show their role and cannot create pipelines',async()=>{
  const task={id:'restricted',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING',subscriptions:[{id:'pg',state:'PAUSED',acknowledged:0,errorGeneration:1}]},statusAvailable:true,operations:[]};
  for(const role of ['VIEWER','OPERATOR']){
    const ui=setup([task],role);try{
      await login(ui);assert.equal(ui.d.getElementById('new-task').disabled,true);assert.equal(ui.d.getElementById('import-task').disabled,true);assert.match(ui.d.getElementById('session-info').textContent,role==='VIEWER'?/只读查看/:/任务运维/);
      for(const button of ui.d.querySelectorAll('#task-detail .detail-actions button'))if(button.textContent!=='修复后恢复')assert.equal(button.disabled,role==='VIEWER'||button.textContent==='配置和历史');assert.equal(ui.d.querySelector('.subscription-card button').disabled,role==='VIEWER');
      ui.d.getElementById('new-task').click();assert.equal(ui.d.getElementById('create-dialog').open,false);assert.equal(ui.calls.some(c=>c.path==='/api/tasks'&&c.method==='POST'),false);
      ui.d.getElementById('logout').click();assert.equal(ui.d.getElementById('session-info').textContent,'');assert.equal(ui.d.getElementById('login-dialog').open,true);
    }finally{ui.dom.window.close();}
  }
});

test('an old unauthorized response cannot clear a newer authenticated session',async()=>{
  const task={id:'session-race',revision:'revision',desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING'},statusAvailable:true,operations:[]};const ui=setup([task]);try{
    await login(ui);let complete;const original=ui.w.fetch;ui.w.fetch=(path,options)=>path.endsWith('/actions')?new Promise(resolve=>{complete=resolve;}):original(path,options);
    ui.d.querySelector('.detail-actions button').click();assert.ok(complete);ui.d.getElementById('logout').click();await login(ui);assert.equal(ui.d.getElementById('login-dialog').open,false);
    complete({ok:false,status:401,json:async()=>({code:'UNAUTHORIZED'})});await flush();assert.equal(ui.d.getElementById('login-dialog').open,false);assert.equal(ui.d.getElementById('new-task').disabled,false);assert.match(ui.d.getElementById('session-info').textContent,/管理员/);
  }finally{ui.dom.window.close();}
});

test('operation history and creation attribution use escaped authenticated identity text',async()=>{
  const actor='<img src=x onerror=alert(1)>',task={id:'attribution',revision:'revision',createdBy:'migration-admin',createdRole:'ADMIN',desired:'STOPPED',workerState:'STOPPED',status:{},statusAvailable:true,operations:[{id:'operation',action:'CHECK',status:'SUCCEEDED',code:'OK',actorId:actor,actorRole:'OPERATOR'}]};const ui=setup([task]);try{
    await login(ui);assert.match(ui.d.getElementById('task-detail').textContent,/migration-admin/);assert.match(ui.d.querySelector('.history').textContent,/任务运维/);assert.ok(ui.d.querySelector('.history').textContent.includes(actor));assert.equal(ui.d.querySelectorAll('#task-detail img').length,0);
  }finally{ui.dom.window.close();}
});

test('wizard validates, masks its plan, creates a stopped task and never auto-starts',async()=>{
  const ui=setup();try{
    await login(ui);ui.d.getElementById('new-task').click();
    ui.d.getElementById('next-step').click();assert.equal(ui.d.getElementById('step-0').classList.contains('hidden'),false);
    const form=ui.d.getElementById('create-form');
    form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));
    for(const [name,value]of Object.entries({jobId:'sales',clusterId:'sales-source',sourceDatabase:'orders',sourceUsername:'reader',sourcePassword:'source-private'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();assert.equal(ui.d.getElementById('step-1').classList.contains('hidden'),false);
    for(const [name,value]of Object.entries({targetType:'POSTGRESQL',targetSchema:'orders',targetUsername:'writer',targetPassword:'target-private'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();
    const preview=ui.d.getElementById('plan-preview').textContent;
    assert.doesNotMatch(preview,/source-private|target-private/);assert.match(preview,/••••/);
    form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();
    const sent=ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST');assert.ok(sent);
    assert.equal(JSON.parse(sent.body).config.source.password,'source-private');
    assert.equal(JSON.parse(sent.body).config.target.type,'POSTGRESQL');
    assert.equal(ui.d.getElementById('create-dialog').open,false);assert.match(ui.d.getElementById('task-list').textContent,/sales/);
    assert.equal(ui.calls.filter(c=>c.path.endsWith('/actions')).length,0);
  }finally{ui.dom.window.close();}
});

test('v3 wizard maps two databases to independent targets and masks every credential',async()=>{
  const ui=setup();try{
    await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');assert.equal(form.elements.formatVersion.value,'3');
    for(const [name,value]of Object.entries({jobId:'fanout',clusterId:'source',sourceDatabase:'sales, inventory',sourceUsername:'reader',sourcePassword:'source-private',sourceExpectedVersion:'8.2.0'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();for(const [name,value]of Object.entries({targetSchema:'sales_copy',targetUsername:'writer',targetPassword:'gauss-private'}))form.elements[name].value=value;
    ui.d.querySelector('#first-extra-mappings input').value='inventory_copy';ui.d.getElementById('add-target').click();const card=ui.d.querySelector('#extra-targets .target-settings');
    card.querySelector('[data-field=username]').value='other';card.querySelector('[data-field=password]').value='pg-private';card.querySelectorAll('.mapping-fields input').forEach(input=>input.value=input.dataset.database+'_other');
    form.elements.journalMode.value='EXTERNAL';form.elements.journalMode.dispatchEvent(new ui.w.Event('change'));
    for(const [name,value]of Object.entries({journalUrl:'jdbc:postgresql://journal/db?password=embedded-private',journalUsername:'journal',journalPassword:'journal-private'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();const preview=ui.d.getElementById('plan-preview').textContent;assert.doesNotMatch(preview,/source-private|gauss-private|pg-private|journal-private|embedded-private/);
    form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const request=ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST');assert.ok(request);const config=JSON.parse(request.body).config;
    assert.equal(config.formatVersion,3);assert.equal(config.source.type,'MYSQL');assert.equal(config.source.expectedVersion,'8.2.0');assert.deepEqual(config.databases,['sales','inventory']);assert.deepEqual(config.targets[0].mappings,{sales:'sales_copy',inventory:'inventory_copy'});assert.deepEqual(config.targets[1].mappings,{sales:'sales_other',inventory:'inventory_other'});assert.equal(config.journal.mode,'EXTERNAL');assert.equal(config.journal.password,'journal-private');assert.equal(ui.calls.filter(c=>c.path.endsWith('/actions')).length,0);
  }finally{ui.dom.window.close();}
});

const additionalSources=[
  {id:'MYSQL',role:'SOURCE',status:'AVAILABLE'},
  {id:'POSTGRESQL',role:'SOURCE',status:'EXPERIMENTAL'},
  {id:'ORACLE',role:'SOURCE',status:'EXPERIMENTAL'}
];

test('PostgreSQL source wizard scopes destinations and submits a masked failover-slot task',async()=>{
  const ui=setup([],'ADMIN',{authenticated:false,capabilities:additionalSources});
  try{
    await login(ui);ui.d.getElementById('new-task').click();
    const form=ui.d.getElementById('create-form'),f=form.elements;
    assert.equal(f.sourceType.querySelector('[value=POSTGRESQL]').disabled,false);
    f.sourceType.value='POSTGRESQL';f.sourceType.dispatchEvent(new ui.w.Event('change'));
    assert.equal(f.sourceHost.disabled,true);assert.equal(f.oracleRootUrl.disabled,true);
    assert.equal(f.targetType.querySelector('[value=GAUSS]').disabled,true);
    assert.equal(f.targetType.querySelector('[value=MYSQL]').disabled,false);
    assert.equal(f.targetType.value,'POSTGRESQL');
    for(const [name,value] of Object.entries({jobId:'web-pg',clusterId:'shared-pg',pgUrl:'jdbc:postgresql://writer:5432/accounts',pgDatabase:'accounts',pgSchemas:'sales, billing',pgVersion:'17.12',pgUsername:'reader',pgPassword:'PRIVATE_PG',pgPublication:'sales_pub'}))f[name].value=value;
    f.pgFailoverSlot.checked=true;
    ui.d.getElementById('next-step').click();f.targetSchema.value='sales_copy';f.targetUsername.value='writer';
    ui.d.querySelector('#first-extra-mappings input').value='billing_copy';
    ui.d.getElementById('add-target').click();
    const card=ui.d.querySelector('#extra-targets .target-settings');
    assert.equal(card.querySelector('option[value=HIVE]').disabled,true);
    card.querySelector('[data-field=username]').value='warehouse';
    card.querySelectorAll('.mapping-fields input').forEach(input=>input.value=input.dataset.database+'_extra');
    ui.d.getElementById('next-step').click();
    assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/PRIVATE_PG/);
    form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();
    const config=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body).config;
    assert.deepEqual(config.source,{type:'POSTGRESQL',clusterId:'shared-pg',database:'accounts',topology:'SINGLE',username:'reader',password:'PRIVATE_PG',expectedVersion:'17.12',postgresql:{url:'jdbc:postgresql://writer:5432/accounts',publication:'sales_pub',slotPrefix:'rivomesh',failoverSlot:true}});
    assert.deepEqual(config.databases,['sales','billing']);
    assert.deepEqual(config.targets.map(target=>target.mappings),[{sales:'sales_copy',billing:'billing_copy'},{sales:'sales_extra',billing:'billing_extra'}]);
    assert.equal(ui.calls.filter(call=>call.path.endsWith('/actions')).length,0);
  }finally{ui.dom.window.close();}
});

test('Oracle source wizard derives owner mappings and keeps root and PDB credentials separate',async()=>{
  const ui=setup([],'ADMIN',{authenticated:false,capabilities:additionalSources});
  try{
    await login(ui);ui.d.getElementById('new-task').click();
    const form=ui.d.getElementById('create-form'),f=form.elements;
    f.sourceType.value='ORACLE';f.sourceType.dispatchEvent(new ui.w.Event('change'));
    assert.equal(f.targetType.querySelector('[value=MYSQL]').disabled,true);
    assert.equal(f.sourceUsername.disabled,true);assert.equal(f.oraclePdbUser.disabled,false);
    assert.equal(f.oracleVirtualNumeric.disabled,false);assert.equal(f.oracleVirtualNumeric.checked,false);
    f.oracleVirtualNumeric.checked=true;
    for(const [name,value] of Object.entries({jobId:'web-oracle',clusterId:'ora',oraclePdb:'FREEPDB1',oracleVersion:'23.26.4.0.0',oracleRootUrl:'jdbc:oracle:thin:@//ora:1521/FREE',oraclePdbUrl:'jdbc:oracle:thin:@//ora:1521/FREEPDB1',oracleRootUser:'C##READER',oraclePdbUser:'PDB_READER',oracleRootPassword:'PRIVATE_ROOT',oraclePdbPassword:'PRIVATE_PDB',oracleTables:'owner.TABLE'}))f[name].value=value;
    ui.d.getElementById('next-step').click();assert.equal(f.oracleTables.validity.customError,true);
    f.oracleTables.value='SALES.ORDERS\nSALES.ITEMS\nBILLING.INVOICE';
    ui.d.getElementById('next-step').click();f.targetSchema.value='sales_copy';f.targetUsername.value='writer';
    const mapping=ui.d.querySelector('#first-extra-mappings input');assert.equal(mapping.dataset.database,'BILLING');mapping.value='billing_copy';
    ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/PRIVATE_ROOT|PRIVATE_PDB/);
    form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();
    const config=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body).config;
    assert.deepEqual(config.source,{type:'ORACLE',clusterId:'ora',database:'FREEPDB1',topology:'SINGLE',expectedVersion:'23.26.4.0.0',oracle:{allowVirtualNumericColumns:true,tables:['SALES.ORDERS','SALES.ITEMS','BILLING.INVOICE'],root:{url:'jdbc:oracle:thin:@//ora:1521/FREE',username:'C##READER',password:'PRIVATE_ROOT'},pdb:{url:'jdbc:oracle:thin:@//ora:1521/FREEPDB1',username:'PDB_READER',password:'PRIVATE_PDB'}}});
    assert.deepEqual(config.targets[0].mappings,{SALES:'sales_copy',BILLING:'billing_copy'});
  }finally{ui.dom.window.close();}
});

test('wizard gates source by advertised role and status and returns to MySQL in V2',async()=>{
  const capabilities=[additionalSources[0],{id:'POSTGRESQL',role:'SINK',status:'AVAILABLE'},{id:'ORACLE',role:'SOURCE',status:'PLANNED'}];
  const ui=setup([],'ADMIN',{authenticated:false,capabilities});
  try{
    await login(ui);ui.d.getElementById('new-task').click();const f=ui.d.getElementById('create-form').elements;
    assert.equal(f.sourceType.querySelector('[value=POSTGRESQL]').disabled,true);
    assert.equal(f.sourceType.querySelector('[value=ORACLE]').disabled,true);
    f.formatVersion.value='2';f.formatVersion.dispatchEvent(new ui.w.Event('change'));
    assert.equal(f.sourceType.disabled,true);assert.equal(f.sourceHost.disabled,false);assert.equal(f.pgUrl.disabled,true);
  }finally{ui.dom.window.close();}
});

test('Kafka wizard saves a dedicated topic and resets its URL when switching to v2',async()=>{
  const ui=setup();try{
    await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');
    for(const [name,value]of Object.entries({jobId:'kafka-events',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();form.elements.targetType.value='KAFKA';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales';const topic=ui.d.querySelector('#primary-messaging [data-field=topic]');topic.value='sales.events';assert.equal(topic.disabled,false);assert.equal(form.elements.targetUsername.required,false);const security=ui.d.querySelector('#primary-messaging [data-field=securityProtocol]');security.value='SASL_SSL';security.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetUsername.required,true);assert.equal(form.elements.targetPassword.required,true);form.elements.targetUsername.value='publisher';form.elements.targetPassword.value='private-sasl-password';ui.d.querySelector('#primary-messaging [data-field=saslMechanism]').value='SCRAM-SHA-512';
    ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-sasl-password/);form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const config=JSON.parse(ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST').body).config;assert.equal(config.targets[0].product,'KAFKA');assert.equal(config.targets[0].messaging.topic,'sales.events');assert.equal(config.targets[0].messaging.securityProtocol,'SASL_SSL');assert.equal(config.targets[0].messaging.saslMechanism,'SCRAM-SHA-512');
    ui.d.getElementById('new-task').click();form.elements.targetType.value='KAFKA';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.value,'POSTGRESQL');assert.match(form.elements.targetUrl.value,/^jdbc:postgresql:/);assert.equal(form.elements.targetType.querySelector('[value=KAFKA]').disabled,true);
  }finally{ui.dom.window.close();}
});

test('source snapshot preparation is distinct from destination confirmation',async()=>{
  const task={id:'loading',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',sourceCluster:'source',sourceDatabase:'sales',targetSchema:'copy',status:{run_state:'RUNNING',phase:'TARGET_SNAPSHOT',table_count:2,completed_tables:2,lastSequence:0,subscriptions:[{id:'copy',state:'RUNNING',acknowledged:0,errorGeneration:0}],snapshotAcknowledgments:{copy:1},tables:[{table:'sales.items',copiedRows:12,done:true}]},statusAvailable:true,operations:[]};
  const ui=setup([task]);try{await login(ui);assert.match(ui.d.getElementById('task-detail').textContent,/目标全量投递中/);assert.match(ui.d.querySelector('.subscription-card').textContent,/已确认源快照: 1 \/ 2/);assert.match(ui.d.querySelector('table').textContent,/源快照行数/);assert.match(ui.d.getElementById('task-detail').textContent,/消息确认不代表下游消费完成/);}finally{ui.dom.window.close();}
});

test('Pulsar wizard saves persistent topic, cluster and admin endpoint with product-scoped credentials',async()=>{
  const ui=setup();try{
    await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');
    for(const [name,value]of Object.entries({jobId:'pulsar-events',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();form.elements.targetType.value='NATS_JETSTREAM';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));ui.d.querySelector('#primary-messaging [data-field=token]').value='old-nats-token';form.elements.targetUsername.value='old-user';form.elements.targetPassword.value='old-password';form.elements.targetType.value='PULSAR';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));
    const field=name=>ui.d.querySelector('#primary-messaging [data-field='+name+']');assert.equal(form.elements.targetUrl.value,'pulsar://127.0.0.1:6650');assert.equal(field('topic').value,'persistent://public/default/rivomesh_events');assert.equal(field('cluster').value,'standalone');assert.equal(field('token').value,'');assert.equal(form.elements.targetUsername.disabled,true);assert.equal(form.elements.targetUsername.value,'');assert.equal(form.elements.targetPassword.value,'');assert.equal(field('securityProtocol').disabled,true);
    form.elements.targetSchema.value='sales_events';field('adminUrl').value='http://pulsar:8080';field('token').value='private-pulsar-token';field('replicas').value='2';ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-pulsar-token|old-nats-token|old-password/);
    ui.d.getElementById('previous-step').click();field('adminUrl').value='https://user:private-admin@broker:8443/?token=private-query';ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-admin|private-query/);ui.d.getElementById('previous-step').click();field('adminUrl').value='http://pulsar:8080';ui.d.getElementById('next-step').click();
    form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST').body).config.targets[0];assert.equal(target.product,'PULSAR');assert.equal(target.messaging.adminUrl,'http://pulsar:8080');assert.equal(target.messaging.cluster,'standalone');assert.equal(target.messaging.token,'private-pulsar-token');assert.equal(target.messaging.replicas,2);assert.equal('securityProtocol' in target.messaging,false);
    ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=PULSAR]').disabled,true);assert.equal(form.elements.targetUsername.disabled,false);
  }finally{ui.dom.window.close();}
});

test('Redis wizard saves a durable stream without leaking hidden fields from another broker',async()=>{
  const ui=setup();try{
    await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');
    for(const [name,value]of Object.entries({jobId:'redis-events',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();form.elements.targetType.value='NATS_JETSTREAM';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));
    ui.d.querySelector('#primary-messaging [data-field=token]').value='hidden-nats-secret';form.elements.targetType.value='REDIS_STREAMS';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));
    assert.equal(form.elements.targetUrl.value,'redis://127.0.0.1:6379/0');assert.equal(form.elements.targetUsername.required,false);assert.equal(form.elements.metadataSchema.disabled,true);assert.equal(ui.d.querySelector('#primary-messaging [data-field=token]').disabled,true);
    form.elements.targetSchema.value='sales_events';form.elements.targetUsername.value='publisher';form.elements.targetPassword.value='redis-secret';ui.d.querySelector('#primary-messaging [data-field=stream]').value='sales:events';
    ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/hidden-nats-secret|redis-secret/);form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();
    const config=JSON.parse(ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST').body).config,target=config.targets[0];assert.equal(target.product,'REDIS_STREAMS');assert.equal(target.messaging.stream,'sales:events');assert.equal(target.password,'redis-secret');assert.equal('token' in target.messaging,false);assert.equal('topic' in target.messaging,false);
    ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=REDIS_STREAMS]').disabled,true);assert.match(form.elements.targetUrl.value,/^jdbc:postgresql:/);
  }finally{ui.dom.window.close();}
});

test('v3 target recovery carries its own generation while global source errors stay distinct',async()=>{
  const task={id:'fanout',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',sourceCluster:'source',sourceDatabase:'sales, inventory',targetSchema:'2 targets',targetType:'MULTIPLE',status:{run_state:'RUNNING',error_count:2,lastSequence:8,subscriptions:[{id:'gauss',state:'PAUSED',acknowledged:4,errorGeneration:9,errorCode:'SQL_23514'},{id:'pg',state:'RUNNING',acknowledged:8,errorGeneration:0}]},statusAvailable:true,problem:null,operations:[]};
  const ui=setup([task]);try{
    await login(ui);assert.match(ui.d.getElementById('task-list').textContent,/部分目标暂停/);const button=[...ui.d.querySelectorAll('.subscription-card button')].find(b=>b.textContent==='恢复此目标');button.click();await flush();const request=ui.calls.find(c=>c.path.endsWith('/actions'));assert.deepEqual(JSON.parse(request.body),{action:'RESUME_TARGET',expectedRevision:'revision',errorCount:9,subscriptionId:'gauss'});
    const globalResume=[...ui.d.querySelectorAll('.detail-actions button')].find(b=>b.textContent==='修复后恢复');assert.equal(globalResume.disabled,true);
  }finally{ui.dom.window.close();}
});

test('retryable destination failures remain automatic and visible without a manual resume action',async()=>{
  const task={id:'retrying',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING',lastSequence:4,subscriptions:[{id:'redis',state:'RUNNING',acknowledged:3,errorGeneration:0,errorCode:'TARGET_CONNECTION_RETRY'}]},statusAvailable:true,operations:[]};
  const ui=setup([task]);try{await login(ui);assert.match(ui.d.getElementById('task-list').textContent,/正在重试/);assert.equal(ui.d.getElementById('attention-count').textContent,'1');const card=ui.d.querySelector('.subscription-card');assert.match(card.textContent,/自动重试/);assert.equal(card.querySelectorAll('button').length,0);assert.equal(card.querySelectorAll('.error').length,0);}finally{ui.dom.window.close();}
});

test('destination cards show consumer resources and delivery guarantees as escaped text',async()=>{
  const task={id:'consumer-setup',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',destinations:[{id:'pulsar',product:'PULSAR',consistency:'AT_LEAST_ONCE_EVENTS',resource:'persistent://public/default/sales',mappings:{'<img src=x onerror=alert(1)>':'sales_events'}}],status:{run_state:'RUNNING',lastSequence:3,subscriptions:[{id:'pulsar',state:'RUNNING',acknowledged:3,errorGeneration:0,errorCode:null}]},statusAvailable:true,operations:[]};
  const ui=setup([task]);try{await login(ui);const card=ui.d.querySelector('.subscription-card');assert.match(card.textContent,/persistent:\/\/public\/default\/sales/);assert.match(card.textContent,/下游需去重并确认业务完成/);assert.match(card.textContent,/sales_events/);assert.equal(card.querySelectorAll('img,script').length,0);ui.d.getElementById('language').click();assert.match(ui.d.querySelector('.subscription-card').textContent,/Physical topic \/ stream/);}finally{ui.dom.window.close();}
});

test('RabbitMQ wizard opts in to a retained receipt stream with the management address',async()=>{
 const ui=setup();
 try{
  await login(ui);ui.d.getElementById('new-task').click();
  const form=ui.d.getElementById('create-form');
  for(const [name,value] of Object.entries({jobId:'rabbit-receipt',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))
   form.elements[name].value=value;
  ui.d.getElementById('next-step').click();
  form.elements.targetType.value='RABBITMQ';
  form.elements.targetType.dispatchEvent(new ui.w.Event('change'));
  form.elements.targetSchema.value='sales_events';
  const root=ui.d.getElementById('primary-messaging');
  const option=root.querySelector('[data-field=rabbitReceiptStream]');
  const admin=root.querySelector('[data-field=adminUrl]');
  assert.equal(option.checked,false);
  assert.equal(option.required,false);
  assert.equal(admin.disabled,true);
  option.checked=true;option.dispatchEvent(new ui.w.Event('change'));
  assert.equal(admin.disabled,false);
  assert.equal(admin.required,true);
  assert.equal(form.elements.targetPassword.required,true);
  admin.value='https://rabbit.example:15671';
  form.elements.targetUsername.value='publisher';form.elements.targetPassword.value='fixture-password';
  ui.d.getElementById('next-step').click();
  form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();
  const target=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body).config.targets[0];
  assert.equal(target.messaging.rabbitReceiptStream,true);
  assert.equal(target.messaging.adminUrl,'https://rabbit.example:15671');
 }finally{ui.dom.window.close();}
});

test('RocketMQ wizard collects its broker and retention profile and excludes unsupported credentials',async()=>{
  const ui=setup();try{
    await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');
    for(const [name,value]of Object.entries({jobId:'rocket',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();form.elements.targetType.value='ROCKETMQ';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales_events';const root=ui.d.getElementById('primary-messaging');
    root.querySelector('[data-field=cluster]').value='Business';root.querySelector('[data-field=broker]').value='broker-a';root.querySelector('[data-field=minimumRetentionHours]').value='87600';
    assert.equal(form.elements.targetUsername.disabled,true);assert.equal(root.querySelector('[data-field=token]').disabled,true);assert.equal(root.querySelector('[data-field=replicas]').max,'1');assert.match(root.textContent,/专用 Topic 前缀/);
    ui.d.getElementById('next-step').click();form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST').body).config.targets[0];
    assert.equal(target.product,'ROCKETMQ');assert.match(target.url,/^rocketmq:/);assert.equal(target.messaging.topic,'rivomesh_events');assert.equal(target.messaging.cluster,'Business');assert.equal(target.messaging.broker,'broker-a');assert.equal(target.messaging.minimumRetentionHours,87600);assert.equal('token' in target.messaging,false);assert.equal('adminUrl' in target.messaging,false);
    ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=ROCKETMQ]').disabled,true);
  }finally{ui.dom.window.close();}
});

test('RocketMQ wizard requires explicit trusted Controller endpoints for the two-copy profile',async()=>{
 const ui=setup();try{
  await login(ui);ui.d.getElementById('new-task').click();
  const form=ui.d.getElementById('create-form');
  for(const [name,value]of Object.entries({jobId:'rocket-ha',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;
  ui.d.getElementById('next-step').click();form.elements.targetType.value='ROCKETMQ';
  form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales_events';
  const root=ui.d.getElementById('primary-messaging'),trusted=root.querySelector('[data-field=rocketmqControllerEndpoints]'),
        copies=root.querySelector('[data-field=replicas]');
  assert.equal(trusted.required,false);assert.equal(copies.value,'1');
  trusted.value='http://controller-1:9877';
  ui.d.getElementById('next-step').click();
  assert.equal(ui.d.getElementById('step-2').classList.contains('hidden'),true);
  assert.match(trusted.validationMessage,/带端口/);
  trusted.value='rocketmq://controller-1:9877, rocketmq://controller-2:9877';
  trusted.dispatchEvent(new ui.w.Event('input'));
  assert.equal(copies.value,'2');assert.equal(copies.readOnly,true);
  ui.d.getElementById('next-step').click();
  form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();
  const config=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body).config;
  assert.equal(config.targets[0].messaging.replicas,2);
  assert.deepEqual(config.targets[0].messaging.rocketmqControllerEndpoints,['rocketmq://controller-1:9877','rocketmq://controller-2:9877']);
 }finally{ui.dom.window.close();}
});

test('Artemis wizard selects a retained queue profile and keeps broker authentication enabled',async()=>{
  const ui=setup();try{
    await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');for(const [name,value]of Object.entries({jobId:'artemis',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();form.elements.targetType.value='ACTIVEMQ_ARTEMIS';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales_events';form.elements.targetUsername.value='publisher';form.elements.targetPassword.value='private-artemis-password';const root=ui.d.getElementById('primary-messaging');assert.equal(form.elements.targetUsername.disabled,false);assert.equal(form.elements.targetUsername.required,true);assert.equal(root.querySelector('[data-field=broker]').required,false);assert.equal(root.querySelector('[data-field=stream]').value,'rivomesh');assert.match(root.textContent,/非破坏队列/);
    ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-artemis-password/);form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST').body).config.targets[0];assert.equal(target.product,'ACTIVEMQ_ARTEMIS');assert.equal(target.messaging.stream,'rivomesh');assert.equal(target.messaging.replicas,1);assert.equal('token' in target.messaging,false);assert.equal('topic' in target.messaging,false);assert.equal(target.password,'private-artemis-password');
  }finally{ui.dom.window.close();}
});

test('message wizard exposes its delivery contract, masks tokens, and keeps v2 SQL-only',async()=>{
  const ui=setup();try{
    await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');
    for(const [name,value]of Object.entries({jobId:'events',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();form.elements.targetType.value='NATS_JETSTREAM';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales_events';
    assert.equal(form.elements.targetUsername.required,false);assert.equal(form.elements.metadataSchema.disabled,true);assert.match(ui.d.getElementById('primary-messaging').textContent,/至少一次/);
    ui.d.querySelector('#primary-messaging [data-field=token]').value='private-nats-token';ui.d.querySelector('#primary-messaging [data-field=stream]').value='SALES';
    ui.d.getElementById('add-target').click();const card=ui.d.querySelector('#extra-targets .target-settings'),product=card.querySelector('[data-field=product]');product.value='RABBITMQ';product.dispatchEvent(new ui.w.Event('change'));
    card.querySelector('[data-field=username]').value='publisher';card.querySelector('.mapping-fields input').value='rabbit_sales';
    ui.d.getElementById('next-step').click();assert.equal(ui.d.getElementById('step-2').classList.contains('hidden'),false);assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-nats-token/);
    form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const config=JSON.parse(ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST').body).config;
    assert.equal(config.targets[0].product,'NATS_JETSTREAM');assert.equal(config.targets[0].messaging.token,'private-nats-token');assert.equal(config.targets[0].messaging.stream,'SALES');assert.equal(config.targets[1].product,'RABBITMQ');assert.equal(config.targets[1].messaging.queueType,'quorum');
    ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=NATS_JETSTREAM]').disabled,true);assert.equal(form.elements.targetType.querySelector('[value=RABBITMQ]').disabled,true);
  }finally{ui.dom.window.close();}
});

test('resume carries the precise error generation and untrusted names remain text',async()=>{
  const task={id:'safe',revision:'current-revision',desired:'PAUSED',workerState:'PAUSED',sourceCluster:'source',sourceDatabase:'<img src=x onerror=alert(1)>',targetSchema:'target',status:{run_state:'PAUSED',error_count:7,phase:'STREAMING',tables:[{table:'<script>bad()</script>',copiedRows:10,done:true}]},statusAvailable:true,problem:null,operations:[]};
  const ui=setup([task]);try{
    await login(ui);assert.equal(ui.d.querySelectorAll('#task-list img, #task-detail script').length,0);
    const resume=[...ui.d.querySelectorAll('.detail-actions button')].find(b=>b.textContent==='修复后恢复');assert.ok(resume);resume.click();await flush();
    const request=ui.calls.find(c=>c.path.endsWith('/actions'));
    assert.deepEqual(JSON.parse(request.body),{action:'RESUME',expectedRevision:'current-revision',errorCount:7});
    assert.match(request.headers['X-Request-ID'],/^ui_[a-f0-9]{32}$/);
  }finally{ui.dom.window.close();}
});


test('ClickHouse wizard exposes versioned visibility and keeps broker fields disabled',async()=>{
  const ui=setup();try{
    await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');for(const [name,value]of Object.entries({jobId:'analytics',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;
    ui.d.getElementById('next-step').click();form.elements.targetType.value='CLICKHOUSE';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales_copy';form.elements.targetUsername.value='writer';form.elements.targetPassword.value='private-clickhouse-password';const root=ui.d.getElementById('primary-messaging');assert.equal(form.elements.targetUrl.value,'http://127.0.0.1:8123');assert.equal(form.elements.metadataSchema.disabled,false);assert.equal(form.elements.targetUsername.disabled,false);assert.equal(root.querySelector('[data-field=stream]').disabled,true);assert.equal(root.querySelector('[data-field=replicas]').parentElement.classList.contains('hidden'),true);assert.match(root.querySelector('p').textContent,/版本化/);assert.match(root.querySelector('p').textContent,/分批可见/);
    ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-clickhouse-password/);form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST').body).config.targets[0];assert.equal(target.product,'CLICKHOUSE');assert.equal(target.clickhouse.secondaryIndexes,'DATA_SKIPPING');assert.equal('messaging' in target,false);assert.equal(target.password,'private-clickhouse-password');
    ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=CLICKHOUSE]').disabled,true);
  }finally{ui.dom.window.close();}
});


test('operation results expose index decisions as safe text',async()=>{
  const task={id:'index-plan',revision:'r1',desired:'STOPPED',workerState:'STOPPED',sourceCluster:'source',sourceDatabase:'sales',targetSchema:'copy',status:{},operations:[{action:'CHECK',status:'SUCCEEDED',code:'OK',result:{clickhouseIndexPlans:[{table:'<img src=x onerror=bad()>',plan:{decisions:[{action:'REQUIRES_PLAN',reason:'EXPRESSION_PREFIX_OR_METHOD_UNSUPPORTED'}]}}],password:'private-should-be-masked'}}]};
  const ui=setup([task]);try{await login(ui);assert.match(ui.d.querySelector('.history').textContent,/检查结果与操作详情/);assert.match(ui.d.querySelector('.history pre').textContent,/REQUIRES_PLAN/);assert.equal(ui.d.querySelector('.history img'),null);assert.doesNotMatch(ui.d.querySelector('.history pre').textContent,/private-should-be-masked/);}finally{ui.dom.window.close();}
});


test('Classic wizard requires broker/admin URL and explains consumer selector',async()=>{
 const ui=setup();try{await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');for(const[name,value]of Object.entries({jobId:'classic-copy',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;ui.d.getElementById('next-step').click();form.elements.targetType.value='ACTIVEMQ_CLASSIC';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales';form.elements.targetUsername.value='admin';form.elements.targetPassword.value='private-classic';const root=ui.d.getElementById('primary-messaging');assert.equal(form.elements.targetUrl.value,'activemq://127.0.0.1:61616');assert.match(root.querySelector('p').textContent,/EVENT/);assert.equal(root.querySelector('[data-field=broker]').required,true);assert.equal(root.querySelector('[data-field=adminUrl]').disabled,false);assert.equal(root.querySelector('[data-field=stream]').disabled,false);assert.equal(root.querySelector('[data-field=token]').disabled,true);assert.equal(root.querySelector('[data-field=replicas]').max,'1');ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-classic/);form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST').body).config.targets[0];assert.equal(target.messaging.broker,'rivomesh-core');assert.equal(target.messaging.stream,'rivomesh');assert.equal(target.messaging.adminUrl,'http://127.0.0.1:8161/api/jolokia/');}finally{ui.dom.window.close();}
});


function setupConfiguration(role='ADMIN'){
 const task={id:'editable',revision:'revision-original',desired:'STOPPED',workerState:'STOPPED',sourceCluster:'source',sourceDatabase:'sales',targetSchema:'copy',status:{},operations:[]},ui=setup([task],role),baseFetch=ui.w.fetch;
 const document={jobId:'editable',source:{username:'reader',password:{$rivomeshSecret:'opaque-source'}},target:{type:'POSTGRESQL',username:'writer',password:{$rivomeshSecret:'opaque-target'},url:{$rivomeshSecret:'opaque-url'}},chunkSize:1000};
 const control={migration:false,failSave:false,deferPreview:null,deferRead:null,restoreCalls:0};
 ui.w.fetch=async(path,options={})=>{
  if(!path.startsWith('/api/tasks/editable/configuration'))return baseFetch(path,options);ui.calls.push({path,...options});let data;
  if(path.endsWith('/restore/validate'))data={migrationRequired:false,changedPaths:['/source/password']};
  else if(path.endsWith('/restore')){control.restoreCalls++;task.revision='restored-revision';data=task;}
  else if(path.includes('/history'))data={versions:[{version:'old-version',reference:'signed-reference',action:'CREATE',createdAt:'2026-11-15T00:00:00Z',actorId:'<img src=x onerror=bad()>'}]};
  else if(path.endsWith('/validate')){if(control.deferPreview)return control.deferPreview();data={migrationRequired:control.migration,changedPaths:['/chunkSize','/source/password']};}
  else if(options.method==='PUT'){if(control.failSave){control.failSave=false;throw new Error('Lost response');}task.revision='new-revision';data=task;}
  else{if(control.deferRead)return control.deferRead();data={revision:task.revision,config:structuredClone(document)};}
  return{ok:true,status:200,json:async()=>data};
 };
 return Object.assign(ui,{task,configuration:control});
}
async function openConfiguration(ui){await login(ui);[...ui.d.querySelectorAll('.detail-actions button')].find(button=>button.textContent==='配置和历史').click();await flush();}

test('configuration editing preserves opaque credentials and requires preview before save',async()=>{
 const ui=setupConfiguration();try{await openConfiguration(ui);assert.equal(ui.d.getElementById('configuration-dialog').open,true);const secret=ui.d.querySelector('[data-configuration-secret="source.password"]');assert.equal(secret.value,'');assert.equal(secret.disabled,true);assert.equal(ui.d.getElementById('configuration-save').disabled,true);const mode=ui.d.querySelector('select[data-configuration-path="source.password"]');mode.value='REPLACE';mode.dispatchEvent(new ui.w.Event('change'));secret.value='new-private-password';secret.dispatchEvent(new ui.w.Event('input'));const chunk=ui.d.querySelector('[data-configuration-number="chunkSize"]');chunk.value='41';chunk.dispatchEvent(new ui.w.Event('input'));ui.d.getElementById('configuration-check').click();await flush();assert.equal(ui.d.getElementById('configuration-save').disabled,false);assert.doesNotMatch(ui.d.getElementById('configuration-preview').textContent,/new-private-password/);ui.d.getElementById('configuration-form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const put=ui.calls.find(call=>call.method==='PUT');assert.ok(put);const body=JSON.parse(put.body);assert.equal(body.expectedRevision,'revision-original');assert.equal(body.config.source.password,'new-private-password');assert.equal(body.config.chunkSize,41);assert.equal(body.config.target.password.$rivomeshSecret,'opaque-target');assert.equal(body.config.target.url.$rivomeshSecret,'opaque-url');assert.equal(ui.d.getElementById('configuration-dialog').open,false);assert.equal(ui.d.getElementById('configuration-document').value,'');assert.equal(ui.d.querySelector('[data-configuration-secret]'),null);}finally{ui.dom.window.close();}
});

test('migration previews and running tasks cannot apply configuration',async()=>{
 const ui=setupConfiguration();try{await openConfiguration(ui);ui.configuration.migration=true;ui.d.getElementById('configuration-check').click();await flush();assert.equal(ui.d.getElementById('configuration-save').disabled,true);assert.match(ui.d.getElementById('configuration-preview').textContent,/迁移计划/);ui.configuration.migration=false;ui.task.desired='RUNNING';ui.task.workerState='RUNNING';ui.d.getElementById('configuration-check').click();await flush();assert.equal(ui.d.getElementById('configuration-save').disabled,true);ui.d.getElementById('configuration-form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();assert.equal(ui.calls.some(call=>call.method==='PUT'),false);}finally{ui.dom.window.close();}
});

test('old preview responses cannot overwrite a newer editor draft',async()=>{
 const ui=setupConfiguration();try{await openConfiguration(ui);let resolve;ui.configuration.deferPreview=()=>new Promise(done=>resolve=done);ui.d.getElementById('configuration-check').click();await flush();const chunk=ui.d.querySelector('[data-configuration-number="chunkSize"]');chunk.value='77';chunk.dispatchEvent(new ui.w.Event('input'));resolve({ok:true,status:200,json:async()=>({migrationRequired:false,changedPaths:['old']})});await flush();assert.equal(ui.d.getElementById('configuration-save').disabled,true);assert.equal(JSON.parse(ui.d.getElementById('configuration-document').value).chunkSize,77);}finally{ui.dom.window.close();}
});

test('save retries retain the same request identity and restore requires reviewing changes',async()=>{
 const ui=setupConfiguration();try{await openConfiguration(ui);ui.d.getElementById('configuration-check').click();await flush();ui.configuration.failSave=true;ui.d.getElementById('configuration-form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();assert.equal(ui.d.getElementById('configuration-save').disabled,false);ui.d.getElementById('configuration-form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const puts=ui.calls.filter(call=>call.method==='PUT');assert.equal(puts.length,2);assert.equal(puts[0].headers['X-Request-ID'],puts[1].headers['X-Request-ID']);assert.equal(puts[0].body,puts[1].body);await openConfiguration(ui);assert.equal(ui.d.querySelector('#configuration-history img'),null);ui.d.querySelector('#configuration-history button').click();await flush();assert.equal(ui.configuration.restoreCalls,0);assert.match(ui.d.getElementById('configuration-preview').textContent,/old-version/);ui.d.getElementById('configuration-form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();assert.equal(ui.configuration.restoreCalls,1);}finally{ui.dom.window.close();}
});

test('logout and close fence late configuration responses and erase drafts',async()=>{
 const ui=setupConfiguration();try{await login(ui);let resolve;ui.configuration.deferRead=()=>new Promise(done=>resolve=done);[...ui.d.querySelectorAll('.detail-actions button')].find(button=>button.textContent==='配置和历史').click();await flush();ui.d.getElementById('logout').click();resolve({ok:true,status:200,json:async()=>({revision:'old',config:{source:{password:'should-never-reappear'}}})});await flush();assert.equal(ui.d.getElementById('configuration-dialog').open,false);assert.equal(ui.d.getElementById('configuration-document').value,'');assert.equal(ui.d.getElementById('configuration-fields').children.length,0);}finally{ui.dom.window.close();}
});


function auditPage(after=0,through=2,next=1){return{after,through,next,anchorSequence:0,anchorHash:'0'.repeat(64),previousHash:'a'.repeat(64),throughHash:'b'.repeat(64),lastSequence:through,lastHash:'b'.repeat(64),retentionEvents:1000,events:[{sequence:after+1,at:'2026-11-15T00:00:00Z',id:'audit-event',kind:'HTTP_END',action:'GET_CONFIGURATION',actorId:'<img src=x onerror=alert(1)>',actorRole:'ADMIN',taskId:'task',httpStatus:403,status:'REJECTED',code:'FORBIDDEN',correlation:'request'}]};}
test('audit permission, safe rendering, fixed-window pagination and page export',async()=>{
 for(const role of ['VIEWER','OPERATOR']){const ui=setup([],role);try{await login(ui);assert.equal(ui.d.getElementById('audit-nav').disabled,true);ui.d.getElementById('audit-nav').click();assert.equal(ui.d.getElementById('audit-dialog').open,false);}finally{ui.dom.window.close();}}
 const ui=setup();try{await login(ui);const original=ui.w.fetch;ui.w.fetch=async(path,options)=>{if(path.startsWith('/api/audit')){ui.calls.push({path,...options});return{ok:true,status:200,json:async()=>path.includes('after=1')?auditPage(1,2,null):auditPage()};}return original(path,options);};
  ui.d.getElementById('audit-nav').click();await flush();assert.equal(ui.d.getElementById('audit-dialog').open,true);assert.match(ui.d.getElementById('audit-events').textContent,/<img src=x/);assert.equal(ui.d.querySelector('#audit-events img'),null);
  ui.d.getElementById('audit-next').click();await flush();assert.ok(ui.calls.some(call=>call.path==='/api/audit?after=1&through=2&limit=100'));assert.equal(ui.d.getElementById('audit-next').disabled,true);
  let blobs=0;ui.w.URL.createObjectURL=()=>{blobs++;return'blob:audit';};ui.w.URL.revokeObjectURL=()=>{};ui.w.HTMLAnchorElement.prototype.click=function(){};ui.d.getElementById('audit-export').click();await flush();assert.equal(blobs,1);assert.ok(ui.calls.some(call=>call.path==='/api/audit/export?after=1&through=2&limit=100'));
  ui.d.getElementById('language').click();assert.match(ui.d.getElementById('audit-dialog').textContent,/Security audit/);
 }finally{ui.dom.window.close();}
});
test('late audit responses after logout cannot restore security records',async()=>{
 const ui=setup();try{await login(ui);const original=ui.w.fetch;let resolve;ui.w.fetch=(path,options)=>path.startsWith('/api/audit')?new Promise(done=>resolve=done):original(path,options);ui.d.getElementById('audit-nav').click();await flush();ui.d.getElementById('logout').click();resolve({ok:true,status:200,json:async()=>auditPage()});await flush();assert.equal(ui.d.getElementById('audit-dialog').open,false);assert.equal(ui.d.getElementById('audit-events').textContent,'');assert.equal(ui.d.getElementById('audit-proof').textContent,'');}finally{ui.dom.window.close();}
});
test('audit export refuses a changed retention boundary',async()=>{
 const ui=setup();try{await login(ui);const original=ui.w.fetch;ui.w.fetch=async(path,options)=>path.startsWith('/api/audit')?{ok:true,status:200,json:async()=>path.includes('/export')?{...auditPage(),after:1}:auditPage()}:original(path,options);ui.d.getElementById('audit-nav').click();await flush();let blobs=0;ui.w.URL.createObjectURL=()=>{blobs++;return'blob:bad';};ui.d.getElementById('audit-export').click();await flush();assert.equal(blobs,0);assert.match(ui.d.getElementById('audit-error').textContent,/窗口已变化/);}finally{ui.dom.window.close();}
});


test('scoped session displays exact grant count and clears grants on logout',async()=>{
 const ui=setup([], 'OPERATOR');try{const original=ui.w.fetch;ui.w.fetch=async(path,options)=>{const response=await original(path,options);return path==='/api/session'&&response.ok?{...response,json:async()=>({...await response.json(),id:'scoped-user',taskScope:['sales-migration','orders-migration']})}:response;};await login(ui);assert.match(ui.d.getElementById('session-info').textContent,/限定任务数 2/);assert.match(ui.d.getElementById('task-list').textContent,/暂无可见任务/);assert.equal(ui.d.getElementById('session-info').title,'sales-migration, orders-migration');ui.d.getElementById('language').click();assert.match(ui.d.getElementById('session-info').textContent,/Granted tasks 2/);ui.d.getElementById('logout').click();assert.equal(ui.d.getElementById('session-info').title,'');assert.equal(ui.d.getElementById('session-info').textContent,'');}finally{ui.dom.window.close();}
});


test('Doris wizard configures native analytical writes, replicas and explicit redirects',async()=>{
 const ui=setup();try{await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');for(const[name,value]of Object.entries({jobId:'doris-copy',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;ui.d.getElementById('next-step').click();form.elements.targetType.value='DORIS';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales_copy';form.elements.targetUsername.value='writer';form.elements.targetPassword.value='private-doris-password';const root=ui.d.getElementById('primary-messaging');assert.match(form.elements.targetUrl.value,/:9030/);assert.equal(root.querySelector('[data-field=dorisReplicas]').value,'3');root.querySelector('[data-field=dorisReplicas]').value='1';root.querySelector('[data-field=dorisRedirectOrigins]').value='http://be1:8040, http://be2:8040';assert.equal(root.querySelector('[data-field=stream]').disabled,true);assert.match(root.querySelector('p').textContent,/Doris/);ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-doris-password/);form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body).config.targets[0];assert.equal(target.product,'DORIS');assert.equal(target.doris.loadUrl,'http://127.0.0.1:8040');assert.equal(target.doris.replicas,1);assert.equal(target.doris.buckets,4);assert.equal(target.doris.secondaryIndexes,'INVERTED');assert.deepEqual(target.doris.redirectOrigins,['http://be1:8040','http://be2:8040']);assert.equal('messaging' in target,false);ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=DORIS]').disabled,true);}finally{ui.dom.window.close();}
});


test('OceanBase wizard requires v3 and keeps tenant JDBC configuration separate from brokers',async()=>{
 const ui=setup();try{await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');for(const[name,value]of Object.entries({jobId:'oceanbase-copy',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;ui.d.getElementById('next-step').click();form.elements.targetType.value='OCEANBASE';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales_copy';form.elements.targetUsername.value='writer@business';form.elements.targetPassword.value='private-ob-password';const root=ui.d.getElementById('primary-messaging');assert.match(form.elements.targetUrl.value,/:2881/);assert.equal(root.querySelector('[data-field=stream]').disabled,true);assert.match(root.querySelector('p').textContent,/租户/);ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-ob-password/);form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body).config.targets[0];assert.equal(target.product,'OCEANBASE');assert.equal(target.username,'writer@business');assert.equal('messaging' in target,false);assert.equal('doris' in target,false);ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=OCEANBASE]').disabled,true);}finally{ui.dom.window.close();}
});

test('Hive wizard requires an explicit index-omission acknowledgement and v3',async()=>{
 const ui=setup();try{await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');for(const[name,value]of Object.entries({jobId:'hive-copy',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;ui.d.getElementById('next-step').click();form.elements.targetType.value='HIVE';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales_copy';form.elements.targetUsername.value='writer';form.elements.targetPassword.value='private-hive-password';const root=ui.d.getElementById('primary-messaging'),ack=root.querySelector('[data-field=hiveAcknowledgement]');assert.match(form.elements.targetUrl.value,/jdbc:hive2:/);assert.equal(ack.checked,false);assert.equal(ack.required,true);assert.equal(root.querySelector('[data-field=stream]').disabled,true);ui.d.getElementById('next-step').click();assert.equal(ui.d.getElementById('step-2').classList.contains('hidden'),true);ack.checked=true;root.querySelector('[data-field=hiveTimeout]').value='600';ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-hive-password/);form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body).config.targets[0];assert.equal(target.product,'HIVE');assert.equal(target.hive.acknowledgeIndexOmission,true);assert.equal(target.hive.queryTimeoutSeconds,600);assert.equal('messaging' in target,false);assert.equal('doris' in target,false);ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=HIVE]').disabled,true);}finally{ui.dom.window.close();}
});

test('Hive cache failure explains safe recovery in both languages',async()=>{
 const task={id:'hive-cache',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',sourceCluster:'source',sourceDatabase:'sales',targetSchema:'copy',status:{run_state:'RUNNING',lastSequence:4,subscriptions:[{id:'warehouse',state:'PAUSED',acknowledged:3,errorGeneration:1,errorCode:'HIVE_QUERY_CACHE_DISABLED_REQUIRED'}]},statusAvailable:true,operations:[]};
 const ui=setup([task]);try{await login(ui);assert.match(ui.d.querySelector('.subscription-card').textContent,/服务端默认和所有读取会话/);assert.match(ui.d.querySelector('.subscription-card').textContent,/不要重置断点/);ui.d.getElementById('language').click();assert.match(ui.d.querySelector('.subscription-card').textContent,/server default and every reader/);assert.match(ui.d.querySelector('.subscription-card').textContent,/Do not reset checkpoints/);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');form.elements.targetType.value='HIVE';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));assert.match(ui.d.getElementById('primary-messaging').querySelector('p').textContent,/hive.query.results.cache.enabled=false/);}finally{ui.dom.window.close();}
});

function setupAccounts(mode='MANAGED'){
 const ui=setup(),base=ui.w.fetch;let revision=1;const control={lose:false,defer:null,calls:0,invalidate:false,shared:false};let page={mode,revision:'accounts-1',version:1,users:[{id:'test-user',role:'ADMIN',enabled:true,deleted:false,taskIds:null},{id:'limited',role:'VIEWER',enabled:true,deleted:false,taskIds:['future-task']}]};
 ui.w.fetch=async(path,options={})=>{
  if(!path.startsWith('/api/accounts'))return base(path,options);ui.calls.push({path,...options});if(!options.method&&control.defer)return control.defer();
  if(options.method){
   control.calls++;
   if(control.lose){control.lose=false;throw new Error('Lost response');}
   if(control.reject)return{ok:false,status:409,json:async()=>({code:control.reject})};
   const body=JSON.parse(options.body);
   if(control.invalidate){ui.browser.authenticated=false;if(path.endsWith('/token'))ui.browser.token=body.token;}
   page={...page,mode:'MANAGED',revision:'accounts-'+(++revision),version:revision};
   if(path==='/api/accounts'&&options.method==='POST')page.users=[...page.users,{...body,token:undefined,enabled:true,deleted:false}];
   if(path.endsWith('/external-identity')){
    const user=page.users.find(user=>user.id===path.split('/')[3]);
    if(options.method==='PUT')user.externalIdentity={issuer:body.issuer,subjectSha256:'a'.repeat(64)};
    else delete user.externalIdentity;
   }
  }
  return{ok:true,status:200,json:async()=>structuredClone({...page,authority:control.shared?'SHARED':'LOCAL',
   externalIdentityIssuer:control.externalIssuer,users:page.users.map(user=>user.id==='limited'&&control.nodeIds!==undefined?{...user,nodeIds:control.nodeIds}:user)})};
 };return Object.assign(ui,{accounts:control});
}
async function openAccounts(ui){await login(ui);ui.d.getElementById('accounts-nav').click();await flush();}
function accountAction(ui,label){
 const card=ui.d.querySelectorAll('#accounts-list .subscription-card')[1];
 return [...card.querySelectorAll('button')].find(button=>button.textContent===label);
}
function submitAccount(ui){ui.d.getElementById('account-form').dispatchEvent(new ui.w.Event('submit',{cancelable:true,bubbles:true}));}
test('an administrator binds and revokes an organization identity with the current directory revision and CSRF',async()=>{
 const ui=setupAccounts();
 try{
  const issuer='https://identity.example.test/tenant',subject='department/Person:+Case';
  ui.accounts.shared=true;
  ui.accounts.externalIssuer=issuer;
  await openAccounts(ui);
  assert.equal(accountAction(ui,'撤销组织身份'),undefined);
  accountAction(ui,'绑定组织身份').click();
  const input=ui.d.getElementById('account-external-subject'),source=ui.d.getElementById('account-external-issuer');
  assert.equal(input.type,'password');
  assert.equal(source.readOnly,true);
  assert.equal(source.value,issuer);
  submitAccount(ui);
  assert.match(ui.d.getElementById('account-error').textContent,/1 至 255/);
  assert.equal(ui.accounts.calls,0);
  input.value=subject;
  input.dispatchEvent(new ui.w.Event('input'));
  const preview=JSON.parse(ui.d.getElementById('account-preview').textContent);
  assert.equal(preview.issuer,issuer);
  assert.equal(preview.subjectProvided,true);
  assert.equal('subject' in preview,false);
  ui.accounts.lose=true;
  submitAccount(ui);
  await flush();
  assert.match(ui.d.getElementById('account-error').textContent,/结果尚未确认/);
  submitAccount(ui);
  await flush();
  const requests=ui.calls.filter(req=>req.path.endsWith('/external-identity'));
  assert.equal(requests.length,2);
  for(const req of requests){
   assert.equal(req.method,'PUT');
   assert.equal(req.headers['X-CSRF-Token'],'test-csrf');
   assert.deepEqual(JSON.parse(req.body),{expectedRevision:'accounts-1',issuer,subject});
  }
  assert.equal(requests[0].headers['X-Request-ID'],requests[1].headers['X-Request-ID']);
  assert.equal(input.value,'');
  assert.equal(ui.d.getElementById('account-preview').textContent,'');
  assert.equal(ui.d.getElementById('accounts-list').textContent.includes(subject),false);
  const identities=[...ui.d.querySelectorAll('.account-external-identity')].map(node=>node.textContent);
  assert.equal(identities.length,2);
  assert.ok(identities[0].includes(issuer));
  assert.ok(identities[1].includes('a'.repeat(64)));
  assert.equal(ui.w.localStorage.length,1);
  assert.equal(ui.w.sessionStorage.length,0);
  ui.d.getElementById('language').click();
  assert.ok(accountAction(ui,'Replace organization identity'));
  accountAction(ui,'Revoke organization identity').click();
  assert.equal(ui.d.getElementById('account-external-bind').classList.contains('hidden'),true);
  assert.match(ui.d.getElementById('account-external-revoke').textContent,/Local tokens stay valid/);
  submitAccount(ui);
  await flush();
  const revoke=ui.calls.find(req=>req.method==='DELETE');
  assert.equal(revoke.path,'/api/accounts/limited/external-identity');
  assert.equal(revoke.headers['X-CSRF-Token'],'test-csrf');
  assert.deepEqual(JSON.parse(revoke.body),{expectedRevision:'accounts-2'});
  assert.notEqual(revoke.headers['X-Request-ID'],requests[1].headers['X-Request-ID']);
  assert.equal(ui.d.querySelectorAll('.account-external-identity').length,0);
  assert.ok(accountAction(ui,'Bind organization identity'));
 }finally{ui.dom.window.close();}
});
test('organization identity controls handle unavailable login, stale revision and global sign-out',async()=>{
 const ui=setupAccounts();
 try{
  await openAccounts(ui);
  assert.equal(accountAction(ui,'绑定组织身份'),undefined);
  ui.accounts.shared=true;
  ui.d.getElementById('accounts-reload').click();
  await flush();
  assert.equal(accountAction(ui,'绑定组织身份'),undefined);
  ui.accounts.externalIssuer='https://identity.example.test/tenant';
  ui.d.getElementById('accounts-reload').click();
  await flush();
  accountAction(ui,'绑定组织身份').click();
  const input=ui.d.getElementById('account-external-subject');
  input.value='fixture-operator';
  ui.accounts.reject='ACCOUNTS_REVISION_CHANGED';
  submitAccount(ui);
  await flush();
  assert.match(ui.d.getElementById('account-error').textContent,/账号目录已被其他管理员修改/);
  ui.d.getElementById('accounts-reload').click();
  await flush();
  assert.equal(input.value,'');
  delete ui.accounts.reject;
  accountAction(ui,'绑定组织身份').click();
  input.value='fixture-operator';
  submitAccount(ui);
  await flush();
  delete ui.accounts.externalIssuer;
  ui.d.getElementById('accounts-reload').click();
  await flush();
  assert.equal(accountAction(ui,'替换组织身份'),undefined);
  assert.ok(ui.d.querySelector('.account-external-identity'));
  accountAction(ui,'撤销组织身份').click();
  ui.accounts.invalidate=true;
  submitAccount(ui);
  await flush();
  assert.equal(ui.d.getElementById('accounts-dialog').open,false);
  assert.equal(ui.d.getElementById('accounts-list').textContent,'');
  assert.equal(input.value,'');
  assert.equal(ui.d.getElementById('login-dialog').open,true);
 }finally{ui.dom.window.close();}
});
test('account activation is explicit and newly generated tokens stay out of previews and storage',async()=>{
 const ui=setupAccounts('CONFIG');try{await openAccounts(ui);assert.equal(ui.d.getElementById('accounts-new').disabled,true);assert.equal(ui.d.getElementById('accounts-activate').disabled,true);ui.d.getElementById('accounts-activation-ack').checked=true;ui.d.getElementById('accounts-activation-ack').dispatchEvent(new ui.w.Event('change'));ui.d.getElementById('accounts-activate').click();await flush();assert.equal(ui.d.getElementById('accounts-new').disabled,false);ui.d.getElementById('accounts-new').click();ui.d.getElementById('account-id').value='new-viewer';ui.d.getElementById('account-generate').click();const secret=ui.d.getElementById('account-token').value;assert.match(secret,/^[a-f0-9]{64}$/);assert.equal(ui.d.getElementById('account-token').type,'password');assert.doesNotMatch(ui.d.getElementById('account-preview').textContent,new RegExp(secret));ui.d.getElementById('account-form').dispatchEvent(new ui.w.Event('submit',{cancelable:true,bubbles:true}));await flush();assert.equal(ui.calls.filter(c=>c.path==='/api/accounts'&&c.method==='POST').length,0);ui.d.getElementById('account-token-saved').checked=true;ui.d.getElementById('account-form').dispatchEvent(new ui.w.Event('submit',{cancelable:true,bubbles:true}));await flush();const call=ui.calls.find(c=>c.path==='/api/accounts'&&c.method==='POST');assert.equal(JSON.parse(call.body).token,secret);assert.deepEqual(JSON.parse(call.body).taskIds,[]);assert.equal(ui.d.getElementById('account-token').value,'');assert.equal(ui.w.localStorage.length,1);assert.equal(ui.w.sessionStorage.length,0);assert.doesNotMatch(ui.d.getElementById('accounts-list').textContent,new RegExp(secret));}finally{ui.dom.window.close();}
});
test('account grants retain unloaded IDs and lost-response retries retain the request identity',async()=>{
 const ui=setupAccounts();try{await openAccounts(ui);const cards=ui.d.querySelectorAll('#accounts-list .subscription-card');cards[1].querySelector('button').click();assert.equal(ui.d.getElementById('account-extra-tasks').value,'future-task');ui.d.getElementById('account-extra-tasks').value='future-task\nnew-task';ui.accounts.lose=true;ui.d.getElementById('account-form').dispatchEvent(new ui.w.Event('submit',{cancelable:true,bubbles:true}));await flush();assert.match(ui.d.getElementById('account-error').textContent,/结果尚未确认/);ui.d.getElementById('account-form').dispatchEvent(new ui.w.Event('submit',{cancelable:true,bubbles:true}));await flush();const puts=ui.calls.filter(c=>c.method==='PUT');assert.equal(puts.length,2);assert.equal(puts[0].body,puts[1].body);assert.equal(puts[0].headers['X-Request-ID'],puts[1].headers['X-Request-ID']);assert.deepEqual(JSON.parse(puts[0].body).taskIds,['future-task','new-task']);assert.equal(ui.d.getElementById('account-form').classList.contains('hidden'),true);}finally{ui.dom.window.close();}
});
test('accounts close clears credentials and old directory responses cannot reopen the view',async()=>{
 const ui=setupAccounts();try{await openAccounts(ui);ui.d.getElementById('accounts-new').click();ui.d.getElementById('account-generate').click();ui.d.getElementById('logout').click();assert.equal(ui.d.getElementById('account-token').value,'');assert.equal(ui.d.getElementById('accounts-list').textContent,'');assert.equal(ui.d.getElementById('accounts-dialog').open,false);await login(ui);let resolve;ui.accounts.defer=()=>new Promise(done=>resolve=done);ui.d.getElementById('accounts-nav').click();await flush();ui.d.getElementById('accounts-close').click();resolve({ok:true,status:200,json:async()=>({mode:'MANAGED',revision:'late',users:[{id:'late-user',role:'ADMIN',taskIds:null}]})});await flush();assert.equal(ui.d.getElementById('accounts-dialog').open,false);assert.equal(ui.d.getElementById('accounts-list').textContent,'');}finally{ui.dom.window.close();}
});
test('only administrators see account controls and languages retain unsaved token masking',async()=>{
 const viewer=setup([],'VIEWER');try{await login(viewer);assert.equal(viewer.d.getElementById('accounts-nav').disabled,true);viewer.d.getElementById('accounts-nav').click();assert.equal(viewer.d.getElementById('accounts-dialog').open,false);}finally{viewer.dom.window.close();}
 const ui=setupAccounts();try{await openAccounts(ui);ui.d.getElementById('accounts-new').click();ui.d.getElementById('account-generate').click();const secret=ui.d.getElementById('account-token').value;ui.d.getElementById('language').click();assert.match(ui.d.getElementById('accounts-dialog').textContent,/Accounts and grants/);assert.equal(ui.d.getElementById('account-token').value,secret);assert.doesNotMatch(ui.d.getElementById('accounts-dialog').textContent,new RegExp(secret));}finally{ui.dom.window.close();}
});

test('account changes show their subject in the audit view without interpreting markup',async()=>{
 const ui=setup();try{const base=ui.w.fetch;ui.w.fetch=async(path,options)=>path.startsWith('/api/audit')?{ok:true,status:200,json:async()=>({events:[{sequence:1,at:'2026-11-15',id:'event',kind:'ACCOUNT_COMMITTED',action:'ACCOUNT_UPDATE',actorId:'admin',subjectId:'<img src=x onerror=bad()>',status:'COMMITTED'}],after:0,through:1,next:null})}:base(path,options);await login(ui);ui.d.getElementById('audit-nav').click();await flush();assert.match(ui.d.getElementById('audit-events').textContent,/<img src=x onerror=bad\(\)>/);assert.equal(ui.d.querySelector('#audit-events img'),null);}finally{ui.dom.window.close();}
});


test('Kingbase wizard is v3-only, uses the vendor URL and preserves explicit mappings',async()=>{
 const ui=setup();try{
  await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');
  for(const[name,value]of Object.entries({jobId:'kingbase-copy',clusterId:'source',sourceDatabase:'sales, billing',sourceUsername:'reader'}))form.elements[name].value=value;
  ui.d.getElementById('next-step').click();form.elements.targetType.value='KINGBASE';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));
  form.elements.targetSchema.value='sales_copy';form.elements.targetUsername.value='system';form.elements.targetPassword.value='private-kingbase-password';
  const root=ui.d.getElementById('primary-messaging');assert.match(form.elements.targetUrl.value,/^jdbc:kingbase8:.*54321/);assert.equal(root.querySelector('[data-field=stream]').disabled,true);assert.match(root.querySelector('p').textContent,/PG.*实验/);
  ui.d.querySelector('#first-extra-mappings input').value='billing_copy';ui.d.getElementById('add-target').click();const extra=ui.d.querySelector('#extra-targets .target-settings'),select=extra.querySelector('[data-field=product]');select.value='KINGBASE';select.dispatchEvent(new ui.w.Event('change'));assert.match(extra.querySelector('[data-field=url]').value,/^jdbc:kingbase8:/);extra.remove();
  ui.d.getElementById('language').click();assert.match(root.querySelector('p').textContent,/Experimental Kingbase/);
  ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-kingbase-password/);
  form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body).config.targets[0];assert.equal(target.product,'KINGBASE');assert.deepEqual(target.mappings,{sales:'sales_copy',billing:'billing_copy'});assert.equal('messaging' in target,false);
  ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=KINGBASE]').disabled,true);
 }finally{ui.dom.window.close();}
});

test('DM wizard exposes its lossless text plan and requires acknowledgement before submit',async()=>{
 const ui=setup();try{
  await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');
  for(const[name,value]of Object.entries({jobId:'dm-copy',clusterId:'source',sourceDatabase:'sales',sourceUsername:'reader'}))form.elements[name].value=value;
  ui.d.getElementById('next-step').click();form.elements.targetType.value='DM';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));form.elements.targetSchema.value='sales_copy';form.elements.targetUsername.value='writer';form.elements.targetPassword.value='private-dm-password';
  const root=ui.d.getElementById('primary-messaging'),ack=root.querySelector('[data-field=dmAcknowledgement]');assert.match(form.elements.targetUrl.value,/^jdbc:dm:.*5236/);assert.equal(ack.disabled,false);assert.equal(ack.checked,false);assert.equal(ack.required,true);assert.match(root.querySelector('p').textContent,/38位/);assert.equal(root.querySelector('[data-field=stream]').disabled,true);
  ui.d.getElementById('next-step').click();assert.equal(ui.d.getElementById('step-2').classList.contains('hidden'),true);ack.checked=true;ui.d.getElementById('language').click();assert.match(root.querySelector('p').textContent,/First DM profile/);
  ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-dm-password/);form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(c=>c.path==='/api/tasks'&&c.method==='POST').body).config.targets[0];assert.equal(target.product,'DM');assert.equal(target.dm.acknowledgeTextConversions,true);assert.equal('messaging'in target,false);
  ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=DM]').disabled,true);
 }finally{ui.dom.window.close();}
});

test('GBase 8c wizard is v3-only, uses the vendor URL and preserves explicit mappings',async()=>{
 const ui=setup();try{
  await login(ui);ui.d.getElementById('new-task').click();const form=ui.d.getElementById('create-form');
  for(const[name,value]of Object.entries({jobId:'gbase8c-copy',clusterId:'source',sourceDatabase:'sales, billing',sourceUsername:'reader'}))form.elements[name].value=value;
  ui.d.getElementById('next-step').click();form.elements.targetType.value='GBASE_8C';form.elements.targetType.dispatchEvent(new ui.w.Event('change'));
  form.elements.targetSchema.value='sales_copy';form.elements.targetUsername.value='system';form.elements.targetPassword.value='private-gbase8c-password';
  const root=ui.d.getElementById('primary-messaging');assert.match(form.elements.targetUrl.value,/^jdbc:gbase8c:.*5432/);assert.equal(root.querySelector('[data-field=stream]').disabled,true);assert.match(root.querySelector('p').textContent,/PG.*实验/);
  ui.d.querySelector('#first-extra-mappings input').value='billing_copy';ui.d.getElementById('add-target').click();const extra=ui.d.querySelector('#extra-targets .target-settings'),select=extra.querySelector('[data-field=product]');select.value='GBASE_8C';select.dispatchEvent(new ui.w.Event('change'));assert.match(extra.querySelector('[data-field=url]').value,/^jdbc:gbase8c:/);extra.remove();
  ui.d.getElementById('language').click();assert.match(root.querySelector('p').textContent,/Experimental GBase 8c/);
  ui.d.getElementById('next-step').click();assert.doesNotMatch(ui.d.getElementById('plan-preview').textContent,/private-gbase8c-password/);
  form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const target=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body).config.targets[0];assert.equal(target.product,'GBASE_8C');assert.deepEqual(target.mappings,{sales:'sales_copy',billing:'billing_copy'});assert.equal('messaging' in target,false);
  ui.d.getElementById('new-task').click();form.elements.formatVersion.value='2';form.elements.formatVersion.dispatchEvent(new ui.w.Event('change'));assert.equal(form.elements.targetType.querySelector('[value=GBASE_8C]').disabled,true);
 }finally{ui.dom.window.close();}
});

function repairFixture(role='ADMIN'){
 const task={id:'repair-ui',revision:'current-revision',formatVersion:3,desired:'PAUSED',workerState:'PAUSED',status:{run_state:'PAUSED'},destinations:[{id:'pg',product:'POSTGRESQL',consistency:'TRANSACTIONAL'}],operations:[{id:'validate',action:'VALIDATE',status:'SUCCEEDED',code:'VALIDATION_DIFFERENCES',validation:{reportId:'report'}},{id:'plan',action:'REPAIR_PLAN',status:'SUCCEEDED',code:'REPAIR_PLAN_READY',repair:{reportId:'report',planId:'plan'}}]},ui=setup([task],role),fallback=ui.w.fetch;
 ui.w.fetch=async(path,options={})=>{let data;
  if(path==='/api/tasks/repair-ui/validations/report')data={state:'COMPLETE',code:'VALIDATION_DIFFERENCES',sourceComplete:true,tables:[{id:'table',database:'db',name:'<img src=x>'}],results:{table:{target:'pg',tableId:'table',sourceRows:2,targetRows:2,missing:1,extra:1,changed:1}}};
  else if(path==='/api/tasks/repair-ui/validations/report/repairs/plan')data={id:'plan',reportId:'report',hash:'a'.repeat(64),createdBy:'<script>operator</script>',createdAt:'time',tables:{table:{target:'pg',tableId:'table',missing:1,extra:1,changed:1}},tableNames:{table:'db.<img src=x>'},maximumAffectedRows:100000,maximumProbeBytes:1048576,maximumMillis:600000};
  else return fallback(path,options);ui.calls.push({path,...options});return{ok:true,status:200,json:async()=>data};
 };return ui;
}
test('repair planning requires explicit scope and carries the current task revision',async()=>{
 const ui=repairFixture();try{await login(ui);ui.d.querySelector('[data-validation-report]').click();await flush();ui.d.querySelector('[data-repair-action="plan"]').click();await flush();assert.equal(ui.calls.filter(c=>c.method==='POST').length,0);const select=ui.d.querySelector('[data-repair-scope]');select.checked=true;select.dispatchEvent(new ui.w.Event('change'));ui.d.querySelector('[data-repair-action="plan"]').click();await flush();const call=ui.calls.find(c=>c.method==='POST');const body=JSON.parse(call.body);assert.equal(body.action,'REPAIR_PLAN');assert.deepEqual(body.repair.scope,['pg/table']);assert.equal(body.expectedRevision,'current-revision');assert.ok(call.headers['X-Request-ID']);}finally{ui.dom.window.close();}
});
test('repair execution requires a freshly reviewed plan and explicit confirmation without HTML execution',async()=>{
 const ui=repairFixture();try{await login(ui);assert.equal(ui.d.querySelector('[data-repair-action="execute"]'),null);ui.d.querySelector('[data-repair-plan]').click();await flush();assert.equal(ui.d.querySelector('[data-repair-action="execute"]').disabled,true);assert.equal(ui.d.querySelector('.repair-review').querySelectorAll('img,script').length,0);assert.match(ui.d.querySelector('.repair-review').textContent,/<script>operator<\/script>/);const confirm=ui.d.querySelector('[data-repair-confirm]');confirm.checked=true;confirm.dispatchEvent(new ui.w.Event('change'));assert.equal(ui.d.querySelector('[data-repair-action="execute"]').disabled,false);ui.d.querySelector('[data-repair-action="execute"]').click();await flush();const body=JSON.parse(ui.calls.find(c=>c.method==='POST').body);assert.equal(body.action,'REPAIR_EXECUTE');assert.equal(body.repair.approvedHash,'a'.repeat(64));assert.equal(body.repair.planId,'plan');assert.equal(body.expectedRevision,'current-revision');ui.d.getElementById('logout').click();await login(ui);assert.equal(ui.d.querySelector('.repair-review'),null);}finally{ui.dom.window.close();}
});
test('restricted roles cannot select, review, confirm or cancel a repair',async()=>{
 for(const role of ['VIEWER','OPERATOR']){const ui=repairFixture(role);try{await login(ui);ui.d.querySelector('[data-validation-report]').click();await flush();assert.equal(ui.d.querySelector('[data-repair-scope]'),null);assert.equal(ui.d.querySelector('[data-repair-plan]'),null);assert.equal(ui.d.querySelector('[data-repair-action]'),null);}finally{ui.dom.window.close();}}
});

test('refresh and another page restore a cookie session without resubmitting the access token',async()=>{
 const browser={authenticated:false},first=setup([], 'ADMIN',browser);try{
  await login(first);assert.equal(first.d.getElementById('login-dialog').open,false);
  assert.equal(first.authCalls.filter(c=>c.method==='POST'&&c.path==='/api/session').length,1);
  const restored=setup([], 'ADMIN',browser);try{
   await flush();assert.equal(restored.d.getElementById('login-dialog').open,false);assert.match(restored.d.getElementById('session-info').textContent,/管理员/);
   assert.equal(restored.authCalls.some(c=>c.method==='POST'),false);assert.equal(restored.calls.some(c=>c.headers.Authorization),false);
   assert.equal(restored.authCalls.filter(c=>c.path==='/api/session').length,1);
   assert.equal(restored.calls.filter(c=>c.path==='/api/tasks').length,1);
   assert.equal(restored.w.localStorage.length,1);assert.equal(restored.w.sessionStorage.length,0);
  }finally{restored.dom.window.close();}
 }finally{first.dom.window.close();}
});

test('organization sign-in is offered only where the Controller enables it',async()=>{
 for(const enabled of [false,true]){
  const ui=setup([],'VIEWER',{authenticated:false,oidc:enabled});try{
   await flush();
   const link=ui.d.getElementById('login-oidc');
   assert.equal(link.getAttribute('href'),'/auth/oidc/start');
   assert.equal(link.classList.contains('hidden'),!enabled);
   assert.equal(link.textContent,'使用组织账号登录');
   assert.equal(ui.d.getElementById('login-dialog').open,true);
   assert.equal(ui.calls.length,0);
  }finally{ui.dom.window.close();}
 }
});
test('an expired startup session shows login without sending control or creation requests',async()=>{
 const ui=setup();try{await flush();assert.equal(ui.d.getElementById('login-dialog').open,true);assert.equal(ui.d.getElementById('new-task').disabled,true);assert.equal(ui.calls.length,0);assert.equal(ui.authCalls.length,1);assert.equal(ui.authCalls[0].path,'/api/session');}finally{ui.dom.window.close();}
});

test('a transient session bootstrap error retries a valid browser cookie without a login prompt',async()=>{
 const ui=setup([], 'ADMIN',{authenticated:true});
 try{
  Object.defineProperty(ui.d,'hidden',{configurable:true,value:false});
  const fetch=ui.w.fetch;
  let available=false;
  ui.w.fetch=(path,options)=>path==='/api/session'&&!available
   ?Promise.resolve({ok:false,status:503,json:async()=>({code:'CONTROL_STORE_UNAVAILABLE'})}):fetch(path,options);
  await flush();
  assert.equal(ui.d.getElementById('login-dialog').open,false);
  assert.equal(ui.d.getElementById('new-task').disabled,true);
  available=true;
  ui.d.dispatchEvent(new ui.w.Event('visibilitychange'));
  await flush();
  assert.equal(ui.d.getElementById('login-dialog').open,false);
  assert.match(ui.d.getElementById('session-info').textContent,/管理员/);
  assert.equal(ui.authCalls.some(c=>c.method==='POST'),false);
 }finally{ui.dom.window.close();}
});

test('normal requests use cookies and CSRF; logout revokes the server session before a new page restores',async()=>{
 const ui=setup();try{
  await login(ui);assert.ok(ui.calls.length>0);assert.ok(ui.calls.every(c=>c.credentials==='same-origin'&&!c.headers.Authorization));
  ui.d.getElementById('logout').click();assert.equal(ui.d.getElementById('session-info').textContent,'');await flush();assert.equal(ui.browser.authenticated,false);
  const logout=ui.authCalls.find(c=>c.path==='/api/session/logout');assert.equal(logout.headers['X-CSRF-Token'],'test-csrf');assert.equal(logout.credentials,'same-origin');
  const restored=setup([], 'ADMIN',ui.browser);try{await flush();assert.equal(restored.d.getElementById('login-dialog').open,true);}finally{restored.dom.window.close();}
 }finally{ui.dom.window.close();}
});

test('logout queues behind a delayed login and revokes its eventual cookie',async()=>{
 const ui=setup();try{
  await flush();let complete;const original=ui.w.fetch;ui.w.fetch=(path,options)=>path==='/api/session'&&options.method==='POST'?new Promise(resolve=>{complete=()=>original(path,options).then(resolve);}):original(path,options);
  ui.d.getElementById('token').value=token;ui.d.getElementById('login-form').dispatchEvent(new ui.w.Event('submit',{cancelable:true,bubbles:true}));await flush();assert.ok(complete);
  ui.d.getElementById('logout').click();assert.equal(ui.d.getElementById('login-dialog').open,true);complete();await flush();assert.equal(ui.browser.authenticated,false);assert.equal(ui.d.getElementById('session-info').textContent,'');assert.equal(ui.d.getElementById('new-task').disabled,true);
 }finally{ui.dom.window.close();}
});

test('a failed logout is visible and does not claim the server credential was revoked',async()=>{
 const ui=setup();try{
  await login(ui);const original=ui.w.fetch;ui.w.fetch=(path,options)=>path==='/api/session/logout'?Promise.resolve({ok:false,status:503,json:async()=>({code:'AUDIT_UNAVAILABLE'})}):original(path,options);
  ui.d.getElementById('logout').click();await flush();assert.equal(ui.browser.authenticated,true);assert.match(ui.d.getElementById('notice').textContent,/服务器未确认退出/);assert.equal(ui.d.getElementById('session-info').textContent,'');
 }finally{ui.dom.window.close();}
});

test('sign-out in another tab clears permissions and outstanding dialogs immediately',async()=>{
 const browser={authenticated:false},first=setup([], 'ADMIN',browser);let second;
 try{
  await login(first);second=setup([], 'ADMIN',browser);await flush();second.d.getElementById('new-task').click();assert.equal(second.d.getElementById('create-dialog').open,true);
  first.d.getElementById('logout').click();assert.equal(second.d.getElementById('session-info').textContent,'');assert.equal(second.d.getElementById('create-dialog').open,false);assert.equal(second.d.getElementById('login-dialog').open,true);await flush();assert.equal(browser.authenticated,false);
 }finally{first.dom.window.close();second?.dom.window.close();}
});

test('an outstanding refresh from an old session cannot block rendering a newer login',async()=>{
 const ui=setup();try{
  await login(ui);let complete;const original=ui.w.fetch;let delay=true;
  ui.w.fetch=(path,options)=>path==='/api/tasks'&&delay?new Promise(resolve=>{complete=()=>original(path,options).then(resolve);}):original(path,options);
  const old=ui.w.RivomeshConfiguration.refresh();await flush();assert.ok(complete);delay=false;
  ui.d.getElementById('logout').click();await flush();await login(ui);assert.equal(ui.d.getElementById('login-dialog').open,false);assert.equal(ui.d.getElementById('new-task').disabled,false);assert.match(ui.d.getElementById('session-info').textContent,/管理员/);
  complete();await old;assert.equal(ui.d.getElementById('new-task').disabled,false);
 }finally{ui.dom.window.close();}
});

test('self token rotation exchanges the saved new credential instead of reusing the revoked cookie',async()=>{
 const ui=setupAccounts();try{
  await openAccounts(ui);ui.accounts.invalidate=true;const card=ui.d.querySelector('#accounts-list .subscription-card');const rotate=[...card.querySelectorAll('button')].find(button=>button.textContent==='轮换令牌');assert.ok(rotate);rotate.click();ui.d.getElementById('account-generate').click();const fresh=ui.d.getElementById('account-token').value;ui.d.getElementById('account-token-saved').checked=true;
  ui.d.getElementById('account-form').dispatchEvent(new ui.w.Event('submit',{cancelable:true,bubbles:true}));await flush();
  const exchanges=ui.authCalls.filter(c=>c.path==='/api/session'&&c.method==='POST');assert.equal(exchanges.length,2);assert.equal(exchanges[1].headers.Authorization,'Bearer '+fresh);assert.equal(ui.d.getElementById('account-token').value,'');assert.equal(ui.d.getElementById('login-dialog').open,false);assert.equal(ui.d.getElementById('new-task').disabled,false);assert.equal(ui.w.localStorage.length,1);assert.equal(ui.w.sessionStorage.length,0);assert.equal(ui.w.token,ui.d.getElementById('token'));
 }finally{ui.dom.window.close();}
});

test('a different cookie session discovered during refresh clears the previous identity drafts',async()=>{
 const ui=setup();try{
  await login(ui);ui.d.getElementById('new-task').click();ui.d.getElementById('create-form').elements.sourcePassword.value='previous-user-private';
  const original=ui.w.fetch;ui.w.fetch=async(path,options)=>{const response=await original(path,options);return path==='/api/session'&&response.ok?{...response,json:async()=>({...await response.json(),id:'another-user',csrfToken:'another-csrf'})}:response;};
  await ui.w.RivomeshConfiguration.refresh();assert.equal(ui.d.getElementById('create-dialog').open,false);assert.equal(ui.d.getElementById('create-form').elements.sourcePassword.value,'');assert.match(ui.d.getElementById('session-info').textContent,/another-user/);
 }finally{ui.dom.window.close();}
});

function setupNodes(role='ADMIN'){
 const ui=setup([],role),base=ui.w.fetch;const control={fail:false,defer:null};
 ui.w.fetch=async(path,options)=>{
  if(!path.startsWith('/api/nodes'))return base(path,options);ui.calls.push({path,...options});if(control.defer)return control.defer();if(control.fail)return{ok:false,status:503,json:async()=>({code:'CONTROL_STORE_UNAVAILABLE'})};
  const next=path.includes('after=');return{ok:true,status:200,json:async()=>({clusterId:'migration',mode:'SHARED_REGISTRY',localNodeId:'node-a',observedAt:'2026-11-16T09:00:00Z',taskControlScope:'LOCAL',next:next?null:'node-b',nodes:[{id:next?'node-c':'node-a',epoch:2,state:next?'STALE':'HEALTHY',version:'0.1',artifactSha256:'a'.repeat(64),jvm:'25.0.1',architecture:'aarch64',processors:4,heapBytes:128*1048576,taskCapacity:20,runningCapacity:2,lastHeartbeat:'2026-11-16T08:59:59Z',leaseExpiresAt:'2026-11-16T09:00:14Z',tasks:[{id:'orders <img src=x>',workerState:'STOPPED',observedAt:'2026-11-16T08:59:58Z'}]}]})};
 };return Object.assign(ui,{nodes:control});
}
test('administrator sees shared node metadata and task observations without suggesting remote task control',async()=>{
 const ui=setupNodes();try{await login(ui);ui.d.getElementById('nodes-nav').click();await flush();assert.equal(ui.d.getElementById('nodes-view').classList.contains('hidden'),false);const content=ui.d.getElementById('nodes-list').textContent;assert.match(content,/node-a/);assert.match(content,/25.0.1.*aarch64/);assert.match(content,/orders <img src=x>/);assert.equal(ui.d.querySelector('#nodes-list img'),null);assert.match(ui.d.getElementById('nodes-state').textContent,/共享节点目录.*migration/);assert.equal(ui.d.querySelectorAll('#nodes-list button').length,0);ui.d.getElementById('language').click();assert.match(ui.d.getElementById('page-title').textContent,/Node registry/);assert.match(ui.d.getElementById('nodes-list').textContent,/Heartbeat valid/);}finally{ui.dom.window.close();}
});
test('restricted accounts cannot open the global node registry',async()=>{
 for(const role of ['VIEWER','OPERATOR']){const ui=setupNodes(role);try{await login(ui);assert.equal(ui.d.getElementById('nodes-nav').disabled,true);ui.d.getElementById('nodes-nav').click();await flush();assert.equal(ui.calls.some(call=>call.path.startsWith('/api/nodes')),false);}finally{ui.dom.window.close();}}
});
test('node pagination preserves server cursor and visibly marks stale leases',async()=>{
 const ui=setupNodes();try{await login(ui);ui.d.getElementById('nodes-nav').click();await flush();ui.d.getElementById('nodes-next').click();await flush();assert.equal(ui.calls.filter(call=>call.path.startsWith('/api/nodes'))[1].path,'/api/nodes?limit=20&after=node-b');assert.match(ui.d.getElementById('nodes-list').textContent,/node-c.*心跳过期/);assert.equal(ui.d.getElementById('nodes-next').classList.contains('hidden'),true);ui.d.getElementById('nodes-reload').click();await flush();assert.match(ui.d.getElementById('nodes-list').textContent,/node-a/);}finally{ui.dom.window.close();}
});
test('registry failure removes old healthy cards instead of presenting cached success',async()=>{
 const ui=setupNodes();try{await login(ui);ui.d.getElementById('nodes-nav').click();await flush();assert.ok(ui.d.querySelector('.node-card'));ui.nodes.fail=true;ui.d.getElementById('nodes-reload').click();await flush();assert.equal(ui.d.getElementById('nodes-list').textContent,'');assert.equal(ui.d.getElementById('nodes-state').textContent,'');assert.match(ui.d.getElementById('nodes-error').textContent,/共享控制库不可达/);}finally{ui.dom.window.close();}
});
test('late node responses cannot restore inventory after logout',async()=>{
 const ui=setupNodes();try{await login(ui);let resolve;ui.nodes.defer=()=>new Promise(done=>{resolve=done;});ui.d.getElementById('nodes-nav').click();await flush();ui.d.getElementById('logout').click();resolve({ok:true,status:200,json:async()=>({mode:'SHARED_REGISTRY',observedAt:'late',nodes:[{id:'should-not-return',tasks:[]} ]})});await flush();assert.equal(ui.d.getElementById('nodes-list').textContent,'');assert.equal(ui.d.getElementById('nodes-state').textContent,'');assert.equal(ui.d.getElementById('nodes-nav').disabled,true);}finally{ui.dom.window.close();}
});

test('shared identity mode explains cross-node revocation in both languages',async()=>{
 const ui=setupAccounts();try{ui.accounts.shared=true;await openAccounts(ui);assert.match(ui.d.getElementById('accounts-mode').textContent,/共享账号与会话权威/);ui.d.getElementById('language').click();assert.match(ui.d.getElementById('accounts-mode').textContent,/Shared identity\/session authority/);assert.equal(ui.d.getElementById('accounts-activation').classList.contains('hidden'),true);}finally{ui.dom.window.close();}
});

test('shared account editing preserves original and unloaded node scopes',async()=>{
 const ui=setupAccounts();try{ui.accounts.shared=true;ui.accounts.nodeIds=['node-a','future-node'];await openAccounts(ui);const card=ui.d.querySelectorAll('#accounts-list .subscription-card')[1];card.querySelector('button').click();assert.equal(ui.d.getElementById('account-node-scope').value,'SELECTED');assert.equal(ui.d.getElementById('account-node-ids').value,'node-a\nfuture-node');ui.d.getElementById('account-form').dispatchEvent(new ui.w.Event('submit',{cancelable:true,bubbles:true}));await flush();const update=ui.calls.find(call=>call.method==='PUT');assert.deepEqual(JSON.parse(update.body).nodeIds,['future-node','node-a']);}finally{ui.dom.window.close();}
});
test('shared new accounts default to the current node and require explicit all-node expansion',async()=>{
 const ui=setupAccounts();try{ui.accounts.shared=true;await openAccounts(ui);ui.d.getElementById('accounts-new').click();assert.equal(ui.d.getElementById('account-node-ids').value,'node-a');assert.deepEqual(JSON.parse(ui.d.getElementById('account-preview').textContent).nodeIds,['node-a']);ui.d.getElementById('account-node-scope').value='ALL';ui.d.getElementById('account-node-scope').dispatchEvent(new ui.w.Event('change'));assert.equal(JSON.parse(ui.d.getElementById('account-preview').textContent).nodeIds,null);ui.d.getElementById('logout').click();assert.equal(ui.d.getElementById('account-node-ids').value,'');}finally{ui.dom.window.close();}
});
test('empty selected node scope does not silently become all nodes',async()=>{
 const ui=setupAccounts();try{ui.accounts.shared=true;ui.accounts.nodeIds=[];await openAccounts(ui);ui.d.querySelectorAll('#accounts-list .subscription-card')[1].querySelector('button').click();assert.equal(ui.d.getElementById('account-node-scope').value,'SELECTED');assert.deepEqual(JSON.parse(ui.d.getElementById('account-preview').textContent).nodeIds,[]);ui.d.getElementById('account-role').value='ADMIN';ui.d.getElementById('account-role').dispatchEvent(new ui.w.Event('change'));assert.equal(ui.d.getElementById('account-node-scope').disabled,true);assert.equal(JSON.parse(ui.d.getElementById('account-preview').textContent).nodeIds,null);}finally{ui.dom.window.close();}
});
test('session header explains node-limited access and clears it on logout',async()=>{
 const ui=setup([], 'VIEWER',{authenticated:false,nodeIds:['node-a']});try{await login(ui);assert.match(ui.d.getElementById('session-info').textContent,/限定节点数 1/);assert.match(ui.d.getElementById('session-info').title,/node-a/);ui.d.getElementById('logout').click();assert.equal(ui.d.getElementById('session-info').title,'');}finally{ui.dom.window.close();}
});

function globalFixture(overrides={},role='ADMIN'){
 const make=id=>({id,revision:'global-revision',formatVersion:2,ownerNodeId:'node-b',ownerState:'HEALTHY',sourceDatabase:'sales',sourceCluster:'mysql-main',targetSchema:'business',desired:'STOPPED',workerState:'STOPPED',status:{},statusAvailable:true,operations:[]}),first={...make('a-task'),...overrides},second=make('b-task'),ui=setup([],role,{taskControlScope:'GLOBAL',nodeIds:role==='ADMIN'?null:['node-b']}),fallback=ui.w.fetch;
 ui.w.fetch=async(path,options={})=>{
  if(!ui.browser.authenticated||path==='/api/session'||path==='/api/session/logout'||path==='/api/capabilities')return fallback(path,options);
  let data;
  if(path.startsWith('/api/nodes?'))data=path.includes('after=')?{nodes:[{id:'node-b',state:'HEALTHY'}],next:null}:{nodes:[{id:'node-a',state:'HEALTHY'}],next:'node-a'};
  else if(path==='/api/tasks'&&options.method==='POST'){const body=JSON.parse(options.body);data={...make(body.config?.jobId||body.config?.pipelineId||'imported'),ownerNodeId:body.ownerNodeId};}
  else if(path.startsWith('/api/tasks')&&!options.method)data=path.includes('after=')?{tasks:[second],invalidConfigs:[],total:2,next:null,authority:'SHARED'}:{tasks:[first],invalidConfigs:[],total:2,next:'a-task',authority:'SHARED'};
  else return fallback(path,options);
  ui.calls.push({path,...options});return{ok:true,status:200,json:async()=>data};
 };return ui;
}
test('global task pagination shows authorized total, owner and page metrics',async()=>{
 const ui=globalFixture();try{
  await login(ui);assert.equal(ui.d.getElementById('total-count').textContent,'2');assert.match(ui.d.querySelector('[data-i18n=running]').textContent,/当前页/);assert.match(ui.d.getElementById('task-list').textContent,/node-b/);assert.equal(ui.d.getElementById('task-pagination').classList.contains('hidden'),false);
  ui.d.getElementById('tasks-next').click();await flush();assert.match(ui.d.getElementById('task-list').textContent,/b-task/);assert.ok(ui.calls.some(call=>call.path.includes('after=a-task')));assert.equal(ui.d.getElementById('tasks-previous').disabled,false);ui.d.getElementById('tasks-previous').click();await flush();assert.match(ui.d.getElementById('task-list').textContent,/a-task/);assert.equal(ui.d.getElementById('tasks-first').disabled,true);
 }finally{ui.dom.window.close();}
});
test('global task filters send exact task and owner, reset pagination and clear on sign-out',async()=>{
 const ui=globalFixture({},'VIEWER');
 const fallback=ui.w.fetch;
 ui.w.fetch=async(path,options={})=>{
  if(path.startsWith('/api/tasks?')&&path.includes('ownerNodeId=')){
   ui.calls.push({path,...options});
   return {ok:true,status:200,json:async()=>({tasks:[],total:0,next:null})};
  }
  return fallback(path,options);
 };
 try{
  await login(ui);ui.d.getElementById('tasks-next').click();await flush();
  const form=ui.d.getElementById('task-filter');
  form.elements.taskId.value='missing';form.elements.ownerNodeId.value='node-b';
  form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();
  const page=ui.calls.find(call=>call.path.includes('ownerNodeId=node-b'));
  assert.ok(page.path.includes('taskId=missing'));assert.ok(!page.path.includes('after='));
  assert.match(ui.d.getElementById('task-list').textContent,/未找到匹配/);assert.equal(ui.d.getElementById('tasks-previous').disabled,true);
  ui.d.getElementById('clear-task-filter').click();await flush();assert.match(ui.d.getElementById('task-list').textContent,/a-task/);
  form.elements.taskId.value='old-scope';ui.d.getElementById('logout').click();
  assert.equal(form.elements.taskId.value,'');assert.equal(form.classList.contains('hidden'),true);
 }finally{ui.dom.window.close();}
});

test('destination filter isolates status and displays only fresh exact backlog counts',async()=>{
 const task={id:'filter-targets',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',sourceCluster:'source',sourceDatabase:'sales',targetSchema:'copy',
  status:{run_state:'RUNNING',subscriptions:[{id:'warehouse',state:'PAUSED',pendingTransactions:'9007199254740993'},
   {id:'events',state:'RUNNING',pendingTransactions:'0'}]},statusAvailable:true,operations:[]};
 const ui=setup([task]);
 try{
  await login(ui);let select=ui.d.getElementById('destination-filter');
  assert.equal(ui.d.querySelectorAll('.subscription-card').length,2);
  select.value='warehouse';select.dispatchEvent(new ui.w.Event('change'));
  assert.equal(ui.d.querySelectorAll('.subscription-card').length,1);
  assert.match(ui.d.querySelector('.subscription-card').textContent,/待目标确认的日志事务 · 9007199254740993/);
  assert.doesNotMatch(ui.d.querySelector('.subscription-card').textContent,/events/);
  task.statusAvailable=false;ui.d.querySelector('.task-card').click();
  assert.doesNotMatch(ui.d.querySelector('.subscription-card').textContent,/待目标确认的日志事务/);
  select=ui.d.getElementById('destination-filter');select.value='';select.dispatchEvent(new ui.w.Event('change'));
  assert.equal(ui.d.querySelectorAll('.subscription-card').length,2);
 }finally{ui.dom.window.close();}
});

test('global wizard loads additional owner choices and submits the selected owner',async()=>{
 const ui=globalFixture();try{
  await login(ui);assert.equal(ui.d.getElementById('task-owner-control').classList.contains('hidden'),false);ui.d.getElementById('owners-more').click();await flush();assert.equal(ui.d.querySelectorAll('#owner-options option').length,2);ui.d.getElementById('new-task').click();const f=ui.d.getElementById('create-form').elements;f.formatVersion.value='2';f.formatVersion.dispatchEvent(new ui.w.Event('change'));
  for(const[name,value]of Object.entries({jobId:'global-created',clusterId:'production-mysql',sourceDatabase:'sales',sourceUsername:'source',sourcePassword:'source-private',targetSchema:'sales',targetUsername:'target'}))f[name].value=value;
  ui.d.getElementById('create-owner').value='node-b';ui.d.getElementById('create-owner').dispatchEvent(new ui.w.Event('input'));ui.d.getElementById('next-step').click();ui.d.getElementById('next-step').click();const plan=JSON.parse(ui.d.getElementById('plan-preview').textContent);assert.equal(plan.ownerNodeId,'node-b');assert.ok(!ui.d.getElementById('plan-preview').textContent.includes('source-private'));
  ui.d.getElementById('create-form').dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();const call=ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST');assert.ok(call);assert.equal(JSON.parse(call.body).ownerNodeId,'node-b');assert.equal(ui.d.getElementById('create-dialog').open,false);assert.match(ui.d.getElementById('notice').textContent,/已接收/);
 }finally{ui.dom.window.close();}
});
test('global YAML import uses placement and never submits a file after session replacement',async()=>{
 const ui=globalFixture();try{
  await login(ui);ui.d.getElementById('task-owner').value='node-b';let resolve;const file={size:24,text:()=>new Promise(done=>resolve=done)},input=ui.d.getElementById('import-file');Object.defineProperty(input,'files',{value:[file],configurable:true});input.dispatchEvent(new ui.w.Event('change'));assert.ok(resolve);ui.d.getElementById('logout').click();await login(ui);resolve('jobId: private-old-import');await flush();assert.equal(ui.calls.filter(call=>call.path==='/api/tasks'&&call.method==='POST').length,0);
  ui.d.getElementById('task-owner').value='node-b';Object.defineProperty(input,'files',{value:[{size:24,text:async()=>'jobId: imported'}],configurable:true});input.dispatchEvent(new ui.w.Event('change'));await flush();const call=ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST');assert.equal(JSON.parse(call.body).ownerNodeId,'node-b');assert.equal(JSON.parse(call.body).yaml,'jobId: imported');
 }finally{ui.dom.window.close();}
});
test('automatic task placement disables manual ownership and sends a single assignment mode',async()=>{
 const ui=globalFixture();try{
  await login(ui);ui.d.getElementById('new-task').click();
  const form=ui.d.getElementById('create-form'),f=form.elements,automatic=ui.d.getElementById('create-auto-placement');
  f.formatVersion.value='2';f.formatVersion.dispatchEvent(new ui.w.Event('change'));
  for(const [name,value] of Object.entries({jobId:'placed',clusterId:'mysql',sourceDatabase:'sales',sourceUsername:'source',
   targetSchema:'sales',targetUsername:'target'}))f[name].value=value;
  automatic.checked=true;automatic.dispatchEvent(new ui.w.Event('change'));
  assert.equal(ui.d.getElementById('create-owner').disabled,true);
  ui.d.getElementById('next-step').click();ui.d.getElementById('next-step').click();
  const preview=JSON.parse(ui.d.getElementById('plan-preview').textContent);
  assert.equal(preview.placement,'AUTO');assert.equal(preview.ownerNodeId,undefined);
  form.dispatchEvent(new ui.w.Event('submit',{bubbles:true,cancelable:true}));await flush();
  const body=JSON.parse(ui.calls.find(call=>call.path==='/api/tasks'&&call.method==='POST').body);
  assert.equal(body.placement,'AUTO');assert.equal(body.ownerNodeId,undefined);
 }finally{ui.dom.window.close();}
});
test('a late global page cannot restore old data after sign-out and sign-in',async()=>{
 const ui=globalFixture();try{
  await login(ui);let resolve;const fallback=ui.w.fetch;ui.w.fetch=(path,options)=>path.includes('after=a-task')?new Promise(done=>resolve=done):fallback(path,options);ui.d.getElementById('tasks-next').click();assert.ok(resolve);ui.d.getElementById('logout').click();await login(ui);resolve({ok:true,status:200,json:async()=>({tasks:[{id:'private-stale-task'}],invalidConfigs:[],total:9,next:null})});await flush();assert.ok(!ui.d.getElementById('task-list').textContent.includes('private-stale-task'));assert.match(ui.d.getElementById('task-list').textContent,/a-task/);assert.equal(ui.d.getElementById('login-dialog').open,false);
 }finally{ui.dom.window.close();}
});
test('global pending configuration and uncertain delivery remain visible without local-control labels',async()=>{
 const ui=globalFixture({configurationPending:true,workerState:'UNKNOWN',ownerState:'STALE',operations:[{id:'uncertain',action:'STOP',status:'PENDING',deliveryStatus:'UNKNOWN',code:'OWNER_ACCEPTANCE_EVIDENCE_REQUIRED'}]});try{
  await login(ui);const detail=ui.d.getElementById('task-detail');assert.match(detail.textContent,/保留旧有效版本/);assert.match(detail.textContent,/停止所属节点/);assert.match(detail.textContent,/所属节点 Worker/);assert.match(detail.textContent,/自动跨节点迁移尚未提供/);assert.ok(detail.querySelectorAll('.history .badge').length>=2);assert.equal(ui.d.querySelector('[data-i18n=localControl]').textContent,'全局任务控制');
 }finally{ui.dom.window.close();}
});

test('controller-only entry skips controller pages and selects an execution Agent',async()=>{
 const ui=globalFixture(),base=ui.w.fetch;ui.browser.nodeRole='CONTROLLER';ui.w.fetch=async(path,options={})=>{
  if(path.startsWith('/api/nodes?')){ui.calls.push({path,...options});const data=path.includes('after=')?{nodes:[{id:'worker-b',role:'AGENT',state:'HEALTHY'}],next:null}:{nodes:[{id:'node-a',role:'CONTROLLER',state:'HEALTHY'}],next:'node-a'};return{ok:true,status:200,json:async()=>data};}return base(path,options);
 };try{await login(ui);assert.equal(ui.d.getElementById('task-owner').value,'worker-b');assert.deepEqual([...ui.d.querySelectorAll('#owner-options option')].map(node=>node.value),['worker-b']);assert.ok(ui.calls.some(call=>call.path.includes('after=node-a')));ui.d.getElementById('new-task').click();assert.equal(ui.d.getElementById('create-owner').value,'worker-b');}finally{ui.dom.window.close();}
});
test('controller-only entry with no Agent never defaults execution to itself',async()=>{
 const ui=globalFixture(),base=ui.w.fetch;ui.browser.nodeRole='CONTROLLER';ui.w.fetch=async(path,options={})=>path.startsWith('/api/nodes?')?{ok:true,status:200,json:async()=>({nodes:[{id:'node-a',role:'CONTROLLER',state:'HEALTHY'}],next:null})}:base(path,options);
 try{await login(ui);assert.equal(ui.d.getElementById('task-owner').value,'');assert.equal(ui.d.querySelectorAll('#owner-options option').length,0);ui.d.getElementById('new-task').click();assert.equal(ui.d.getElementById('create-owner').value,'');assert.equal(ui.d.getElementById('create-owner').required,true);}finally{ui.dom.window.close();}
});

test('node cards distinguish management, execution Agent and combined roles in both languages',async()=>{
 const ui=globalFixture(),base=ui.w.fetch;ui.browser.nodeRole='CONTROLLER';ui.w.fetch=async(path,options={})=>path.startsWith('/api/nodes?')?{ok:true,status:200,json:async()=>({mode:'SHARED_REGISTRY',clusterId:'migration',localNodeId:'node-a',nodes:[{id:'node-a',role:'CONTROLLER',state:'HEALTHY',tasks:[]},{id:'agent',role:'AGENT',state:'HEALTHY',tasks:[]},{id:'combined',role:'CONTROLLER_WORKER',state:'HEALTHY',tasks:[]}],next:null})}:base(path,options);
 try{await login(ui);ui.d.getElementById('nodes-nav').click();await flush();let content=ui.d.getElementById('nodes-list').textContent;assert.match(content,/管理入口/);assert.match(content,/执行 Agent/);assert.match(content,/控制器与 Worker/);ui.d.getElementById('language').click();content=ui.d.getElementById('nodes-list').textContent;assert.match(content,/Management entry/);assert.match(content,/Execution Agent/);assert.match(content,/Controller and Worker/);}finally{ui.dom.window.close();}
});

test('paused destinations show phase-specific recovery guidance in both languages without adding skip',async()=>{
  const task={id:'phase-review',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING',lastSequence:8,subscriptions:[{id:'apply',state:'PAUSED',acknowledged:4,errorGeneration:2,errorCode:'SQL_23514',failurePhase:'TARGET_APPLY',failedSequence:5},{id:'activate',state:'PAUSED',acknowledged:0,errorGeneration:1,errorCode:'SQL_23514',failurePhase:'TARGET_ACTIVATE'},{id:'receipt',state:'PAUSED',acknowledged:6,errorGeneration:3,errorCode:'COMMITTED_SINK_CONTENT_MISMATCH',failurePhase:'RECONCILE',failedSequence:7}]},statusAvailable:true,operations:[]};
  const ui=setup([task]);try{
    await login(ui);let cards=[...ui.d.querySelectorAll('.subscription-card')];
    assert.match(cards[0].textContent,/应用目标事务/);assert.match(cards[0].textContent,/失败事务序号: 5/);assert.match(cards[0].textContent,/完整原事务/);
    assert.match(cards[1].textContent,/目标连接或元数据激活/);assert.doesNotMatch(cards[1].textContent,/失败事务序号/);assert.match(cards[2].textContent,/不要手工推进位点/);
    assert.equal(cards.flatMap(c=>[...c.querySelectorAll('button')]).length,3);
    ui.d.getElementById('language').click();cards=[...ui.d.querySelectorAll('.subscription-card')];assert.match(cards[0].textContent,/Destination transaction apply/);assert.match(cards[1].textContent,/metadata activation/);assert.match(cards[2].textContent,/Resume checks them first/);
  }finally{ui.dom.window.close();}
});

test('missing or corrupt phase diagnostics remain unknown and render untrusted fields as text',async()=>{
  const task={id:'legacy-phase',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING',lastSequence:1,subscriptions:[{id:'old',state:'PAUSED',acknowledged:0,errorGeneration:1,errorCode:'SQL_23514'},{id:'corrupt',state:'PAUSED',acknowledged:0,errorGeneration:2,errorCode:'SQL_23514',failurePhase:'<img src=x onerror=alert(1)>',failedSequence:'<script>bad()</script>',failureDiagnosticProblem:'untrusted value'}]},statusAvailable:true,operations:[]};
  const ui=setup([task]);try{
    await login(ui);const cards=[...ui.d.querySelectorAll('.subscription-card')];assert.match(cards[0].textContent,/未知（缺少可信阶段记录）/);assert.match(cards[0].textContent,/不会跳过数据/);assert.match(cards[1].textContent,/阶段记录无法验证/);assert.equal(cards[1].querySelectorAll('img,script').length,0);assert.doesNotMatch(cards[1].textContent,/失败事务序号/);
    ui.d.getElementById('language').click();assert.match(ui.d.querySelector('.subscription-card').textContent,/no trusted phase record/);
  }finally{ui.dom.window.close();}
});

test('initial preparation and snapshot faults have distinct guidance without an invented incremental sequence',async()=>{
  const task={id:'snapshot-phase',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING',lastSequence:8,subscriptions:[{id:'ddl',state:'PAUSED',acknowledged:0,errorGeneration:1,errorCode:'SQL_42501',failurePhase:'TARGET_PREPARE'},{id:'copy',state:'PAUSED',acknowledged:0,errorGeneration:2,errorCode:'SQL_23514',failurePhase:'TARGET_SNAPSHOT'}]},statusAvailable:true,operations:[]};
  const ui=setup([task]);try{
    await login(ui);let cards=[...ui.d.querySelectorAll('.subscription-card')];assert.match(cards[0].textContent,/目标表结构准备/);assert.match(cards[0].textContent,/建表权限/);assert.match(cards[1].textContent,/全量快照投递/);assert.match(cards[1].textContent,/快照分块断点/);assert.doesNotMatch(cards[0].textContent+cards[1].textContent,/失败事务序号/);
    ui.d.getElementById('language').click();cards=[...ui.d.querySelectorAll('.subscription-card')];assert.match(cards[0].textContent,/Destination schema preparation/);assert.match(cards[1].textContent,/reconciles snapshot part checkpoints/);
  }finally{ui.dom.window.close();}
});

test('failure evidence creation uses the selected subscription generation and no raw values are loaded automatically',async()=>{
  const task={id:'review-job',revision:'review-revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING',lastSequence:3,subscriptions:[{id:'pg',state:'PAUSED',acknowledged:1,errorGeneration:7,errorCode:'SQL_23514',failurePhase:'TARGET_APPLY'}]},operations:[]};
  const ui=setup([task],'ADMIN',{authenticated:false,recoveryReview:true});try{await login(ui);const button=[...ui.d.querySelectorAll('.subscription-card button')].find(b=>b.textContent==='保存事务审阅证据');assert.ok(button);button.click();await flush();const call=ui.calls.find(c=>c.path==='/api/tasks/review-job/actions');assert.deepEqual(JSON.parse(call.body),{action:'FAILURE_REVIEW',expectedRevision:'review-revision',errorCount:7,subscriptionId:'pg'});assert.equal(ui.calls.filter(c=>c.path.includes('/failure-evidence/')).length,0);}finally{ui.dom.window.close();}
});

test('evidence opens summary first,escapes raw events,pages with the retained hash and switches language',async()=>{
  const id='11111111-1111-1111-1111-111111111111',hash='a'.repeat(64),task={id:'review-job',revision:'revision',formatVersion:3,desired:'RUNNING',workerState:'RUNNING',status:{run_state:'RUNNING'},operations:[{id:'review_request',action:'FAILURE_REVIEW',status:'SUCCEEDED',failureEvidenceId:id,result:{id,hash}}]};
  const ui=setup([task],'ADMIN',{authenticated:false,recoveryReview:true});const original=ui.w.fetch;let rawCalls=0;ui.w.fetch=async(path,options={})=>{if(path.includes('/failure-evidence/')){ui.calls.push({path,...options});if(path.endsWith('/events')){rawCalls++;assert.equal(options.headers['X-CSRF-Token'],'test-csrf');const body=JSON.parse(options.body);return{ok:true,status:200,json:async()=>({id,hash,total:2,next:body.from===0?1:null,events:[{ordinal:body.from,event:{after:[{value:'<img src=x onerror=alert(1)>'}]}}]})};}return{ok:true,status:200,json:async()=>({id,hash,rawEvents:2,normalizedEvents:2})};}return original(path,options);};
  try{await login(ui);[...ui.d.querySelectorAll('.history button')].find(b=>b.textContent==='查看事务证据').click();await flush();let dialog=ui.d.querySelector('.failure-review-dialog');assert.ok(dialog);assert.equal(rawCalls,0);[...dialog.querySelectorAll('button')].find(b=>b.textContent==='显示原始事件').click();await flush();assert.equal(rawCalls,1);assert.match(dialog.textContent,/<img src=x onerror=alert\(1\)>/);assert.equal(dialog.querySelectorAll('img,script').length,0);[...dialog.querySelectorAll('button')].find(b=>b.textContent==='下一页事件').click();await flush();assert.equal(rawCalls,2);assert.equal(JSON.parse(ui.calls.filter(c=>c.path.endsWith('/events')).at(-1).body).from,1);ui.d.getElementById('language').click();assert.match(dialog.textContent,/Show raw events/);assert.match(dialog.textContent,/historical observation/);[...dialog.querySelectorAll('button')].find(b=>b.textContent==='Close review').click();assert.equal(ui.d.querySelector('.failure-review-dialog'),null);}finally{ui.dom.window.close();}
});

test('logout closes evidence and discards late raw responses',async()=>{
  const id='11111111-1111-1111-1111-111111111111',hash='a'.repeat(64),task={id:'review-job',revision:'revision',formatVersion:3,status:{},operations:[{id:'review_request',action:'FAILURE_REVIEW',status:'SUCCEEDED',failureEvidenceId:id}]};const browser={authenticated:false,recoveryReview:true},ui=setup([task],'ADMIN',browser);const original=ui.w.fetch;let release;
  ui.w.fetch=async(path,options={})=>{if(path.includes('/failure-evidence/')){if(path.endsWith('/events'))return new Promise(resolve=>{release=()=>resolve({ok:true,status:200,json:async()=>({id,hash,total:1,next:null,events:[{value:'late-private-value'}]})});});return{ok:true,status:200,json:async()=>({id,hash,rawEvents:1,normalizedEvents:null})};}return original(path,options);};
  try{await login(ui);[...ui.d.querySelectorAll('.history button')].find(b=>b.textContent==='查看事务证据').click();await flush();const dialog=ui.d.querySelector('.failure-review-dialog');[...dialog.querySelectorAll('button')].find(b=>b.textContent==='显示原始事件').click();await flush();assert.ok(release);ui.d.getElementById('logout').click();await flush();assert.equal(ui.d.querySelector('.failure-review-dialog'),null);release();await flush();assert.doesNotMatch(ui.d.body.textContent,/late-private-value/);}finally{ui.dom.window.close();}
});

test('retained historical columns render named typed images without conflating SQL NULL,JSON null and missing cells',async()=>{
  const id='11111111-1111-1111-1111-111111111111',hash='a'.repeat(64),task={id:'review-job',revision:'revision',formatVersion:3,status:{},operations:[{id:'review_request',action:'FAILURE_REVIEW',status:'SUCCEEDED',failureEvidenceId:id}]};const ui=setup([task],'ADMIN',{authenticated:false,recoveryReview:true});const original=ui.w.fetch;
  ui.w.fetch=async(path,options={})=>{if(path.includes('/failure-evidence/')){return{ok:true,status:200,json:async()=>path.endsWith('/events')?{id,hash,total:1,next:null,events:[{ordinal:0,table:'sales.items',columns:[{name:'email',type:'STRING',sourceType:'varchar(50)'},{name:'document',type:'JSON'},{name:'amount',type:'DECIMAL',precision:35,scale:15}],event:{eventId:'event-1',operation:'INSERT',before:null,after:[{present:true,value:null},{present:true,value:'null'},{present:false,value:null}]}}]}:{id,hash,rawEvents:1,normalizedEvents:null}};}return original(path,options);};
  try{await login(ui);[...ui.d.querySelectorAll('.history button')].find(b=>b.textContent==='查看事务证据').click();await flush();let dialog=ui.d.querySelector('.failure-review-dialog');[...dialog.querySelectorAll('button')].find(b=>b.textContent==='显示原始事件').click();await flush();const rows=[...dialog.querySelectorAll('table tr')];assert.match(dialog.textContent,/sales\.items/);assert.match(rows[1].textContent,/emailvarchar\(50\)不适用SQL NULL/);assert.match(rows[2].textContent,/documentJSON不适用null/);assert.match(rows[3].textContent,/amountDECIMAL\(35,15\)不适用未提供/);ui.d.getElementById('language').click();assert.match(dialog.querySelector('table').textContent,/Before image/);assert.match(dialog.querySelector('table').textContent,/Not provided/);}finally{ui.dom.window.close();}
});

test('checkpoint review binds historical commit observation to retained evidence and translates its limits',async()=>{
  const id='11111111-1111-1111-1111-111111111111',hash='a'.repeat(64),observation={evidenceId:id,evidenceHash:hash,observedAt:'2026-11-17T08:00:00Z',verdict:'COMMITTED_UNACKNOWLEDGED',targetFenceRequired:true,targetCheckpoint:{sequence:2}},task={id:'review-job',revision:'revision',formatVersion:3,status:{},operations:[{id:'review-request',action:'FAILURE_REVIEW',status:'SUCCEEDED',failureEvidenceId:id,result:{checkpointObservation:observation}}]};
  const ui=setup([task],'ADMIN',{authenticated:false,recoveryReview:true}),original=ui.w.fetch;
  ui.w.fetch=async(path,options={})=>path.includes('/failure-evidence/')?{ok:true,status:200,json:async()=>({id,hash,rawEvents:1})}:original(path,options);
  try{await login(ui);[...ui.d.querySelectorAll('.history button')].find(b=>b.textContent==='查看事务证据').click();await flush();const dialog=ui.d.querySelector('.failure-review-dialog');assert.match(dialog.querySelector('.failure-checkpoint').textContent,/目标已提交该事务/);assert.match(dialog.textContent,/不代表当前状态/);assert.match(dialog.textContent,/目标 epoch 落后/);assert.equal(dialog.querySelectorAll('.failure-checkpoint button').length,0);ui.d.getElementById('language').click();assert.match(dialog.textContent,/destination committed this transaction/);assert.match(dialog.textContent,/not current status/);assert.match(dialog.textContent,/epoch lags/);}finally{ui.dom.window.close();}
});

test('checkpoint review refuses mismatched evidence binding and handles older result fields',async()=>{
  const id='11111111-1111-1111-1111-111111111111',hash='a'.repeat(64),task={id:'review-job',revision:'revision',formatVersion:3,status:{},operations:[{id:'review-request',action:'FAILURE_REVIEW',status:'SUCCEEDED',failureEvidenceId:id,result:{checkpointObservation:{evidenceId:id,evidenceHash:'b'.repeat(64),verdict:'COMMITTED_UNACKNOWLEDGED'}}}]};
  const ui=setup([task],'ADMIN',{authenticated:false,recoveryReview:true}),original=ui.w.fetch;ui.w.fetch=async(path,options={})=>path.includes('/failure-evidence/')?{ok:true,status:200,json:async()=>({id,hash,rawEvents:1})}:original(path,options);
  try{await login(ui);const button=()=>[...ui.d.querySelectorAll('.history button')].find(b=>b.textContent==='查看事务证据');button().click();await flush();let section=ui.d.querySelector('.failure-checkpoint');assert.match(section.textContent,/证据内容无法验证/);assert.doesNotMatch(section.textContent,/目标已提交该事务/);[...ui.d.querySelectorAll('.failure-review-dialog button')].find(b=>b.textContent==='关闭审阅').click();task.operations[0].result={checkpointProblem:'<img src=x onerror=alert(1)>'};button().click();await flush();section=ui.d.querySelector('.failure-checkpoint');assert.match(section.textContent,/没有可用的断点核对结果/);assert.match(section.textContent,/<img/);assert.equal(section.querySelectorAll('img,script').length,0);}finally{ui.dom.window.close();}
});

test('commit confirmation requires a reason and review checkbox and sends evidence-bound action without resume',async()=>{
  const id='11111111-1111-1111-1111-111111111111',hash='a'.repeat(64),observation={evidenceId:id,evidenceHash:hash,verdict:'COMMITTED_UNACKNOWLEDGED',journalEpoch:4,errorGeneration:2,acknowledged:1,subscriptionId:'pg',targetFenceRequired:false},task={id:'review-job',revision:'paused-revision',formatVersion:3,desired:'PAUSED',status:{run_state:'PAUSED',epoch:4,subscriptions:[{id:'pg',state:'PAUSED',errorGeneration:2,acknowledged:1}]},operations:[{id:'review-request',action:'FAILURE_REVIEW',status:'SUCCEEDED',failureEvidenceId:id,result:{checkpointObservation:observation}}]};
  const ui=setup([task],'ADMIN',{authenticated:false,recoveryReview:true,recoveryCommit:true}),original=ui.w.fetch;ui.w.fetch=async(path,options={})=>path.includes('/failure-evidence/')?{ok:true,status:200,json:async()=>({id,hash,rawEvents:1})}:original(path,options);
  try{await login(ui);[...ui.d.querySelectorAll('.history button')].find(b=>b.textContent==='查看事务证据').click();await flush();const section=ui.d.querySelector('.failure-checkpoint'),button=[...section.querySelectorAll('button')].find(b=>b.textContent==='确认已提交事务');assert.ok(button.disabled);const reason=section.querySelector('textarea'),approval=section.querySelector('input[type=checkbox]');reason.value='Verified original transaction';reason.dispatchEvent(new ui.w.Event('input'));assert.ok(button.disabled);approval.checked=true;approval.dispatchEvent(new ui.w.Event('change'));assert.equal(button.disabled,false);button.click();await flush();const actions=ui.calls.filter(c=>c.path.endsWith('/actions'));assert.equal(actions.length,1);assert.deepEqual(JSON.parse(actions[0].body),{action:'CONFIRM_COMMIT',expectedRevision:'paused-revision',errorCount:2,subscriptionId:'pg',commit:{evidenceId:id,evidenceHash:hash,reason:'Verified original transaction'}});assert.equal(ui.d.querySelector('.failure-review-dialog'),null);}finally{ui.dom.window.close();}
});

test('running pipeline or stale checkpoint observation cannot expose a commit confirmation button',async()=>{
  for(const stale of [false,true]){
    const id='11111111-1111-1111-1111-111111111111',hash='a'.repeat(64),task={id:'review-job',revision:'revision',formatVersion:3,desired:stale?'PAUSED':'RUNNING',status:{run_state:stale?'PAUSED':'RUNNING',epoch:4,subscriptions:[{id:'pg',state:'PAUSED',errorGeneration:3,acknowledged:1}]},operations:[{id:'review-request',action:'FAILURE_REVIEW',status:'SUCCEEDED',failureEvidenceId:id,result:{checkpointObservation:{evidenceId:id,evidenceHash:hash,verdict:'COMMITTED_UNACKNOWLEDGED',journalEpoch:4,errorGeneration:2,acknowledged:1,subscriptionId:'pg'}}}]};
    const ui=setup([task],'ADMIN',{authenticated:false,recoveryReview:true,recoveryCommit:true}),original=ui.w.fetch;ui.w.fetch=async(path,options={})=>path.includes('/failure-evidence/')?{ok:true,status:200,json:async()=>({id,hash,rawEvents:1})}:original(path,options);
    try{await login(ui);[...ui.d.querySelectorAll('.history button')].find(b=>b.textContent==='查看事务证据').click();await flush();const section=ui.d.querySelector('.failure-checkpoint');assert.equal(section.querySelector('textarea'),null);assert.match(section.textContent,/先暂停整个任务/);}finally{ui.dom.window.close();}
  }
});

registerRecoveryTests({setup,flush,login});

registerGapVerificationTests({setup,flush,login});
