import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {isDeepStrictEqual} from 'node:util';
import {launchChrome,waitFor} from './chromium.mjs';

const [binary,scenario]=process.argv.slice(2);
assert(binary&&['handoff','handoff-preparation','handoff-adopted','handoff-owner-commit'].includes(scenario));
const chunks=[];
for await(const chunk of process.stdin)chunks.push(chunk);
const {first,second,taskId,token,owner,candidate,revision,previousReceiptId,
 preparationId,configurationVersion,adoption,validationReportId,closedReceiptId,commitRequestId}=JSON.parse(Buffer.concat(chunks));
for(const address of [first,second]){
 const url=new URL(address);
 assert(url.protocol==='http:'&&url.hostname==='127.0.0.1'&&url.port);
}
assert.notEqual(first,second);
if(scenario==='handoff-adopted'){
 const receiptId=/^[A-Za-z0-9_-]{8,128}$/;
 assert([preparationId,validationReportId,closedReceiptId].every(id=>typeof id==='string'&&receiptId.test(id)));
 assert(typeof configurationVersion==='string'&&typeof revision==='string'&&owner!==candidate);
 assert(Number.isSafeInteger(adoption?.validationReports)&&adoption.validationReports>0);
 assert(Number.isSafeInteger(adoption?.closedReceipts)&&adoption.closedReceipts>0);
}

const profile=await mkdtemp(join(tmpdir(),'rivomesh-handoff-chrome-'));
const checks=[];
let browser;
try{
 browser=await launchChrome(binary,profile);
 const {cdp,evaluate,key,version}=browser;
 const quote=JSON.stringify;
 const poll=(expression,label)=>waitFor(()=>evaluate(expression),label,45);
 const launcher='document.querySelector("[data-handoff-task]")';
 const receipt='document.querySelector("[data-handoff-receipt] details code")?.textContent';
 async function navigate(address,entry,login=false){
  await cdp('Page.navigate',{url:address});
  await poll(`location.origin===${quote(address)}`,entry+' navigation');
  if(login){
   await poll('document.querySelector("#login-dialog")?.open','native login dialog');
   await cdp('Input.insertText',{text:token});await key('Enter',13);
  }
  await poll(`(async()=>{
   const response=await fetch('/api/session');
   if(!response.ok)return false;
   const session=await response.json();
   return session.authentication==='COOKIE'&&session.nodeId===${quote(entry)}
     &&${launcher}?.dataset.handoffTask===${quote(taskId)};
  })()`,entry+' authorized task');
  await evaluate('if(!document.documentElement.lang.startsWith("zh"))document.querySelector("#language").click()');
 }
 async function open(watchRefresh=false){
  await evaluate(`window.handoffBeforeRefresh=${launcher};window.handoffBeforeRefresh.focus()`);
  if(watchRefresh){
   await poll('!window.handoffBeforeRefresh.isConnected','ordinary task polling rebuilds the launcher');
   assert(await evaluate(`document.activeElement===${launcher}`));
  }
  await key('Enter',13);
  await poll('document.querySelector(".handoff-dialog")?.open','handoff assessment dialog');
 }
 async function clickByKeyboard(selector){
  const action='document.querySelector('+quote(selector)+')';
  await poll(action+'&&!'+action+'.hidden&&!'+action+'.disabled','enabled '+selector);
  await evaluate(action+'.focus()');await key('Enter',13);
 }
 async function preparationFlow(){
  const reservation='document.querySelector("[data-handoff-preparation]")';
  const preparedPath='/api/tasks/'+encodeURIComponent(taskId)+'/handoff/preparation';
  const readPreparation=()=>evaluate(`fetch(${quote(preparedPath)}).then(response=>response.json())`);
  await clickByKeyboard('[data-handoff-probe]');
  await poll('document.querySelector("[data-handoff-receipt]")?.textContent.includes("预留所需证据已核验")',
             'real candidate checkpoint and shared receipt proof');
  assert(await evaluate(`${reservation}.textContent.includes(${quote(owner)})
   &&${reservation}.textContent.includes(${quote(candidate)})
   &&${reservation}.textContent.includes("本任务暂停接收新命令")`));
  await evaluate(`${reservation}.querySelector("details").open=true`);
  assert(await evaluate('document.querySelector(".handoff-dialog").scrollWidth<=document.querySelector(".handoff-dialog").clientWidth'));
  checks.push('The keyboard probe verifies the real checkpoint and stored receipts and shows both Agents and the reservation impact');

  await clickByKeyboard('[data-handoff-prepare]');
  await poll(`${reservation}.textContent.includes("预留中")`,'capacity reserved through the management UI');
  const plan=await readPreparation();
  assert.equal(plan.state,'PREPARED');assert.equal(plan.assessmentId,previousReceiptId);
  assert.equal(plan.taskRevision,revision);assert.equal(plan.originalNodeId,owner);assert.equal(plan.candidateNodeId,candidate);
  assert.equal(plan.reservedTaskSlots,1);assert.equal(plan.reservedWorkerSlots,1);assert.equal(plan.executionAuthorized,false);
  assert.equal(await evaluate(`${reservation}.querySelector("time").dateTime`),plan.expiresAt);
  assert(await evaluate('document.querySelector("[data-handoff-assess]").disabled'));
  assert(await evaluate('document.querySelector(".handoff-dialog").scrollWidth<=document.querySelector(".handoff-dialog").clientWidth'));
  checks.push('The live reservation keeps the writer unchanged, shows expiry and reserved slots, and fits the narrow viewport');

  const stageButton='document.querySelector("[data-handoff-stage]")';
  const stageStatus='document.querySelector("[data-handoff-stage-status]")';
  async function stageReports(){
   await clickByKeyboard('[data-handoff-stage]');
   return poll(`${stageStatus}?.textContent.includes("已核验暂存文件")&&!${stageButton}.disabled&&${stageStatus}.textContent`,
               'candidate verifies staged evidence');
  }
  const staged=await stageReports();
  assert.match(staged,/校验报告 1 · 已办结回执 [1-9][0-9]*/);
  assert.match(await evaluate(stageButton+'.textContent'),/复核已暂存/);
  assert.equal(await stageReports(),staged);
  assert.equal((await readPreparation()).id,plan.id);
  checks.push('The first entry stages actual private reports and closed receipts, then explicitly reverifies the same receipt');

  await navigate(second,'node-b');await open();
  await poll(`${reservation}.querySelector("details code")?.textContent===${quote(plan.id)}`,
             'same preparation receipt through the second management entry');
  assert.equal(await stageReports(),staged);
  assert(await evaluate('document.querySelector(".handoff-dialog").scrollWidth<=document.querySelector(".handoff-dialog").clientWidth'));
  checks.push('The second entry explicitly verifies the same staged evidence and shows only counts');
  await clickByKeyboard('[data-handoff-release]');
  await poll(`${reservation}.textContent.includes("已解除")`,'release through the second management entry');
  const released=await readPreparation();
  assert.equal(released.id,plan.id);assert.equal(released.state,'RELEASED');
  assert.equal(released.reservedTaskSlots,0);assert.equal(released.reservedWorkerSlots,0);
  assert.equal(await evaluate(stageButton+'.parentElement.hidden'),true);
  const task=await evaluate(`fetch(${quote('/api/tasks/'+encodeURIComponent(taskId))}).then(response=>response.json())
    .then(value=>({owner:value.ownerNodeId,revision:value.revision,worker:value.workerState}))`);
  assert.deepEqual(task,{owner,revision,worker:'STOPPED'});
  await key('Escape',27);
  assert(await evaluate(`!document.querySelector(".handoff-dialog")&&document.activeElement===${launcher}`));
  checks.push('The second entry releases the same reservation, preserves ownership and revision, and restores focus after Escape');
 }

 async function adoptedFlow(){
  const taskPath='/api/tasks/'+encodeURIComponent(taskId);
  const preparationPath=taskPath+'/handoff/preparation';
  const stage='document.querySelector("[data-handoff-preparation]")';
  const summaryFields=[
   'id','taskId','state','assessmentId','taskRevision','activeConfigurationVersion',
   'originalNodeId','candidateNodeId','originalNodeEpoch','candidateNodeEpoch','cut','custodySha256','evidenceSha256',
   'createdAt','expiresAt','executionAuthorized','reservedTaskSlots','reservedWorkerSlots','activatedJournalEpoch',
   'committedRevision','committedConfigurationVersion','adoptedEvidence','writerActivation'
  ];
  const activationFields=['checkedInControlTransaction','activationSha256','originalAssignmentSha256',
   'journalGenerationSha256','nativeEvidenceSha256'];
  const expectMasked=(actual,expected,description)=>assert(isDeepStrictEqual(actual,expected),description);
  const expectKeys=(data,fields)=>expectMasked(Object.keys(data).sort(),[...fields].sort(),'Public receipt fields match the masked schema');
  const read=path=>evaluate(`fetch(${quote(path)}).then(async response=>response.ok?await response.json():null)`);
  let firstSummary;
  for(const [entry,url,theme,width] of [['node-a',first,'light',390],['node-b',second,'dark',320]]){
   const light=theme==='light';
   await cdp('Emulation.setEmulatedMedia',{features:[{name:'prefers-color-scheme',value:theme}]});
   await cdp('Emulation.setDeviceMetricsOverride',{width,height:844,deviceScaleFactor:1,mobile:false});
   await navigate(url,entry,light);
   if(!light)await evaluate('if(document.documentElement.lang!=="en")document.querySelector("#language").click()');

   const [task,summary,storage,history]=await Promise.all([
    read(taskPath),read(preparationPath),read(taskPath+'/validations/'+encodeURIComponent(validationReportId)+'/storage'),
    read(taskPath+'/recovery-receipts/'+encodeURIComponent(closedReceiptId))
   ]);
   assert(task&&summary&&storage&&history,'Signed receipt and installed evidence are readable');
   expectMasked([task.ownerNodeId,task.revision,task.configurationVersion,task.workerState,task.desired],
     [candidate,revision,configurationVersion,'STOPPED','STOPPED'],'The current candidate owns the exact stopped task');
   expectMasked([summary.id,summary.taskId,summary.state,summary.originalNodeId,summary.candidateNodeId,
    summary.committedRevision,summary.committedConfigurationVersion,summary.executionAuthorized,
    summary.reservedTaskSlots,summary.reservedWorkerSlots],
    [preparationId,taskId,'EVIDENCE_ADOPTED',owner,candidate,revision,configurationVersion,false,0,0],
    'The signed adoption is bound to the expected task, preparation and revision with no execution authority');
   assert.notEqual(summary.taskRevision,revision);
   const fenceOffered=summary.targetFenceAvailable===true;
   const publicFields=fenceOffered?[...summaryFields,'targetFenceAvailable']:[...summaryFields];
   if(summary.commitRequestId!=null){
    assert.match(summary.commitRequestId,/^[A-Za-z0-9_-]{8,128}$/);
    publicFields.push('commitRequestId');
   }
   expectKeys(summary,publicFields);
   expectKeys(summary.cut,['epoch','sequence','checkpointSha256']);
   expectKeys(summary.writerActivation,activationFields);
   expectMasked(summary.adoptedEvidence,adoption,'The receipt exposes only the exact adopted evidence counts');
   assert(storage.adoptedReadOnly===true&&!storage.archiveEnabled);
   expectMasked([history.id,history.status,history.candidateCustody],[closedReceiptId,'SUCCEEDED','VERIFIED'],
    'Candidate custody verifies the exact inherited closed receipt');
   if(firstSummary)expectMasked(summary,firstSummary,'Both Controllers return the same signed summary');
   else firstSummary=summary;

   await open(light);
   const label=light?'查看 Agent 交接状态':'View Agent handoff status';
   const adopted=light?'证据已接收':'Evidence received';
   await poll(`${stage}?.textContent.includes(${quote(adopted)})`,'signed evidence adoption renders');
   const ax=await cdp('Accessibility.getFullAXTree');
   assert(ax.nodes.some(node=>node.role?.value==='dialog'&&node.name?.value===label));
   const visible=await evaluate(`(()=>{
    const dialog=document.querySelector('.handoff-dialog'),panel=${stage};
    panel.querySelector('details').open=true;
    const facts=Array.from(panel.querySelectorAll('dl dt'),key=>[key.textContent,key.nextElementSibling.textContent]);
    const controls=Array.from(dialog.querySelectorAll('button,a,[role=button]'))
     .filter(control=>control.getClientRects().length).map(control=>control.textContent.trim());
    const editable=Array.from(dialog.querySelectorAll('input,select,textarea,[contenteditable=true]'))
     .some(control=>!control.disabled&&control.getClientRects().length);
    const brand=document.querySelector('.sidebar ${light?'.brand-day':'.brand-night'}');
    return {facts,controls,editable,codes:Array.from(panel.querySelectorAll('code'),code=>code.textContent),
     theme:document.documentElement.dataset.theme,accent:getComputedStyle(document.documentElement).getPropertyValue('--accent').trim(),
     brandVisible:brand?.naturalWidth>0&&getComputedStyle(brand).display!=='none',
     contained:dialog.scrollWidth<=dialog.clientWidth&&document.documentElement.scrollWidth<=innerWidth};
   })()`);
   const names=light?['原 Agent','当前归属 Agent','已提交日志代次','校验报告','已办结回执']:
    ['Original Agent','Current owner Agent','Committed journal epoch','Validation reports','Closed receipts'];
   const expectedFacts=names.map((name,index)=>[name,[
    owner,candidate,summary.activatedJournalEpoch,String(adoption.validationReports),String(adoption.closedReceipts)
   ][index]]);
   if(adoption.rawCaptures){
    expectedFacts.push([light?'只读原始采集档案':'Read-only raw capture archives',String(adoption.rawCaptures)]);
    const raw=await read(taskPath+'/handoff/raw-history');
    expectMasked([raw.state,raw.preparationId,raw.rawCaptures,raw.executionAuthorized],
     ['READ_ONLY_HISTORY',preparationId,adoption.rawCaptures,false],'The candidate serves the corresponding raw history');
   }
   expectMasked(visible.facts,expectedFacts,'The dialog shows exact node names, decimal journal epoch and counts');
   const allowed=light?['关闭','刷新交接状态']:['Close','Refresh handoff status'];
   if(fenceOffered)allowed.push(light?'隔离 SQL 目标':'Fence SQL targets');
   expectMasked(visible.controls,allowed,'Only the signed approval and read-only controls are visible');
   assert.equal(visible.editable,false);
   expectMasked(visible.codes,[preparationId,revision,configurationVersion],'The expanded receipt shows only public IDs');
   assert.equal(visible.theme,theme);assert.equal(visible.accent,light?'#2859a6':'#94bcfa');
   assert(visible.brandVisible&&visible.contained,'Branded signed-status dialog fits the mobile viewport');
   await clickByKeyboard('[data-handoff-reload]');
   await poll(`${stage}?.textContent.includes(${quote(adopted)})&&!document.querySelector("[data-handoff-reload]").disabled`,
     'read-only refresh retains the signed state');
   expectMasked(await read(preparationPath),summary,'Refresh preserves the exact signed summary');
   await key('Escape',27);
   assert(await evaluate(`!document.querySelector(".handoff-dialog")&&document.activeElement===${launcher}`));
   checks.push(light?'The first Controller shows the parked candidate, exact adopted counts and readable original evidence in the light console':
    'The second Controller shows the same masked receipt and authorized controls in the dark narrow console');
  }
 }

 async function commitOwnerFlow(){
  const taskPath='/api/tasks/'+encodeURIComponent(taskId);
  const reservation='document.querySelector("[data-handoff-preparation]")';
  const commit='[data-handoff-action="commit-owner"]';
  const prep='fetch('+quote(taskPath+'/handoff/preparation')+').then(response=>response.json())';
  await cdp('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:false});
  await navigate(second,'node-b',true);
  await open();
  await poll(`${reservation}?.textContent.includes("预留中")`,'bound handoff preparation');
  const plan=await evaluate(prep);
  assert.deepEqual([plan.state,plan.id,plan.commitRequestId,plan.originalNodeId,plan.candidateNodeId],
    ['PREPARED',preparationId,commitRequestId,owner,candidate]);
  assert(await evaluate(`document.querySelector(${quote(commit)})===null`));
  await clickByKeyboard('[data-handoff-stage]');
  await poll(`document.querySelector("[data-handoff-stage-status]")?.textContent.includes("已核验暂存文件")`,'candidate re-verifies exact staging');
  assert.match(await evaluate(`document.querySelector(${quote(commit)}).textContent`),/重试同一归属切换/);
  assert(await evaluate('document.querySelector(".handoff-dialog").scrollWidth<=document.querySelector(".handoff-dialog").clientWidth'));
  await clickByKeyboard(commit);
  await poll(`fetch(${quote(taskPath)}).then(response=>response.json()).then(task=>
   task.ownerNodeId===${quote(candidate)}&&task.desired==="STOPPED"&&task.workerState==="STOPPED")`,'task ownership moves to the stopped candidate');
  await poll(`${reservation}?.textContent.match(/归属已提交|候选配置已安装|证据已接收/)!=null`,'dialog follows the current owner');
  const committed=await evaluate(prep);
  assert.deepEqual([committed.id,committed.commitRequestId,committed.candidateNodeId,committed.executionAuthorized],
    [preparationId,commitRequestId,candidate,false]);
  checks.push('The second Controller stages signed custody and commits the already bound owner transfer by keyboard at 390px, keeping the Worker stopped');
 }

 if(scenario==='handoff-adopted'){
  await adoptedFlow();
 }else if(scenario==='handoff-owner-commit'){
  await commitOwnerFlow();
 }else{
  await cdp('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:false});
  await navigate(first,'node-a',true);
  await open(true);
  const ax=await cdp('Accessibility.getFullAXTree');
  assert(ax.nodes.some(node=>node.role?.value==='dialog'&&node.name?.value==='评估 Agent 接管'));
  await poll(`${receipt}===${quote(previousReceiptId)}`,'existing receipt read from control store');
  assert(await evaluate('document.querySelector(".handoff-dialog").scrollWidth<=document.querySelector(".handoff-dialog").clientWidth'));
  checks.push('An administrator opens the named assessment dialog by keyboard after actual polling and reads the existing receipt');

  if(scenario==='handoff-preparation'){
   await preparationFlow();
  }else{
   const select='document.querySelector(".handoff-dialog select")';
   await poll(`[...${select}.options].some(option=>option.value===${quote(candidate)})`,'candidate from live node directory');
   const ids=await evaluate(`[...${select}.options].map(option=>option.value)`);
   assert(!ids.includes(owner)&&!ids.includes('node-a')&&!ids.includes('node-b'));
   await evaluate(`${select}.value=${quote(candidate)};${select}.dispatchEvent(new Event('change'))`);
   const submit='document.querySelector("[data-handoff-assess]")';
   await poll(`${submit}&&!${submit}.disabled`,'explicit assessment ready');
   await evaluate(`${submit}.focus()`);await key('Enter',13);
   const newId=await poll(`(()=>{const id=${receipt};return id&&id!==${quote(previousReceiptId)}&&id;})()`,'new assessment receipt');
   await poll('document.querySelector("[data-handoff-receipt]")?.textContent.includes("待补证据")','evidence-required status');
   checks.push('Submitting the selected live candidate replaces the known receipt and shows the additional evidence required');

   await navigate(second,'node-b');
   await open();
   await poll(`${receipt}===${quote(newId)}`,'same durable receipt from the second Controller');
   const task=await evaluate(`fetch(${quote('/api/tasks/'+encodeURIComponent(taskId))}).then(response=>response.json())
    .then(value=>({owner:value.ownerNodeId,revision:value.revision,worker:value.workerState}))`);
   assert.deepEqual(task,{owner,revision,worker:'STOPPED'});
   checks.push('The same cookie sees the updated receipt through the second Controller while the task keeps its original owner and revision');
  }
 }
 console.log(JSON.stringify({status:'PASS',scenario,browser:version,checks}));
}catch(error){
 for(const check of checks)console.error('Completed before failure: '+check);
 throw error;
}finally{
 try{await browser?.close();}finally{await rm(profile,{recursive:true,force:true,maxRetries:3});}
}
