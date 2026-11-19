import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {launchChrome,waitFor} from './chromium.mjs';

const [binary,scenario]=process.argv.slice(2);
assert(binary&&['handoff','handoff-preparation'].includes(scenario));
const chunks=[];
for await(const chunk of process.stdin)chunks.push(chunk);
const {first,second,taskId,token,owner,candidate,revision,previousReceiptId}=JSON.parse(Buffer.concat(chunks));
for(const address of [first,second]){
 const url=new URL(address);
 assert(url.protocol==='http:'&&url.hostname==='127.0.0.1'&&url.port);
}
assert.notEqual(first,second);

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
 console.log(JSON.stringify({status:'PASS',scenario,browser:version,checks}));
}catch(error){
 for(const check of checks)console.error('Completed before failure: '+check);
 throw error;
}finally{
 try{await browser?.close();}finally{await rm(profile,{recursive:true,force:true,maxRetries:3});}
}
