import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {launchChrome,waitFor} from './chromium.mjs';

const [binary,scenario]=process.argv.slice(2);
assert(binary&&scenario==='handoff');
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

 await cdp('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:false});
 await navigate(first,'node-a',true);
 await open(true);
 const ax=await cdp('Accessibility.getFullAXTree');
 assert(ax.nodes.some(node=>node.role?.value==='dialog'&&node.name?.value==='评估 Agent 接管'));
 await poll(`${receipt}===${quote(previousReceiptId)}`,'existing receipt read from control store');
 assert(await evaluate('document.querySelector(".handoff-dialog").scrollWidth<=document.querySelector(".handoff-dialog").clientWidth'));
 checks.push('An administrator opens the named assessment dialog by keyboard after actual polling and reads the existing receipt');

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
 console.log(JSON.stringify({status:'PASS',scenario,browser:version,checks}));
}catch(error){
 for(const check of checks)console.error('Completed before failure: '+check);
 throw error;
}finally{
 try{await browser?.close();}finally{await rm(profile,{recursive:true,force:true,maxRetries:3});}
}
