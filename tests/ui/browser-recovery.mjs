import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {launchChrome,waitFor} from './chromium.mjs';

// The disposable native fixture supplies its loopback endpoints and token via stdin.
const [binary,scenario]=process.argv.slice(2);
assert(binary&&['repair','correction','receipt'].includes(scenario));
const chunks=[];
for await(const chunk of process.stdin)chunks.push(chunk);
const fixture=JSON.parse(Buffer.concat(chunks));
const {first,second,taskId,token}=fixture;
for(const address of [first,second]){
  const url=new URL(address);
  assert(url.protocol==='http:'&&url.hostname==='127.0.0.1'&&url.port);
}
assert.notEqual(first,second);
const profile=await mkdtemp(join(tmpdir(),'rivomesh-recovery-chrome-'));
const checks=[];
let browser;
try{
  browser=await launchChrome(binary,profile);
  const {cdp,evaluate,key,version}=browser;
  const quoted=JSON.stringify;
  const query=selector=>'document.querySelector('+quoted(selector)+')';
  const route='/api/tasks/'+encodeURIComponent(taskId);
  const poll=(expression,label)=>waitFor(()=>evaluate(expression),label,45);
  const operations=action=>`(async()=>{
    const response=await fetch(${quoted(route)});
    if(!response.ok)return;
    return (await response.json()).operations.find(op=>op.action===${quoted(action)}&&op.status==='SUCCEEDED');
  })()`;
  async function activate(selector,label){
    const target=query(selector);
    await poll(`${target}&&!${target}.disabled`,label);
    await evaluate(`${target}.focus()`);
    await key('Enter',13);
  }
  async function clickNamed(selector,name){
    const button=`[...document.querySelectorAll(${quoted(selector)})].find(button=>button.textContent===${quoted(name)}&&!button.disabled)`;
    await poll(`Boolean(${button})`,name);
    await evaluate(`(${button}).click()`);
  }
  async function navigate(address,entry,login=false){
    await cdp('Page.navigate',{url:address});
    await poll(`location.origin===${quoted(address)}`,entry+' navigation');
    if(login){
      await poll('document.querySelector("#login-dialog")?.open','login dialog');
      await cdp('Input.insertText',{text:token});
      await key('Enter',13);
    }
    await poll(`(async()=>{
      const response=await fetch('/api/session');
      if(!response.ok)return false;
      const s=await response.json();
      return s.authentication==='COOKIE'&&s.nodeId===${quoted(entry)}
        &&!document.querySelector('#login-dialog')?.open
        &&document.querySelector('#task-detail .detail-title')?.textContent.includes(${quoted(taskId)});
    })()`,entry+' task with shared session');
    await evaluate(`if(!document.documentElement.lang.startsWith('zh'))document.querySelector('#language').click()`);
    assert(await evaluate("document.documentElement.lang.startsWith('zh')"));
  }
  await cdp('Emulation.setDeviceMetricsOverride',{width:1280,height:900,deviceScaleFactor:1,mobile:false});
  await navigate(first,'node-a',true);
  await navigate(second,'node-b');
  checks.push('One real cookie signs in on the first Controller and opens the same task workbench on the second');

  if(scenario==='repair'){
    await activate('[data-validation-catalog]','historical report directory');
    await activate('[data-validation-search]','open retained report by exact ID');
    const reportInput='.validation-report-search[open] input[name="reportId"]';
    await poll(`Boolean(${query(reportInput)})`,'report ID input');
    assert(await evaluate(`document.activeElement===${query(reportInput)}`));
    await cdp('Input.insertText',{text:fixture.reportId});
    await key('Enter',13);
    await poll('document.querySelectorAll("[data-repair-scope]").length===2','direct lookup opens the retained report summary');
    await clickNamed('.validation-catalog-actions button','返回历史目录');
    checks.push('Keyboard report lookup opens one authorized historical report by ID and returns to the original directory page');
    if(fixture.inspectStorage){
      await activate('[data-validation-storage="'+fixture.reportId+'"]','authorized report storage inspection');
      await poll('document.querySelector("[data-validation-storage-result]")?.textContent.includes("活动状态未知")',
        'real file metadata and unknown cross-process activity');
      assert(await evaluate('document.querySelector("[data-validation-storage-result]").textContent.includes("报告数据库")'));
      await key('Escape',27);
      checks.push('Report storage displays file metadata with the correct unknown-activity state');
    }
    await poll('document.querySelectorAll("[data-repair-scope]").length===2','both destination repair scopes');
    await evaluate(`window.reportDifferenceBeforeRefresh=[...document.querySelectorAll('[data-repair-scope]')]
      .find(input=>input.dataset.repairScope.startsWith('pg/')).closest('tr').querySelector('[data-validation-differences]');
      window.reportDifferenceBeforeRefresh.focus()`);
    await poll('!window.reportDifferenceBeforeRefresh.isConnected','ordinary polling refreshes the focused destination difference');
    assert(await evaluate('document.activeElement.dataset.validationFocus===window.reportDifferenceBeforeRefresh.dataset.validationFocus'));
    await key('Enter',13);
    await poll('performance.getEntriesByType("resource").some(entry=>entry.name.endsWith("/differences"))',
      'native difference request was sent by the selected report control');
    await poll('document.querySelectorAll("[data-validation-values]").length===25','first page of native differences');
    const firstPage=await evaluate('[...document.querySelectorAll("[data-validation-values]")].map(button=>button.dataset.validationValues)');
    assert.equal(new Set(firstPage).size,25);
    assert(!await evaluate('document.querySelector(".validation-differences").textContent.includes("reviewed-drift")'));
    await activate('[data-validation-values="'+firstPage[0]+'"]','explicit raw value reveal');
    await poll('document.querySelector(".validation-differences").textContent.includes("reviewed-drift")','stored raw difference after reveal');
    await clickNamed('.validation-differences button','下一页');
    await poll('document.querySelectorAll("[data-validation-values]").length===2','second page of native differences');
    const secondPage=await evaluate('[...document.querySelectorAll("[data-validation-values]")].map(button=>button.dataset.validationValues)');
    assert(secondPage.every(id=>!firstPage.includes(id)));
    await clickNamed('.validation-differences button','上一页');
    await poll(`document.querySelector("[data-validation-values]")?.dataset.validationValues===${quoted(firstPage[0])}`,'return to first page');
    checks.push('A retained report survives Agent restart and a real 27-row difference set pages both ways with explicit raw value access');

    await evaluate('document.querySelectorAll("[data-repair-scope]").forEach(input=>input.click())');
    await activate('[data-repair-action="plan"]','create a scoped two-target repair plan on the second Controller');
    const prepared=await poll(operations('REPAIR_PLAN'),'persisted repair plan');
    await navigate(first,'node-a');
    await activate('[data-repair-plan="'+prepared.repair.planId+'"]','review second Controller plan on first Controller');
    const apply='[data-repair-action="execute"]';
    await poll(`Boolean(${query(apply)})`,'repair approval rendered');
    assert(await evaluate(query(apply)+'.disabled'));
    await evaluate(query('[data-repair-confirm]')+'.click()');
    await activate(apply,'explicit reviewed repair approval');
    const completed=await poll(operations('REPAIR_EXECUTE'),'persisted target repair confirmation');
    assert.equal(completed.code,'REPAIR_COMPLETED');
    assert.equal(Object.keys(completed.result.receipts).length,2);
    await cdp('Page.reload');
    await poll('[...document.querySelectorAll(".repair-panel h3")].some(heading=>heading.textContent==="目标提交凭据")',
      'visible receipts after browser reload');
    checks.push('The second Controller creates the plan, the first requires explicit approval, and both target receipts remain visible after a reload');
  }else if(scenario==='correction'){
    await clickNamed('.history button','查看事务证据');
    await activate('button[data-recovery-label="correctionOpen"]','typed correction for the retained failure');
    const dialog='.recovery-dialog[open]';
    await poll(`Boolean(${query(dialog+' select')})`,'typed correction schema');
    const accessibility=await cdp('Accessibility.getFullAXTree');
    assert(accessibility.nodes.some(node=>node.role?.value==='dialog'&&node.name?.value==='手工补数据'));
    await evaluate(`(()=>{
      const mode=${query(dialog+' select')};
      mode.value='import';
      mode.dispatchEvent(new Event('change'));
    })()`);
    await evaluate(query(dialog+' textarea')+'.focus()');
    await cdp('Input.insertText',{text:JSON.stringify(fixture.patch)});
    await activate(dialog+' button[data-recovery-label="correctionPreview"]','review imported corrections');
    const apply=dialog+' [data-recovery-action="CORRECTION_EXECUTE"]';
    await poll(`Boolean(${query(apply)})`,'correction preview');
    const content=await evaluate(query(dialog)+'.textContent');
    for(const value of ['reviewed pre-image','private extra row','18446744073709551615'])assert(content.includes(value));
    assert(await evaluate(query(apply)+'.disabled'));
    const reason=dialog+' label:has([data-recovery-label="recoveryReason"]) textarea';
    await evaluate(query(reason)+'.focus()');
    await cdp('Input.insertText',{text:'Restore the reviewed PostgreSQL rows for the retained transaction'});
    assert(await evaluate(query(apply)+'.disabled'));
    await evaluate(query(dialog+' label:has([data-recovery-label="correctionApproval"]) input')+'.click()');
    await activate(apply,'submit exact reviewed correction with both confirmations');
    const completed=await poll(operations('CORRECTION_EXECUTE'),'persisted correction operation and receipt');
    assert.equal(completed.result.code,'CORRECTION_COMPLETED');
    assert(!JSON.stringify(completed).includes('reviewed pre-image'));
    checks.push('The real correction preview preserves a 64-bit JSON integer and requires both a reason and approval');
    await navigate(first,'node-a');
    const observed=await poll(operations('CORRECTION_EXECUTE'),'correction receipt at other Controller');
    assert.deepEqual(observed.result,completed.result);
    checks.push('Both management entries show the same correction receipt without the reviewed business values');
  }else{
    await activate('[data-recovery-receipt-lookup]','look up a recovery action with the owner offline');
    const input='.recovery-dialog[open] input[name="requestId"]';
    await poll(`Boolean(${query(input)})`,'request ID input');
    assert(await evaluate(`document.activeElement===${query(input)}`));
    await cdp('Input.insertText',{text:fixture.receiptId});
    await key('Enter',13);
    const result=await poll(`(()=>{const summary=document.querySelector('[data-recovery-receipt-result]')?.textContent;
      return summary?.includes(${quoted(fixture.receiptId)})&&summary;})()`,'older operational receipt');
    assert(result.includes('手工修正')&&result.includes(fixture.ownerNodeId));
    for(const value of ['reviewed pre-image','private extra row','18446744073709551615'])assert(!result.includes(value));
    checks.push('Chrome reads the correction receipt after history rollover and Agent loss without exposing imported values');
  }
  console.log(JSON.stringify({status:'PASS',scenario,browser:version,checks}));
}catch(error){
  for(const check of checks)console.error('Completed before failure: '+check);
  throw error;
}finally{
  try{await browser?.close();}finally{await rm(profile,{recursive:true,force:true,maxRetries:3});}
}
