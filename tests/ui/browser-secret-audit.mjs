import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {launchChrome,waitFor} from './chromium.mjs';

const [binary]=process.argv.slice(2);
const input=[];
for await(const chunk of process.stdin)input.push(chunk);
const {entries,taskId,token,privateText}=JSON.parse(Buffer.concat(input));
const profile=await mkdtemp(join(tmpdir(),'rivomesh-secret-audit-'));
let browser;
try{
  browser=await launchChrome(binary,profile);
  const {cdp,evaluate,key,version}=browser;
  const poll=(expression,label)=>waitFor(()=>evaluate(expression),label,45);
  const checks=[];
  for(const [index,[node,address]] of entries.entries()){
    const url=new URL(address);
    assert(url.protocol==='http:'&&url.hostname==='127.0.0.1'&&url.port);
    await cdp('Page.navigate',{url:address});
    await poll('location.origin==='+JSON.stringify(address),'management entry');
    if(index===0){
      await poll('document.querySelector("#login-dialog")?.open','login dialog');
      await cdp('Input.insertText',{text:token});
      await key('Enter',13);
    }
    const button=`[...document.querySelectorAll('#task-detail .detail-actions button')]
      .find(element=>['Configuration & history','配置和历史'].includes(element.textContent)&&!element.disabled)`;
    await poll(`document.querySelector('#task-detail .detail-title h3')?.textContent===${JSON.stringify(taskId)}
      && Boolean(${button})`,node+' configuration button');
    await evaluate(`(${button}).click()`);
    await poll('document.querySelector("#configuration-dialog")?.open && document.querySelectorAll("[data-configuration-secret]").length>=3',
      node+' configuration credential fields');
    assert(await evaluate('[...document.querySelectorAll("[data-configuration-secret]")].every(input=>!input.value&&input.type==="password")'));
    const configuration=await evaluate(`[
      document.querySelector('#configuration-dialog').textContent,
      document.querySelector('#configuration-document').value
    ]`);
    assert(configuration[1].includes('$rivomeshSecret'));
    assert(privateText.every(value=>configuration.every(rendered=>!rendered.includes(value))),node+' configuration leaked private text');
    await evaluate('document.querySelector("#configuration-close").click(); document.querySelector("#audit-nav").click()');
    await poll('document.querySelector("#audit-dialog")?.open && document.querySelector("#audit-events")?.childElementCount>0',node+' audit events');
    const audit=await evaluate('document.querySelector("#audit-dialog").textContent');
    assert(privateText.every(value=>!audit.includes(value)),node+' audit leaked private text');
    checks.push(node+' renders empty password inputs, opaque retain references and redacted audit');
  }
  process.stdout.write(JSON.stringify({status:'PASS',version,checks})+'\n');
}finally{
  await browser?.close();
  await rm(profile,{recursive:true,force:true});
}
