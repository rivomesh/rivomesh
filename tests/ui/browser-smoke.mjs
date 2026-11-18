import assert from 'node:assert/strict';
import {createHash, randomBytes} from 'node:crypto';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdir, mkdtemp, readFile, rm, writeFile} from 'node:fs/promises';
import {createServer} from 'node:net';
import {join, resolve} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';

const [artifactInput, outputInput, chrome]=process.argv.slice(2);
assert(artifactInput&&outputInput&&chrome,'Usage: node tests/ui/browser-smoke.mjs <artifact> <output> <Chrome executable>');
const artifact=resolve(artifactInput);
const output=resolve(outputInput);
await mkdir(output,{recursive:true});
const fixture=await mkdtemp(join(output,'fixture-'));
const tasks=join(fixture,'tasks');
await mkdir(tasks);
const auth=randomBytes(32).toString('hex');
const binder=createServer();
await new Promise((done,reject)=>binder.once('error',reject).listen(0,'127.0.0.1',done));
const port=binder.address().port;
await new Promise(done=>binder.close(done));
const url='http://127.0.0.1:'+port;
const config=join(fixture,'platform.json');
await writeFile(config,JSON.stringify({bind:'127.0.0.1',port,authToken:'${RM_BROWSER_TOKEN}',tasksDirectory:tasks,
  dataDirectory:join(fixture,'data'),sessions:{secureCookie:false},autoStart:false,pollMillis:300}),{mode:0o600});
const children=[];
const manifest={artifactSha256:createHash('sha256').update(await readFile(artifact)).digest('hex'),status:'FAIL',checks:[]};
let socket;

function launch(binary,args,env=process.env){
  const child=spawn(binary,args,{stdio:'ignore',env});
  children.push(child);
  child.on('error',()=>{});
  return child;
}
async function waitFor(test,label){
  for(let i=0;i<100;i++){
    try{const result=await test();if(result)return result;}catch{}
    await delay(100);
  }
  throw new Error('Timed out: '+label);
}
function passed(label){manifest.checks.push(label);console.log('PASS '+label);}
async function accept(response){assert(response.ok,'Unexpected HTTP '+response.status);return response.json();}

try{
  launch(process.env.JAVA_HOME?join(process.env.JAVA_HOME,'bin','java'):'java',
         ['-Xmx256m','-jar',artifact,'serve',config],{...process.env,RM_BROWSER_TOKEN:auth});
  await waitFor(async()=>(await fetch(url+'/healthz')).ok,'actual console server');
  await accept(await fetch(url+'/api/tasks',{method:'POST',headers:{Authorization:'Bearer '+auth,
    'Content-Type':'application/json','X-Request-ID':'browser_smoke_create'},body:JSON.stringify({config:{
      formatVersion:3,pipelineId:'browser-smoke',nodeId:'browser-smoke',databases:['demo'],
      source:{type:'MYSQL',clusterId:'browser-source',username:'fixture',serverId:910099},
      targets:[{id:'warehouse',product:'POSTGRESQL',url:'jdbc:postgresql://fixture.invalid/warehouse',
                username:'fixture',mappings:{demo:'browser_copy'}}]}})}));

  const profile=join(fixture,'chrome');
  launch(chrome,['--headless','--remote-debugging-port=0','--user-data-dir='+profile,'--no-first-run','about:blank']);
  const debugPort=(await waitFor(()=>readFile(join(profile,'DevToolsActivePort'),'utf8'),'Chrome DevTools')).split('\n')[0];
  const debug='http://127.0.0.1:'+debugPort;
  manifest.browser=(await accept(await fetch(debug+'/json/version'))).Browser;
  const pages=await accept(await fetch(debug+'/json/list'));
  socket=new WebSocket(pages.find(page=>page.type==='page').webSocketDebuggerUrl);
  await once(socket,'open');
  const pending=new Map();
  let sequence=0;
  socket.onmessage=message=>{
    const event=JSON.parse(message.data);
    const request=pending.get(event.id);
    if(!request)return;
    pending.delete(event.id);clearTimeout(request.timeout);
    if(event.error)request.reject(new Error(event.error.message));else request.done(event.result);
  };
  function cdp(method,params={}){
    const id=++sequence;
    return new Promise((done,reject)=>{
      const timeout=setTimeout(()=>{pending.delete(id);reject(new Error(method+' timed out'));},10000);
      pending.set(id,{done,reject,timeout});
      socket.send(JSON.stringify({id,method,params}));
    });
  }
  async function evaluate(expression){
    const {result,exceptionDetails}=await cdp('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});
    assert(!exceptionDetails,exceptionDetails?.text);
    return result.value;
  }
  async function key(name,code){
    const params={key:name,code:name,windowsVirtualKeyCode:code};
    await cdp('Input.dispatchKeyEvent',name==='Enter'?{...params,type:'keyDown',text:'\r'}:{...params,type:'rawKeyDown'});
    await cdp('Input.dispatchKeyEvent',{...params,type:'keyUp'});
  }
  async function viewport(width,height){
    await cdp('Emulation.setDeviceMetricsOverride',{width,height,deviceScaleFactor:1,mobile:false});
  }
  async function screenshot(name){
    const {data}=await cdp('Page.captureScreenshot',{format:'png'});
    await writeFile(join(output,name+'.png'),Buffer.from(data,'base64'));
  }

  await viewport(1280,900);
  await cdp('Page.navigate',{url});
  await waitFor(()=>evaluate('document.querySelector("#login-dialog")?.open'),'login dialog');
  assert.equal(await evaluate('document.activeElement.id'),'token');
  await key('Tab',9);
  assert.equal(await evaluate('document.activeElement.type'),'submit');
  await key('Tab',9);
  assert.equal(await evaluate('document.activeElement.id'),'token');
  await key('Escape',27);
  assert(await evaluate('document.querySelector("#login-dialog").open'));
  passed('Native Tab navigation stays in the login dialog and Escape keeps login required');
  await cdp('Input.insertText',{text:auth});
  assert.equal(await evaluate("document.querySelector('#token').value.length"),auth.length);
  await key('Enter',13);
  await waitFor(()=>evaluate('!document.querySelector("#login-dialog")?.open && !!document.querySelector(".task-card")'),
                'native login and task data');
  const state=await evaluate(`(async()=>({
    cleared:document.querySelector('#token').value==='',
    hidden:!document.cookie.includes('rivomesh') &&
      !JSON.stringify([localStorage,sessionStorage]).includes(${JSON.stringify(auth)}),
    auth:(await (await fetch('/api/session')).json()).authentication
  }))()`);
  assert.deepEqual(state,{cleared:true,hidden:true,auth:'COOKIE'});
  await cdp('Page.reload');
  await waitFor(()=>evaluate('!!document.querySelector(".task-card") && !document.querySelector("#login-dialog")?.open'),
                'cookie session restored after reload');
  passed('Actual token sign-in issues a cookie, clears the form, and restores the task after reload');

  await evaluate('document.querySelector("[data-worker-diagnostics]").focus()');
  await key('Enter',13);
  await waitFor(()=>evaluate('!!document.querySelector(".worker-diagnostics[open] section p")&&document.querySelector(".worker-diagnostics section p").textContent.includes("暂无")'),
                'diagnostic dialog and native empty result');
  const ax=await cdp('Accessibility.getFullAXTree');
  assert(ax.nodes.some(node=>node.role?.value==='dialog'&&node.name?.value==='采集诊断记录'));
  await screenshot('capture-diagnostics');
  await key('Escape',27);
  assert(await evaluate('!document.querySelector(".worker-diagnostics")'));
  passed('Capture diagnostics has an accessible dialog name and works through Enter and Escape');

  await evaluate('document.querySelector("#language").click()');
  assert.equal(await evaluate('document.documentElement.lang'),'en');
  await screenshot('workspace-wide');
  await viewport(390,844);
  assert(await evaluate('document.documentElement.scrollWidth<=innerWidth'));
  await screenshot('workspace-narrow');
  passed('English workspace stays within the mobile viewport');

  await evaluate('document.querySelector("#logout").click()');
  await waitFor(()=>evaluate('document.querySelector("#login-dialog").open'),'signed-out dialog');
  await waitFor(()=>evaluate("fetch('/api/session').then(response=>response.status===401)"),'server revokes cookie');
  passed('Actual sign-out revokes the browser session');
  manifest.status='PASS';
}finally{
  if(socket?.readyState===WebSocket.OPEN)socket.close();
  for(const child of children.reverse()){
    if(child.exitCode!==null||!child.pid)continue;
    child.kill('SIGTERM');
    await Promise.race([once(child,'exit').catch(()=>{}),delay(2000)]);
    if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');
  }
  await rm(fixture,{recursive:true,force:true,maxRetries:3});
  await writeFile(join(output,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');
}
