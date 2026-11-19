import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash, randomUUID} from 'node:crypto';
import {mkdir, mkdtemp, rm} from 'node:fs/promises';
import {homedir, tmpdir} from 'node:os';
import {join} from 'node:path';
import {launchChrome, waitFor} from './chromium.mjs';

const checks=[];
let stage='READ_FIXTURE_INPUT',browser,profile,diagnosis;
const safeCode=value=>typeof value==='string'&&/^[A-Z][A-Z0-9_]{1,119}$/.test(value)?value:null;
try{
 assert(process.platform==='linux'&&homedir()==='/home/node'&&process.env.RM_OIDC_FIXTURE_BROWSER==='1');
 const chunks=[];
 for await(const chunk of process.stdin)chunks.push(chunk);
 const input=JSON.parse(Buffer.concat(chunks));
 const {console:origin,issuer,clientId,adminToken,viewerToken,providerPassword,providerSubject,clientSecret}=input;
 assert(origin==='https://console.rivomesh.test:8443'&&issuer==='https://identity.rivomesh.test:8443/realms/rivomesh-oidc-fixture');
 const entryHeader='X-Rivomesh-Fixture-Entry';
 const account='/api/accounts/fixture-viewer/external-identity';
 const auth=token=>({'Authorization':'Bearer '+token});
 const bodyHeaders=requestId=>({'Content-Type':'application/json',...(requestId?{'X-Request-ID':requestId}:{})});
 async function request(path,entry='a',{method='GET',headers={},body}={}){
  const response=await fetch(origin+path,{method,headers:{[entryHeader]:entry,...headers},body,redirect:'manual'});
  const data=await response.json();
  diagnosis={status:response.status,code:safeCode(data?.code)};
  return {status:response.status,data};
 }
 function requestId(){return 'oidc_fixture_'+randomUUID().replaceAll('-','');}
 async function admin(path,entry='a',method='GET',data,expected=200){
  const reply=await request(path,entry,{method,headers:{...auth(adminToken),...bodyHeaders(method==='GET'?null:requestId())},
   body:data===undefined?undefined:JSON.stringify(data)});
  assert.equal(reply.status,expected);
  return reply.data;
 }
 function taskConfig(id,owner,index){
  return {
   formatVersion:3,pipelineId:id,nodeId:owner,databases:['source_fixture'],
   source:{type:'MYSQL',clusterId:'unused-'+index,host:'source-'+index+'.invalid',username:'fixture',serverId:910000+index},
   targets:[{id:'pg',product:'POSTGRESQL',url:'jdbc:postgresql://target-'+index+'.invalid/fixture',
    username:'fixture',metadataSchema:'fixture_meta_'+index,mappings:{source_fixture:'copy_fixture_'+index}}]
  };
 }

 stage='READ_SHARED_ACCOUNT_AUTHORITY';
 const start=await admin('/api/accounts');
 assert.equal(start.authority,'SHARED');
 if(start.mode!=='MANAGED'){
  stage='ACTIVATE_SHARED_ACCOUNT_AUTHORITY';
  await admin('/api/accounts/activate','a','POST',{expectedRevision:start.revision});
 }
 for(const [id,owner,index] of [['allowed','b',1],['outside','a',2]]){
  stage='CREATE_STOPPED_TASK_'+owner.toUpperCase();
  await admin('/api/tasks','a','POST',{ownerNodeId:owner,config:taskConfig(id,owner,index)},202);
  stage='AWAIT_STOPPED_TASK_'+owner.toUpperCase();
  await waitFor(async()=>{
   const task=await admin('/api/tasks/'+id,'b');
   diagnosis={matchesOwner:task.ownerNodeId===owner,workerState:safeCode(task.workerState),
    desired:safeCode(task.desired),problem:safeCode(task.problem),configurationPending:task.configurationPending===true,
    operations:task.operations?.map(operation=>({status:safeCode(operation.status),code:safeCode(operation.code)}))};
   return task.ownerNodeId===owner&&task.desired==='STOPPED'&&task.workerState==='STOPPED'
     &&task.operations?.some(op=>op.status==='SUCCEEDED');
  },'stopped scoped task',60);
 }
 stage='CREATE_SCOPED_VIEWER';
 const directory=await admin('/api/accounts','b');
 assert.equal(directory.externalIdentityIssuer,issuer);
 await admin('/api/accounts','a','POST',{
  expectedRevision:directory.revision,id:'fixture-viewer',role:'VIEWER',enabled:true,
  token:viewerToken,taskIds:['allowed'],nodeIds:['b']
 },201);

 stage='TRUST_EPHEMERAL_CA_AND_OPEN_CHROME';
 const nss=join(homedir(),'.pki','nssdb');
 await mkdir(nss,{recursive:true,mode:0o700});
 execFileSync('certutil',['-N','--empty-password','-d','sql:'+nss],{stdio:'ignore'});
 execFileSync('certutil',['-A','-n','rivomesh-test-root','-t','C,,','-d','sql:'+nss,'-i','/test/root.pem'],{stdio:'ignore'});
 profile=await mkdtemp(join(tmpdir(),'rivomesh-oidc-chrome-'));
 browser=await launchChrome('/usr/local/bin/fixture-chromium',profile);
 const {cdp,evaluate,key,version}=browser;
 const quote=JSON.stringify;
 const poll=(expression,label,seconds=25)=>waitFor(()=>evaluate(expression),label,seconds);
 const browserCall=(path,entry='a',method='GET',body,headers={})=>evaluate(`(async()=>{
  const r=await fetch(${quote(path)},{method:${quote(method)},credentials:'same-origin',
   headers:${quote({[entryHeader]:entry,...headers})},body:${quote(body===undefined?undefined:JSON.stringify(body))}});
  return {status:r.status,data:await r.json()};
 })()`);
 const session=(entry='a')=>browserCall('/api/session',entry);
 await cdp('Page.navigate',{url:origin+'/'});
 await poll('document.querySelector("#login-dialog")?.open','secure login challenge');
 await evaluate('document.querySelector("#token").focus()');
 await cdp('Input.insertText',{text:adminToken});await key('Enter',13);
 await poll('!document.querySelector("#login-dialog")?.open','authenticated administrator');
 const localAdmin=(await session('b')).data;
 assert(localAdmin.authentication==='COOKIE'&&localAdmin.role==='ADMIN'&&localAdmin.nodeId==='b');

 stage='BIND_PROVIDER_SUBJECT_VIA_COOKIE_ADMIN';
 const accountDirectory=await browserCall('/api/accounts','a');
 assert.equal(accountDirectory.status,200);
 const bind={expectedRevision:accountDirectory.data.revision,issuer,subject:providerSubject};
 const missingCsrf=await browserCall(account,'b','PUT',bind,bodyHeaders(requestId()));
 assert(missingCsrf.status===403&&missingCsrf.data.code==='CSRF_REJECTED');
 const bound=await browserCall(account,'b','PUT',bind,{...bodyHeaders(requestId()),'X-CSRF-Token':localAdmin.csrfToken});
 assert.equal(bound.status,200);
 const viewer=bound.data.users.find(user=>user.id==='fixture-viewer');
 assert(viewer.externalIdentity.issuer===issuer);
 assert(viewer.externalIdentity.subjectSha256===createHash('sha256').update(providerSubject).digest('hex'));
 assert(!JSON.stringify(bound.data).includes(providerSubject));
 assert.equal((await session('a')).status,401);
 checks.push('A real browser administrator binds a local Viewer through Controller B with CSRF required');

 stage='KEYCLOAK_AUTHORIZATION_CODE_AND_PKCE';
 await cdp('Page.reload');
 await poll('document.querySelector("#login-dialog")?.open && !document.querySelector("#login-oidc")?.classList.contains("hidden")',
  'organization sign-in becomes available');
 await evaluate('document.querySelector("#login-oidc").focus()');await key('Enter',13);
 await poll('document.querySelector("#username") && document.querySelector("#password")','Keycloak login form over trusted TLS');
 assert(await evaluate(`(()=>{
  const uri=new URL(location.href),params=uri.searchParams;
  return uri.origin===new URL(${quote(issuer)}).origin&&params.get('response_type')==='code'
   &&params.get('client_id')===${quote(clientId)}&&params.get('redirect_uri')===${quote(origin+'/auth/oidc/callback')}
   &&params.get('scope')==='openid'&&params.get('code_challenge_method')==='S256'
   &&/^[A-Za-z0-9_-]{43}$/.test(params.get('code_challenge'))&&params.has('state')&&params.has('nonce');
 })()`));
 await evaluate('document.querySelector("#username").focus()');await cdp('Input.insertText',{text:'fixture-user'});
 await evaluate('document.querySelector("#password").focus()');await cdp('Input.insertText',{text:providerPassword});
 await key('Enter',13);
 await poll(`location.origin===${quote(origin)}&&location.pathname==='/'&&!document.querySelector("#login-dialog")?.open`,
  'Keycloak callback returns to shared HTTPS console',45);
 const counts=(await request('/__fixture/oidc-counts')).data;
 assert.equal(counts['a.start.302'],1);
 assert.equal(counts['b.callback.303'],1);
 checks.push('Actual Keycloak HTTPS code flow with S256 PKCE starts on Controller A and successfully calls back through Controller B');

 stage='COOKIE_AND_LOCAL_TASK_GRANTS';
 for(const entry of ['a','b']){
  const response=await session(entry), identity=response.data;
  assert(response.status===200&&identity.id==='fixture-viewer'&&identity.role==='VIEWER'&&identity.nodeId===entry);
  assert.deepEqual(identity.taskScope,['allowed']);assert.deepEqual(identity.nodeScope,['b']);
  const page=(await browserCall('/api/tasks',entry)).data;
  assert.deepEqual(page.tasks.map(task=>task.id),['allowed']);
  assert.equal((await browserCall('/api/tasks/outside',entry)).status,404);
  assert.equal((await browserCall('/api/accounts',entry)).status,403);
 }
 const {cookies}=await cdp('Network.getCookies',{urls:[origin]});
 const signed=cookies.find(cookie=>cookie.name==='__Host-rivomesh-session');
 assert(signed?.secure&&signed.httpOnly&&signed.sameSite==='Strict'&&signed.path==='/');
 assert(!cookies.some(cookie=>cookie.name==='__Host-rivomesh-oidc'));
 assert(await evaluate(`!document.cookie.includes('rivomesh')&&
  !JSON.stringify([localStorage,sessionStorage]).includes(${quote(signed.value)})`));
 await cdp('Page.reload');
 await poll('document.querySelector(".task-card")?.textContent.includes("allowed")','authorized task survives reload');
 checks.push('The signed provider role does not expand local Viewer, task or node grants, and reload keeps a Secure HttpOnly cookie');

 stage='CROSS_ENTRY_REVOKE_AND_ACTIVE_IDP_SESSION';
 const latest=await admin('/api/accounts','a');
 const revoked=await admin(account,'b','DELETE',{expectedRevision:latest.revision});
 assert(!revoked.users.find(user=>user.id==='fixture-viewer').externalIdentity);
 for(const entry of ['a','b']){
  assert.equal((await request('/api/session',entry,{headers:{Cookie:signed.name+'='+signed.value}})).status,401);
  assert.equal((await session(entry)).status,401);
 }
 await cdp('Page.reload');
 await poll('document.querySelector("#login-dialog")?.open && !document.querySelector("#login-oidc")?.classList.contains("hidden")',
  'revoked session returns to login');
 await evaluate('document.querySelector("#login-oidc").focus()');await key('Enter',13);
 await poll(`location.origin===${quote(origin)}&&location.pathname==='/auth/oidc/callback'
  &&document.body.textContent.includes('OIDC_LOGIN_DENIED')`,'unbound active provider identity is denied',45);
 const denied=(await request('/__fixture/oidc-counts')).data;
 assert.equal(denied['a.start.302'],2);
 assert.equal(denied['b.callback.401'],1);
 assert.equal((await request('/api/session','a',{headers:auth(viewerToken)})).data.role,'VIEWER');
 checks.push('Revoking the binding through Controller B invalidates both browser routes and denies the still-authenticated Keycloak subject');

 stage='AUDIT_REDACTION';
 const audit=JSON.stringify(await admin('/api/audit/export?limit=1000','b'));
 for(const label of ['ACCOUNT_EXTERNAL_BIND','ACCOUNT_EXTERNAL_REVOKE','GET_OIDC_CALLBACK'])assert(audit.includes(label));
 for(const secret of [adminToken,viewerToken,clientSecret,providerPassword,providerSubject,signed.value])assert(!audit.includes(secret));
 assert(!/code=|code_verifier=|access_token|id_token/i.test(audit));
 checks.push('The authorized audit export records the binding, callback and revocation without the fixture credentials or callback code');
 console.log(JSON.stringify({status:'PASS',browser:version,checks}));
}catch{
 // Never print exceptions, URLs, headers, DOM state, child stderr, or request bodies.
 console.log(JSON.stringify({status:'FAIL',failureStage:stage,diagnosis,checks}));
}finally{
 try{await browser?.close();}finally{if(profile)await rm(profile,{recursive:true,force:true,maxRetries:3});}
}
