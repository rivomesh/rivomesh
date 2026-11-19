(()=>{'use strict';
const bridge=window.RivomeshConfiguration,$=id=>document.getElementById(id),t=key=>bridge.t(key);
bridge.register({accountsSharedMode:'共享账号与会话权威已启用，所有入口使用同一目录；账号变更会撤销全部共享会话。任务操作范围仍以任务控制档案为准。',accountNodeScope:'可访问节点',accountSelectedNodes:'指定节点',accountAllNodes:'所有当前及未来节点',accountNodeIds:'节点ID（每行一个，允许尚未加入的节点）',accountNodeHelp:'空的指定列表表示没有节点任务权限。迁移保留原节点范围；选择所有节点会扩展可访问范围。ADMIN始终为全局权限。',accountNoNodes:'没有节点任务权限',ACCOUNT_NODE_SCOPE_INVALID:'节点范围无效：请填写唯一、合法的节点ID，ADMIN不能限制节点。',accountNodes:'节点',accountsTitle:'账号与授权',accountsHelp:'仅管理员可管理。权限变更对后续请求生效，已接受的异步操作可能继续。令牌不进入列表、历史或浏览器存储。',accountsReload:'刷新账号',accountsNew:'新增账号',accountsConfigMode:'当前使用部署配置。启用界面管理后，持久账号目录成为权限来源，重启不会用旧配置覆盖。',accountsManagedMode:'界面管理已启用。请协调备份控制状态与审计库；丢失全部管理员令牌时使用离线恢复命令。',accountsActivateAck:'我已核对下方账号，并了解启用后由持久目录管理权限。',accountsActivate:'启用界面管理',accountId:'账号标识',accountRole:'角色',accountEnabled:'允许登录',accountScope:'任务范围',accountSelected:'指定任务',accountAll:'所有当前及未来任务',accountExtraTasks:'其他任务 ID（每行一个，可填写尚未加载的任务）',accountScopeHelp:'指定任务列表为空时没有任务权限。ADMIN 必须拥有全局范围。',accountNewToken:'新访问令牌',accountGenerate:'生成随机令牌',accountShowToken:'显示 / 隐藏令牌',accountTokenHelp:'请在提交前保存新令牌；服务端只保存摘要，之后不能取回。轮换会立即使旧令牌失效。',accountTokenSaved:'我已保存新令牌',accountEdit:'编辑角色与范围',accountRotate:'轮换令牌',accountDelete:'删除账号',accountDeleteHelp:'删除后禁止登录并保留审计身份；此 ID 不能重新分配。最后一个可用管理员不能删除。',accountCancel:'取消编辑',accountSave:'提交变更',accountDeleted:'已删除',accountDisabled:'已停用',accountEmpty:'尚无账号',accountSaveTokenFirst:'请先保存新令牌并勾选确认。',accountInvalid:'请检查账号、令牌及任务范围。',accountSaved:'账号变更已持久保存。',accountLostResponse:'结果尚未确认，请保留新令牌并使用同一请求重试。若轮换自己的令牌后旧令牌失效，请用已保存的新令牌登录。',ACCOUNTS_REVISION_CHANGED:'账号目录已被其他管理员修改，请刷新后重新核对。',AUTHORIZATION_CHANGED:'权限已变化，请刷新后重试。',ACCOUNT_MANAGEMENT_NOT_ENABLED:'请先核对并启用界面账号管理。',ACCOUNT_LAST_ADMIN:'必须保留至少一个可用的全局管理员。',ACCOUNT_EXISTS:'账号 ID 已使用或保留，不能重新分配。',ACCOUNT_DELETED:'账号已删除，不能重新启用。',ACCOUNT_TOKEN_COLLISION:'此令牌已分配给其他身份，请生成新令牌。',ACCOUNT_TOKEN_INVALID:'新令牌需要至少32个可见ASCII字符。',ACCOUNT_SCOPE_INVALID:'任务范围无效；不能使用通配符或限定ADMIN范围。',ACCOUNT_NOT_FOUND:'账号不存在。',ACCOUNT_LIMIT:'账号目录已达上限（含保留身份）。'},
{accountsSharedMode:'Shared identity/session authority is enabled. Every endpoint uses this directory; account changes revoke all shared sessions. Task routing still follows its control profile.',accountNodeScope:'Accessible nodes',accountSelectedNodes:'Selected nodes',accountAllNodes:'All current and future nodes',accountNodeIds:'Node IDs (one per line; future nodes allowed)',accountNodeHelp:'An empty selected list grants no node tasks. Migration preserves the original node scope; All nodes expands access. ADMIN is always global.',accountNoNodes:'No node-task access',ACCOUNT_NODE_SCOPE_INVALID:'Invalid node scope. Use unique valid IDs; ADMIN cannot be node-scoped.',accountNodes:'Nodes',accountsTitle:'Accounts and grants',accountsHelp:'Administrator only. Changes affect subsequent requests; accepted asynchronous operations may continue. Tokens never enter lists, history or browser storage.',accountsReload:'Reload accounts',accountsNew:'New account',accountsConfigMode:'Deployment configuration is authoritative. Activation makes the durable directory authoritative; restart will not overwrite it with old configuration.',accountsManagedMode:'Managed accounts are enabled. Coordinate controller/audit backups. If all administrator tokens are lost, use offline access recovery.',accountsActivateAck:'I reviewed these identities and understand that the durable directory will manage access.',accountsActivate:'Enable managed accounts',accountId:'Account ID',accountRole:'Role',accountEnabled:'Allow sign-in',accountScope:'Task scope',accountSelected:'Selected tasks',accountAll:'All current and future tasks',accountExtraTasks:'Additional task IDs (one per line; unloaded tasks allowed)',accountScopeHelp:'An empty selected list grants no task access. ADMIN requires global scope.',accountNewToken:'New access token',accountGenerate:'Generate random token',accountShowToken:'Show / hide token',accountTokenHelp:'Save the token before submitting. The server stores only its digest and cannot retrieve it. Rotation immediately revokes the old token.',accountTokenSaved:'I saved the new token',accountEdit:'Edit role and grants',accountRotate:'Rotate token',accountDelete:'Delete account',accountDeleteHelp:'Sign-in is revoked and the audit identity is retained. This ID cannot be reassigned. The last enabled administrator cannot be deleted.',accountCancel:'Cancel edit',accountSave:'Apply change',accountDeleted:'Deleted',accountDisabled:'Disabled',accountEmpty:'No accounts',accountSaveTokenFirst:'Save the new token and acknowledge it first.',accountInvalid:'Check the account ID, token and task scope.',accountSaved:'Account change durably saved.',accountLostResponse:'Outcome is unconfirmed. Keep the new token and retry the same request. If your own rotation revoked the old token, sign in with the saved new one.',ACCOUNTS_REVISION_CHANGED:'Another administrator changed the directory. Reload and review again.',AUTHORIZATION_CHANGED:'Authorization changed. Refresh and retry.',ACCOUNT_MANAGEMENT_NOT_ENABLED:'Review and enable managed accounts first.',ACCOUNT_LAST_ADMIN:'Keep at least one enabled global administrator.',ACCOUNT_EXISTS:'This ID is used or retained and cannot be reassigned.',ACCOUNT_DELETED:'This identity was deleted and cannot be re-enabled.',ACCOUNT_TOKEN_COLLISION:'This token belongs to another identity. Generate a new one.',ACCOUNT_TOKEN_INVALID:'A new token needs at least32 visible ASCII characters.',ACCOUNT_SCOPE_INVALID:'Invalid scope. Wildcards and scoped ADMIN identities are not allowed.',ACCOUNT_NOT_FOUND:'Account not found.',ACCOUNT_LIMIT:'Account directory limit reached, including retained identities.'});
bridge.register({
 accountExternalBind:'绑定组织身份',accountExternalReplace:'替换组织身份',accountExternalRevoke:'撤销组织身份',
 accountExternalIssuer:'签发方（issuer）',accountExternalSubject:'主体标识（sub）',accountExternalDigest:'主体摘要（SHA-256）',
 accountExternalBindHelp:'填写组织身份提供方向此客户端签发的、区分大小写的 sub。提交后需重新登录。',
 accountExternalRevokeHelp:'撤销后不能用这个组织身份登录。本地令牌仍可使用，现有共享会话会失效。',
 accountExternalSubjectInvalid:'请填写 1 至 255 个可见 ASCII 字符。',
 accountExternalLostResponse:'结果尚未确认。仍在登录时可以原样重试。会话失效后，请重新登录并核对账号目录。',
 ACCOUNT_EXTERNAL_IDENTITY_CONFLICT:'该组织身份已绑定到其他账号。',ACCOUNT_EXTERNAL_ISSUER_UNTRUSTED:'当前入口的签发方配置已变化。请刷新账号后重试。',
 ACCOUNT_EXTERNAL_IDENTITY_INVALID:'组织身份格式无效。'
},{
 accountExternalBind:'Bind organization identity',accountExternalReplace:'Replace organization identity',accountExternalRevoke:'Revoke organization identity',
 accountExternalIssuer:'Issuer',accountExternalSubject:'Subject identifier (sub)',accountExternalDigest:'Subject digest (SHA-256)',
 accountExternalBindHelp:'Enter the case-sensitive sub that the provider issued to this client. Applying the change requires signing in again.',
 accountExternalRevokeHelp:'Revocation blocks sign-in with this organization identity. Local tokens stay valid and existing shared sessions expire.',
 accountExternalSubjectInvalid:'Enter 1 to 255 visible ASCII characters.',
 accountExternalLostResponse:'The outcome is unconfirmed. Retry unchanged while signed in. If your session expires, sign in and check the directory.',
 ACCOUNT_EXTERNAL_IDENTITY_CONFLICT:'This organization identity is already bound to another account.',ACCOUNT_EXTERNAL_ISSUER_UNTRUSTED:'This entry’s configured issuer changed. Reload the directory and review the binding.',
 ACCOUNT_EXTERNAL_IDENTITY_INVALID:'Invalid organization identity format.'
});
let state=null,generation=0;
function node(tag,value){const element=document.createElement(tag);element.textContent=value==null?'':String(value);return element;}
function current(ticket){return state&&state.ticket===ticket&&bridge.can('ACCOUNT_ADMIN');}
function erase(){const input=$('account-token');input.value='';input.type='password';$('account-token-saved').checked=false;$('account-external-subject').value='';$('account-external-issuer').value='';}
function cancel(){if(state)state.draft=null;erase();$('account-form').classList.add('hidden');$('account-preview').textContent='';$('account-error').textContent='';}
function close(){generation++;state=null;erase();$('accounts-list').replaceChildren();$('accounts-error').textContent='';$('accounts-mode').textContent='';$('account-form').classList.add('hidden');$('account-preview').textContent='';$('account-task-options').replaceChildren();$('account-extra-tasks').value='';$('account-node-ids').value='';$('account-id').value='';$('account-error').textContent='';$('accounts-activation-ack').checked=false;$('accounts-dialog').close();}
function button(label,handler){const b=node('button',t(label));b.type='button';b.disabled=state.busy;b.onclick=handler;return b;}
function render(){
 $('accounts-nav').textContent=t('accountsTitle');$('accounts-nav').disabled=!bridge.can('ACCOUNT_ADMIN');if(!bridge.can('ACCOUNT_ADMIN')&&state){close();return;}
 document.querySelectorAll('[data-accounts-i18n]').forEach(e=>e.textContent=t(e.dataset.accountsI18n));if(!state)return;
 const managed=state.page?.mode==='MANAGED',busy=state.busy;$('accounts-mode').textContent=t(state.page?.authority==='SHARED'?'accountsSharedMode':managed?'accountsManagedMode':'accountsConfigMode');$('accounts-reload').disabled=busy;$('accounts-new').disabled=busy||!managed;$('accounts-activation').classList.toggle('hidden',managed||!state.page);$('accounts-activate').disabled=busy||!$('accounts-activation-ack').checked;$('accounts-activation-ack').disabled=busy;$('account-fields').disabled=busy;
 const root=$('accounts-list');root.replaceChildren();
 const shared=state.page?.authority==='SHARED';
 for(const user of state.page?.users||[]){
  const card=node('section','');
  card.className='subscription-card';
  card.append(node('strong',user.id),node('p',user.role+' · '+t(user.deleted?'accountDeleted':user.enabled?'accountEnabled':'accountDisabled')),node('p',user.taskIds===null?t('accountAll'):user.taskIds.join(', ')));
  if(shared)card.append(node('p',Array.isArray(user.nodeIds)?user.nodeIds.length?t('accountNodes')+': '+user.nodeIds.join(', '):t('accountNoNodes'):t('accountAllNodes')));
  const binding=shared?user.externalIdentity:null;
  if(binding){
   const issuer=node('p',t('accountExternalIssuer')+' · '+binding.issuer);
   const digest=node('p',t('accountExternalDigest')+' · '+binding.subjectSha256);
   issuer.className=digest.className='account-external-identity';
   card.append(issuer,digest);
  }
  if(managed&&!user.deleted){
   const actions=node('div','');
   actions.className='detail-actions';
   actions.append(button('accountEdit',()=>edit('UPDATE',user)),button('accountRotate',()=>edit('TOKEN',user)),button('accountDelete',()=>edit('DELETE',user)));
   if(shared&&state.page.externalIdentityIssuer)actions.append(button(binding?'accountExternalReplace':'accountExternalBind',()=>edit('EXTERNAL_BIND',user)));
   if(binding)actions.append(button('accountExternalRevoke',()=>edit('EXTERNAL_REVOKE',user)));
   card.append(actions);
  }
  root.append(card);
 }
 if(state.draft){$('account-edit-title').textContent=t(state.draft.title);preview();}
}
function grants(){const shared=state?.page?.authority==='SHARED';$('account-node-grants').classList.toggle('hidden',!shared||!['CREATE','UPDATE'].includes(state?.draft?.action));const nodeAdmin=$('account-role').value==='ADMIN';if(nodeAdmin)$('account-node-scope').value='ALL';$('account-node-scope').disabled=nodeAdmin;$('account-node-list-label').classList.toggle('hidden',$('account-node-scope').value==='ALL');const admin=$('account-role').value==='ADMIN';if(admin)$('account-scope').value='ALL';$('account-scope').disabled=admin;const selected=$('account-scope').value==='SELECTED';$('account-task-options').classList.toggle('hidden',!selected);$('account-extra-label').classList.toggle('hidden',!selected);}
function body(){
 const d=state.draft,result={expectedRevision:d.revision};if(d.action==='CREATE')result.id=$('account-id').value.trim();
 if(d.action==='CREATE'||d.action==='UPDATE'){result.role=$('account-role').value;result.enabled=$('account-enabled').checked;result.taskIds=result.role==='ADMIN'||$('account-scope').value==='ALL'?null:[...new Set([...$('account-task-options').querySelectorAll('input:checked')].map(input=>input.value).concat($('account-extra-tasks').value.split(/\r?\n/).map(id=>id.trim()).filter(Boolean)))].sort();}
 if((d.action==='CREATE'||d.action==='UPDATE')&&state.page.authority==='SHARED')result.nodeIds=result.role==='ADMIN'||$('account-node-scope').value==='ALL'?null:[...new Set($('account-node-ids').value.split(/\r?\n/).map(id=>id.trim()).filter(Boolean))].sort();
 if(d.action==='CREATE'||d.action==='TOKEN')result.token=$('account-token').value;
 if(d.action==='EXTERNAL_BIND'){
  result.issuer=state.page.externalIdentityIssuer;
  result.subject=$('account-external-subject').value;
 }
 return result;
}
function preview(){
 if(!state?.draft)return;
 const safe=body();
 if('token' in safe){safe.tokenChanged=Boolean(safe.token);delete safe.token;}
 if('subject' in safe){safe.subjectProvided=Boolean(safe.subject);delete safe.subject;}
 $('account-preview').textContent=JSON.stringify({action:state.draft.action,account:state.draft.id||safe.id,...safe},null,2);
}
function edit(action,user){
 if(state.busy)return;
 cancel();
 const titles={CREATE:'accountsNew',UPDATE:'accountEdit',TOKEN:'accountRotate',DELETE:'accountDelete',
               EXTERNAL_BIND:user?.externalIdentity?'accountExternalReplace':'accountExternalBind',EXTERNAL_REVOKE:'accountExternalRevoke'};
 state.draft={action,title:titles[action],id:user?.id,revision:state.page.revision,pending:null};
 $('account-external-bind').classList.toggle('hidden',action!=='EXTERNAL_BIND');
 $('account-external-revoke').classList.toggle('hidden',action!=='EXTERNAL_REVOKE');
 if(action==='EXTERNAL_BIND')$('account-external-issuer').value=state.page.externalIdentityIssuer;
 $('account-id').value=user?.id||'';$('account-id').disabled=action!=='CREATE';$('account-role').value=user?.role||'VIEWER';$('account-enabled').checked=user?user.enabled:true;$('account-scope').value=user?.taskIds===null?'ALL':'SELECTED';
 $('account-node-scope').value=user?Array.isArray(user.nodeIds)?'SELECTED':'ALL':'SELECTED';$('account-node-ids').value=Array.isArray(user?.nodeIds)?user.nodeIds.join('\n'):!user?(bridge.session()?.nodeId||''):'';
 const known=bridge.tasks(),selected=new Set(user?.taskIds||[]),root=$('account-task-options');root.replaceChildren();for(const id of known){const label=node('label',''),input=node('input','');input.type='checkbox';input.value=id;input.checked=selected.has(id);input.onchange=preview;label.append(input,node('span',id));root.append(label);}$('account-extra-tasks').value=[...selected].filter(id=>!known.includes(id)).join('\n');
 $('account-grants').classList.toggle('hidden',!['CREATE','UPDATE'].includes(action));$('account-role').parentElement.classList.toggle('hidden',!['CREATE','UPDATE'].includes(action));$('account-enabled').parentElement.classList.toggle('hidden',!['CREATE','UPDATE'].includes(action));$('account-token-section').classList.toggle('hidden',!['CREATE','TOKEN'].includes(action));$('account-delete-help').classList.toggle('hidden',action!=='DELETE');$('account-form').classList.remove('hidden');grants();render();
}
async function load(){if(!state||state.busy)return;const ticket=state.ticket;state.busy=true;cancel();render();try{const page=await bridge.api('/accounts');if(!current(ticket))return;state.page=page;$('accounts-error').textContent='';}catch(e){if(current(ticket))$('accounts-error').textContent=e.message;}finally{if(current(ticket)){state.busy=false;render();}}}
async function submit(event){
 event.preventDefault();if(!state?.draft||state.busy)return;const ticket=state.ticket,draft=state.draft,payload=body(),secret=payload.token,self=draft.id===bridge.session()?.id;
 if('token'in payload&&!$('account-token-saved').checked){$('account-error').textContent=t('accountSaveTokenFirst');return;}
 if('subject' in payload&&!/^[!-~]{1,255}$/.test(payload.subject)){$('account-error').textContent=t('accountExternalSubjectInvalid');return;}
 if(payload.id&&!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(payload.id)||payload.token!==undefined&&(payload.token.length<32||payload.token.length>4096||!/^[!-~]+$/.test(payload.token))||payload.taskIds?.some(id=>!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(id))||payload.nodeIds?.some(id=>!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(id))){$('account-error').textContent=t('accountInvalid');return;}
 const json=JSON.stringify(payload);if(!draft.pending||draft.pending.json!==json)draft.pending={json,key:bridge.requestId()};state.busy=true;$('account-error').textContent='';render();
 const external=draft.action==='EXTERNAL_BIND'||draft.action==='EXTERNAL_REVOKE';
 const method=draft.action==='CREATE'||draft.action==='TOKEN'?'POST':draft.action==='UPDATE'||draft.action==='EXTERNAL_BIND'?'PUT':'DELETE';
 const suffix=external?'/external-identity':draft.action==='TOKEN'?'/token':'';
 const path=draft.action==='CREATE'?'/accounts':'/accounts/'+encodeURIComponent(draft.id)+suffix;
 try{const page=await bridge.api(path,{method,headers:{'Content-Type':'application/json','X-Request-ID':draft.pending.key},body:json});if(!current(ticket))return;state.page=page;cancel();bridge.notify(t('accountSaved'));if(self&&secret)await bridge.credential(secret);else await bridge.refresh();}
 catch(e){if(current(ticket))$('account-error').textContent=e.code?e.message:t(external?'accountExternalLostResponse':'accountLostResponse');}
 finally{if(current(ticket)){state.busy=false;render();}}
}
async function activate(){if(!state||state.busy||!$('accounts-activation-ack').checked)return;const ticket=state.ticket,payload=JSON.stringify({expectedRevision:state.page.revision});if(!state.activation||state.activation.body!==payload)state.activation={body:payload,key:bridge.requestId()};state.busy=true;render();try{const page=await bridge.api('/accounts/activate',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':state.activation.key},body:payload});if(current(ticket)){state.page=page;state.activation=null;}}catch(e){if(current(ticket))$('accounts-error').textContent=e.message;}finally{if(current(ticket)){state.busy=false;render();}}}
async function open(){if(!bridge.can('ACCOUNT_ADMIN'))return;state={ticket:++generation,page:null,draft:null,busy:false};$('accounts-dialog').showModal();await load();}
$('accounts-nav').onclick=open;$('accounts-close').onclick=close;$('accounts-dialog').addEventListener('cancel',event=>{event.preventDefault();close();});$('accounts-reload').onclick=load;$('accounts-new').onclick=()=>edit('CREATE');$('accounts-activate').onclick=activate;$('accounts-activation-ack').onchange=render;$('account-form').onsubmit=submit;$('account-cancel').onclick=cancel;
$('account-generate').onclick=()=>{$('account-token').value=Array.from(crypto.getRandomValues(new Uint8Array(32)),n=>n.toString(16).padStart(2,'0')).join('');$('account-token-saved').checked=false;preview();};$('account-show-token').onclick=()=>{$('account-token').type=$('account-token').type==='password'?'text':'password';};$('account-role').onchange=()=>{grants();preview();};$('account-scope').onchange=()=>{grants();preview();};$('account-node-scope').onchange=()=>{grants();preview();};$('account-node-ids').addEventListener('input',preview);for(const id of ['account-id','account-enabled','account-token','account-extra-tasks','account-external-subject'])$(id).addEventListener('input',()=>{if(id==='account-token')$('account-token-saved').checked=false;preview();});
window.RivomeshAccounts={close,render,translate:render};render();
})();
