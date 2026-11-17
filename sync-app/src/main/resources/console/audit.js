(()=>{'use strict';
const bridge=window.RivomeshConfiguration,$=id=>document.getElementById(id),t=key=>bridge.t(key);
bridge.register({auditTitle:'安全审计',auditHelp:'按序号从旧到新查看；每页100条。导出当前页包含链校验信息。完整流式导出可使用审计工具。这里只记录管理事件，不含配置正文、凭据或数据行。',auditReload:'刷新保留窗口',auditExport:'导出当前页',auditNext:'下一页',auditProof:'校验与保留边界',auditWindow:'当前页 / 保留窗口',auditSequence:'序号 / 时间',auditActor:'身份 / 角色',auditAction:'事件 / 操作',auditTask:'任务 / 账号',auditResult:'结果',auditCorrelation:'关联标识',auditRetained:'保留上限',auditUnavailable:'审计存储不可用，管理请求已阻止。检查持久目录、容量及备份。',AUDIT_UNAVAILABLE:'审计不可用，管理请求已阻止；已提交操作的结果会从持久待写记录恢复。',AUDIT_HISTORY_EXPIRED:'保留窗口已前移，请刷新；旧窗口不能作为完整导出。',AUDIT_HISTORY_CHANGED:'审计窗口已变化，请重新读取。'},
{auditTitle:'Security audit',auditHelp:'Oldest first, 100 events per page. Page exports include chain proofs; use the audit tool for a streaming full export. Events exclude configuration bodies, credentials and data rows.',auditReload:'Refresh retained window',auditExport:'Export this page',auditNext:'Next page',auditProof:'Integrity and retention boundary',auditWindow:'Page / retained window',auditSequence:'Sequence / time',auditActor:'Identity / role',auditAction:'Event / action',auditTask:'Task / account',auditResult:'Result',auditCorrelation:'Correlation',auditRetained:'Retention limit',auditUnavailable:'Audit storage is unavailable; management requests are blocked. Check persistent storage, capacity and backups.',AUDIT_UNAVAILABLE:'Audit unavailable; management requests are blocked. Committed operation results recover from the durable outbox.',AUDIT_HISTORY_EXPIRED:'Retention advanced. Refresh; the old window cannot be a complete export.',AUDIT_HISTORY_CHANGED:'The audit window changed. Reload.'});
let state=null,generation=0;
function node(tag,value){const element=document.createElement(tag);element.textContent=value==null?'':String(value);return element;}
function current(ticket){return state&&state.generation===ticket&&bridge.can('AUDIT_READ');}
function render(){
 $('audit-nav').disabled=!bridge.can('AUDIT_READ');if(!bridge.can('AUDIT_READ')&&state){close();return;}
 document.querySelectorAll('[data-audit-i18n]').forEach(element=>element.textContent=t(element.dataset.auditI18n));
 const page=state?.page,busy=Boolean(state?.busy);$('audit-reload').disabled=busy||!state;$('audit-export').disabled=busy||!page;$('audit-next').disabled=busy||page?.next==null;
 $('audit-window').textContent=page?t('auditWindow')+': '+page.after+' → '+page.through+' · '+t('auditRetained')+': '+page.retentionEvents:'';
 $('audit-proof').textContent=page?JSON.stringify({storeId:page.storeId,anchorSequence:page.anchorSequence,anchorHash:page.anchorHash,after:page.after,previousHash:page.previousHash,through:page.through,throughHash:page.throughHash,lastSequence:page.lastSequence,lastHash:page.lastHash,next:page.next},null,2):'';
 const root=$('audit-events');root.replaceChildren();if(!page)return;
 const table=node('table',''),head=node('tr','');['auditSequence','auditActor','auditAction','auditTask','auditResult','auditCorrelation'].forEach(label=>head.append(node('th',t(label))));const thead=node('thead','');thead.append(head);table.append(thead);const body=node('tbody','');
 for(const event of page.events){const row=node('tr','');for(const value of [event.sequence+'\n'+event.at,[event.actorId||'—',event.actorRole||''].join('\n'),[event.kind,event.action||''].join('\n'),event.taskId||event.subjectId||'—',[event.status||'',event.httpStatus||'',event.code||''].filter(Boolean).join('\n'),event.correlation||event.operationHash||event.id])row.append(node('td',value));body.append(row);}table.append(body);root.append(table);
}
async function load(next=false){
 if(!state||state.busy||!bridge.can('AUDIT_READ'))return;const ticket=state.generation,previous=state.page;
 state.busy=true;$('audit-error').textContent='';render();
 try{const query=next?'?after='+previous.next+'&through='+previous.through+'&limit=100':'?limit=100';const page=await bridge.api('/audit'+query);if(!current(ticket))return;state.page=page;}
 catch(error){if(current(ticket))$('audit-error').textContent=error.message;}
 finally{if(current(ticket)){state.busy=false;render();}}
}
function close(){generation++;state=null;$('audit-events').replaceChildren();$('audit-proof').textContent='';$('audit-window').textContent='';$('audit-error').textContent='';$('audit-dialog').close();}
async function open(){if(!bridge.can('AUDIT_READ'))return;state={generation:++generation,page:null,busy:false};$('audit-dialog').showModal();await load();}
async function exportPage(){
 if(!state||state.busy||!state.page||!bridge.can('AUDIT_READ'))return;const ticket=state.generation,page=state.page;state.busy=true;render();$('audit-error').textContent='';
 try{const exported=await bridge.api('/audit/export?after='+page.after+'&through='+page.through+'&limit=100');if(!current(ticket))return;
  if(exported.storeId!==page.storeId||exported.after!==page.after||exported.through!==page.through||exported.previousHash!==page.previousHash||exported.throughHash!==page.throughHash)throw new Error(t('AUDIT_HISTORY_CHANGED'));
  const blob=new Blob([JSON.stringify(exported,null,2)+'\n'],{type:'application/json'}),url=URL.createObjectURL(blob),link=node('a','');link.href=url;link.download='rivomesh-audit-'+page.after+'-'+(page.events.at(-1)?.sequence||page.after)+'.json';link.click();setTimeout(()=>URL.revokeObjectURL(url),0);
 }catch(error){if(current(ticket))$('audit-error').textContent=error.message;}
 finally{if(current(ticket)){state.busy=false;render();}}
}
$('audit-nav').onclick=open;$('audit-close').onclick=close;$('audit-dialog').addEventListener('cancel',event=>{event.preventDefault();close();});$('audit-reload').onclick=()=>load();$('audit-next').onclick=()=>load(true);$('audit-export').onclick=exportPage;
window.RivomeshAudit={close,render,translate:render};render();
})();
