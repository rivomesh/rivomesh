'use strict';
window.RivomeshValidation=(()=>{
 const bridge=window.RivomeshConfiguration,{api,can,notify,refresh,requestId,t,explain}=bridge;
 const text=(tag,value,cls)=>{const element=document.createElement(tag);element.textContent=value;if(cls)element.className=cls;return element;};
 const badge=value=>text('span',t(value),'badge');
 const drafts=new Map(),reports=new Map(),plans=new Map(),catalogs=new Map();
 const reportDisclosure=new Set();
 function control(element,...scope){
  element.dataset.validationFocus=JSON.stringify(scope);
  return element;
 }
 function rememberFocus(root,taskId){
  const before=document.activeElement,key=before?.dataset.validationFocus,area=before?.closest('[data-validation-task]');
  if(!key||area?.dataset.validationTask!==taskId||!root.contains(area))return;
  return()=>{
   if(before.isConnected||document.activeElement!==document.body)return;
   const matches=[...root.querySelectorAll('[data-validation-focus]')].filter(element=>
    element.dataset.validationFocus===key&&element.closest('[data-validation-task]')?.dataset.validationTask===taskId);
   if(matches.length===1&&!matches[0].disabled)matches[0].focus({preventScroll:true});
  };
 }
 let reportDialog=null;
 const catalogLabels={
  zh:{open:'浏览历史校验报告',retained:'已留存',empty:'暂无留存报告',back:'返回最新报告',
      search:'按报告 ID 打开',id:'报告 ID',close:'取消',invalid:'请输入有效的报告 ID。',
      mismatch:'返回的报告 ID 与输入不符。',directory:'返回历史目录',
      storage:'查看存储',closeStorage:'关闭',storageTitle:'报告存储检查',observed:'观测时间',
      protected:'该 Agent 正在使用此报告，报告受保护。',unknown:'报告在其他进程中的活动状态未知。',
      fileHelp:'以下为已知文件的观测字节长度。',database:'报告数据库',trace:'数据库诊断',summary:'报告摘要',bytes:'字节',
      retention:'保留审阅',retainUntil:'至少保留至',reviewer:'审阅人',retentionHelp:'审批会记录最低保留期限。到期后，报告和恢复证据继续保留。',
      retentionDays:'审核后至少保留天数',retentionApproval:'确认保留期限',retentionSubmit:'提交保留审阅',request:'请求 ID',
      archive:'报告恢复副本',archived:'归档时间',archiveCopy:'创建恢复副本',archiveRestore:'恢复报告',
      archiveCopyHelp:'把已审阅的报告数据库和摘要复制到 Agent 的受保护归档目录。原报告继续保留在原位置。',
      archiveRestoreHelp:'从核对过的恢复副本补齐缺失文件。已有不同内容的文件会被保留。',
      archiveApproval:'确认此次报告归档操作',archiveReclaim:'回收原位报告空间',
      archiveReclaimHelp:'复核此副本的 SHA-256。审批后只回收原位报告数据库，摘要和恢复副本继续保留。查看行明细前需要先恢复。',
      archiveReclaimApproval:'批准回收原位报告数据库',archiveSha256:'副本描述的 SHA-256'},
  en:{open:'Browse historical reports',retained:'Retained',empty:'No retained reports',back:'Back to latest reports',
      search:'Open by report ID',id:'Report ID',close:'Cancel',invalid:'Enter a valid report ID.',
      mismatch:'The returned report ID differs from the requested ID.',directory:'Back to report directory',
      storage:'View storage',closeStorage:'Close',storageTitle:'Report storage inspection',observed:'Observed at',
      protected:'The Agent is using this report and protecting it.',unknown:'Activity in other processes is unknown.',
      fileHelp:'Observed byte lengths for known files.',database:'Report database',trace:'Database diagnostics',
      summary:'Report summary',bytes:'bytes',retention:'Retention review',retainUntil:'Retain until at least',reviewer:'Reviewed by',
      retentionHelp:'Approval records a minimum retention period. Reports and recovery evidence remain retained after this date.',
      retentionDays:'Minimum days to retain after review',retentionApproval:'Confirm minimum retention',
      retentionSubmit:'Submit retention review',request:'Request ID',archive:'Report recovery copy',archived:'Archived at',
      archiveCopy:'Create recovery copy',archiveRestore:'Restore report',
      archiveCopyHelp:'Copy the reviewed report database and summary to the protected Agent archive. The live report remains at its original location.',
      archiveRestoreHelp:'Restore missing files from the verified recovery copy. Existing files with different content are preserved.',
      archiveApproval:'Confirm report archive operation',archiveReclaim:'Reclaim live report storage',
      archiveReclaimHelp:'Review the recovery copy SHA-256. Approval reclaims the live report database and keeps its summary and recovery copy. Restore it before accessing row details.',
      archiveReclaimApproval:'Approve reclaiming the live report database',archiveSha256:'Recovery copy descriptor SHA-256'}
 };
 const catalogLabel=key=>catalogLabels[document.documentElement.lang.startsWith('zh')?'zh':'en'][key];
 const fetchReport=(task,id)=>api('/tasks/'+encodeURIComponent(task.id)+'/validations/'+encodeURIComponent(id));

 function closeDialog(){
  if(!reportDialog)return;
  const dialog=reportDialog;reportDialog=null;dialog.close();dialog.remove();
 }
 function searchReport(task){
  closeDialog();
  const dialog=text('dialog','','validation-report-search'),heading=text('h2',catalogLabel('search')),
   form=text('form'),field=text('label'),input=text('input'),message=text('p','','error'),actions=text('div','','dialog-actions'),
   cancel=text('button',catalogLabel('close'),'quiet'),submit=text('button',label('view'),'primary');
  reportDialog=dialog;
  heading.id='validation-report-search-title';dialog.setAttribute('aria-labelledby',heading.id);
  input.required=true;input.maxLength=64;input.autocomplete='off';input.name='reportId';
  message.setAttribute('role','alert');
  cancel.type='button';cancel.onclick=closeDialog;submit.type='submit';
  dialog.addEventListener('cancel',event=>{event.preventDefault();closeDialog();});
  field.append(text('span',catalogLabel('id')),input);actions.append(cancel,submit);form.append(field,message,actions);
  form.onsubmit=async event=>{
   event.preventDefault();
   if(submit.disabled)return;
   const id=input.value.trim().toLowerCase();
   if(!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(id)){
    message.textContent=catalogLabel('invalid');input.focus();return;
   }
   submit.disabled=true;message.textContent='';
   try{
    const report=await fetchReport(task,id);
    if(reportDialog!==dialog)return;
    if(report?.id!==id)throw new Error(catalogLabel('mismatch'));
    const state=catalogs.get(task.id);
    if(!state?.page){closeDialog();return;}
    reports.set(task.id+'/'+id,{report,page:0});
    catalogs.set(task.id,{...state,selectedId:id});
    closeDialog();bridge.render();
   }catch(error){
    if(reportDialog===dialog){message.textContent=error.message;submit.disabled=false;}
   }
  };
  dialog.append(heading,form);document.body.append(dialog);dialog.showModal();input.focus();
 }
 async function inspectStorage(task,id){
  if(!can('TASK_CONFIG'))return;
  closeDialog();
  const dialog=text('dialog','','validation-storage-dialog'),heading=text('h2',catalogLabel('storageTitle')),
   close=text('button',catalogLabel('closeStorage'),'quiet'),body=text('section',t('loading'));
  reportDialog=dialog;
  heading.id='validation-storage-title';dialog.setAttribute('aria-labelledby',heading.id);
  close.type='button';close.onclick=closeDialog;
  dialog.addEventListener('cancel',event=>{event.preventDefault();closeDialog();});
  body.setAttribute('role','status');body.dataset.validationStorageResult='';
  dialog.append(heading,close,body);document.body.append(dialog);dialog.showModal();
  const current=()=>reportDialog===dialog&&can('TASK_CONFIG')&&!!bridge.task(task.id);
  try{
   const path='/tasks/'+encodeURIComponent(task.id)+'/validations/'+encodeURIComponent(id);
   const storage=await api(path+'/storage');
   if(!current())return;
   if(storage?.id!==id||!['PROTECTED_ACTIVE','ACTIVITY_UNKNOWN'].includes(storage.activity)
      ||!Number.isFinite(Date.parse(storage.observedAt)))throw new Error(catalogLabel('mismatch'));
   const format=new Intl.NumberFormat(document.documentElement.lang),facts=text('dl');
   for(const [filename,key] of [['rows.mv.db','database'],['rows.trace.db','trace'],['summary.json','summary']]){
    const bytes=storage.files?.[filename];
    if(Number.isSafeInteger(bytes)&&bytes>=0)facts.append(text('dt',catalogLabel(key)),
     text('dd',format.format(bytes)+' '+catalogLabel('bytes')));
   }
   body.replaceChildren(text('p',catalogLabel(storage.activity==='PROTECTED_ACTIVE'?'protected':'unknown'),'callout'),
    text('p',catalogLabel('observed')+' '+new Date(storage.observedAt).toLocaleString()),text('p',catalogLabel('fileHelp')),facts);
   const retention=storage.retention;
   if(retention&&(retention.reportId!==id||!/^[A-Za-z0-9_-]{8,128}$/.test(retention.requestId||'')
      ||!Number.isFinite(Date.parse(retention.minimumRetainUntil))))throw new Error(catalogLabel('mismatch'));
   if(retention)facts.append(text('dt',catalogLabel('retainUntil')),text('dd',new Date(retention.minimumRetainUntil).toLocaleString()),
    text('dt',catalogLabel('reviewer')),text('dd',retention.actorId));
   const minimum=storage.retentionMinimumDays,revision=bridge.task(task.id)?.revision;
   if(revision&&storage.activity==='ACTIVITY_UNKNOWN'&&!storage.restoreRequired&&Number.isInteger(minimum)&&minimum>0&&minimum<=3650)
    dialog.append(retentionReview(path,revision,minimum,retention,current));
   const archive=storage.archive;
   if(archive&&(!/^[A-Za-z0-9_-]{8,128}$/.test(archive.id||'')
      ||!Number.isFinite(Date.parse(archive.archivedAt))))throw new Error(catalogLabel('mismatch'));
   if(archive)facts.append(text('dt',catalogLabel('archived')),text('dd',new Date(archive.archivedAt).toLocaleString()));
   if(storage.restoreRequired)body.append(text('p',explain('VALIDATION_REPORT_ARCHIVE_RESTORE_REQUIRED'),'callout'));
   if(revision&&storage.archiveEnabled&&storage.activity==='ACTIVITY_UNKNOWN'&&
      (storage.restoreRequired&&archive||!archive&&retention))
    dialog.append(archiveReview(path,revision,retention,archive,current,archive?'RESTORE':'COPY'));
   if(revision&&storage.reclaimEnabled&&storage.activity==='ACTIVITY_UNKNOWN'&&!storage.restoreRequired
      &&bridge.task(task.id)?.desired==='STOPPED'&&/^[0-9a-f]{64}$/.test(archive?.sha256||'')
      &&retention&&Date.parse(retention.minimumRetainUntil)<=Date.now())
    dialog.append(archiveReview(path,revision,retention,archive,current,'RECLAIM'));
  }catch(error){
   if(current())body.replaceChildren(text('p',error.message,'error'));
  }
 }

 function retentionReview(path,revision,minimum,previous,current){
  const form=text('form'),days=text('input'),dateLabel=text('label'),approval=text('input'),
   approvalLabel=text('label','','check'),button=text('button',catalogLabel('retentionSubmit'),'primary'),
   feedback=text('p','','error');
  days.type='number';days.name='minimumDays';days.min=String(minimum);days.max='3650';
  days.required=true;days.value=String(Math.max(minimum,previous?.minimumDays||0));
  approval.type='checkbox';approval.name='approved';approval.required=true;
  button.type='submit';feedback.setAttribute('role','status');
  dateLabel.append(text('span',catalogLabel('retentionDays')),days);
  approvalLabel.append(approval,text('span',catalogLabel('retentionApproval')));
  form.append(text('h3',catalogLabel('retention')),text('p',catalogLabel('retentionHelp')),dateLabel,approvalLabel,button,feedback);
  wireReview(form,button,[days,approval],approval,feedback,current,path+'/retention',()=>({
   minimumDays:days.valueAsNumber,approved:true,expectedRevision:revision,...(previous?{expectedReviewId:previous.requestId}:{})
  }));
  return form;
 }
 function archiveReview(path,revision,retention,archive,current,operation){
  const reclaim=operation==='RECLAIM',labelKey=reclaim?'archiveReclaim':operation==='RESTORE'?'archiveRestore':'archiveCopy';
  const form=text('form'),approval=text('input'),approvalLabel=text('label','','check'),
   button=text('button',catalogLabel(labelKey),'primary'),feedback=text('p','','error');
  approval.type='checkbox';approval.name='archiveApproved';approval.required=true;
  button.type='submit';feedback.setAttribute('role','status');form.dataset.validationArchiveForm='';
  approvalLabel.append(approval,text('span',catalogLabel(reclaim?'archiveReclaimApproval':'archiveApproval')));
  form.append(text('h3',catalogLabel(labelKey)),text('p',catalogLabel(labelKey+'Help')));
  if(reclaim)form.append(text('p',catalogLabel('archiveSha256')),text('code',archive.sha256));
  form.append(approvalLabel,button,feedback);
  wireReview(form,button,[approval],approval,feedback,current,path+'/archive',()=>({
   operation,expectedRevision:revision,approved:true,
   ...(archive?{expectedArchiveId:archive.id}:{}),
   ...(reclaim||!archive?{expectedReviewId:retention.requestId}:{}),
   ...(reclaim?{expectedArchiveSha256:archive.sha256}:{})
  }));
  return form;
 }
 function wireReview(form,button,inputs,approval,feedback,current,path,body){
  let delivery=null;
  form.onsubmit=async event=>{
   event.preventDefault();
   if(!current()||button.disabled||!approval.checked||!form.checkValidity())return;
   if(!delivery)delivery={id:requestId(),body:JSON.stringify(body())};
   button.disabled=true;inputs.forEach(input=>input.disabled=true);feedback.textContent='';
   try{
    await api(path,{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':delivery.id},body:delivery.body});
    if(!current())return;
    feedback.className='';feedback.textContent=t('accepted')+' · '+catalogLabel('request')+' '+delivery.id;
    await refresh();
   }catch(error){
    if(current()){feedback.textContent=error.message;button.disabled=false;}
   }
  };
 }

 async function loadCatalog(task,after='',previous=[]){
  const current=catalogs.get(task.id);
  if(current?.loading)return;
  const pending={...current,loading:true};
  catalogs.set(task.id,pending);
  bridge.render();
  try{
   const path='/tasks/'+encodeURIComponent(task.id)+'/validations?limit=20'
    +(after?'&after='+encodeURIComponent(after):'');
   const page=await api(path);
   if(catalogs.get(task.id)===pending)catalogs.set(task.id,{page,after,previous});
  }catch(error){
   if(catalogs.get(task.id)===pending){
    if(current)catalogs.set(task.id,current);else catalogs.delete(task.id);
    notify(error.message);
   }
  }
  bridge.render();
 }
 function renderCatalog(section,task){
  const state=catalogs.get(task.id);
  const controls=text('div','','validation-catalog-actions');
  const browse=control(text('button',catalogLabel('open'),'quiet'),'catalog','browse');
  browse.dataset.validationCatalog='open';
  browse.disabled=!!state?.loading;
  browse.onclick=()=>loadCatalog(task);
  controls.append(browse);
  if(state?.page){
   const back=control(text('button',catalogLabel('back'),'quiet'),'catalog','latest');
   back.onclick=()=>{catalogs.delete(task.id);bridge.render();};
   const search=control(text('button',catalogLabel('search'),'quiet'),'catalog','search');
   search.dataset.validationSearch='';
   search.disabled=!!state.loading;search.onclick=()=>searchReport(task);
   controls.append(back,search);
   if(state.selectedId){
    const directory=control(text('button',catalogLabel('directory'),'quiet'),'catalog',state.selectedId,'directory');
    directory.onclick=()=>{catalogs.set(task.id,{...state,selectedId:null});bridge.render();};
    controls.append(directory);
   }else{
    const previous=control(text('button',label('previous'),'quiet'),'catalog',state.after,'previous'),
     next=control(text('button',label('next'),'quiet'),'catalog',state.after,'next');
    previous.disabled=!!state.loading||!state.previous.length;
    next.disabled=!!state.loading||!state.page.nextCursor;
    previous.onclick=()=>loadCatalog(task,state.previous.at(-1),state.previous.slice(0,-1));
    next.onclick=()=>loadCatalog(task,state.page.nextCursor,[...state.previous,state.after]);
    controls.append(previous,next);
   }
  }
  section.append(controls);
  if(state?.selectedId)return {reports:[state.page.reports.find(report=>report.id===state.selectedId)||{id:state.selectedId}]};
  return state?.page;
 }
 const words={zh:{title:'数据校验',help:'校验会短时阻塞 MySQL 写入，并暂停同步。完成后需检查结果，再显式恢复。当前首批真实验收目标为 MySQL/PostgreSQL；其他 SQL 目标须单独验收。',targets:'选择事务型目标',gate:'源写锁上限（秒）',read:'源扫描上限（秒）',targetRead:'目标表扫描上限（秒）',row:'单行预算（MiB）',quota:'暂存逻辑预算（MiB）',start:'开始逐行校验',cancel:'取消校验',view:'查看报告',retry:'继续目标比对',report:'校验报告',source:'源行数',target:'目标行数',missing:'缺失',extra:'多余',changed:'内容变更',table:'表',destination:'目标',previous:'上一页',next:'下一页',empty:'暂无校验记录',pending:'作业已保存，正在等待或执行。取消不恢复同步。',MATCH:'数据匹配',DIFF:'发现差异',scope:'至少选择一个事务型 SQL 目标。',invalid:'请输入有效的时间和资源预算。',differences:'查看差异',raw:'查看原始值（敏感数据）',expected:'源校验点的值',actual:'目标校验点的值',rowId:'内部行身份',noRow:'不存在该行',nullValue:'SQL NULL',column:'字段',kind:'差异类型',historical:'这是指定历史校验点的结果，不表示当前数据仍一致，也不执行修复或业务切换。'},en:{title:'Data validation',help:'Validation briefly blocks MySQL writes and pauses replication. Review results, then resume explicitly. First native profiles cover MySQL/PostgreSQL; other SQL destinations require separate acceptance.',targets:'Select transactional destinations',gate:'Source write gate (seconds)',read:'Source scan limit (seconds)',targetRead:'Target table scan limit (seconds)',row:'Per-row budget (MiB)',quota:'Logical staging budget (MiB)',start:'Compare rows',cancel:'Cancel validation',view:'View report',retry:'Retry target comparison',report:'Validation report',source:'Source rows',target:'Target rows',missing:'Missing',extra:'Extra',changed:'Changed',table:'Table',destination:'Destination',previous:'Previous',next:'Next',empty:'No validation jobs yet',pending:'Job saved; queued or executing. Cancellation does not resume replication.',MATCH:'Data matches',DIFF:'Differences found',scope:'Select at least one transactional SQL destination.',invalid:'Enter valid time and resource limits.',differences:'View differences',raw:'Reveal raw values (sensitive)',expected:'Source cut value',actual:'Target cut value',rowId:'Internal row identity',noRow:'Row absent',nullValue:'SQL NULL',column:'Column',kind:'Difference kind',historical:'This result describes a historical cut, not current equality, repair or permission to cut over.'}};
 const label=key=>words[document.documentElement.lang.startsWith('zh')?'zh':'en'][key]||key;
 const translationZh={validate:'数据校验',validate_retry:'继续目标比对',validate_cancel:'取消校验',VALIDATION_MATCH:'校验数据匹配',VALIDATION_DIFFERENCES:'校验发现差异',VALIDATION_CANCELLED:'校验已取消，请检查任务暂停状态',VALIDATION_CANCEL_REQUESTED:'已请求取消校验',VALIDATION_REPORT_BUSY:'校验正在使用报告，请稍后查看。',VALIDATION_REPORT_NOT_CREATED:'尚未建立报告。请查看操作记录中的失败原因。',VALIDATION_WORKER_REQUIRED:'请先启动当前 Worker，完成全量并进入增量后再校验。',VALIDATION_NEW_SOURCE_VIEW_REQUIRED:'源扫描未完成且原视图已丢失。检查并恢复任务后，重新开始校验。',VALIDATION_BARRIER_MOVED:'任务状态或断点已变化，需要新的校验。',VALIDATION_V3_REQUIRED:'数据校验仅支持 V3 任务。',VALIDATION_DETAIL_TOO_LARGE:'原始行超过界面显示预算（1MiB），请保留私有报告并使用受控离线工具处理。',VALIDATION_DIFFERENCE_NOT_FOUND:'该身份没有对应的差异记录。',VALIDATION_DIFF_CORRUPT:'差异内容校验失败，未返回原始值。'};
 const translationEn={validate:'Validate data',validate_retry:'Retry target comparison',validate_cancel:'Cancel validation',VALIDATION_MATCH:'Validation matched',VALIDATION_DIFFERENCES:'Validation found differences',VALIDATION_CANCELLED:'Validation cancelled; inspect the task pause state',VALIDATION_CANCEL_REQUESTED:'Validation cancellation requested',VALIDATION_REPORT_BUSY:'Validation is using this report; retry later.',VALIDATION_REPORT_NOT_CREATED:'No report was created yet. Inspect the operation failure.',VALIDATION_WORKER_REQUIRED:'Start this worker, complete snapshots and enter CDC before validation.',VALIDATION_NEW_SOURCE_VIEW_REQUIRED:'Incomplete source view was lost. Inspect/resume the task, then start a new validation.',VALIDATION_BARRIER_MOVED:'Task state or checkpoints changed; establish a new validation.',VALIDATION_V3_REQUIRED:'Data validation requires a V3 pipeline.',VALIDATION_DETAIL_TOO_LARGE:'Raw rows exceed the console display budget (1MiB). Retain the private report and use controlled offline tooling.',VALIDATION_DIFFERENCE_NOT_FOUND:'No difference exists for this identity.',VALIDATION_DIFF_CORRUPT:'Difference integrity check failed; raw values were withheld.'};
 bridge.register(translationZh,translationEn);
 bridge.register({
  validation_archive_copy:'创建报告恢复副本',validation_archive_restore:'恢复报告',
  validation_archive_reclaim:'回收原位报告空间',VALIDATION_ARCHIVE_RECLAIMED:'原位报告数据库占用的空间已回收。',
  VALIDATION_ARCHIVE_RECOVERY_DEPENDENCY:'报告仍需用于恢复核对，或存在已有的恢复计划。',
  VALIDATION_ARCHIVE_RECLAIM_REQUIRES_STOPPED:'请先停止同步任务。',
  VALIDATION_RETENTION_HOLD_ACTIVE:'报告尚未达到最低保留期限。',
  VALIDATION_ARCHIVE_SEALED:'报告恢复副本已核对并保存。',VALIDATION_ARCHIVE_RESTORED:'已从核对的副本恢复报告。',
  VALIDATION_REPORT_ARCHIVE_RESTORE_REQUIRED:'报告需要恢复。请在“查看存储”中查看恢复副本。',
  VALIDATION_ARCHIVE_HOT_FILE_CHANGED:'现有报告文件与恢复副本不同。请保留两份证据并核对。',
  VALIDATION_ARCHIVE_BYTES_CHANGED:'恢复副本字节校验失败，请核对归档卷和备份。',
  validation_retention_review:'审阅报告保留期限',VALIDATION_RETENTION_REVIEWED:'已记录最低保留期限。',
  VALIDATION_RETENTION_REVIEW_DISABLED:'该 Agent 未启用保留审阅。',
  VALIDATION_RETENTION_REVIEW_CHANGED:'已有新的保留审阅。请关闭并重新打开存储检查。',
  VALIDATION_RETENTION_CANNOT_SHORTEN:'最低保留期限只能延长。',
  VALIDATION_RETENTION_MINIMUM_NOT_MET:'请选择符合 Agent 策略的保留天数。',
  VALIDATION_RETENTION_REPORT_INCOMPLETE:'校验报告完成后才能审阅保留期限。'
 },{
  validation_archive_copy:'Create report recovery copy',validation_archive_restore:'Restore report',
  validation_archive_reclaim:'Reclaim live report storage',VALIDATION_ARCHIVE_RECLAIMED:'Live report database storage was reclaimed.',
  VALIDATION_ARCHIVE_RECOVERY_DEPENDENCY:'The report may be needed for recovery, or already has an existing recovery plan.',
  VALIDATION_ARCHIVE_RECLAIM_REQUIRES_STOPPED:'Stop the synchronization task first.',
  VALIDATION_RETENTION_HOLD_ACTIVE:'The minimum report retention period is still active.',
  VALIDATION_ARCHIVE_SEALED:'The report recovery copy was verified and saved.',VALIDATION_ARCHIVE_RESTORED:'The report was restored from its verified copy.',
  VALIDATION_REPORT_ARCHIVE_RESTORE_REQUIRED:'This report requires restoration. Open View storage to find its recovery copy.',
  VALIDATION_ARCHIVE_HOT_FILE_CHANGED:'An existing report file differs from the recovery copy. Preserve both and inspect them.',
  VALIDATION_ARCHIVE_BYTES_CHANGED:'The recovery copy failed its integrity check. Inspect the archive volume and backup.',
  validation_retention_review:'Review report retention',VALIDATION_RETENTION_REVIEWED:'Minimum retention recorded.',
  VALIDATION_RETENTION_REVIEW_DISABLED:'This Agent has retention reviews disabled.',
  VALIDATION_RETENTION_REVIEW_CHANGED:'A newer retention review exists. Close and reopen storage inspection.',
  VALIDATION_RETENTION_CANNOT_SHORTEN:'The existing minimum retention can only be extended.',
  VALIDATION_RETENTION_MINIMUM_NOT_MET:'Choose a duration that meets the Agent policy.',
  VALIDATION_RETENTION_REPORT_INCOMPLETE:'The report must finish before its retention can be reviewed.'
 });
 async function submit(task,action,validation,repair){
  try{await api('/tasks/'+encodeURIComponent(task.id)+'/actions',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':requestId()},body:JSON.stringify({action,expectedRevision:task.revision,...(validation?{validation}:{}),...(repair?{repair}:{})})});notify(t('accepted'));await refresh();}
  catch(error){notify(error.message);await refresh();}
 }
 async function loadDifferences(task,id,cached,result,after,previous){
  try{const page=await api('/tasks/'+encodeURIComponent(task.id)+'/validations/'+encodeURIComponent(id)+'/differences',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':requestId()},body:JSON.stringify({target:result.target,tableId:result.tableId,after,limit:25})});cached.details={...page,after,previous,values:new Map()};bridge.render();}catch(error){notify(error.message);}
 }
 function renderDifferences(root,task,id,entry,cached,names){
  const page=cached.details,section=text('section','','validation-differences');section.append(text('h3',page.target+' · '+(names.get(page.tableId)||page.tableId)));
  for(const item of page.items||[]){const article=text('article','','subscription-card');article.append(text('strong',item.kind),text('p',item.identityHash,'subtle'));const reveal=control(text('button',label('raw')),'report',id,entry,page.target,page.tableId,page.after,item.identityHash,'reveal');reveal.dataset.validationValues=item.identityHash;
   reveal.onclick=async()=>{try{const value=await api('/tasks/'+encodeURIComponent(task.id)+'/validations/'+encodeURIComponent(id)+'/values',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':requestId()},body:JSON.stringify({target:page.target,tableId:page.tableId,identityHash:item.identityHash})});page.values.set(item.identityHash,value);bridge.render();}catch(error){notify(error.message);}};article.append(reveal);
   const value=page.values.get(item.identityHash);if(value){if(value.values.expectedRowId||value.values.actualRowId)article.append(text('pre',label('rowId')+' · '+label('expected')+': '+(value.values.expectedRowId||label('noRow'))+' · '+label('actual')+': '+(value.values.actualRowId||label('noRow'))));const table=text('table'),head=text('tr');for(const key of ['column','expected','actual'])head.append(text('th',label(key)));table.append(head);
    value.columns.forEach((column,index)=>{const row=text('tr');row.append(text('td',column.name+' · '+column.type));for(const side of ['expected','actual']){const values=value.values[side],cell=text('td');cell.append(!values?text('span',label('noRow'),'subtle'):values[index].value===null?text('span',label('nullValue'),'badge'):text('pre',values[index].value));row.append(cell);}table.append(row);});article.append(table);
   }section.append(article);
  }
  const previous=control(text('button',label('previous')),'report',id,entry,page.target,page.tableId,page.after,'differences-previous');previous.disabled=!page.previous.length;previous.onclick=()=>loadDifferences(task,id,cached,page,page.previous[page.previous.length-1],page.previous.slice(0,-1));const next=control(text('button',label('next')),'report',id,entry,page.target,page.tableId,page.after,'differences-next');next.disabled=!page.next;next.onclick=()=>loadDifferences(task,id,cached,page,page.next,[...page.previous,page.after]);section.append(previous,next);root.append(section);
 }
 const repairWords={zh:{title:'数据修复',plan:'生成修复计划',review:'审阅修复计划',execute:'确认执行 / 重试修复',cancel:'请求取消修复',select:'选择修复',confirm:'我已核对目标、表、改动数量和预算，确认修改目标数据。',boundary:'修复按表提交。取消不撤销已提交的表，重试会检查凭据。完成后仍保持暂停；历史差异报告不会改写。当前支持 MySQL/PostgreSQL，触发器、外键等依赖需要独立计划。',scope:'请显式选择1至200张有差异的表。',actor:'计划创建人',limits:'预算',receipt:'目标提交凭据',replayed:'此前已提交',committed:'本次已提交',remove:'删除',insert:'插入'},en:{title:'Data repair',plan:'Create repair plan',review:'Review repair plan',execute:'Confirm / retry repair',cancel:'Request repair cancellation',select:'Select repair',confirm:'I reviewed destinations, tables, change counts and budgets and approve these target changes.',boundary:'Repair commits per table. Cancellation does not undo committed tables; retries check receipts. The task remains paused and historical differences are retained. MySQL/PostgreSQL only; triggers and foreign keys require a separate dependency plan.',scope:'Explicitly select 1 to 200 differing tables.',actor:'Plan creator',limits:'Budgets',receipt:'Target commit receipts',replayed:'Previously committed',committed:'Committed this attempt',remove:'Delete',insert:'Insert'}};
 const repairLabel=key=>repairWords[document.documentElement.lang.startsWith('zh')?'zh':'en'][key]||key;
 bridge.register({repair_plan:'生成修复计划',repair_execute:'执行修复',repair_cancel:'取消修复',REPAIR_PLAN_READY:'修复计划已生成，请审阅后确认',REPAIR_COMPLETED:'修复提交已确认，任务保持暂停',REPAIR_CANCELLED:'修复已取消；已提交的表仍然有效，请查看凭据或重试',REPAIR_CANCEL_REQUESTED:'已请求取消修复',REPAIR_DEPENDENCY_PLAN_REQUIRED:'目标存在触发器、外键、行策略或继承等依赖，需要单独迁移计划。',REPAIR_METADATA_VISIBILITY_REQUIRED:'无法确认 MySQL 元数据完整可见。首版需要直接授予全局 SELECT 和 TRIGGER，暂不解析仅通过角色授予的权限。',REPAIR_TARGET_DRIFTED:'目标数据已变化。请重新校验并生成新计划。',REPAIR_APPROVAL_HASH_MISMATCH:'确认摘要与计划不一致，未执行修复。',REPAIR_TARGET_PROFILE_UNVERIFIED:'此目标尚未开放修复，请使用已验收的 MySQL/PostgreSQL 目标。'}, {repair_plan:'Create repair plan',repair_execute:'Execute repair',repair_cancel:'Cancel repair',REPAIR_PLAN_READY:'Repair plan ready; review before confirmation',REPAIR_COMPLETED:'Repair commits confirmed; task remains paused',REPAIR_CANCELLED:'Repair cancelled; committed tables remain. Inspect receipts or retry.',REPAIR_CANCEL_REQUESTED:'Repair cancellation requested',REPAIR_DEPENDENCY_PLAN_REQUIRED:'Triggers, foreign keys, row policies or inheritance require a separate dependency plan.',REPAIR_METADATA_VISIBILITY_REQUIRED:'Complete MySQL metadata visibility is unconfirmed. Initial support requires direct global SELECT and TRIGGER grants; role-only grants are not resolved.',REPAIR_TARGET_DRIFTED:'Target data changed. Obtain a new validation and plan.',REPAIR_APPROVAL_HASH_MISMATCH:'Approval does not match the plan; repair was refused.',REPAIR_TARGET_PROFILE_UNVERIFIED:'Repair is not enabled for this target profile; use accepted MySQL/PostgreSQL profiles.'});
 function renderRepairs(root,task,pending){
  if(!can('TASK_CONFIG'))return;const jobs=task.operations.filter(op=>op.repair);if(!jobs.length)return;const section=text('section','','repair-panel');section.dataset.validationTask=task.id;section.append(text('h2',repairLabel('title')),text('p',repairLabel('boundary'),'callout'));
  for(const job of jobs.slice().reverse()){const card=text('article','','subscription-card');card.append(text('strong',job.repair.planId),badge(job.status),text('p',job.code?explain(job.code):label('pending')));
   if(job.status==='PENDING'){const cancel=control(text('button',repairLabel('cancel')),'repair',job.repair.planId,job.id,'cancel');cancel.dataset.repairAction='cancel';cancel.disabled=job.cancelRequested;cancel.onclick=()=>submit(task,'REPAIR_CANCEL');card.append(cancel);}
   const review=control(text('button',repairLabel('review')),'repair',job.repair.planId,job.id,'review');review.dataset.repairPlan=job.repair.planId;review.disabled=pending||(job.action==='REPAIR_PLAN'&&job.code!=='REPAIR_PLAN_READY');review.onclick=async()=>{try{const plan=await api('/tasks/'+encodeURIComponent(task.id)+'/validations/'+encodeURIComponent(job.repair.reportId)+'/repairs/'+encodeURIComponent(job.repair.planId));plans.set(task.id,{plan,confirmed:false});bridge.render();}catch(error){notify(error.message);}};card.append(review);
   if(job.result&&job.result.receipts){card.append(text('h3',repairLabel('receipt')));for(const [scope,receipt] of Object.entries(job.result.receipts)){card.append(text('p',scope+' · '+repairLabel(receipt.alreadyApplied?'replayed':'committed')+' · '+repairLabel('remove')+' '+receipt.deleted+' / '+repairLabel('insert')+' '+receipt.inserted));}}
   section.append(card);
  }
  const cached=plans.get(task.id);if(cached){const plan=cached.plan,review=text('article','','repair-review');review.append(text('h3',repairLabel('review')),text('p',repairLabel('actor')+': '+plan.createdBy+' · '+plan.createdAt),text('pre',plan.id+'\n'+plan.hash),text('p',repairLabel('limits')+': '+plan.maximumAffectedRows+' / '+(plan.maximumProbeBytes/1048576)+' MiB / '+(plan.maximumMillis/1000)+' s'));
   const table=text('table'),head=text('tr');for(const key of ['destination','table','missing','extra','changed'])head.append(text('th',label(key)));table.append(head);for(const row of Object.values(plan.tables)){const line=text('tr');for(const value of [row.target,(plan.tableNames||{})[row.tableId]||row.tableId,row.missing,row.extra,row.changed])line.append(text('td',String(value)));table.append(line);}review.append(table);
   const confirm=text('label'),checkbox=control(document.createElement('input'),'repair',plan.id,plan.hash,'confirm');checkbox.type='checkbox';checkbox.checked=cached.confirmed;checkbox.disabled=pending;checkbox.dataset.repairConfirm='true';checkbox.onchange=()=>{cached.confirmed=checkbox.checked;bridge.render();};confirm.append(checkbox,document.createTextNode(repairLabel('confirm')));review.append(confirm);const execute=control(text('button',repairLabel('execute'),'primary'),'repair',plan.id,plan.hash,'execute');execute.dataset.repairAction='execute';execute.disabled=pending||!cached.confirmed||task.status.run_state!=='PAUSED';execute.onclick=()=>{if(!cached.confirmed)return;cached.confirmed=false;submit(task,'REPAIR_EXECUTE',null,{reportId:plan.reportId,planId:plan.id,approvedHash:plan.hash});};review.append(execute);section.append(review);
  }root.append(section);
 }
 function render(root,task,pending){
  if(task.formatVersion!==3)return;
  const section=text('section','','validation-panel'),heading=text('h2',label('title'));
  section.dataset.validationTask=task.id;section.append(heading,text('p',label('help'),'callout'));
  const choices=(task.destinations||[]).filter(target=>target.consistency==='TRANSACTIONAL');
  let draft=drafts.get(task.id);if(!draft){draft={targets:new Set(choices.map(target=>target.id)),gate:30,read:3600,targetRead:600,quota:1024,row:4};drafts.set(task.id,draft);}
  if(can('TASK_CONFIG')){
   const fields=text('fieldset'),legend=text('legend',label('targets'));fields.append(legend);
   for(const target of choices){const row=text('label'),input=document.createElement('input');input.type='checkbox';input.checked=draft.targets.has(target.id);input.disabled=pending;input.dataset.validationTarget=target.id;input.onchange=()=>input.checked?draft.targets.add(target.id):draft.targets.delete(target.id);row.append(input,document.createTextNode(target.id+' · '+target.product));fields.append(row);}section.append(fields);
   const controls=text('div','','form-grid');for(const [key,min,max] of [['gate',1,120],['read',1,86400],['targetRead',1,86400],['quota',4,1048576],['row',1,32]]){const row=text('label'),input=document.createElement('input');input.type='number';input.min=min;input.max=max;input.value=draft[key];input.dataset.validationLimit=key;input.disabled=pending;input.oninput=()=>draft[key]=Number(input.value);row.append(text('span',label(key)),input);controls.append(row);}section.append(controls);
   const start=text('button',label('start'),'primary');start.dataset.validationAction='start';start.disabled=pending||task.workerState!=='RUNNING'||task.status.run_state!=='RUNNING'||!choices.length;
   start.onclick=()=>{const ids=[...draft.targets].filter(id=>choices.some(target=>target.id===id)).sort();if(!ids.length){notify(label('scope'));return;}if(!Number.isInteger(draft.gate)||draft.gate<1||draft.gate>120||!Number.isInteger(draft.read)||draft.read<draft.gate||draft.read>86400||!Number.isInteger(draft.quota)||draft.quota<4||draft.quota>1048576||!Number.isInteger(draft.row)||draft.row<1||draft.row>32||draft.quota<draft.row*4||!Number.isInteger(draft.targetRead)||draft.targetRead<1||draft.targetRead>86400){notify(label('invalid'));return;}submit(task,'VALIDATE',{targets:ids,gateMillis:draft.gate*1000,sourceMillis:draft.read*1000,targetMillis:draft.targetRead*1000,limits:{maximumBytes:draft.quota*1048576,maximumRowBytes:draft.row*1048576,maximumRows:1000000000}});};section.append(start);
  }
  const page=renderCatalog(section,task);
  const recent=task.operations.filter(operation=>operation.validation).slice().reverse();
  const retained=page?.reports||[];
  const jobs=page?[
   ...recent.filter(op=>op.status==='PENDING'&&!retained.some(report=>report.id===op.validation.reportId)),
   ...retained.map(report=>({...recent.find(op=>op.validation.reportId===report.id),
    validation:{reportId:report.id},createdAt:report.createdAt,retained:true}))
  ]:recent;
  if(!jobs.length)section.append(text('p',page?catalogLabel('empty'):label('empty'),'subtle'));
  for(const job of jobs){
   const card=text('article','','subscription-card'),id=job.validation.reportId,entry=job.id||id;
   const disclosureKey=JSON.stringify([task.id,id,entry]);
   const date=Date.parse(job.createdAt||job.finishedAt),identity=text('details');
   identity.open=reportDisclosure.has(disclosureKey);
   identity.ontoggle=()=>{if(identity.isConnected)identity.open?reportDisclosure.add(disclosureKey):reportDisclosure.delete(disclosureKey);};
   identity.append(control(text('summary',catalogLabel('id')),'report',id,entry,'identity'),text('code',id));
   card.append(text('strong',Number.isNaN(date)?label('report'):new Date(date).toLocaleString()),
    badge(job.status||catalogLabel('retained')),identity);
   if(job.code||!job.retained)card.append(text('p',job.code?explain(job.code):label('pending')));
   if(job.status==='PENDING'&&can('TASK_CONFIG')){const cancel=control(text('button',label('cancel')),'report',id,entry,'cancel');cancel.dataset.validationAction='cancel';cancel.disabled=job.cancelRequested;cancel.onclick=()=>submit(task,'VALIDATE_CANCEL');card.append(cancel);}
   const view=control(text('button',label('view')),'report',id,entry,'view');view.dataset.validationReport=id;view.disabled=job.status==='PENDING';view.onclick=async()=>{try{const report=await fetchReport(task,id);reports.set(task.id+'/'+id,{report,page:0});bridge.render();}catch(error){notify(error.message);}};card.append(view);
   if(can('TASK_CONFIG')){
    const storage=control(text('button',catalogLabel('storage'),'quiet'),'report',id,entry,'storage');
    storage.dataset.validationStorage=id;storage.onclick=()=>inspectStorage(task,id);card.append(storage);
   }
   const cached=reports.get(task.id+'/'+id);if(cached){const report=cached.report;if(!cached.selected)cached.selected=new Set();card.append(text('p',report.code==='VALIDATION_MATCH'?label('MATCH'):report.code==='VALIDATION_DIFFERENCES'?label('DIFF'):report.state),text('p',label('historical'),'subtle'));const table=text('table'),head=text('tr');for(const key of ['destination','table','source','target','missing','extra','changed'])head.append(text('th',label(key)));table.append(head);
    const results=Object.values(report.results||{}),names=new Map((report.tables||[]).map(item=>[item.id,item.database+'.'+item.name]));for(const result of results.slice(cached.page*50,cached.page*50+50)){const row=text('tr');for(const value of [result.target,names.get(result.tableId)||result.tableId,result.sourceRows,result.targetRows,result.missing,result.extra,result.changed])row.append(text('td',String(value)));if(can('TASK_CONFIG')&&(result.missing||result.extra||result.changed)){if((task.destinations||[]).some(target=>target.id===result.target&&['MYSQL','POSTGRESQL'].includes(target.product))){const cell=text('td'),key=result.target+'/'+result.tableId,input=control(document.createElement('input'),'report',id,entry,key,'scope');input.type='checkbox';input.checked=cached.selected.has(key);input.disabled=pending;input.dataset.repairScope=key;input.setAttribute('aria-label',repairLabel('select')+' '+result.target+' '+(names.get(result.tableId)||result.tableId));input.onchange=()=>input.checked?cached.selected.add(key):cached.selected.delete(key);cell.append(input);row.append(cell);}const cell=text('td'),button=control(text('button',label('differences')),'report',id,entry,result.target,result.tableId,'differences');button.dataset.validationDifferences=result.tableId;button.onclick=()=>loadDifferences(task,id,cached,result,'',[]);cell.append(button);row.append(cell);}table.append(row);}card.append(table);if(can('TASK_CONFIG')&&report.state==='COMPLETE'&&report.code==='VALIDATION_DIFFERENCES'){const create=control(text('button',repairLabel('plan')),'report',id,entry,'repair-plan');create.dataset.repairAction='plan';create.disabled=pending||task.status.run_state!=='PAUSED'||!results.some(result=>(result.missing||result.extra||result.changed)&&(task.destinations||[]).some(target=>target.id===result.target&&['MYSQL','POSTGRESQL'].includes(target.product)));create.onclick=()=>{const scope=[...cached.selected].sort();if(scope.length<1||scope.length>200){notify(repairLabel('scope'));return;}submit(task,'REPAIR_PLAN',null,{reportId:id,scope});};card.append(create);}if(can('TASK_CONFIG')&&cached.details)renderDifferences(card,task,id,entry,cached,names);window.RivomeshRecovery?.renderVerificationFromReport(card,task,{...report,id});
    if(results.length>50){for(const [key,step] of [['previous',-1],['next',1]]){const button=control(text('button',label(key)),'report',id,entry,cached.page,key);button.disabled=cached.page+step<0||(cached.page+step)*50>=results.length;button.onclick=()=>{cached.page+=step;bridge.render();};card.append(button);}}
    if(can('TASK_CONFIG')&&!pending&&report.sourceComplete&&report.state!=='COMPLETE'){const retry=control(text('button',label('retry')),'report',id,entry,'retry');retry.dataset.validationAction='retry';retry.onclick=()=>submit(task,'VALIDATE_RETRY',{reportId:id});card.append(retry);}
   }section.append(card);
  }root.append(section);renderRepairs(root,task,pending);
 }
 return{render,rememberFocus,close(){closeDialog();drafts.clear();reports.clear();plans.clear();catalogs.clear();reportDisclosure.clear();}};
})();
