'use strict';
const messages={
zh:{workspace:'工作空间',tasks:'同步任务',adapters:'连接器能力',localControl:'本机控制节点',previewScope:'当前：MySQL → Gauss / PostgreSQL',subtitle:'管理数据流、查看进度，并从故障处继续。',logout:'退出',newTask:'+ 新建任务',totalTasks:'全部任务',running:'运行中的 Worker',attention:'需要关注',pendingTransactions:'待应用事务',pipelines:'数据流',refresh:'每 3 秒更新',footer:'独立任务 · 持久断点 · 故障恢复',loginTitle:'连接你的工作空间',loginHelp:'输入访问令牌以建立短期安全会话，刷新页面可继续使用。',accessToken:'访问令牌',connect:'连接',createTitle:'创建同步任务',sourceHelp:'配置一个 MySQL 逻辑源。主备候选地址等高级配置可通过独立 YAML 文件加载。',taskId:'任务标识',sourceCluster:'逻辑源集群标识',sourceHost:'MySQL 地址',port:'端口',sourceDatabase:'源 database',replicationId:'复制客户端 serverId',username:'用户名',password:'密码',sourceRequirements:'需要 GTID、ROW/FULL 日志及受支持的有主键 InnoDB 表。新对象支持正在开发中。',targetHelp:'请选择 openGauss / GaussDB 或原生 PostgreSQL。已实测 openGauss 7 和 PostgreSQL 17；其他版本须先验收。',targetSchema:'目标业务 schema',metadataSchema:'管理 schema',passwordHelp:'支持 ${ENV_NAME} 密码引用；任务配置保存在服务端私有文件中。页面不会返回已存密码。',planHelp:'创建后先执行检查，再启动同步。只有启动才会创建缺失表和写入数据。',currentLimit:'当前每份任务为一个 database → 一个 Gauss / PostgreSQL 目标。已有目标首次要求为空；本次不会清空已有数据。',previous:'上一步',next:'下一步',saveTask:'保存任务',step0:'01 · 源库',step1:'02 · 数据库目标',targetType:'目标产品',step2:'03 · 检查计划',adapterHelp:'可用状态反映实际实现与验证。规划中的连接器不能作为当前可运行适配器。',emptyTitle:'让第一条数据流开始流动',emptyHelp:'连接 MySQL 与目标库，先检查，再开始迁移。',selectTask:'选择一个任务查看运行状态',start:'启动 / 重试',stop:'停止本机 Worker',pause:'暂停共享任务',resume:'修复后恢复',check:'检查连接与对象',phase:'同步阶段',sharedState:'共享运行状态',worker:'本机 Worker',copied:'已复制行数',table:'表',snapshot:'全量状态',complete:'完成',pending:'处理中',observed:'上次成功读取',history:'操作记录',position:'源断点与故障详情',scope:'停止只影响本机 Worker；暂停已初始化任务会写入共享状态，阻止备用节点继续。',uninitialized:'任务尚未初始化。可以先检查，再启动。',stale:'当前状态读取失败；以下可能是上次成功读取的结果。',invalid:'未加载的配置文件',created:'任务已保存。建议先检查，然后启动。',accepted:'操作已接收，可在操作记录中查看执行结果。',UNAUTHORIZED:'令牌不正确或已失效。',REVISION_CHANGED:'任务刚被其他操作更新，请刷新后重试。',OPERATION_PENDING:'该任务还有正在执行的操作。',DESTINATION_OVERLAP:'目标业务 schema 与其他任务重叠，请使用独立 schema。',REPLICATION_SERVER_ID_COLLISION:'同一源上的任务必须使用不同的复制客户端 serverId。',TASK_EXISTS:'任务标识已经存在。',CONFIG_FILE_EXISTS:'同名配置文件已存在。',TASK_PAUSED_USE_RESUME:'共享任务已暂停，请修复原因后使用“恢复”。',ERROR_GENERATION_CHANGED:'故障已经变化，旧恢复请求未执行。请检查最新错误。',CONFIG_INVALID:'配置格式或字段无效，请检查配置。',REQUEST_INVALID:'请求字段无效。',NOT_INITIALIZED:'尚未初始化',SHARED_PAUSED:'共享任务已暂停',LOCAL_PAUSED_NOT_INITIALIZED:'仅暂停本机：共享任务尚未初始化',LOCAL_STOPPED:'本机 Worker 已停止',RESUME_REQUESTED:'已允许恢复原失败事务',START_REQUESTED:'已保存启动意图',SQL:'数据库操作未完成，请检查连接、权限及错误码。',UNKNOWN:'操作未完成，请查看错误码与部署日志。'},
en:{workspace:'WORKSPACE',tasks:'Pipelines',adapters:'Adapter capabilities',localControl:'Local control node',previewScope:'Current: MySQL → Gauss / PostgreSQL',subtitle:'Move data, follow progress, and recover from interruptions.',logout:'Sign out',newTask:'+ New pipeline',totalTasks:'Total pipelines',running:'Running workers',attention:'Needs attention',pendingTransactions:'Pending transactions',pipelines:'Your pipelines',refresh:'UPDATES EVERY 3s',footer:'Isolated tasks · Durable checkpoints · Recovery',loginTitle:'Connect your workspace',loginHelp:'Enter an access token to establish a short-lived session that survives page reload.',accessToken:'Access token',connect:'Connect',createTitle:'Create a pipeline',sourceHelp:'Configure one logical MySQL source. Load independent YAML files for advanced topology settings.',taskId:'Task ID',sourceCluster:'Logical source cluster ID',sourceHost:'MySQL host',port:'Port',sourceDatabase:'Source database',replicationId:'Replication client serverId',username:'Username',password:'Password',sourceRequirements:'Requires GTID, ROW/FULL logging, and supported InnoDB tables with primary keys. New object support is in development.',targetHelp:'Select openGauss / GaussDB or native PostgreSQL. Tested on openGauss 7 and PostgreSQL 17; validate other versions first.',targetSchema:'Business schema',metadataSchema:'Metadata schema',passwordHelp:'${ENV_NAME} password references are supported. Configurations use private server files; stored passwords are never returned.',planHelp:'Check the configuration before starting. Only start creates missing tables and writes data.',currentLimit:'Currently one database → one Gauss / PostgreSQL target per task. Initial target tables must be empty; existing data is never cleared.',previous:'Back',next:'Continue',saveTask:'Save pipeline',step0:'01 · Source',step1:'02 · Database target',targetType:'Target product',step2:'03 · Review plan',adapterHelp:'Availability reflects actual implementation and validation. Planned adapters cannot be used yet.',emptyTitle:'Start your first data flow',emptyHelp:'Connect MySQL and your target, check the plan, then start.',selectTask:'Select a pipeline to inspect its progress',start:'Start / retry',stop:'Stop local worker',pause:'Pause shared task',resume:'Resume after repair',check:'Check connections & objects',phase:'Replication phase',sharedState:'Shared run state',worker:'Local worker',copied:'Copied rows',table:'Table',snapshot:'Snapshot',complete:'Complete',pending:'In progress',observed:'Last successful read',history:'Operations',position:'Source position & error details',scope:'Stop affects this worker only. Pausing an initialized task persists shared state and blocks standby writers.',uninitialized:'Not initialized yet. Check connections, then start.',stale:'Status read failed; displayed data may be from the last successful read.',invalid:'Configurations not loaded',created:'Pipeline saved. Check it before starting.',accepted:'Operation accepted. Follow its result in operation history.',UNAUTHORIZED:'The token is incorrect or expired.',REVISION_CHANGED:'The task changed. Refresh and retry.',OPERATION_PENDING:'Another operation is pending.',DESTINATION_OVERLAP:'Business schemas overlap another task. Use an independent schema.',REPLICATION_SERVER_ID_COLLISION:'Tasks on the same source need distinct replication client serverIds.',TASK_EXISTS:'Task ID already exists.',CONFIG_FILE_EXISTS:'A configuration file already uses this name.',TASK_PAUSED_USE_RESUME:'The shared task is paused. Repair it and choose Resume.',ERROR_GENERATION_CHANGED:'A newer failure exists. The old resume request was rejected.',CONFIG_INVALID:'Invalid configuration or fields.',REQUEST_INVALID:'Invalid request fields.',NOT_INITIALIZED:'Not initialized',SHARED_PAUSED:'Shared task paused',LOCAL_PAUSED_NOT_INITIALIZED:'Local pause only: shared task is not initialized',LOCAL_STOPPED:'Local worker stopped',RESUME_REQUESTED:'Original failed transaction may resume',START_REQUESTED:'Start intent saved',SQL:'Database operation failed. Check connection, privileges, and error codes.',UNKNOWN:'Operation failed. Inspect the error code and deployment logs.'}
};
Object.assign(messages.zh,{OK:'检查通过',RUNNING:'运行中',STOPPED:'已停止',PAUSED:'已暂停',READY:'等待执行',PENDING:'进行中',SUCCEEDED:'成功',FAILED:'失败',SNAPSHOT:'全量同步',CATCHUP:'增量追平',STREAMING:'持续增量',RETRYING:'正在重试',WAITING_SOURCE:'等待源恢复',CRASH_LOOP:'反复退出',BACKOFF:'等待重启',QUEUED:'等待资源',AVAILABLE:'可用',PARTIALLY_VERIFIED:'部分验证',PLANNED:'规划中',SOURCE:'源端',SINK:'目标端',OPERATOR_PAUSED:'操作员已暂停',CONTROL_FAILED:'操作失败，请检查部署日志',STORAGE_ERROR:'持久目录读写失败，请检查磁盘和权限'});
Object.assign(messages.en,{OK:'Check passed',RUNNING:'Running',STOPPED:'Stopped',PAUSED:'Paused',READY:'Ready',PENDING:'Pending',SUCCEEDED:'Succeeded',FAILED:'Failed',SNAPSHOT:'Snapshot',CATCHUP:'Catch-up',STREAMING:'Streaming',RETRYING:'Retrying',WAITING_SOURCE:'Waiting for source',CRASH_LOOP:'Crash loop',BACKOFF:'Restart backoff',QUEUED:'Waiting for capacity',AVAILABLE:'Available',PARTIALLY_VERIFIED:'Partially verified',PLANNED:'Planned',SOURCE:'Source',SINK:'Target',OPERATOR_PAUSED:'Paused by operator',CONTROL_FAILED:'Operation failed; inspect deployment logs',STORAGE_ERROR:'Cannot write persistent storage; check disk and permissions'});
messages.zh.importTask='导入 YAML';messages.en.importTask='Import YAML';messages.zh.tablePreview='当前显示表数（其余表仍正常同步）：';messages.en.tablePreview='Tables shown (other tables continue syncing): ';
Object.assign(messages.zh,{nodeScope:'限定节点数',nodesVersion:'版本',nodesJvm:'JVM / CPU',nodesHeap:'堆容量',nodes:'节点目录',nodesReload:'刷新目录',nodesNext:'下一页节点',nodesHelp:'所有入口查询同一共享节点目录。当前任务操作仍在本机；节点观察不代表已实现远端控制或故障接管。',nodesStandalone:'单机模式：仅当前节点',nodesShared:'共享节点目录',nodesLocal:'当前入口',nodesObserved:'数据库观察时间',nodesHeartbeat:'最近心跳',nodesExpires:'租约到期',nodesEpoch:'注册代次',nodesCapacity:'任务 / 运行容量',nodesTasks:'本节点任务观察',nodesNoTasks:'暂无任务观察',nodesNoLease:'不适用（单机）',nodesUnknown:'未知',nodesRole:'控制器与 Worker',HEALTHY:'心跳有效',STALE:'心跳过期',CONTROL_STORE_UNAVAILABLE:'共享控制库不可达，无法确认节点状态。',NODE_REGISTRATION_FENCED:'当前节点注册已被其他进程替代，请检查重复 nodeId。'});
Object.assign(messages.en,{nodeScope:'Granted nodes',nodesVersion:'Version',nodesJvm:'JVM / CPU',nodesHeap:'Heap',nodes:'Node registry',nodesReload:'Refresh registry',nodesNext:'Next nodes',nodesHelp:'Every endpoint reads the shared registry. Task actions still run locally; observations do not provide remote control or failover.',nodesStandalone:'Standalone: current node only',nodesShared:'Shared node registry',nodesLocal:'Current endpoint',nodesObserved:'Database observation time',nodesHeartbeat:'Last heartbeat',nodesExpires:'Lease expiry',nodesEpoch:'Registration epoch',nodesCapacity:'Task / running capacity',nodesTasks:'Local task observations',nodesNoTasks:'No task observations',nodesNoLease:'Not applicable (standalone)',nodesUnknown:'Unknown',nodesRole:'Controller and Worker',HEALTHY:'Heartbeat valid',STALE:'Heartbeat expired',CONTROL_STORE_UNAVAILABLE:'The shared control store is unavailable; node health cannot be confirmed.',NODE_REGISTRATION_FENCED:'This registration was replaced. Check duplicate nodeId settings.'});
let nodePage=null,nodeAfter='',nodeLoading=false,nodeTicket=0;
Object.assign(messages.zh,{OBSERVATION_STALE:'观察已过期',REGISTERING:'正在完成登记',UNOBSERVED:'等待首次观察',TASK_CONFIGURATION_NOT_APPLIED:'配置尚未在所属节点应用，请先处理创建或配置操作的错误。',OWNER_WAITING_LOCAL_OPERATION:'所属节点正在处理其他操作，稍后自动继续。',configurationActive:'当前有效',configurationDesired:'目标版本'});
Object.assign(messages.en,{OBSERVATION_STALE:'Observation expired',REGISTERING:'Admission in progress',UNOBSERVED:'Awaiting first observation',TASK_CONFIGURATION_NOT_APPLIED:'Configuration is not active on the owner; resolve the creation/configuration error first.',OWNER_WAITING_LOCAL_OPERATION:'Waiting for another local operation; retry continues automatically.',configurationActive:'Currently active',configurationDesired:'Desired version'});
Object.assign(messages.zh,{globalControl:'全局任务控制',ownerNode:'执行节点',ownerHelp:'新建和导入任务将交给所选节点。可选择建议节点或输入已登记的节点ID；离线节点的命令会等待。',ownerRequired:'请选择有效的执行节点。',tasksFirst:'首页',tasksNext:'下一页任务',taskPage:'当前页 / 可见任务总数',pageRunning:'当前页运行中的 Worker',pageAttention:'当前页需要关注',pagePending:'当前页待应用事务',globalTotal:'可见任务总数',globalScope:'操作交给所属节点执行；离线命令等待，未知结果需要核对。自动跨节点迁移尚未提供。',ownerObserved:'节点观察发布于',configurationPending:'目标配置尚未成为有效配置，请查看操作结果；拒绝提案会保留旧有效版本。',configurationAccepted:'配置提案已接受，等待所属节点应用。',globalNodesHelp:'所有入口读取同一任务权威并控制所属节点。节点失联显示未知；节点心跳不等于已经完成执行接管。',globalStop:'停止所属节点 Worker',globalWorker:'所属节点 Worker',DISPATCHED:'正在交付节点',ACCEPTED:'节点已持久接受',REJECTED:'已拒绝',CANCELLED:'已取消',OWNER_QUERY_UNAVAILABLE:'所属节点不在线或尚未完成注册，请稍后重试。',AGENT_NOT_REGISTERED:'执行节点尚未登记，请先启动该节点。',AGENT_DIRECTORY_CHANGED:'节点的数据目录身份不符，请恢复原目录并检查部署。',GLOBAL_TASK_IMPORT_REQUIRED:'已有本机任务需要先完成导入方案，不能直接切换全局模式。',GLOBAL_TASK_QUIESCENCE_REQUIRED:'切换全局协议前需正常停止旧成员，失联或心跳过期不能替代正常停止。',CONTROL_KEY_MISMATCH:'共享任务密钥不匹配，请检查各节点配置。',OWNER_COMMAND_EVIDENCE_REQUIRED:'本机操作回执缺失，需核对原状态后恢复。'});
Object.assign(messages.en,{globalControl:'Global task control',ownerNode:'Execution node',ownerHelp:'New/imported tasks run on the selected node. Choose a suggestion or enter a registered node ID; offline commands wait.',ownerRequired:'Choose a valid execution node.',tasksFirst:'First page',tasksNext:'Next tasks',taskPage:'This page / visible total',pageRunning:'Running workers on this page',pageAttention:'Attention on this page',pagePending:'Pending transactions on this page',globalTotal:'Visible task total',globalScope:'Actions execute on the owner. Offline commands wait; unknown results need reconciliation. Automatic node relocation is not available.',ownerObserved:'Owner observation published',configurationPending:'The proposed configuration is not active. Check the operation outcome; rejected proposals retain the previous active version.',configurationAccepted:'Configuration proposal accepted; waiting for the owner to apply it.',globalNodesHelp:'Every entry reads the same task authority and controls the owner. Offline state is unknown; heartbeats do not establish execution takeover.',globalStop:'Stop owner worker',globalWorker:'Owner worker',DISPATCHED:'Dispatching to owner',ACCEPTED:'Durably accepted by owner',REJECTED:'Rejected',CANCELLED:'Cancelled',OWNER_QUERY_UNAVAILABLE:'The owner is offline or not admitted yet; retry later.',AGENT_NOT_REGISTERED:'Start/register the execution node first.',AGENT_DIRECTORY_CHANGED:'The node directory identity differs; restore the original directory and review deployment.',GLOBAL_TASK_IMPORT_REQUIRED:'Existing local tasks need a reviewed import before enabling global mode.',GLOBAL_TASK_QUIESCENCE_REQUIRED:'Stop old members normally before upgrading; expiry/offline state is insufficient.',CONTROL_KEY_MISMATCH:'The shared task key differs; check member configuration.',OWNER_COMMAND_EVIDENCE_REQUIRED:'A local operation receipt is missing; reconcile original state before recovery.'});
let taskAfter='',taskHistory=[],taskTicket=0,taskLoading=false,ownerChoices=[],ownerNext=null,ownerLoaded=false,ownerLoading=false,ownerTicket=0;
let taskFilters={},selectedDestination='';
Object.assign(messages.zh,{filterTaskId:'任务 ID',filterOwner:'执行节点 ID',applyTaskFilter:'筛选任务',clearTaskFilter:'清除筛选',filteredTotal:'匹配的任务',noTaskMatch:'未找到匹配的任务',noTaskMatchHelp:'修改任务 ID 或执行节点再查询。',allTargets:'全部目标',targetBacklog:'待目标确认的日志事务'});
Object.assign(messages.en,{filterTaskId:'Task ID',filterOwner:'Execution node ID',applyTaskFilter:'Filter tasks',clearTaskFilter:'Clear filters',filteredTotal:'Matching tasks',noTaskMatch:'No matching tasks',noTaskMatchHelp:'Change the task ID or execution node and search again.',allTargets:'All destinations',targetBacklog:'Journal transactions awaiting destination acknowledgment'});

Object.assign(messages.zh,{nodeController:'管理入口',nodeAgent:'执行 Agent',OWNER_ROLE_REQUIRED:'所选节点只负责管理，不能执行任务，请选择执行 Agent。',TASK_OWNER_REQUIRED:'请指定执行 Agent。',CONTROLLER_OWNS_TASKS:'该节点仍拥有执行任务，请保留原节点和目录作为 Agent。',CONTROLLER_EXECUTION_DIRECTORY_NOT_EMPTY:'含执行任务的目录不能作为纯管理入口打开。'});
Object.assign(messages.en,{nodeController:'Management entry',nodeAgent:'Execution Agent',OWNER_ROLE_REQUIRED:'This node only serves management. Choose an execution Agent.',TASK_OWNER_REQUIRED:'Select an execution Agent.',CONTROLLER_OWNS_TASKS:'This node still owns tasks. Keep its original identity/directory as an Agent.',CONTROLLER_EXECUTION_DIRECTORY_NOT_EMPTY:'A directory with execution tasks cannot open as a controller-only entry.'});
Object.assign(messages.zh,{automaticPlacement:'自动分配执行 Agent',AGENT_PLACEMENT_UNAVAILABLE:'当前授权范围内没有可用的 Agent 任务槽。请核对节点状态和任务容量。',TASK_PLACEMENT_INVALID:'请选择自动分配或指定节点。'});
Object.assign(messages.en,{automaticPlacement:'Assign an Agent automatically',AGENT_PLACEMENT_UNAVAILABLE:'No authorized Agent currently has a free task slot. Check node status and capacity.',TASK_PLACEMENT_INVALID:'Choose automatic assignment or select a node.'});
function globalTasks(){return session?.taskControlScope==='GLOBAL';}
function clearTaskPages(){taskTicket++;taskAfter='';taskHistory=[];taskLoading=false;}
function clearTaskFilters(){taskFilters={};selectedDestination='';$('task-filter').reset();}
function filteredTasks(){return Boolean(taskFilters.taskId||taskFilters.ownerNodeId);}

function clearOwners(){
 ownerTicket++;ownerChoices=[];ownerNext=null;ownerLoaded=false;ownerLoading=false;
 $('task-owner').value='';$('create-owner').value='';
 $('task-auto-placement').checked=false;$('create-auto-placement').checked=false;
 $('owner-options').replaceChildren();
}
function renderOwners(){
 const enabled=globalTasks()&&can('TASK_CREATE');
 $('task-owner-control').classList.toggle('hidden',!enabled);
 $('create-owner-row').classList.toggle('hidden',!enabled);
 $('create-placement-row').classList.toggle('hidden',!enabled);
 const globalOption=$('task-auto-placement'),wizardOption=$('create-auto-placement');
 globalOption.disabled=!enabled;wizardOption.disabled=!enabled;
 const globalInput=$('task-owner'),wizardInput=$('create-owner');
 globalInput.disabled=!enabled||globalOption.checked;wizardInput.disabled=!enabled||wizardOption.checked;
 wizardInput.required=enabled&&!wizardOption.checked;
 $('owners-reload').disabled=ownerLoading;$('owners-more').disabled=ownerLoading;
 $('owners-more').classList.toggle('hidden',!ownerNext);
 if(enabled){
  if(session.nodeRole==='CONTROLLER'&&globalInput.value===session.nodeId)globalInput.value='';
  if(!globalInput.value){
   const choice=ownerChoices.find(node=>node.state==='HEALTHY')||ownerChoices[0];
   const preferred=session.nodeRole==='CONTROLLER'?choice?.id:session.nodeId;
   globalInput.value=preferred||'';
   if(!wizardInput.value)wizardInput.value=preferred||'';
  }
 }
 const options=$('owner-options');options.replaceChildren();
 for(const node of ownerChoices){
  const option=document.createElement('option');
  option.value=node.id;option.label=node.id+' · '+t(node.state||'UNKNOWN');
  options.append(option);
 }
}
async function loadOwners(after='',hops=0){
 if(!globalTasks()||!can('TASK_CREATE')||ownerLoading)return;const ticket=++ownerTicket;ownerLoading=true;renderOwners();
 try{const page=await api('/nodes?limit=20'+(after?'&after='+encodeURIComponent(after):''));if(ticket!==ownerTicket)return;const nodes=after?[...ownerChoices]:[];for(const node of page.nodes||[])if((!node.role||['CONTROLLER_WORKER','AGENT'].includes(node.role))&&!nodes.some(value=>value.id===node.id))nodes.push(node);ownerChoices=nodes;ownerNext=page.next;ownerLoaded=true;}
 catch(error){if(ticket===ownerTicket)notify(error.message);}finally{if(ticket===ownerTicket){ownerLoading=false;renderOwners();}}
 if(ticket===ownerTicket&&!ownerChoices.length&&ownerNext&&ownerNext!==after&&hops<500)await loadOwners(ownerNext,hops+1);
}
function ownerSelection(){
 if(!globalTasks())return{};
 const wizard=$('create-dialog').open;
 if($(wizard?'create-auto-placement':'task-auto-placement').checked)return{placement:'AUTO'};
 const id=$(wizard?'create-owner':'task-owner').value.trim();
 if(!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(id))throw new Error(t('ownerRequired'));
 return{ownerNodeId:id};
}
async function loadTaskPage(after=taskAfter,filters=taskFilters){
 const ticket=++taskTicket;taskLoading=true;
 const query=new URLSearchParams({limit:'20'});
 if(after)query.set('after',after);
 for(const [name,value] of Object.entries(filters))if(value)query.set(name,value);
 try{
  const page=await api('/tasks'+(globalTasks()?'?'+query:''));
  if(ticket!==taskTicket)return false;
  taskAfter=after;taskFilters=filters;latest={...page,tasks:page.tasks||[],invalidConfigs:page.invalidConfigs||[]};return true;
 }finally{if(ticket===taskTicket){taskLoading=false;render();}}
}
async function applyTaskFilters(filters){
 if(!globalTasks()||taskLoading)return;
 try{if(await loadTaskPage('',filters)){taskHistory=[];render();}}catch(error){notify(error.message);}
}
async function taskPage(direction){if(taskLoading)return;const before=taskAfter,history=[...taskHistory],after=direction==='first'?'':direction==='previous'?history.at(-1)||'':latest.next;if(after==null)return;
 try{if(await loadTaskPage(after)){taskHistory=direction==='first'?[]:direction==='previous'?history.slice(0,-1):[...history,before];render();}}catch(error){notify(error.message);}
}
let activeView='tasks';
Object.assign(messages.zh,{skipToContent:'跳到主要内容',darkTheme:'深色外观',loginOidc:'使用组织账号登录'});
Object.assign(messages.en,{skipToContent:'Skip to main content',darkTheme:'Dark theme',loginOidc:'Sign in with your organization'});
let lang=localStorage.getItem('rivomesh-language')||(navigator.language.startsWith('zh')?'zh':'en'),selected='',latest={tasks:[],invalidConfigs:[]},capabilities=[],session=null,sessionGeneration=0,busy=null,step=0;
let failureDialog=null,failureTicket=0;
const $=id=>document.getElementById(id),t=key=>messages[lang][key]||key;
const can=permission=>Boolean(session?.permissions?.includes(permission));
function forgetSession(){clearTaskFilters();closeFailureEvidence();closeCaptureRecovery();closeWorkerDiagnostics();window.RivomeshRecovery?.close();window.RivomeshHandoff?.close();window.RivomeshValidation?.close();window.RivomeshAccounts?.close();window.RivomeshEditor?.close();window.RivomeshAudit?.close();sessionGeneration++;session=null;busy=null;clearTaskPages();clearOwners();clearNodes();clearDraft();$('create-dialog').close();capabilities=[];latest={tasks:[],invalidConfigs:[]};render();renderAdapters();}
const text=(tag,value,cls)=>{const e=document.createElement(tag);e.textContent=value;if(cls)e.className=cls;return e;};
function translate(){if(failureDialog){failureDialog.querySelectorAll('[data-failure-label]').forEach(e=>e.textContent=t(e.dataset.failureLabel));const events=failureDialog.querySelector('.failure-event-list');if(events?._failureEvents)renderFailureEvents(events,events._failureEvents);}document.documentElement.lang=lang==='zh'?'zh-CN':'en';document.querySelectorAll('[data-i18n]').forEach(e=>e.textContent=t(e.dataset.i18n));$('language').textContent=lang==='zh'?'English':'中文';[0,1,2].forEach(i=>$('step-label-'+i).textContent=t('step'+i));render();renderAdapters();renderNodes();$('page-title').textContent=t(activeView);if($('create-dialog').open)wizardMode();window.RivomeshEditor?.translate();window.RivomeshAudit?.translate();window.RivomeshAccounts?.translate();window.RivomeshRecovery?.translate();}
function explain(code){return messages[lang][code]||(code&&code.startsWith('SQL_')?t('SQL'):t('UNKNOWN'))+(code?' ['+code+']':'');}
function notify(message){$('notice').textContent=message;$('notice').classList.remove('hidden');}
function requestId(){return 'ui_'+Array.from(crypto.getRandomValues(new Uint8Array(16)),v=>v.toString(16).padStart(2,'0')).join('');}
async function api(path,options={}){
 const generation=sessionGeneration;
 const r=await fetch('/api'+path,{...options,credentials:'same-origin',headers:{...(options.method&&options.method!=='GET'&&session?.csrfToken?{'X-CSRF-Token':session.csrfToken}:{}),...options.headers}});
 const data=await r.json();if(generation!==sessionGeneration)throw new Error(t('SESSION_CHANGED'));
 if(!r.ok){const e=new Error(explain(data.code));e.code=data.code;e.status=r.status;if(r.status===401){forgetSession();if(!$('login-dialog').open)$('login-dialog').showModal();}throw e;}return data;
}
async function refresh(sessionVerified=false){
 let generation=sessionGeneration;
 if(!session||busy===generation)return;
 busy=generation;
 try{
  if(!sessionVerified){
   const recovered=await api('/session');
   if(session?.csrfToken&&session.csrfToken!==recovered.csrfToken){
    forgetSession();
    generation=sessionGeneration;
    busy=generation;
   }
   session=recovered;
  }
  await loadTaskPage();
  if(generation!==sessionGeneration)return;
  if(!capabilities.length)capabilities=await api('/capabilities');
  if(globalTasks()&&can('TASK_CREATE')&&!ownerLoaded)await loadOwners();
  if(generation!==sessionGeneration)return;
  render();renderAdapters();
  if(activeView==='nodes')await loadNodes();
  if(generation===sessionGeneration)$('last-refresh').textContent=new Date().toLocaleTimeString();
 }catch(error){
  if(generation===sessionGeneration)notify(error.message);
 }finally{
  if(busy===generation)busy=null;
 }
}
let authQueue=Promise.resolve();
let restoringSession=false;
const sessionChannel=typeof BroadcastChannel==='function'?new BroadcastChannel('rivomesh-session'):null;
function queueAuth(work){const result=authQueue.then(work);authQueue=result.catch(()=>{});return result;}
function showLogin(){if(!$('login-dialog').open)$('login-dialog').showModal();}
if(sessionChannel)sessionChannel.onmessage=e=>{if(e.data?.type==='signed-out'){forgetSession();showLogin();queueAuth(revokeBrowserSession).catch(error=>notify(error.message));}};
function restoreSession(){
 if(restoringSession)return;
 const generation=sessionGeneration;
 restoringSession=true;
 queueAuth(async()=>{
  if(generation!==sessionGeneration)return;
  try{
   session=await api('/session');
   $('login-dialog').close();
   await refresh(true);
  }catch(error){
   if(generation===sessionGeneration)notify(error.message);
  }
 }).finally(()=>{restoringSession=false;});
}
function pollConsole(){
 if(document.hidden)return;
 if(session)refresh();
 else if(!$('login-dialog').open)restoreSession();
}
async function revokeBrowserSession(){
 // Serialize behind an outstanding login: a delayed Set-Cookie must also be revoked.
 const response=await fetch('/api/session',{credentials:'same-origin',headers:{}});
 if(response.status===401)return;
 if(!response.ok)throw new Error(t('SESSION_LOGOUT_FAILED'));
 const current=await response.json();
 const revoked=await fetch('/api/session/logout',{method:'POST',credentials:'same-origin',headers:{'X-CSRF-Token':current.csrfToken,'X-Request-ID':requestId()}});
 if(!revoked.ok&&revoked.status!==401)throw new Error(t('SESSION_LOGOUT_FAILED'));
}

function clearNodes(){nodeTicket++;nodePage=null;nodeAfter='';nodeLoading=false;$('nodes-list').replaceChildren();$('nodes-state').textContent='';$('nodes-error').textContent='';$('nodes-next').classList.add('hidden');}
function renderNodes(){
 const root=$('nodes-list');root.replaceChildren();$('nodes-reload').disabled=nodeLoading||!can('ACCOUNT_ADMIN');$('nodes-next').disabled=nodeLoading;
 $('nodes-next').classList.toggle('hidden',!nodePage?.next);if(!nodePage)return;
 $('nodes-state').textContent=t(nodePage.mode==='STANDALONE'?'nodesStandalone':'nodesShared')+(nodePage.clusterId?' · '+nodePage.clusterId:'')+' · '+t('nodesObserved')+': '+nodePage.observedAt;
 for(const node of nodePage.nodes||[]){
  const card=text('article','','node-card'),heading=text('div','','panel-heading');heading.append(text('strong',node.id),badge(node.state));card.append(heading,text('p',t(node.role==='CONTROLLER'?'nodeController':node.role==='AGENT'?'nodeAgent':'nodesRole')+(node.id===nodePage.localNodeId&&(!nodePage.localNodeEpoch||node.epoch===nodePage.localNodeEpoch)?' · '+t('nodesLocal'):''),'subtle'));
  const details=document.createElement('dl');for(const [label,value]of [[t('nodesVersion'),node.version],[t('nodesJvm'),node.jvm+' / '+node.architecture+' / '+node.processors],[t('nodesHeap'),Math.round(Number(node.heapBytes)/1048576)+' MiB'],[t('nodesCapacity'),node.taskCapacity+' / '+node.runningCapacity],[t('nodesEpoch'),node.epoch],[t('nodesHeartbeat'),node.lastHeartbeat],[t('nodesExpires'),node.leaseExpiresAt||t('nodesNoLease')],['SHA256',node.artifactSha256||t('nodesUnknown')]]){details.append(text('dt',label),text('dd',value??t('nodesUnknown')));}card.append(details,text('h3',t('nodesTasks')));
  if(!node.tasks?.length)card.append(text('p',t('nodesNoTasks'),'subtle'));
  else{const list=document.createElement('ul');for(const task of node.tasks){list.append(text('li',task.id+' · '+t(task.workerState||'UNKNOWN')+' · '+(task.observedAt||t('nodesUnknown'))));}card.append(list);}root.append(card);
 }
 if(nodePage.localProblem)$('nodes-error').textContent=explain(nodePage.localProblem);
}
async function loadNodes(after=nodeAfter){
 if(!can('ACCOUNT_ADMIN')||nodeLoading)return;const ticket=++nodeTicket;nodeLoading=true;renderNodes();
 try{const page=await api('/nodes?limit=20'+(after?'&after='+encodeURIComponent(after):''));if(ticket!==nodeTicket)return;nodeAfter=after;nodePage=page;$('nodes-error').textContent='';}
 catch(error){if(ticket!==nodeTicket)return;nodePage=null;$('nodes-state').textContent='';$('nodes-error').textContent=error.message;}
 finally{if(ticket===nodeTicket){nodeLoading=false;renderNodes();}}
}
function status(task){if(task.workerState!=='RUNNING')return task.workerState;if(task.status.run_state==='RUNNING'&&task.status.subscriptions?.some(s=>s.state==='PAUSED'))return 'PARTIAL_FAILURE';if(task.status.run_state==='RUNNING'&&task.status.subscriptions?.some(s=>s.state==='RUNNING'&&s.errorCode))return 'RETRYING';return task.status.run_state||task.workerState;}
function badge(value){return text('span',t(value),'badge '+(['RUNNING','STREAMING','SUCCEEDED','AVAILABLE','HEALTHY'].includes(value)?'good':['PAUSED','CRASH_LOOP','FAILED','ERROR','PARTIAL_FAILURE','STALE'].includes(value)?'bad':['PENDING','BACKOFF','WAITING_SOURCE','RETRYING','PARTIALLY_VERIFIED'].includes(value)?'warn':''));}
function render(){
 window.RivomeshAudit?.render();window.RivomeshAccounts?.render();
 renderOwners();const global=globalTasks();document.querySelector('[data-i18n=localControl]').textContent=t(global?'globalControl':'localControl');document.querySelector('[data-i18n=nodesHelp]').textContent=t(global?'globalNodesHelp':'nodesHelp');
 $('task-filter').classList.toggle('hidden',!global);
 for(const [key,paged]of [['totalTasks',filteredTasks()?'filteredTotal':'globalTotal'],['running','pageRunning'],['attention','pageAttention'],['pendingTransactions','pagePending']])document.querySelector('[data-i18n='+key+']').textContent=t(global?paged:key);
 $('task-pagination').classList.toggle('hidden',!global);$('tasks-first').disabled=taskLoading||!taskHistory.length;$('tasks-previous').disabled=taskLoading||!taskHistory.length;$('tasks-next').disabled=taskLoading||!latest.next;$('task-page-status').textContent=t('taskPage')+': '+latest.tasks.length+' / '+(latest.total??latest.tasks.length);
 $('nodes-nav').disabled=!can('ACCOUNT_ADMIN');$('new-task').disabled=!can('TASK_CREATE');$('import-task').disabled=!can('TASK_CREATE');$('session-info').textContent=session?session.id+' · '+t('ROLE_'+session.role)+(Array.isArray(session.taskScope)?' · '+t('taskScope')+' '+session.taskScope.length:'')+(Array.isArray(session.nodeScope)?' · '+t('nodeScope')+' '+session.nodeScope.length:''):'';$('session-info').title=[Array.isArray(session?.taskScope)?session.taskScope.join(', '):'',Array.isArray(session?.nodeScope)?t('nodeScope')+': '+session.nodeScope.join(', '):''].filter(Boolean).join(' | ');
 $('total-count').textContent=global?(latest.total??latest.tasks.length):latest.tasks.length;$('running-count').textContent=latest.tasks.filter(x=>x.workerState==='RUNNING').length;$('attention-count').textContent=latest.tasks.filter(x=>['PAUSED','CRASH_LOOP','RETRYING','WAITING_SOURCE','ERROR','PARTIAL_FAILURE'].includes(status(x))||(x.problem&&x.problem!=='NOT_INITIALIZED')).length;$('pending-count').textContent=latest.tasks.reduce((n,x)=>n+(Number(x.status.pending_transactions)||0),0).toLocaleString();
 const list=$('task-list');list.replaceChildren();if(!latest.tasks.length){const empty=text('div','','empty');empty.append(text('span','⇄','empty-icon'),text('h2',t(filteredTasks()?'noTaskMatch':(Array.isArray(session?.taskScope)||Array.isArray(session?.nodeScope))?'noGrantedTasks':'emptyTitle')),text('p',t(filteredTasks()?'noTaskMatchHelp':(Array.isArray(session?.taskScope)||Array.isArray(session?.nodeScope))?'noGrantedTasksHelp':'emptyHelp')));const button=text('button',t('newTask'),'primary');button.disabled=!can('TASK_CREATE');button.onclick=openCreate;empty.append(button);list.append(empty);}
 if(!latest.tasks.some(x=>x.id===selected)){selected=latest.tasks[0]?.id||'';selectedDestination='';}
 latest.tasks.forEach(task=>{const card=text('button','','task-card'+(task.id===selected?' selected':'')),title=text('div','','card-title');title.append(text('span',task.id),badge(status(task)));card.append(title,text('div',task.sourceDatabase+' → '+(task.targetCount?t('targetInstances')+' '+task.targetCount:task.targetSchema),'route'));if(task.ownerNodeId)card.append(text('div',t('ownerNode')+': '+task.ownerNodeId,'subtle'));card.onclick=()=>{if(selected!==task.id)selectedDestination='';selected=task.id;render();};list.append(card);});
 const task=latest.tasks.find(x=>x.id===selected),detail=$('task-detail');
 const restoreFocus=window.RivomeshValidation?.rememberFocus?.(detail,task?.id)
  ||window.RivomeshHandoff?.rememberFocus(detail,task?.id)
  ||window.RivomeshRecovery?.rememberReceiptFocus?.(detail,task?.id);
 renderDetail(task);restoreFocus?.();window.RivomeshHandoff?.sync();
 window.RivomeshEditor?.refreshState();const invalid=$('invalid-configs');invalid.replaceChildren();invalid.classList.toggle('hidden',!latest.invalidConfigs.length);if(latest.invalidConfigs.length){invalid.append(text('h2',t('invalid')));latest.invalidConfigs.forEach(x=>invalid.append(text('p',x.file+' · '+explain(x.code),'error')));}
}
Object.assign(messages.zh,{
 sourceExpectedVersion:'预期源库版本（可选）',
 SOURCE_TYPE_CONFLICT:'源类型字段冲突，请使用一致的配置。',
 SOURCE_ADAPTER_UNAVAILABLE:'该源类型尚未开放。',
 SOURCE_VERSION_PROFILE_INVALID:'请输入主版本和次版本，可加补丁号。',
 MYSQL_BINLOG_REQUIRED:'请在源库启用 binlog。',
 MYSQL_ROW_BINLOG_REQUIRED:'源库 binlog 必须使用 ROW 格式。',
 MYSQL_FULL_ROW_IMAGE_REQUIRED:'源库 binlog 必须使用 FULL 行镜像。',
 MYSQL_PARTIAL_JSON_UNSUPPORTED:'请关闭源库的部分 JSON 行镜像，并处理仍需读取的历史 binlog。',
 MYSQL_COMPRESSED_TX_UNSUPPORTED:'请关闭源库的事务压缩，并处理仍需读取的历史 binlog。',
 MYSQL_TAGGED_GTID_UNSUPPORTED:'源端包含带标签 GTID。请核对所需日志和受支持的源档案。',
 MYSQL_GTID_REQUIRED:'请使用完整 GTID 模式并启用 GTID 一致性。',
 MYSQL_REPLICA_UPDATES_REQUIRED:'请启用副本更新写入 binlog。',
 MYSQL_VERSION_PROFILE_MISMATCH:'实际 MySQL 版本与配置的预期版本不符。'
});
Object.assign(messages.en,{
 sourceExpectedVersion:'Expected source version (optional)',
 SOURCE_TYPE_CONFLICT:'The configured source type fields disagree.',
 SOURCE_ADAPTER_UNAVAILABLE:'This source type is unavailable.',
 SOURCE_VERSION_PROFILE_INVALID:'Enter the major and minor version, with an optional patch number.',
 MYSQL_BINLOG_REQUIRED:'Enable binary logging on the source.',
 MYSQL_ROW_BINLOG_REQUIRED:'The source binary log must use ROW format.',
 MYSQL_FULL_ROW_IMAGE_REQUIRED:'The source binary log must use FULL row images.',
 MYSQL_PARTIAL_JSON_UNSUPPORTED:'Disable partial JSON row images and review the retained logs still needed for capture.',
 MYSQL_COMPRESSED_TX_UNSUPPORTED:'Disable transaction compression and review the retained logs still needed for capture.',
 MYSQL_TAGGED_GTID_UNSUPPORTED:'The source contains tagged GTIDs. Review the required logs and supported source profile.',
 MYSQL_GTID_REQUIRED:'Enable full GTID mode and GTID consistency.',
 MYSQL_REPLICA_UPDATES_REQUIRED:'Enable logging of replica updates.',
 MYSQL_VERSION_PROFILE_MISMATCH:'The MySQL server version differs from the configured version.'
});
messages.zh.captureLsn='已采集 LSN';messages.en.captureLsn='Captured LSN';
messages.zh.captureScn='已采集 SCN';messages.en.captureScn='Captured SCN';
Object.assign(messages.zh,{ownerControlLease:'执行节点控制租约',captureLease:'采集租约',lease_CLAIMED:'已持有',lease_EXPIRED:'已过期',lease_UNCLAIMED:'未持有',lease_UNKNOWN:'未知'});
Object.assign(messages.en,{ownerControlLease:'Execution node control lease',captureLease:'Capture lease',lease_CLAIMED:'Claimed',lease_EXPIRED:'Expired',lease_UNCLAIMED:'Unclaimed',lease_UNKNOWN:'Unknown'});

const pipelineFaultStages=new Set(['SOURCE_CONNECT','SOURCE_DISCOVERY','SOURCE_READ','SOURCE_DECODE',
 'SOURCE_SNAPSHOT','DDL','NORMALIZE','JOURNAL_STORAGE','JOURNAL_ACK']);
let captureRecoveryDialog;
function closeCaptureRecovery(){
 if(captureRecoveryDialog){captureRecoveryDialog.remove();captureRecoveryDialog=null;}
}
Object.assign(messages.zh,{decodeBoundary:'原生 binlog 事务边界',decodeBoundaryScope:'仅核对到提交标记及受影响表，仍需原始事件内容才能审阅跳过。',retainCapture:'留存原始事务',capture_retain:'留存源证据',CAPTURE_EVIDENCE_RETAINED:'已留存原始源事务',MYSQL_BINLOG_CONTENT_REVIEW_UNAVAILABLE:'原始事务内容暂时无法读取。请核对原 binlog 和采集租约。'});
Object.assign(messages.en,{decodeBoundary:'Native binlog transaction boundary',decodeBoundaryScope:'The commit marker and affected tables are available. Reviewing a skip still requires the original event contents.',retainCapture:'Retain original transaction',capture_retain:'Retain source evidence',CAPTURE_EVIDENCE_RETAINED:'Original source transaction retained',MYSQL_BINLOG_CONTENT_REVIEW_UNAVAILABLE:'The original transaction is unavailable. Check the source binlog and capture lease.'});
Object.assign(messages.zh,{
 reviewCaptureExclusion:'审阅源事务跳过',capture_exclude:'源事务跳过',submitCaptureExclusion:'批准跳过并保持暂停',
 captureExclusionImpact:'跳过后，这笔事务的变更不会写入以下目标。请补齐数据并核对共同点后再接管。',
 captureExclusionAgreement:'我已核对这些表和目标，并接受这笔事务暂不投递。',
 captureExclusionReason:'跳过原因（至少 8 字）',
 CAPTURE_TRANSACTION_EXCLUDED:'源事务已跳过，任务仍暂停',CAPTURE_EXCLUSION_PROFILE_UNSUPPORTED:'当前表或目标类型需要人工重建方案。',
 CAPTURE_EXCLUSION_APPROVAL_REQUIRED:'请选择风险确认并填写跳过原因。',
 CAPTURE_EXCLUSION_APPROVAL_STALE:'审阅内容已变更，请刷新并重新留存证据。',
 CAPTURE_EXCLUSION_BOUNDARY_UNAVAILABLE:'源日志中无法重新验证该事务，请恢复原日志。',
 CAPTURE_EXCLUSION_ARCHIVE_REQUIRED:'请先留存原始事务。',
 sourceExclusionOpen:'任务存在未补核的源事务缺口。接管前请检查受影响表并补齐数据。',
 firstSourceExclusion:'首次源事务跳过在此序号之后',viewCaptureExclusions:'查看源事务缺口',captureExclusionsNext:'下一页源事务缺口'
});
Object.assign(messages.en,{
 reviewCaptureExclusion:'Review source transaction skip',capture_exclude:'Skip source transaction',submitCaptureExclusion:'Approve skip and keep paused',
 captureExclusionImpact:'The following destinations will miss the changes in this transaction. Repair and verify the data before cutover.',
 captureExclusionAgreement:'I reviewed these tables and destinations and accept that this transaction will be omitted.',
 captureExclusionReason:'Reason for the skip (at least 8 characters)',
 CAPTURE_TRANSACTION_EXCLUDED:'Source transaction skipped. The task remains paused.',
 CAPTURE_EXCLUSION_PROFILE_UNSUPPORTED:'These table or destination types need a rebuild plan.',
 CAPTURE_EXCLUSION_APPROVAL_REQUIRED:'Confirm the impact and enter a reason.',
 CAPTURE_EXCLUSION_APPROVAL_STALE:'The reviewed evidence changed. Refresh and retain the evidence again.',
 CAPTURE_EXCLUSION_BOUNDARY_UNAVAILABLE:'The transaction could not be verified in the source logs. Recover the original log.',
 CAPTURE_EXCLUSION_ARCHIVE_REQUIRED:'Retain the original transaction first.',
 sourceExclusionOpen:'This pipeline has an unreconciled source gap. Review and repair affected tables before cutover.',
 firstSourceExclusion:'First source transaction skip after sequence',viewCaptureExclusions:'View source gaps',captureExclusionsNext:'Next source gaps'
});
Object.assign(messages.zh,{
 schemaDriftTitle:'MySQL 结构差异',postgresSchemaDriftTitle:'PostgreSQL 结构差异',schemaDriftScope:'当前源目录与日志中记录的结构对比。当前目录可能包含后续 DDL。',
 schemaDriftCount:'变化的表',schemaDriftLimited:'仅显示前 64 张变化表。',
 schemaDriftNew:'新增表',schemaDriftMissing:'当前目录缺少',schemaDriftChanged:'结构变化',
 schemaDriftAdded:'新增列',schemaDriftRemoved:'移除列',schemaDriftModified:'修改列',
 schemaDriftIdentity:'行身份键已变化',schemaDriftSame:'当前目录未见结构差异。',
 schemaDriftNoHistory:'日志中没有可用于对比的结构记录。',schemaDriftUnavailable:'当前无法核对结构差异。'
});
Object.assign(messages.en,{
 schemaDriftTitle:'MySQL schema differences',postgresSchemaDriftTitle:'PostgreSQL schema differences',schemaDriftScope:'The current source catalog is compared with recorded journal schemas. It can include later DDL.',
 schemaDriftCount:'Changed tables',schemaDriftLimited:'Showing the first 64 changed tables.',
 schemaDriftNew:'New table',schemaDriftMissing:'Missing from current catalog',schemaDriftChanged:'Changed definition',
 schemaDriftAdded:'Added columns',schemaDriftRemoved:'Removed columns',schemaDriftModified:'Modified columns',
 schemaDriftIdentity:'Row identity changed',schemaDriftSame:'The current catalog has no visible structural differences.',
 schemaDriftNoHistory:'The journal has no recorded schema to compare.',schemaDriftUnavailable:'Schema differences are currently unavailable.'
});
function showSourceSchemaDrift(content,review){
 const titles={CURRENT_MYSQL_CATALOG_VS_JOURNAL:'schemaDriftTitle',
  CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL:'postgresSchemaDriftTitle'};
 if(!Object.hasOwn(titles,review?.scope))return;
 const title=titles[review.scope];
 const section=text('section','','callout');
 section.append(text('h3',t(title)),text('p',t('schemaDriftScope')));
 if(review.state==='CHANGES_VISIBLE'){
  const tableLabels={TABLE_ADDED:'schemaDriftNew',TABLE_MISSING:'schemaDriftMissing',TABLE_CHANGED:'schemaDriftChanged'};
  const tables=text('ul');
  section.append(text('p',t('schemaDriftCount')+' · '+review.changedTables),tables);
  for(const row of review.tables.slice(0,64)){
   const detail=text('li',row.database+'.'+row.table+' · '+t(tableLabels[row.change]));
   const columns=text('ul');
   for(const [key,label] of [['added','schemaDriftAdded'],['removed','schemaDriftRemoved'],['modified','schemaDriftModified']]){
    const count=row.columns[key+'Count'];
    if(count>0)columns.append(text('li',t(label)+' ('+count+') · '+row.columns[key].slice(0,32).join(', ')));
   }
   if(row.identityChanged)columns.append(text('li',t('schemaDriftIdentity')));
   if(columns.children.length)detail.append(columns);
   tables.append(detail);
  }
  if(review.detailsLimited)section.append(text('p',t('schemaDriftLimited')));
 }else{
  const labels={NO_VISIBLE_CHANGE:'schemaDriftSame',NO_JOURNAL_SCHEMA:'schemaDriftNoHistory',UNAVAILABLE:'schemaDriftUnavailable'};
  section.append(text('p',t(labels[review.state]||'schemaDriftUnavailable')));
 }
 content.append(section);
}
async function openCaptureRecovery(task){
 closeCaptureRecovery();
 const dialog=text('dialog','','failure-review-dialog'),content=text('section'),close=text('button',t('closeFailureEvidence'));
 captureRecoveryDialog=dialog;dialog.setAttribute('aria-label',t('reviewSource'));
 close.onclick=()=>closeCaptureRecovery();
 dialog.addEventListener('cancel',()=>closeCaptureRecovery());
 dialog.append(text('h2',t('reviewSource')),content,close);
 content.append(text('p',t('pending')));document.body.append(dialog);dialog.showModal();
 try{
  const report=await api('/tasks/'+encodeURIComponent(task.id)+'/capture-recovery');
  const steps=text('ul');
  for(const action of report.actions)steps.append(text('li',t(action)));
  content.replaceChildren(text('p',t('UNPROVEN_SOURCE_RANGE')),
   text('p',t('sourceAdmission')+' · '+explain(report.sourceAdmission)),
   text('p',t('affectedSubscriptions')+' · '+report.affectedSubscriptions.join(', ')));
  showSourceSchemaDrift(content,report.schemaDrift);
  const boundary=report.nextTransactionHeaders;
  if(boundary?.scope==='BINLOG_HEADERS_ONLY'){
   const evidence=text('section','','callout');
   evidence.append(text('strong',t('decodeBoundary')),text('p',t('decodeBoundaryScope')),
    text('p',boundary.transactionId),text('p',`${boundary.logFile} · ${boundary.startOffset}–${boundary.endOffset}`),
    text('p',boundary.tables.join(', ')));
   if(can('TASK_CONFIG')&&task.captureLeaseState!=='CLAIMED'&&Number.isSafeInteger(report.errorGeneration)
      &&report.errorGeneration>=0&&/^[a-f0-9]{64}$/.test(boundary.boundaryHash||'')){
    const retain=text('button',t('retainCapture'),'quiet'),request=requestId();
    retain.onclick=async()=>{
     retain.disabled=true;
     try{
      await api('/tasks/'+encodeURIComponent(task.id)+'/actions',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':request},
       body:JSON.stringify({action:'CAPTURE_RETAIN',expectedRevision:task.revision,errorCount:report.errorGeneration,boundaryHash:boundary.boundaryHash})});
      closeCaptureRecovery();notify(t('accepted'));await refresh();
     }catch(error){notify(error.message);retain.disabled=false;}
    };
    evidence.append(retain);
   }
   content.append(evidence);
  }
  content.append(steps);
 }catch(error){content.replaceChildren(text('p',error.message,'callout warning'));}
}
function openCaptureExclusion(task,plan){
 closeCaptureRecovery();
 const dialog=text('dialog','','failure-review-dialog'),impact=text('ul'),reason=document.createElement('textarea');
 const agreement=document.createElement('input'),submit=text('button',t('submitCaptureExclusion'),'danger');
 const close=text('button',t('closeFailureEvidence'),'quiet'),request=requestId();
 captureRecoveryDialog=dialog;
 dialog.setAttribute('aria-label',t('reviewCaptureExclusion'));
 for(const [target,tables] of Object.entries(plan.affectedSubscriptions))impact.append(text('li',target+' · '+tables.join(', ')));
 reason.maxLength=1000;
 agreement.type='checkbox';
 const reasonLabel=text('label',t('captureExclusionReason')),agreementLabel=text('label');
 reasonLabel.append(reason);
 agreementLabel.append(agreement,text('span',t('captureExclusionAgreement')));
 const update=()=>{submit.disabled=!agreement.checked||reason.value.trim().length<8;};
 reason.oninput=update;agreement.onchange=update;update();
 close.onclick=()=>closeCaptureRecovery();
 dialog.addEventListener('cancel',()=>closeCaptureRecovery());
 submit.onclick=async()=>{
  submit.disabled=true;
  try{
   await api('/tasks/'+encodeURIComponent(task.id)+'/actions',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':request},
    body:JSON.stringify({action:'CAPTURE_EXCLUDE',expectedRevision:task.revision,errorCount:plan.errorGeneration,
     captureExclusion:{planHash:plan.planHash,archiveSha256:plan.archiveSha256,boundaryHash:plan.boundaryHash,
      acceptDataLoss:agreement.checked,reason:reason.value.trim()}})});
   closeCaptureRecovery();notify(t('accepted'));await refresh();
  }catch(error){notify(error.message);update();}
 };
 dialog.append(text('h2',t('reviewCaptureExclusion')),text('p',plan.transactionId),
  text('p',t('captureExclusionImpact')),impact,reasonLabel,agreementLabel,submit,close);
 document.body.append(dialog);dialog.showModal();
}
async function openCaptureExclusions(task){
 closeCaptureRecovery();
 const dialog=text('dialog','','failure-review-dialog'),content=text('section');
 const close=text('button',t('closeFailureEvidence'),'quiet');
 captureRecoveryDialog=dialog;dialog.setAttribute('aria-label',t('viewCaptureExclusions'));
 close.onclick=()=>closeCaptureRecovery();
 dialog.addEventListener('cancel',()=>closeCaptureRecovery());
 dialog.append(text('h2',t('viewCaptureExclusions')),content,close);
 document.body.append(dialog);dialog.showModal();
 async function load(after=''){
  content.replaceChildren(text('p',t('pending')));
  try{
   const page=await api('/tasks/'+encodeURIComponent(task.id)+'/capture-exclusions?limit=20'
    +(after?'&after='+encodeURIComponent(after):''));
   if(captureRecoveryDialog!==dialog)return;
   content.replaceChildren();
   for(const gap of page.exclusions){
    const record=text('article','','callout'),impact=text('ul');
    for(const [target,tables] of Object.entries(gap.affectedSubscriptions))impact.append(text('li',target+' · '+tables.join(', ')));
    record.append(text('strong',gap.transactionId),text('p',gap.reason),impact);
    content.append(record);
   }
   if(page.nextCursor){
    const next=text('button',t('captureExclusionsNext'),'quiet');
    next.onclick=()=>load(page.nextCursor);content.append(next);
   }
  }catch(error){if(captureRecoveryDialog===dialog)content.replaceChildren(text('p',error.message,'callout warning'));}
 }
 await load();
}
Object.assign(messages.zh,{
 workerDiagnostics:'诊断记录',diagnosticsCapture:'采集端',diagnosticsScope:'查看范围',
 diagnosticsEmpty:'暂无诊断记录',diagnosticsOlder:'查看更早记录',
 DIAGNOSTICS_CURSOR_EXPIRED:'这页记录已轮换，请重新打开诊断。',
 DIAGNOSTICS_RATE_LIMITED:'该 Agent 的诊断读取繁忙，请等一秒再试。',
 DIAGNOSTICS_TARGET_UNKNOWN:'当前配置中没有这个目标，请刷新任务后重试。',
 diagnostic_CAPTURE_READY:'采集已启动',diagnostic_CAPTURE_FAILURE:'采集失败',diagnostic_RETRY:'准备重试',
 diagnostic_BACKPRESSURE:'日志空间受限',diagnostic_BACKPRESSURE_CLEARED:'日志空间已恢复',
 diagnostic_TARGET_PAUSED:'目标因故障暂停',diagnostic_TARGET_RETRYING:'目标正在重试',
 diagnostic_TARGET_RETRY_CLEARED:'目标重试已结束'
});
Object.assign(messages.en,{
 workerDiagnostics:'Diagnostics',diagnosticsCapture:'Source capture',diagnosticsScope:'View events for',
 diagnosticsEmpty:'No diagnostics recorded',diagnosticsOlder:'Older events',
 DIAGNOSTICS_CURSOR_EXPIRED:'This page has rotated. Reopen diagnostics.',
 DIAGNOSTICS_RATE_LIMITED:'This Agent is busy serving diagnostics. Try again in one second.',
 DIAGNOSTICS_TARGET_UNKNOWN:'This destination is unavailable in the current configuration. Refresh the task and retry.',
 diagnostic_CAPTURE_READY:'Capture started',diagnostic_CAPTURE_FAILURE:'Capture failed',diagnostic_RETRY:'Retry scheduled',
 diagnostic_BACKPRESSURE:'Journal backpressure',diagnostic_BACKPRESSURE_CLEARED:'Journal backpressure cleared',
 diagnostic_TARGET_PAUSED:'Destination paused on failure',diagnostic_TARGET_RETRYING:'Destination retrying',
 diagnostic_TARGET_RETRY_CLEARED:'Destination retry cleared'
});
let workerDiagnosticsDialog;
function closeWorkerDiagnostics(){workerDiagnosticsDialog?.remove();workerDiagnosticsDialog=null;}
function openWorkerDiagnostics(task){
 closeWorkerDiagnostics();
 const dialog=text('dialog','','failure-review-dialog worker-diagnostics'),content=text('section'),close=text('button',t('closeFailureEvidence'));
 const scope=text('select'),source=text('option',t('diagnosticsCapture'));
 let requestSequence=0;
 source.value='';scope.append(source);
 for(const destination of task.destinations||[]){
  const option=text('option',destination.id);option.value=destination.id;scope.append(option);
 }
 workerDiagnosticsDialog=dialog;
 dialog.setAttribute('aria-label',t('workerDiagnostics'));
 close.onclick=closeWorkerDiagnostics;
 dialog.addEventListener('cancel',closeWorkerDiagnostics);
 dialog.append(text('h2',t('workerDiagnostics')));
 if(scope.options.length>1){
  const label=text('label');label.append(text('span',t('diagnosticsScope')),scope);dialog.append(label);
 }
 scope.onchange=()=>load();
 dialog.append(content,close);
 document.body.append(dialog);dialog.showModal();
 async function load(cursor=''){
  const sequence=++requestSequence,target=scope.value;
  content.replaceChildren(text('p',t('pending')));
  try{
   const page=await api('/tasks/'+encodeURIComponent(task.id)+'/diagnostics?limit=20'
    +(target?'&subscriptionId='+encodeURIComponent(target):'')+(cursor?'&after='+encodeURIComponent(cursor):''));
   if(workerDiagnosticsDialog!==dialog||sequence!==requestSequence)return;
   const list=text('ul');
   for(const item of page.events){
    const label=t('diagnostic_'+item.event)+(item.phase?' · '+t('fault_'+item.phase):'');
    list.append(text('li',new Date(item.at).toLocaleString()+' · '+label));
   }
   content.replaceChildren(page.events.length?list:text('p',t('diagnosticsEmpty')));
   if(page.ownerNodeId)content.prepend(text('p',t('ownerNode')+' · '+page.ownerNodeId,'subtle'));
   if(page.nextCursor){
    const more=text('button',t('diagnosticsOlder'),'quiet');
    more.onclick=()=>load(page.nextCursor);content.append(more);
   }
  }catch(error){if(workerDiagnosticsDialog===dialog&&sequence===requestSequence)content.replaceChildren(text('p',error.message,'callout warning'));}
 }
 load();
}
function renderPipelineFault(root,task){
 if(task.formatVersion!==3)return;
 const status=task.status||{};
 const stage=task.workerErrorStage||status.error_stage||status.worker_error_stage;
 if(!pipelineFaultStages.has(stage))return;
 const code=task.workerErrorCode||status.error_code||status.worker_error_code;
 const card=text('section','','callout warning pipeline-failure');
 card.append(text('strong',t('fault_'+stage)),text('p',t('faultHelp_'+stage)));
 if(code)card.append(text('span',explain(code),'subtle'));
 if(can('TASK_CONFIG')&&status.run_state==='ERROR'&&(stage.startsWith('SOURCE_')||['DDL','NORMALIZE'].includes(stage))){
  const review=text('button',t('reviewSource'),'quiet');review.onclick=()=>openCaptureRecovery(task);card.append(review);
 }
 root.append(card);
}
Object.assign(messages.zh,{
 reviewSource:'检查采集恢复',sourceAdmission:'源位置检查',affectedSubscriptions:'可能受影响的订阅',
 UNPROVEN_SOURCE_RANGE:'尚未证明缺口的事务边界及内容。请保留原始日志和当前断点。',
 SOURCE_CHECKPOINT_AVAILABLE:'合格源仍能读取原断点',
 SOURCE_POSITION_UNAVAILABLE:'尚无持久采集断点',POSTGRESQL_REQUIRED_WAL_UNAVAILABLE:'复制槽或所需 WAL 无法读取',
 SOURCE_REVIEW_REQUIRES_CAPTURE_ERROR:'仅在采集错误暂停后检查',SOURCE_REVIEW_STALE:'检查期间任务已变更，请刷新后重新检查',
 PRESERVE_SOURCE_LOGS:'保留原始日志和故障位置。',RETRY_ORIGINAL_CHECKPOINT:'修复后从原断点重试。',
 REPAIR_CAPTURE_CAUSE:'先处理解码、结构或快照故障。',
 RESTORE_LOGS_OR_SELECT_ELIGIBLE_SOURCE:'恢复所需日志，或切换到持有完整日志的合格源。',
 REVIEW_NEW_BASELINE:'无法恢复日志时，审阅新基线及受影响的订阅。',
 fault_SOURCE_CONNECT:'源库连接',faultHelp_SOURCE_CONNECT:'检查源库地址、认证和可用主库。主库切换后需保留所需 GTID 历史。',
 fault_SOURCE_DISCOVERY:'源表发现',faultHelp_SOURCE_DISCOVERY:'检查数据库映射、表权限和源表结构。',
 fault_SOURCE_READ:'读取 binlog',faultHelp_SOURCE_READ:'确认源端读取连接和 binlog 保留范围。需要时恢复日志或切换到保有历史的合格源。',
 fault_SOURCE_DECODE:'解码 binlog',faultHelp_SOURCE_DECODE:'保留故障日志，检查行镜像及事件兼容性。修复后恢复原采集位置。',
 fault_SOURCE_SNAPSHOT:'读取源快照',faultHelp_SOURCE_SNAPSHOT:'检查源表读权限、快照锁和连接，恢复后从可用分块继续。',
 fault_DDL:'源表结构变更',faultHelp_DDL:'核对源表结构和失败位置，审阅 DDL 处理方案后再恢复。',
 fault_NORMALIZE:'转换源事务',faultHelp_NORMALIZE:'检查历史结构、字段类型和无主键行标识，保留原事务用于复核。',
 fault_JOURNAL_STORAGE:'日志存储',faultHelp_JOURNAL_STORAGE:'检查日志库连接、存储容量与元数据，然后核对最后的采集位置。',
 fault_JOURNAL_ACK:'写入同步确认',faultHelp_JOURNAL_ACK:'核对目标库的提交回执和日志中的确认位置。恢复日志写入后继续核对。'
});
Object.assign(messages.en,{
 reviewSource:'Review capture recovery',sourceAdmission:'Source position check',affectedSubscriptions:'Potentially affected subscriptions',
 UNPROVEN_SOURCE_RANGE:'The gap boundaries and transaction contents remain unverified. Retain the source logs and checkpoint.',
 SOURCE_CHECKPOINT_AVAILABLE:'An eligible source still has the original checkpoint',
 SOURCE_POSITION_UNAVAILABLE:'There is no durable capture checkpoint',POSTGRESQL_REQUIRED_WAL_UNAVAILABLE:'The slot or required WAL is unavailable',
 SOURCE_REVIEW_REQUIRES_CAPTURE_ERROR:'Review requires a capture error',SOURCE_REVIEW_STALE:'The task changed during review. Refresh and try again.',
 PRESERVE_SOURCE_LOGS:'Retain the original logs and failure position.',RETRY_ORIGINAL_CHECKPOINT:'Retry from the original checkpoint after recovery.',
 REPAIR_CAPTURE_CAUSE:'Correct the decoding, schema or snapshot failure first.',
 RESTORE_LOGS_OR_SELECT_ELIGIBLE_SOURCE:'Restore the required logs or select an eligible source with complete history.',
 REVIEW_NEW_BASELINE:'Review a new baseline and affected subscriptions if the logs cannot be recovered.',
 fault_SOURCE_CONNECT:'Source connection',faultHelp_SOURCE_CONNECT:'Check addresses, authentication and primary availability. Preserve the required GTID history during promotion.',
 fault_SOURCE_DISCOVERY:'Source table discovery',faultHelp_SOURCE_DISCOVERY:'Check database mappings, table permissions and source schemas.',
 fault_SOURCE_READ:'Read binlog',faultHelp_SOURCE_READ:'Check the source read session and retained binlog range. Restore logs or choose an eligible source with the required history.',
 fault_SOURCE_DECODE:'Decode binlog',faultHelp_SOURCE_DECODE:'Retain the failing logs and check row images and event support. Resume from the original capture position after correcting the fault.',
 fault_SOURCE_SNAPSHOT:'Read source snapshot',faultHelp_SOURCE_SNAPSHOT:'Check source permissions, snapshot locks and connections. Continue from available chunks after recovery.',
 fault_DDL:'Source schema change',faultHelp_DDL:'Review the source schema, the failing position and the DDL plan before resuming.',
 fault_NORMALIZE:'Normalize source transaction',faultHelp_NORMALIZE:'Check the historical schema, column types and keyless row identities. Retain the original transaction for review.',
 fault_JOURNAL_STORAGE:'Journal storage',faultHelp_JOURNAL_STORAGE:'Check log storage connectivity, capacity and metadata. Verify the latest captured position.',
 fault_JOURNAL_ACK:'Persist delivery acknowledgment',faultHelp_JOURNAL_ACK:'Compare the destination commit receipt with the journal acknowledgment. Restore journal writes and resume reconciliation.'
});
function renderDetail(task){const root=$('task-detail');root.replaceChildren();if(!task){root.append(text('div',t('selectTask'),'empty'));return;}const body=text('div','','detail-body'),title=text('div','','detail-title'),heading=text('div');heading.append(text('span',(task.sourceType||'MYSQL')+' → '+(task.targetType||'GAUSS'),'eyebrow'),text('h3',task.id),text('p',task.sourceCluster,'subtle'));if(task.createdBy)heading.append(text('p',t('createdBy')+': '+task.createdBy+' ('+t('ROLE_'+task.createdRole)+')','subtle'));title.append(heading,badge(status(task)));body.append(title);const actions=text('div','','detail-actions'),pending=task.operations.some(x=>x.status==='PENDING');
 ['CHECK','START','STOP','PAUSE','RESUME'].forEach(action=>{const button=text('button',t(globalTasks()&&action==='STOP'?'globalStop':action.toLowerCase()),action==='START'?'primary':'');button.disabled=!can('TASK_CONTROL')||pending||(action==='RESUME'&&!['PAUSED','ERROR'].includes(task.status.run_state));button.onclick=()=>command(task,action);actions.append(button);});const configure=text('button',t('configuration'),'quiet');configure.disabled=!can('TASK_CONFIG')||!window.RivomeshEditor;configure.onclick=()=>window.RivomeshEditor.open(task);actions.append(configure);const handoff=window.RivomeshHandoff?.button(task);if(handoff)actions.append(handoff);body.append(actions,text('div',t(globalTasks()?'globalScope':'scope'),'callout'));if(task.ownerNodeId)body.append(text('p',t('ownerNode')+': '+task.ownerNodeId+' · '+t(task.ownerState||'UNKNOWN'),'callout'));if(task.ownerObservedAt)body.append(text('p',t('ownerObserved')+': '+new Date(task.ownerObservedAt).toLocaleString(),'subtle'));if(task.configurationPending)body.append(text('p',t('configurationPending'),'callout warning'));if(task.configurationProjectionPending)body.append(text('p',t('CONFIG_PROJECTION_PENDING'),'callout warning'));
 if(task.formatVersion===3&&can('TASK_CONFIG')){
  const diagnostics=text('button',t('workerDiagnostics'),'quiet');
  diagnostics.dataset.workerDiagnostics='';diagnostics.onclick=()=>openWorkerDiagnostics(task);
  body.append(diagnostics);
 }
 renderPipelineFault(body,task);
 if(task.status.hasSourceExclusions){
  const gap=text('section','','callout warning');
  gap.append(text('p',t('sourceExclusionOpen')));
  if(can('TASK_CONFIG')){
   const view=text('button',t('viewCaptureExclusions'),'quiet');
   view.onclick=()=>openCaptureExclusions(task);
   gap.append(view);
  }
  body.append(gap);
 }
 if(task.problem)body.append(text('div',task.problem==='NOT_INITIALIZED'?t('uninitialized'):(task.statusAvailable?'':t('stale')+' ')+explain(task.problem),'callout warning'));
 const grid=text('div','','status-grid');[['phase',task.status.phase],['sharedState',task.status.run_state],[globalTasks()?'globalWorker':'worker',task.workerState],...(globalTasks()&&task.ownerControlState?[['ownerControlLease',task.ownerControlState]]:[]),...(task.formatVersion===3||globalTasks()&&task.captureLeaseState?[['captureLease','lease_'+(task.captureLeaseState||'UNKNOWN')]]:[]),['pendingTransactions',task.status.pending_transactions],...(task.status.capture_lsn?[['captureLsn',task.status.capture_lsn]]:[]),...(task.status.capture_scn?[['captureScn',task.status.capture_scn]]:[]),['observed',task.observedAt?new Date(task.observedAt).toLocaleTimeString():'—']].forEach(([key,value])=>{const item=text('div');item.append(text('span',t(key)),text('strong',value==null?'—':t(String(value))));grid.append(item);});body.append(grid);if(task.formatVersion===3&&task.status.maxBytes){const used=Number(task.status.pendingBytes||0)+Number(task.status.snapshotBytes||0)+Number(task.status.identityBytes||0),quota=Number(task.status.maxBytes),storage=text('section','','callout'+(used/quota>.8?' warning':'')),progress=document.createElement('progress');progress.max=quota;progress.value=used;progress.setAttribute('aria-label',t('journal'));storage.append(text('strong',t('journal')+': '+(used/1048576).toFixed(1)+' / '+(quota/1048576).toFixed(0)+' MiB'),progress,text('p',t('journalUsageHelp')));body.append(storage);}
 if(task.status.tables?.length){const table=text('table'),head=text('tr');['table',task.formatVersion===3?'sourceSnapshotRows':'copied','snapshot'].forEach(x=>head.append(text('th',t(x))));table.append(head);task.status.tables.forEach(row=>{const tr=text('tr');tr.append(text('td',row.table),text('td',Number(row.copiedRows).toLocaleString()),text('td',t(row.done?'complete':'pending')));table.append(tr);});if(task.formatVersion===3)body.append(text('p',t('sourceSnapshotHelp'),'subtle'));body.append(table);if(Number(task.status.table_count)>task.status.tables.length)body.append(text('p',t('tablePreview')+task.status.tables.length+' / '+task.status.table_count,'subtle'));}
 renderSubscriptions(body,task,pending);const details=text('details'),summary=text('summary',t('position'));details.append(summary,text('pre',JSON.stringify(task.status,null,2)));body.append(details,text('h2',t('history')));const history=text('div','','history');task.operations.slice(-8).reverse().forEach(op=>{const row=text('div'),left=text('span',t(op.action.toLowerCase())+' · '+(op.code?explain(op.code):op.status)+(op.actorId?' · '+op.actorId+' ('+t('ROLE_'+op.actorRole)+')':''));row.append(left,badge(op.status));if(op.deliveryStatus&&op.deliveryStatus!==op.status)row.append(badge(op.deliveryStatus));const receipt=window.RivomeshRecovery?.receiptAction?.(task,op);if(receipt)row.append(receipt);if(op.result&&Object.keys(op.result).length){const details=text('details');details.append(text('summary',t('operationDetails')),text('pre',JSON.stringify(maskPlan(op.result),null,2)));row.append(details);}const plan=op.result?.exclusionPlan;if(plan&&op.action==='CAPTURE_RETAIN'&&op.status==='SUCCEEDED'&&task.statusAvailable&&task.status.run_state==='ERROR'&&task.status.error_count===plan.errorGeneration&&!pending&&can('TASK_CONFIG')){const review=text('button',t('reviewCaptureExclusion'),'quiet');review.onclick=()=>openCaptureExclusion(task,plan);row.append(review);}if(op.failureEvidenceId&&op.status==='SUCCEEDED'&&session?.recoveryReview&&can('TASK_CONFIG')){const review=text('button',t('viewFailureEvidence'),'quiet');review.onclick=()=>openFailureEvidence(task,op.failureEvidenceId);row.append(review);}history.append(row);});body.append(history);window.RivomeshValidation?.render(body,task,pending);window.RivomeshRecovery?.render(body,task);root.append(body);}
async function command(task,action,subscription){try{await api('/tasks/'+encodeURIComponent(task.id)+'/actions',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':requestId()},body:JSON.stringify({action,expectedRevision:task.revision,errorCount:subscription?subscription.errorGeneration:(task.status.error_count??-1),...(subscription?{subscriptionId:subscription.id}:{})})});notify(t('accepted'));await refresh();}catch(e){notify(e.message);await refresh();}}
Object.assign(messages.zh,{adapterRecoverySkip:'跳过事务',adapterRecoveryCompensation:'前镜像补偿',adapterRecoveryRebuild:'重建',adapterRecovery_REVIEWED_LOCAL_TRANSACTION:'需审阅的本地事务',adapterRecovery_REVIEWED_BEFORE_IMAGES:'使用已审阅的前镜像',adapterRecovery_MANUAL_NEW_BASELINE:'人工规划新基线',adapterRecovery_UNAVAILABLE:'不可用'});
Object.assign(messages.en,{adapterRecoverySkip:'Skip transaction',adapterRecoveryCompensation:'Compensation',adapterRecoveryRebuild:'Rebuild',adapterRecovery_REVIEWED_LOCAL_TRANSACTION:'Reviewed local transaction',adapterRecovery_REVIEWED_BEFORE_IMAGES:'Reviewed before-images',adapterRecovery_MANUAL_NEW_BASELINE:'Plan a new baseline manually',adapterRecovery_UNAVAILABLE:'Unavailable'});
function renderAdapters(){
 const root=$('adapter-list');root.replaceChildren();
 for(const adapter of capabilities){
  const box=text('article','',adapter.status==='PLANNED'?'planned':'');
  box.append(text('strong',adapter.id),badge(adapter.status),text('p',t(adapter.role)+(adapter.note?' · '+adapter.note:'')));
  if(adapter.role==='SINK'){
   const recovery=[['transactionSkip','adapterRecoverySkip'],['transactionCompensation','adapterRecoveryCompensation'],['rebuildMode','adapterRecoveryRebuild']]
    .filter(([field])=>adapter[field]!=null)
    .map(([field,label])=>t(label)+' · '+t('adapterRecovery_'+adapter[field]));
   if(recovery.length)box.append(text('p',recovery.join(' / '),'subtle'));
  }
  root.append(box);
 }
}
function openCreate(){if(!can('TASK_CREATE'))return;step=0;$('create-form').reset();$('create-owner').value=$('task-owner').value;$('create-auto-placement').checked=$('task-auto-placement').checked;renderOwners();$('extra-targets').replaceChildren();$('first-extra-mappings').replaceChildren();$('primary-messaging').replaceChildren();extraTargetCounter=1;$('create-error').textContent='';const id=crypto.getRandomValues(new Uint32Array(1))[0]||910001;$('create-form').elements.serverId.value=id;wizardMode();showStep();$('create-dialog').showModal();}
function sourceConfiguration(f){
 const type=wizardSource(),source={clusterId:f.clusterId.value.trim()};
 let version;
 if(type==='POSTGRESQL'){
  Object.assign(source,{type,database:f.pgDatabase.value.trim(),topology:'SINGLE',username:f.pgUsername.value.trim(),password:f.pgPassword.value,
   postgresql:{url:f.pgUrl.value.trim(),publication:f.pgPublication.value.trim(),slotPrefix:f.pgSlotPrefix.value.trim(),failoverSlot:f.pgFailoverSlot.checked}});
  version=f.pgVersion.value.trim();
 }else if(type==='ORACLE'){
  Object.assign(source,{type,database:f.oraclePdb.value.trim(),topology:'SINGLE',oracle:{
   root:{url:f.oracleRootUrl.value.trim(),username:f.oracleRootUser.value.trim(),password:f.oracleRootPassword.value},
   pdb:{url:f.oraclePdbUrl.value.trim(),username:f.oraclePdbUser.value.trim(),password:f.oraclePdbPassword.value},
   tables:oracleTables()}});
  if(f.oracleVirtualNumeric.checked)source.oracle.allowVirtualNumericColumns=true;
  version=f.oracleVersion.value.trim();
 }else{
  Object.assign(source,{host:f.sourceHost.value.trim(),port:Number(f.sourcePort.value),username:f.sourceUsername.value,password:f.sourcePassword.value,serverId:Number(f.serverId.value)});
  if(v3())source.type=type;
  else source.database=f.sourceDatabase.value.trim();
  version=f.sourceExpectedVersion.value.trim();
 }
 if(version)source.expectedVersion=version;
 return source;
}
function configFromForm(){const f=$('create-form').elements,source=sourceConfiguration(f);
 const target={type:f.targetType.value,url:f.targetUrl.value.trim(),username:f.targetUsername.value,password:f.targetPassword.value,schema:f.targetSchema.value.trim(),metadataSchema:f.metadataSchema.value.trim()};
 if(!v3())return {jobId:f.jobId.value.trim(),nodeId:'console-worker',source,target};
 const dbs=sourceDatabases(),mappings={};if(dbs.length)mappings[dbs[0]]=target.schema;$('first-extra-mappings').querySelectorAll('input').forEach(input=>mappings[input.dataset.database]=input.value.trim());
 const targets=[{id:f.targetId.value.trim(),product:target.type,url:target.url,username:target.username,password:target.password,metadataSchema:target.metadataSchema,mappings,uniquePolicy:'DOWNGRADE_BUSINESS'}];
 document.querySelectorAll('#extra-targets .target-settings').forEach(card=>{const value=field=>card.querySelector('[data-field="'+field+'"]').value,rows={};card.querySelectorAll('.mapping-fields input').forEach(input=>rows[input.dataset.database]=input.value.trim());targets.push({id:value('id').trim(),product:value('product'),url:value('url').trim(),username:value('username'),password:value('password'),metadataSchema:value('metadataSchema').trim(),mappings:rows,uniquePolicy:'DOWNGRADE_BUSINESS'});});
 targets.forEach((target,index)=>{if(target.product==='DM'){const root=index===0?$('primary-messaging'):document.querySelectorAll('#extra-targets .messaging-settings')[index-1];target.dm={acknowledgeTextConversions:root.querySelector('[data-field=dmAcknowledgement]').checked};}if(target.product==='HIVE'){const root=index===0?$('primary-messaging'):document.querySelectorAll('#extra-targets .messaging-settings')[index-1];target.hive={acknowledgeIndexOmission:root.querySelector('[data-field=hiveAcknowledgement]').checked,queryTimeoutSeconds:Number(root.querySelector('[data-field=hiveTimeout]').value),maxMessageBytes:Number(root.querySelector('[data-field=maxKiB]').value)*1024};}if(target.product==='CLICKHOUSE')target.clickhouse={secondaryIndexes:'DATA_SKIPPING'};if(target.product==='DORIS'){const root=index===0?$('primary-messaging'):document.querySelectorAll('#extra-targets .messaging-settings')[index-1],value=field=>root.querySelector('[data-field='+field+']').value;target.doris={loadUrl:value('dorisLoadUrl').trim(),redirectOrigins:value('dorisRedirectOrigins').split(',').map(s=>s.trim()).filter(Boolean),replicas:Number(value('dorisReplicas')),buckets:Number(value('dorisBuckets')),secondaryIndexes:value('dorisIndexes'),maxMessageBytes:Number(value('maxKiB'))*1024};}if(messagingProduct(target.product))target.messaging=messageSettings(index===0?$('primary-messaging'):document.querySelectorAll('#extra-targets .messaging-settings')[index-1],target.product);});
 const journal={mode:f.journalMode.value,maxBytes:Number(f.journalMaxMiB.value)*1024*1024};if(journal.mode==='EXTERNAL')Object.assign(journal,{product:f.journalProduct.value,url:f.journalUrl.value.trim(),username:f.journalUsername.value,password:f.journalPassword.value,namespace:f.journalNamespace.value.trim()});
 return {formatVersion:3,pipelineId:f.jobId.value.trim(),nodeId:'console-worker',source,databases:dbs,targets,journal};}
function showStep(){[0,1,2].forEach(i=>{$('step-'+i).classList.toggle('hidden',i!==step);$('step-label-'+i).classList.toggle('current',i===step);});$('previous-step').disabled=step===0;$('next-step').classList.toggle('hidden',step===2);$('save-task').classList.toggle('hidden',step!==2);if(step===1&&v3())mappingFields();if(step===2){const plan=maskPlan(configFromForm());$('plan-preview').textContent=JSON.stringify(globalTasks()?{...ownerSelection(),config:plan}:plan,null,2);}}
$('next-step').onclick=()=>{if(!validateStep(step))return;step++;showStep();};$('previous-step').onclick=()=>{step=Math.max(0,step-1);showStep();};$('new-task').onclick=openCreate;$('close-create').onclick=()=>{$('create-dialog').close();clearDraft();};
$('create-form').addEventListener('submit',async e=>{e.preventDefault();if(step!==2)return;for(let i=0;i<2;i++)if(!validateStep(i)){step=i;showStep();return;}const button=$('save-task');button.disabled=true;try{const task=await api('/tasks',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':requestId()},body:JSON.stringify({config:configFromForm(),...ownerSelection()})});selected=task.id;$('create-dialog').close();clearDraft();clearTaskPages();notify(t(globalTasks()?'accepted':'created'));await refresh();}catch(error){$('create-error').textContent=error.message;}finally{button.disabled=false;}});
function signIn(credential){
 forgetSession();const generation=sessionGeneration;
 return queueAuth(async()=>{if(generation!==sessionGeneration)return;try{
  session=await api('/session',{method:'POST',headers:{Authorization:'Bearer '+credential}});
  $('login-dialog').close();$('login-error').textContent='';
  await refresh(true);
 }catch(error){if(generation===sessionGeneration||error.code==='UNAUTHORIZED')$('login-error').textContent=error.message;showLogin();throw error;}});
}
$('login-form').addEventListener('submit',e=>{
 e.preventDefault();const credential=$('token').value;$('token').value='';signIn(credential).catch(()=>{});
});
$('login-dialog').addEventListener('cancel',e=>e.preventDefault());$('logout').onclick=()=>{forgetSession();showLogin();sessionChannel?.postMessage({type:'signed-out'});queueAuth(revokeBrowserSession).catch(error=>notify(error.message));};$('language').onclick=()=>{lang=lang==='zh'?'en':'zh';localStorage.setItem('rivomesh-language',lang);translate();};
function view(name){
 activeView=name;
 for(const item of ['tasks','adapters','nodes']){
  const current=name===item, navigation=$(item+'-nav');
  $(item+'-view').classList.toggle('hidden',!current);
  navigation.classList.toggle('active',current);
  if(current)navigation.setAttribute('aria-current','page');
  else navigation.removeAttribute('aria-current');
 }
 $('page-title').textContent=t(name);
}

Object.assign(messages.zh,{runtime:'运行核心',targetId:'目标标识',addTarget:'+ 添加目标实例',removeTarget:'移除此目标',journal:'持久日志',journalMode:'日志方式',journalLocal:'本机持久卷（无需独立数据库）',journalExternal:'外部事务数据库（多节点共享）',journalHelp:'本机日志由控制台放在私有目录；跨主机接管请选择外部日志。配额包含快照和无主键身份负载，数据库物理空间需另留余量。',journalQuota:'逻辑负载配额（MiB）',subscriptions:'目标投递状态',acknowledged:'已确认事务序号',resume_target:'恢复此目标',TARGET_RESUME_REQUESTED:'已允许该目标重试原事务。',PARTIAL_FAILURE:'部分目标暂停',ERROR:'源端已阻断',DISCOVERY:'发现对象',PAUSE_PENDING_TARGET_FENCE:'共享暂停已保存，部分目标的隔离尚未确认。',LOCAL_JOURNAL_MIGRATION_REQUIRED:'本机日志只能使用控制台托管目录。导入旧 CLI 日志需要迁移计划；新建任务请删除 journal.path，外部日志不受此限制。',databaseListInvalid:'请使用逗号分隔不同的 database；每项不能为空。',targetIdDuplicate:'每个目标需要不同的标识。',v3SourceRequirements:'V3 支持有主键和无主键 InnoDB、JSON、受支持的存储/虚拟生成列。仍需 GTID、ROW/FULL 和全量短锁权限。',sourceDatabase:'源 database（V3 可用逗号分隔多个）',targetHelp:'逐库填写目标 schema/database；可添加多个目标实例。当前支持 Gauss、PostgreSQL、MySQL，先检查再启动。',currentLimit:'V3 的每个目标独立投递和恢复；已有表首次要求为空。业务 UNIQUE 会转换为普通索引，接管前需要校验和恢复约束。V2 保留原单库单目标行为。',step1:'02 · 目标与日志',previewScope:'MySQL → 多种事务目标'});
Object.assign(messages.en,{runtime:'Runtime',targetId:'Target ID',addTarget:'+ Add target instance',removeTarget:'Remove target',journal:'Durable journal',journalMode:'Journal mode',journalLocal:'Local persistent volume',journalExternal:'External transactional database',journalHelp:'Local journals use a private managed directory. Choose external storage for cross-host takeover. Logical quota includes snapshots/keyless identities; allow additional physical database space.',journalQuota:'Logical payload quota (MiB)',subscriptions:'Target deliveries',acknowledged:'Acknowledged transaction sequence',resume_target:'Resume this target',TARGET_RESUME_REQUESTED:'This target may retry its original transaction.',PARTIAL_FAILURE:'Target paused',ERROR:'Source blocked',DISCOVERY:'Discovering objects',PAUSE_PENDING_TARGET_FENCE:'Shared pause saved; some target fences remain unconfirmed.',LOCAL_JOURNAL_MIGRATION_REQUIRED:'Local journals must use the console-managed directory. Existing CLI journals need a migration plan; omit journal.path for new tasks. External journals are unaffected.',databaseListInvalid:'Use comma-separated, distinct, nonempty database names.',targetIdDuplicate:'Each target needs a distinct ID.',v3SourceRequirements:'V3 supports keyed/keyless InnoDB, JSON and supported stored/virtual generated columns. GTID, ROW/FULL and short snapshot-lock privileges are required.',sourceDatabase:'Source databases (comma-separated in V3)',targetHelp:'Map each database explicitly and add target instances. Current products are Gauss, PostgreSQL and MySQL. Check before starting.',currentLimit:'V3 targets deliver and recover independently. Initial existing tables must be empty. Business UNIQUE becomes ordinary indexes; validate/restore constraints before takeover. V2 retains its single-database core.',step1:'02 · Targets & journal',previewScope:'MySQL → Transactional targets'});
let extraTargetCounter=1;
function v3(){return $('create-form').elements.formatVersion.value==='3';}
Object.assign(messages.zh,{
 sourceEngine:'源数据库类型',sourceJdbc:'源 JDBC URL',pgDatabase:'PostgreSQL 数据库',pgSchemas:'源 Schema（逗号分隔）',pgPublication:'逻辑发布名称',pgSlotPrefix:'逻辑槽前缀',pgFailoverSlot:'使用可切换逻辑槽',
 pgSourceRequirements:'实验性档案。填写可写主库的地址、数据库及发布名称。切主续传需使用稳定写入入口和已同步逻辑槽。支持 PostgreSQL 和 MySQL 目标。创建后先执行检查。',
 oraclePdb:'可写 PDB 名称',oracleRootService:'CDB 根服务（归档读取）',oraclePdbService:'PDB 服务（读取快照）',oracleTables:'源表（大写 OWNER.TABLE，每行一张）',
 oracleSourceRequirements:'实验性档案。Agent 需安装 Oracle JDBC 驱动。填写 CDB 根和可写 PDB 两个服务，归档模式与全列补充日志须已配置。支持 PostgreSQL 目标。创建后先执行检查。',
 oracleVirtualNumeric:'同步受支持的整数虚拟列',oracleTableListInvalid:'填写 1–200 张不重复的表，每张表使用大写 OWNER.TABLE。',sourceJdbcInvalid:'请填写该源数据库的 JDBC URL。'
});
Object.assign(messages.en,{
 sourceEngine:'Source database engine',sourceJdbc:'Source JDBC URL',pgDatabase:'PostgreSQL database',pgSchemas:'Source schemas (comma-separated)',pgPublication:'Logical publication',pgSlotPrefix:'Logical slot prefix',pgFailoverSlot:'Use a failover logical slot',
 pgSourceRequirements:'Experimental. Provide the writable primary address, database and publication. Failover needs a stable writer endpoint and a synchronized logical slot. PostgreSQL and MySQL targets are supported. Run Check after creating the task.',
 oraclePdb:'Writable PDB name',oracleRootService:'CDB root service (archive capture)',oraclePdbService:'PDB service (snapshot reads)',oracleTables:'Source tables (uppercase OWNER.TABLE, one per line)',
 oracleSourceRequirements:'Experimental. Install Oracle JDBC on the Agent. Supply the CDB root and writable PDB services with archive mode and all-column supplemental logging. PostgreSQL targets are supported. Run Check after creating the task.',
 oracleVirtualNumeric:'Sync supported integer virtual columns',oracleTableListInvalid:'Enter 1–200 distinct tables using uppercase OWNER.TABLE.',sourceJdbcInvalid:'Enter a JDBC URL for this source engine.'
});
function wizardSource(){return v3()?$('create-form').elements.sourceType.value:'MYSQL';}
function oracleTables(){return $('create-form').elements.oracleTables.value.split(/[,\n]/).map(name=>name.trim()).filter(Boolean);}
function sourceDatabases(){
 const f=$('create-form').elements,type=wizardSource();
 if(type==='ORACLE')return [...new Set(oracleTables().map(table=>table.split('.')[0]))];
 return (type==='POSTGRESQL'?f.pgSchemas:f.sourceDatabase).value.split(',').map(name=>name.trim()).filter(Boolean);
}
function clearDraft(){$('create-form').reset();$('extra-targets').replaceChildren();$('first-extra-mappings').replaceChildren();$('primary-messaging').replaceChildren();}
function sourceMode(){
 const f=$('create-form').elements;
 for(const option of f.sourceType.options){
  option.disabled=option.value!=='MYSQL'&&(!v3()||!capabilities.some(entry=>entry.role==='SOURCE'&&entry.id===option.value&&['AVAILABLE','EXPERIMENTAL','PARTIALLY_VERIFIED'].includes(entry.status)));
 }
 if(!f.sourceType.value||f.sourceType.selectedOptions[0]?.disabled)f.sourceType.value='MYSQL';
 const type=wizardSource();
 if(type==='MYSQL')f.clusterId.removeAttribute('pattern');
 else f.clusterId.pattern='[A-Za-z0-9][A-Za-z0-9._-]{0,127}';
 document.querySelectorAll('[data-wizard-source]').forEach(section=>{
  const active=section.dataset.wizardSource===type;
  section.classList.toggle('hidden',!active);
  section.querySelectorAll('input,textarea').forEach(input=>input.disabled=!active);
 });
 const requirements=$('source-requirements');
 requirements.dataset.i18n=type==='POSTGRESQL'?'pgSourceRequirements':type==='ORACLE'?'oracleSourceRequirements':v3()?'v3SourceRequirements':'sourceRequirements';
 requirements.textContent=t(requirements.dataset.i18n);
}
function wizardMode(){
 const modern=v3();
 document.querySelectorAll('.v3-field').forEach(section=>{
  section.classList.toggle('hidden',!modern);
  section.querySelectorAll('input,select').forEach(input=>input.disabled=!modern);
 });
 sourceMode();
 targetMode();
 document.querySelectorAll('#extra-targets .target-settings').forEach(card=>targetMode(card));
 journalMode();
}
function journalMode(){const f=$('create-form').elements,external=v3()&&f.journalMode.value==='EXTERNAL';$('external-journal').classList.toggle('hidden',!external);$('external-journal').querySelectorAll('input,select').forEach(input=>input.disabled=!external);}
function makeInput(label,field,value,type='text'){const box=text('label'),span=text('span',t(label));span.dataset.i18n=label;const input=document.createElement('input');input.dataset.field=field;input.value=value||'';input.type=type;input.required=type!=='password';if(type==='password')input.autocomplete='new-password';box.append(span,input);return box;}
function mappingRows(root,databases){const values=new Map([...root.querySelectorAll('input')].map(input=>[input.dataset.database,input.value]));root.replaceChildren();databases.forEach(database=>{const box=text('label'),input=document.createElement('input');box.append(text('span',database+' → schema / database / namespace'));input.required=true;input.dataset.database=database;input.value=values.get(database)||'';box.append(input);root.append(box);});}
function mappingFields(){const dbs=sourceDatabases();$('primary-map-source').textContent=dbs.length?dbs[0]+' → schema / database / namespace':'';mappingRows($('first-extra-mappings'),dbs.slice(1));document.querySelectorAll('#extra-targets .mapping-fields').forEach(root=>mappingRows(root,dbs));}
function defaultUrl(product){if(product==='GBASE_8C')return 'jdbc:gbase8c://127.0.0.1:5432/postgres?connectTimeout=10&socketTimeout=30';if(product==='DM')return 'jdbc:dm://127.0.0.1:5236?connectTimeout=5000&socketTimeout=30000';if(product==='KINGBASE')return 'jdbc:kingbase8://127.0.0.1:54321/test?connectTimeout=10&socketTimeout=30';if(product==='HIVE')return 'jdbc:hive2://127.0.0.1:10000/default';if(product==='OCEANBASE')return 'jdbc:mysql://127.0.0.1:2881/?useSSL=false&connectTimeout=5000&socketTimeout=30000';if(product==='DORIS')return 'jdbc:mysql://127.0.0.1:9030/?useSSL=false&connectTimeout=5000&socketTimeout=30000';if(product==='ACTIVEMQ_CLASSIC')return 'activemq://127.0.0.1:61616';if(product==='CLICKHOUSE')return 'http://127.0.0.1:8123';if(product==='ACTIVEMQ_ARTEMIS')return 'artemis://127.0.0.1:61616';if(product==='ROCKETMQ')return 'rocketmq://127.0.0.1:9876';if(product==='PULSAR')return 'pulsar://127.0.0.1:6650';if(product==='REDIS_STREAMS')return 'redis://127.0.0.1:6379/0';if(product==='KAFKA')return 'kafka://127.0.0.1:9092';if(product==='RABBITMQ')return 'amqp://127.0.0.1:5672';if(product==='NATS_JETSTREAM')return 'nats://127.0.0.1:4222';return product==='MYSQL'?'jdbc:mysql://127.0.0.1:3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=30000':product==='POSTGRESQL'?'jdbc:postgresql://127.0.0.1:5432/business?connectTimeout=10&socketTimeout=30':'jdbc:postgresql://127.0.0.1:8000/gaussdb?prepareThreshold=0&connectTimeout=10&socketTimeout=30';}
function addTarget(){const card=text('section','','target-settings'),header=text('header'),remove=text('button',t('removeTarget'),'quiet');remove.type='button';remove.dataset.i18n='removeTarget';header.append(text('h3',t('targetId')),remove);remove.onclick=()=>card.remove();card.append(header,makeInput('targetId','id','target-'+(++extraTargetCounter)));const typeLabel=text('label'),typeSpan=text('span',t('targetType')),select=document.createElement('select');typeSpan.dataset.i18n='targetType';select.dataset.field='product';['POSTGRESQL','GAUSS','MYSQL','OCEANBASE','GBASE_8C','KINGBASE','DM','RABBITMQ','NATS_JETSTREAM','KAFKA','REDIS_STREAMS','PULSAR','ROCKETMQ','ACTIVEMQ_ARTEMIS','ACTIVEMQ_CLASSIC','CLICKHOUSE','DORIS','HIVE'].forEach(product=>{const option=text('option',product);option.value=product;select.append(option);});typeLabel.append(typeSpan,select);card.append(typeLabel,makeInput('jdbcUrl','url',defaultUrl('POSTGRESQL')));select.onchange=()=>{card.querySelector('[data-field=url]').value=defaultUrl(select.value);targetMode(card);};const fields=text('div','','form-grid');fields.append(makeInput('username','username',''),makeInput('password','password','','password'),makeInput('metadataSchema','metadataSchema','rivomesh_v3'));card.append(fields);const messaging=text('div','','messaging-settings hidden');card.append(messaging);const mappings=text('div','','form-grid mapping-fields');mappingRows(mappings,sourceDatabases());card.append(mappings);$('extra-targets').append(card);targetMode(card);}
Object.assign(messages.zh,{targetSchema:'目标 schema / database / 消息命名空间',jdbcUrl:'连接地址 / JDBC URL',kafkaSecurity:'Kafka 连接认证',kafkaMechanism:'Kafka SASL 机制',messageTopic:'Kafka 专用 Topic',messageStream:'持久 Stream 名称',messageToken:'NATS 访问令牌（可选）',messageReplicas:'Broker 副本数',messageMaxKiB:'单条消息上限（KiB）',messageHelp:'消息目标使用确认后至少一次投递。消费者必须按事件 ID 去重，按事务边界与计数重组；暂停不能撤回在途消息。',targetHelp:'逐库填写目标 schema/database 或消息命名空间，可添加多个目标。支持 Gauss、PostgreSQL、MySQL、RabbitMQ、NATS JetStream、Kafka、Redis Streams、Pulsar。高级 TLS 和多地址可导入 YAML。',previewScope:'数据库迁移与同步',RABBIT_UNROUTABLE:'RabbitMQ 未找到路由队列，投递已暂停。检查队列后恢复此目标。',BROKER_AUTHENTICATION_REJECTED:'Broker 拒绝认证，请检查凭据。',JETSTREAM_CONFIGURATION_REQUIRES_PLAN:'JetStream 所有权、持久化或保留策略不符，需要调整配置。'});
Object.assign(messages.en,{targetSchema:'Target schema / database / message namespace',jdbcUrl:'Connection / JDBC URL',kafkaSecurity:'Kafka security protocol',kafkaMechanism:'Kafka SASL mechanism',messageTopic:'Dedicated Kafka topic',messageStream:'Durable stream name',messageToken:'NATS token (optional)',messageReplicas:'Broker replicas',messageMaxKiB:'Maximum message size (KiB)',messageHelp:'Confirmed at-least-once events. Consumers must deduplicate event IDs and assemble transaction boundaries/counts. Pause cannot retract in-flight messages.',targetHelp:'Map each database to a schema/database or message namespace and add destinations. Gauss, PostgreSQL, MySQL, RabbitMQ, NATS JetStream, Kafka, Redis Streams and Pulsar are implemented. Import YAML for advanced TLS/multiple endpoints.',previewScope:'Database migration and synchronization',RABBIT_UNROUTABLE:'RabbitMQ returned an unroutable publication. Repair the queue and resume this target.',BROKER_AUTHENTICATION_REJECTED:'Broker authentication was rejected. Check credentials.',JETSTREAM_CONFIGURATION_REQUIRES_PLAN:'JetStream ownership, persistence or retention differs; review its configuration.'});
function makeSelect(label,field,values){const box=text('label'),span=text('span',t(label)),select=document.createElement('select');span.dataset.i18n=label;select.dataset.field=field;values.forEach(value=>{const option=text('option',value);option.value=value;select.append(option);});box.append(span,select);return box;}
function messagingProduct(product){return ['RABBITMQ','NATS_JETSTREAM','KAFKA','REDIS_STREAMS','PULSAR','ROCKETMQ','ACTIVEMQ_ARTEMIS','ACTIVEMQ_CLASSIC'].includes(product);}
function messageSettings(root,product){const value=name=>root.querySelector('[data-field='+name+']').value,result={replicas:Number(value('replicas')),maxMessageBytes:Number(value('maxKiB'))*1024,autoCreate:true};if(product==='NATS_JETSTREAM')Object.assign(result,{stream:value('stream').trim(),token:value('token')});if(product==='REDIS_STREAMS')result.stream=value('stream').trim();if(product==='PULSAR')Object.assign(result,{topic:value('topic').trim(),adminUrl:value('adminUrl').trim(),cluster:value('cluster').trim(),token:value('token')});if(product==='ACTIVEMQ_CLASSIC')Object.assign(result,{stream:value('stream').trim(),broker:value('broker').trim(),adminUrl:value('adminUrl').trim()});if(product==='ACTIVEMQ_ARTEMIS')Object.assign(result,{stream:value('stream').trim(),broker:value('broker').trim()});if(product==='ROCKETMQ')Object.assign(result,{topic:value('topic').trim(),cluster:value('cluster').trim(),broker:value('broker').trim(),minimumRetentionHours:Number(value('minimumRetentionHours'))});if(product==='RABBITMQ')result.queueType='quorum';if(product==='KAFKA')Object.assign(result,{topic:value('topic').trim(),securityProtocol:value('securityProtocol'),saslMechanism:value('saslMechanism')});return result;}

function targetMode(card){
 const f=$('create-form').elements,select=card?card.querySelector('[data-field=product]'):f.targetType;
 const type=wizardSource(),allowed=!v3()?['POSTGRESQL','GAUSS']:type==='POSTGRESQL'?['POSTGRESQL','MYSQL']:type==='ORACLE'?['POSTGRESQL']:null;
 for(const option of select.options)option.disabled=Boolean(allowed&&!allowed.includes(option.value));
 if(!select.value||select.selectedOptions[0]?.disabled){
  select.value='POSTGRESQL';
  const url=card?card.querySelector('[data-field=url]'):f.targetUrl;
  url.value=defaultUrl(select.value);
 }
 const product=select.value,messaging=v3()&&messagingProduct(product),doris=v3()&&product==='DORIS',hive=v3()&&product==='HIVE',dm=v3()&&product==='DM',root=card?card.querySelector('.messaging-settings'):$('primary-messaging');
 if(!root.children.length){root.append(text('p',t('messageHelp'),'callout'),makeInput('messageStream','stream','RIVOMESH'),makeInput('messageTopic','topic','rivomesh.events'),makeInput('pulsarAdmin','adminUrl','http://127.0.0.1:8080'),makeInput('pulsarCluster','cluster','standalone'),makeInput('rocketBroker','broker','broker-a'),makeInput('rocketRetention','minimumRetentionHours','87600','number'),makeSelect('kafkaSecurity','securityProtocol',['PLAINTEXT','SSL','SASL_PLAINTEXT','SASL_SSL']),makeSelect('kafkaMechanism','saslMechanism',['PLAIN','SCRAM-SHA-256','SCRAM-SHA-512']),makeInput('messageToken','token','','password'),makeInput('messageReplicas','replicas','1','number'),makeInput('messageMaxKiB','maxKiB','1024','number'),makeInput('dorisLoadUrl','dorisLoadUrl','http://127.0.0.1:8040'),makeInput('dorisRedirectOrigins','dorisRedirectOrigins',''),makeInput('dorisReplicas','dorisReplicas','3','number'),makeInput('dorisBuckets','dorisBuckets','4','number'),makeSelect('dorisIndexes','dorisIndexes',['INVERTED','NONE']),makeInput('dmAcknowledgement','dmAcknowledgement','','checkbox'),makeInput('hiveAcknowledgement','hiveAcknowledgement','','checkbox'),makeInput('hiveTimeout','hiveTimeout','300','number'));root.querySelector('[data-field=hiveTimeout]').min=1;root.querySelector('[data-field=hiveTimeout]').max=1800;root.querySelector('[data-field=dorisRedirectOrigins]').required=false;root.querySelector('[data-field=dorisReplicas]').min=1;root.querySelector('[data-field=dorisReplicas]').max=5;root.querySelector('[data-field=dorisBuckets]').min=1;root.querySelector('[data-field=dorisBuckets]').max=64;root.querySelector('[data-field=minimumRetentionHours]').min=1;root.querySelector('[data-field=minimumRetentionHours]').max=2147483647;root.querySelector('[data-field=replicas]').min=1;root.querySelector('[data-field=replicas]').max=5;root.querySelector('[data-field=maxKiB]').min=1;root.querySelector('[data-field=maxKiB]').max=16384;}
 const username=card?card.querySelector('[data-field=username]'):f.targetUsername,password=card?card.querySelector('[data-field=password]'):f.targetPassword,metadata=card?card.querySelector('[data-field=metadataSchema]'):f.metadataSchema;
 if(root.dataset.product&&root.dataset.product!==product){username.value='';password.value='';root.querySelector('[data-field=token]').value='';if(product==='ROCKETMQ')root.querySelector('[data-field=cluster]').value='BusinessCluster';if(product==='PULSAR')root.querySelector('[data-field=cluster]').value='standalone';if(product==='ACTIVEMQ_CLASSIC'){root.querySelector('[data-field=broker]').value='rivomesh-core';root.querySelector('[data-field=stream]').value='rivomesh';root.querySelector('[data-field=adminUrl]').value='http://127.0.0.1:8161/api/jolokia/';}if(product==='ACTIVEMQ_ARTEMIS'){root.querySelector('[data-field=broker]').value='';root.querySelector('[data-field=stream]').value='rivomesh';}}
 const streamCaption=root.querySelector('[data-field=stream]').previousElementSibling;streamCaption.dataset.i18n=['ACTIVEMQ_ARTEMIS','ACTIVEMQ_CLASSIC'].includes(product)?'artemisQueuePrefix':'messageStream';streamCaption.textContent=t(streamCaption.dataset.i18n);const brokerCaption=root.querySelector('[data-field=broker]').previousElementSibling;brokerCaption.dataset.i18n=product==='ACTIVEMQ_CLASSIC'?'classicBroker':product==='ACTIVEMQ_ARTEMIS'?'artemisBrokerName':'rocketBroker';brokerCaption.textContent=t(brokerCaption.dataset.i18n);const topicCaption=root.querySelector('[data-field=topic]').previousElementSibling;topicCaption.dataset.i18n=product==='ROCKETMQ'?'rocketTopicPrefix':'messageTopic';topicCaption.textContent=t(topicCaption.dataset.i18n);const clusterCaption=root.querySelector('[data-field=cluster]').previousElementSibling;clusterCaption.dataset.i18n=product==='ROCKETMQ'?'rocketCluster':'pulsarCluster';clusterCaption.textContent=t(clusterCaption.dataset.i18n);const copies=root.querySelector('[data-field=replicas]').previousElementSibling;copies.dataset.i18n=product==='ACTIVEMQ_ARTEMIS'?'artemisCopies':product==='PULSAR'?'pulsarCopies':'messageReplicas';copies.textContent=t(copies.dataset.i18n);root.dataset.product=product;const topic=root.querySelector('[data-field=topic]');if(product==='PULSAR'&&!topic.value.startsWith('persistent://'))topic.value='persistent://public/default/rivomesh_events';if(product==='KAFKA'&&topic.value.startsWith('persistent://'))topic.value='rivomesh.events';if(product==='ROCKETMQ'&&!/^[A-Za-z][A-Za-z0-9_-]{0,47}$/.test(topic.value))topic.value='rivomesh_events';root.querySelector('[data-field=replicas]').max=['ROCKETMQ','ACTIVEMQ_ARTEMIS','ACTIVEMQ_CLASSIC'].includes(product)?1:5;if(['ROCKETMQ','ACTIVEMQ_ARTEMIS','ACTIVEMQ_CLASSIC'].includes(product))root.querySelector('[data-field=replicas]').value='1';
 const adminCaption=root.querySelector('[data-field=adminUrl]').previousElementSibling;adminCaption.dataset.i18n=product==='ACTIVEMQ_CLASSIC'?'classicAdmin':'pulsarAdmin';adminCaption.textContent=t(adminCaption.dataset.i18n);const help=root.querySelector('p');help.dataset.i18n=product==='DM'?'dmHelp':product==='GBASE_8C'?'gbase8cHelp':product==='KINGBASE'?'kingbaseHelp':product==='HIVE'?'hiveHelp':product==='OCEANBASE'?'oceanBaseHelp':product==='DORIS'?'dorisHelp':product==='CLICKHOUSE'?'clickHouseHelp':product==='ACTIVEMQ_CLASSIC'?'classicHelp':product==='ACTIVEMQ_ARTEMIS'?'artemisHelp':product==='ROCKETMQ'?'rocketHelp':'messageHelp';help.textContent=t(help.dataset.i18n);root.classList.toggle('hidden',!messaging&&!doris&&product!=='CLICKHOUSE'&&product!=='OCEANBASE'&&product!=='KINGBASE'&&product!=='GBASE_8C'&&!hive&&!dm);root.querySelectorAll('input,select').forEach(input=>{
   const field=input.dataset.field;let visible=messaging;
   if(field==='stream')visible=['NATS_JETSTREAM','REDIS_STREAMS','ACTIVEMQ_ARTEMIS','ACTIVEMQ_CLASSIC'].includes(product);
   if(field==='token')visible=['NATS_JETSTREAM','PULSAR'].includes(product);
   if(field==='topic')visible=['KAFKA','PULSAR','ROCKETMQ'].includes(product);
   if(field==='adminUrl')visible=['PULSAR','ACTIVEMQ_CLASSIC'].includes(product);if(field==='cluster')visible=['PULSAR','ROCKETMQ'].includes(product);if(field==='broker'){visible=['ROCKETMQ','ACTIVEMQ_ARTEMIS','ACTIVEMQ_CLASSIC'].includes(product);input.required=['ROCKETMQ','ACTIVEMQ_CLASSIC'].includes(product);}if(field==='minimumRetentionHours')visible=product==='ROCKETMQ';
   if(field==='securityProtocol')visible=product==='KAFKA';
   if(field==='saslMechanism')visible=product==='KAFKA'&&root.querySelector('[data-field=securityProtocol]').value.startsWith('SASL_');
   if(field.startsWith('doris'))visible=doris;if(field.startsWith('hive'))visible=hive;if(field==='dmAcknowledgement')visible=dm;if(field==='maxKiB')visible=messaging||doris||hive;input.disabled=!(messaging||doris||hive||dm)||!visible;input.parentElement.classList.toggle('hidden',!visible);
 });
 const sasl=product==='KAFKA'&&root.querySelector('[data-field=securityProtocol]').value.startsWith('SASL_');username.required=!['NATS_JETSTREAM','KAFKA','REDIS_STREAMS','PULSAR','ROCKETMQ'].includes(product)||sasl;password.required=sasl;
 for(const credential of [username,password]){credential.disabled=['PULSAR','ROCKETMQ'].includes(product);credential.parentElement.classList.toggle('hidden',['PULSAR','ROCKETMQ'].includes(product));if(['PULSAR','ROCKETMQ'].includes(product))credential.value='';}
 root.querySelector('[data-field=securityProtocol]').onchange=()=>targetMode(card);metadata.disabled=messaging;metadata.parentElement.classList.toggle('hidden',messaging);
}

function maskPlan(value){if(Array.isArray(value))return value.map(maskPlan);if(value&&typeof value==='object'){const result={};Object.entries(value).forEach(([key,item])=>result[key]=/password|token|secret/i.test(key)?'••••':/url$/i.test(key)&&typeof item==='string'?item.replace(/\/\/[^/@]+@/,'//••••@').replace(/([?&](?:password|sslpassword|token|access_token|secret)=)[^&]*/ig,'$1••••'):maskPlan(item));return result;}return value;}
function validateStep(index){const f=$('create-form').elements;
 if(index===0){
  const type=wizardSource();
  if(type==='ORACLE'){
   const tables=oracleTables(),valid=tables.length>0&&tables.length<=200&&new Set(tables).size===tables.length&&tables.every(name=>/^[A-Z][A-Z0-9_]{0,127}\.[A-Z][A-Z0-9_]{0,127}$/.test(name));
   f.oracleTables.setCustomValidity(valid?'':t('oracleTableListInvalid'));
   for(const field of [f.oracleRootUrl,f.oraclePdbUrl])field.setCustomValidity(field.value.trim().startsWith('jdbc:oracle:thin:@//')?'':t('sourceJdbcInvalid'));
  }else{
   const names=type==='POSTGRESQL'?f.pgSchemas:f.sourceDatabase,parts=names.value.split(',').map(name=>name.trim());
   names.setCustomValidity(parts.some(name=>!name)||new Set(parts).size!==parts.length||!v3()&&parts.length!==1?t('databaseListInvalid'):'');
   if(type==='POSTGRESQL')f.pgUrl.setCustomValidity(f.pgUrl.value.trim().startsWith('jdbc:postgresql://')?'':t('sourceJdbcInvalid'));
  }
 }
 if(index===1){const ids=[f.targetId,...document.querySelectorAll('#extra-targets [data-field=id]')];ids.forEach(input=>input.setCustomValidity(''));if(v3()){const seen=new Set();ids.forEach(input=>{const value=input.value.trim();if(seen.has(value))input.setCustomValidity(t('targetIdDuplicate'));seen.add(value);});}}
 for(const field of $('step-'+index).querySelectorAll('input,select,textarea'))if(!field.reportValidity())return false;return true;}
function renderFailureDiagnostic(card,sub){
 const phases=['TARGET_ACTIVATE','TARGET_PREPARE','TARGET_SNAPSHOT','JOURNAL_READ','NORMALIZE','PROJECT','RECONCILE','TARGET_APPLY','JOURNAL_ACK'],phase=phases.includes(sub.failurePhase)?sub.failurePhase:'UNKNOWN';
 card.append(text('p',t('failurePhase')+': '+t('failurePhase_'+phase),'subtle'));
 if(/^[1-9][0-9]*$/.test(String(sub.failedSequence||'')))card.append(text('p',t('failedSequence')+': '+sub.failedSequence,'subtle'));
 const hint=['TARGET_ACTIVATE','TARGET_PREPARE','TARGET_SNAPSHOT','TARGET_APPLY','RECONCILE','PROJECT'].includes(phase)?phase:'UNKNOWN';card.append(text('p',t('recoveryHint_'+hint),'callout'));
 if(sub.failureDiagnosticProblem)card.append(text('p',t('FAILURE_DIAGNOSTIC_UNAVAILABLE'),'error'));
}
Object.assign(messages.zh,{
 deliveryTiming:'上次提交至目标确认',deliveryTimingHistorical:'历史确认样本',
 deliveryTimingUnavailable:'暂无可核验样本',deliveryTimingClockSkew:'源库和 Worker 的时钟存在偏差，请检查时间同步',
 deliveryTimingMilliseconds:'毫秒',deliveryTimingSequence:'事务序号'
});
Object.assign(messages.en,{
 deliveryTiming:'Last commit-to-confirmation delay',deliveryTimingHistorical:'Historical confirmation sample',
 deliveryTimingUnavailable:'No verifiable sample yet',deliveryTimingClockSkew:'Check clock synchronization between the source and Worker',
 deliveryTimingMilliseconds:'ms',deliveryTimingSequence:'Transaction sequence'
});
function renderDeliveryTiming(card,task,sub){
 const sample=sub.lastDeliveryTiming;
 if(!sample||!task.statusAvailable)return;
 const historical=task.workerState!=='RUNNING'||task.status.run_state!=='RUNNING'
  ||sub.state!=='RUNNING'||sub.errorCode||sub.pendingTransactions!=='0';
 const caption=t(historical?'deliveryTimingHistorical':'deliveryTiming');
 let detail=t('deliveryTimingUnavailable');
 if(sample.state==='CLOCK_SKEW')detail=t('deliveryTimingClockSkew');
 if(sample.state==='OBSERVED'&&typeof sample.millis==='string'&&/^(0|[1-9][0-9]{0,15})$/.test(sample.millis)
  &&typeof sample.sequence==='string'&&/^[1-9][0-9]{0,18}$/.test(sample.sequence)){
  const observed=new Date(sample.targetCommitObservedAt);
  if(Number.isFinite(observed.getTime()))
   detail=sample.millis+' '+t('deliveryTimingMilliseconds')+' · '+t('deliveryTimingSequence')+' '+sample.sequence
    +' · '+observed.toLocaleString();
 }
 const label=text('p',caption+' · '+detail,'subtle');label.dataset.deliveryTiming='';card.append(label);
}
function renderSubscriptions(root,task,pending){
 const subscriptions=task.status.subscriptions||[];
 if(!subscriptions.length)return;
 const list=text('section','','subscription-list');list.append(text('h2',t('subscriptions')));
 if(!subscriptions.some(sub=>sub.id===selectedDestination))selectedDestination='';
 if(subscriptions.length>1){
  const label=text('label'),select=text('select');select.id='destination-filter';
  label.append(text('span',t('targetInstances')),select);
  const all=text('option',t('allTargets'));all.value='';select.append(all);
  for(const sub of subscriptions){const option=text('option',sub.id);option.value=sub.id;select.append(option);}
  select.value=selectedDestination;
  select.onchange=()=>{selectedDestination=select.value;renderDetail(task);$('destination-filter')?.focus();};
  list.append(label);
 }
 subscriptions.filter(sub=>!selectedDestination||sub.id===selectedDestination).forEach(sub=>{
  const card=text('article','','subscription-card'),title=text('div');
  title.append(text('strong',sub.id),badge(sub.state==='RUNNING'&&sub.errorCode?'RETRYING':sub.state));card.append(title);
  const destination=task.destinations?.find(output=>output.id===sub.id);
  if(destination){
   card.append(text('p',destination.product+' · '+t(destination.consistency),'subtle'));
   if(destination.resource)card.append(text('p',t('physicalResource')+': '+destination.resource));
   Object.entries(destination.mappings||{}).forEach(([source,target])=>card.append(text('p',source+' → '+target,'subtle')));
  }
  if(sub.safeCoveredSequence!=null){
   card.append(text('p',t('safeCoveredSequence')+': '+sub.safeCoveredSequence+' / '+(task.status.captured_sequence??'—')),
    text('p',t(sub.progressSemantics==='SQL_COMMITTED'?'contiguousAppliedSequence':'contiguousDeliveredSequence')+': '+sub.contiguousDeliveredSequence),
    text('p',t('progress_'+sub.progressSemantics),'subtle'));
   if(sub.firstSkippedSequence!=null)card.append(text('p',t('firstSkippedSequence')+': '+sub.firstSkippedSequence,'subtle'));
   if(sub.firstSourceExclusionAfterSequence!=null)card.append(text('p',t('firstSourceExclusion')+' · '+sub.firstSourceExclusionAfterSequence,'error'));
  }else card.append(text('p',t('acknowledged')+': '+sub.acknowledged+' / '+(task.status.lastSequence??'—')));
  if(task.statusAvailable&&typeof sub.pendingTransactions==='string'&&/^(0|[1-9][0-9]{0,18})$/.test(sub.pendingTransactions))
   card.append(text('p',t('targetBacklog')+' · '+sub.pendingTransactions,'subtle'));
  renderDeliveryTiming(card,task,sub);
  if(Number(task.status.table_count)>0)
   card.append(text('p',t('snapshotAcknowledged')+': '+(task.status.snapshotAcknowledgments?.[sub.id]||0)+' / '+task.status.table_count));
  if(sub.errorCode)card.append(text('p',explain(sub.errorCode),sub.state==='PAUSED'?'error':'subtle'));
  if(sub.state==='PAUSED'){
   renderFailureDiagnostic(card,sub);renderFailureActions(card,task,sub,pending);
   if(destination?.product==='KAFKA'&&task.formatVersion===3&&task.statusAvailable
    &&task.desired==='PAUSED'&&task.status.run_state==='PAUSED'&&sub.failurePhase==='TARGET_APPLY'
    &&session?.taskControlScope==='GLOBAL'&&session.recoveryReview&&can('TASK_CONFIG')
    &&Number.isSafeInteger(sub.errorGeneration)&&sub.errorGeneration>=0){
    const review=text('button',t('review_kafka_receipts'),'quiet');
    review.dataset.kafkaReceiptReview=sub.id;review.disabled=pending;
    review.onclick=()=>{review.disabled=true;command(task,'REVIEW_KAFKA_RECEIPTS',sub);};
    card.append(review);
   }
   const button=text('button',t('resume_target'));button.disabled=!can('TASK_CONTROL')||pending;
   button.onclick=()=>command(task,'RESUME_TARGET',sub);card.append(button);
  }
  list.append(card);
 });
 root.append(list);
}
$('create-form').elements.formatVersion.onchange=wizardMode;$('create-form').elements.journalMode.onchange=journalMode;$('add-target').onclick=addTarget;
$('create-form').elements.targetType.onchange=()=>{$('create-form').elements.targetUrl.value=defaultUrl($('create-form').elements.targetType.value);targetMode();};
$('create-form').elements.sourceType.onchange=wizardMode;
for(const name of ['sourceDatabase','pgSchemas','pgUrl','oracleTables','oracleRootUrl','oraclePdbUrl'])
 $('create-form').elements[name].oninput=e=>e.target.setCustomValidity('');


Object.assign(messages.zh,{targetInstances:'目标实例',journalUsageHelp:'逻辑负载包含日志、快照、无主键身份和标准化预留；物理磁盘、索引及 WAL 空间另外计算。',DDL_PLAN_REQUIRED:'源端遇到 DDL，需要结构演进计划；普通恢复不会跳过它。',DESTINATION_ALREADY_OWNED:'目标表已被其他任务或目标订阅占用。请检查地址别名和映射，先完成迁移计划。',TARGET_TABLE_MISSING_REBUILD_REQUIRED:'已同步目标表丢失，需要重建计划，不能从旧断点自动建空表继续。',TARGET_SNAPSHOT_INCOMPLETE:'目标全量尚未完成。',INDEX_CONVERSION_REQUIRES_PLAN:'该索引需要单独转换计划，未静默省略。'});
Object.assign(messages.en,{targetInstances:'Target instances',journalUsageHelp:'Logical payload includes events, snapshots, keyless identities and normalization reservations. Physical disk, indexes and WAL need additional space.',DDL_PLAN_REQUIRED:'Source DDL needs a schema-evolution plan; ordinary resume does not skip it.',DESTINATION_ALREADY_OWNED:'Another pipeline/subscription owns this target table. Check URL aliases/mappings and create a migration plan.',TARGET_TABLE_MISSING_REBUILD_REQUIRED:'An initialized target table is missing. A rebuild plan is required; resuming into an empty replacement is unsafe.',TARGET_SNAPSHOT_INCOMPLETE:'The target snapshot is not complete.',INDEX_CONVERSION_REQUIRES_PLAN:'This index needs a conversion plan; it was not silently omitted.'});
Object.assign(messages.zh,{TARGET_SNAPSHOT:'目标全量投递中',sourceSnapshotRows:'源快照行数',snapshotAcknowledged:'已确认源快照',sourceSnapshotHelp:'这里显示源快照准备进度；各目标的全量确认另列。消息确认不代表下游消费完成。',TRANSACTION_EXCEEDS_JOURNAL_QUOTA:'单个事务及标准化空间超过日志配额。停止 Worker、扩容并重载配置后，按当前错误版本恢复。源断点未跳过。',JOURNAL_WORKSPACE_REQUIRES_QUOTA_PLAN:'快照、身份或旧日志的标准化空间不足，需要扩容计划。不要删除日志或快照分片。',BROKER_IDENTITY_CHANGED:'连接到了不同的 Broker 集群，不能沿用原断点。请恢复正确入口或制定重建计划。',KAFKA_AUTO_CREATION_MUST_BE_DISABLED:'请在全部 Kafka Broker 关闭自动建 Topic，并允许读取该配置；服务仍可显式首次创建。'});
Object.assign(messages.en,{TARGET_SNAPSHOT:'Delivering snapshots',sourceSnapshotRows:'Source snapshot rows',snapshotAcknowledged:'Source snapshots confirmed',sourceSnapshotHelp:'This shows source snapshot preparation. Each destination reports its own confirmations; message confirmation is not consumer completion.',TRANSACTION_EXCEEDS_JOURNAL_QUOTA:'One transaction and its workspace exceed the journal quota. Stop workers, expand/reload configuration, and resume the current error generation. The source position was not skipped.',JOURNAL_WORKSPACE_REQUIRES_QUOTA_PLAN:'Snapshot, identity or old-log normalization needs a capacity plan. Do not delete journal/snapshot parts.',BROKER_IDENTITY_CHANGED:'A different broker cluster was reached. Restore the correct endpoint or create a rebuild plan before reusing checkpoints.',KAFKA_AUTO_CREATION_MUST_BE_DISABLED:'Disable automatic topic creation on every Kafka broker and allow reading that configuration. Explicit first-time creation remains available.'});
$('task-filter').addEventListener('submit',event=>{
 event.preventDefault();
 const fields=event.currentTarget.elements;
 applyTaskFilters({ownerNodeId:fields.ownerNodeId.value.trim(),taskId:fields.taskId.value.trim()});
});
$('clear-task-filter').onclick=()=>{
 if(taskLoading)return;
 $('task-filter').reset();applyTaskFilters({});
};
$('nodes-nav').onclick=()=>{if(!can('ACCOUNT_ADMIN'))return;view('nodes');loadNodes();};$('nodes-reload').onclick=()=>loadNodes('');$('nodes-next').onclick=()=>{if(nodePage?.next)loadNodes(nodePage.next);};$('tasks-nav').onclick=()=>view('tasks');$('adapters-nav').onclick=()=>view('adapters');
translate();
restoreSession();
const oidcEntry=$('login-oidc');
if(oidcEntry)fetch('/auth/oidc/available',{credentials:'omit'})
 .then(response=>response.ok?response.json():null)
 .then(profile=>oidcEntry.classList.toggle('hidden',profile?.enabled!==true))
 .catch(()=>{});
setInterval(pollConsole,3000);
document.addEventListener('visibilitychange',pollConsole);

$('import-task').onclick=()=>{if(can('TASK_CREATE'))$('import-file').click();};
$('import-file').addEventListener('change',async e=>{const file=e.target.files[0];if(!file||!can('TASK_CREATE'))return;const generation=sessionGeneration;try{if(file.size>512*1024)throw new Error(lang==='zh'?'配置文件不能超过 512 KiB':'Configuration exceeds 512 KiB');const placement=ownerSelection(),yaml=await file.text();if(generation!==sessionGeneration||!can('TASK_CREATE'))return;const task=await api('/tasks',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':requestId()},body:JSON.stringify({yaml,...placement})});selected=task.id;clearTaskPages();notify(t(globalTasks()?'accepted':'created'));await refresh();}catch(error){if(generation===sessionGeneration)notify(error.message);}finally{e.target.value='';}});
for(const [source,target] of [['task-auto-placement','create-auto-placement'],['create-auto-placement','task-auto-placement']]){
 $(source).onchange=()=>{$(target).checked=$(source).checked;renderOwners();};
}
$('task-owner').addEventListener('input',()=>{$('create-owner').value=$('task-owner').value;});$('create-owner').addEventListener('input',()=>{$('task-owner').value=$('create-owner').value;});$('owners-reload').onclick=()=>loadOwners('');$('owners-more').onclick=()=>{if(ownerNext)loadOwners(ownerNext);};$('tasks-first').onclick=()=>taskPage('first');$('tasks-previous').onclick=()=>taskPage('previous');$('tasks-next').onclick=()=>taskPage('next');

Object.assign(messages.zh,{TARGET_CONNECTION_RETRY:'目标连接暂不可用，保留断点并自动重试；无需手动恢复。'});
Object.assign(messages.en,{TARGET_CONNECTION_RETRY:'Destination connection is temporarily unavailable. Checkpoints are retained and retry is automatic.'});

Object.assign(messages.zh,{messageTopic:'专用 Topic',messageToken:'访问令牌（NATS / Pulsar，可选）',pulsarAdmin:'Pulsar 管理 API 地址',pulsarCluster:'Pulsar 集群名称',pulsarCopies:'BookKeeper 最少确认副本数',PULSAR_TOPIC_CONFIGURATION_REQUIRES_PLAN:'Pulsar 持久化、副本、过期或保留策略不符，需要配置计划。',PULSAR_AUTO_CREATION_MUST_BE_DISABLED:'请在所有 Broker 和命名空间关闭自动创建 Topic，避免历史丢失后被误建空。'});
Object.assign(messages.en,{messageTopic:'Dedicated topic',messageToken:'NATS / Pulsar token (optional)',pulsarAdmin:'Pulsar admin API URL',pulsarCluster:'Pulsar cluster name',pulsarCopies:'Minimum BookKeeper ACK copies',PULSAR_TOPIC_CONFIGURATION_REQUIRES_PLAN:'Pulsar persistence, copies, expiration or retention differs; review the configuration.',PULSAR_AUTO_CREATION_MUST_BE_DISABLED:'Disable topic auto-creation on every broker and namespace to prevent empty recreation after loss.'});

Object.assign(messages.zh,{physicalResource:'物理 Topic / Stream',TRANSACTIONAL:'数据与断点在本目标事务中提交',AT_LEAST_ONCE_EVENTS:'确认后至少一次；下游需去重并确认业务完成'});
Object.assign(messages.en,{physicalResource:'Physical topic / stream',TRANSACTIONAL:'Data and checkpoint commit in this destination transaction',AT_LEAST_ONCE_EVENTS:'Confirmed at-least-once; downstream deduplication and business completion required'});

Object.assign(messages.zh,{rocketBroker:'RocketMQ Broker 名称',rocketCluster:'RocketMQ 集群名称',rocketTopicPrefix:'专用 Topic 前缀（自动追加任务代次标识）',rocketRetention:'要求的最短历史保留小时数',ROCKETMQ_SYNCHRONOUS_DISK_REQUIRED:'Broker 必须同步刷盘并关闭暂存池。',ROCKETMQ_RETENTION_REQUIRES_PLAN:'历史保留或强制清理配置不符合要求，请检查恢复计划。'});
Object.assign(messages.en,{rocketBroker:'RocketMQ broker name',rocketCluster:'RocketMQ cluster name',rocketTopicPrefix:'Dedicated topic prefix (generation suffix added)',rocketRetention:'Minimum required retention hours',ROCKETMQ_SYNCHRONOUS_DISK_REQUIRED:'Broker must use synchronous disk flush and disable the transient pool.',ROCKETMQ_RETENTION_REQUIRES_PLAN:'Retention or forced cleanup does not meet the recovery plan.'});

Object.assign(messages.zh,{rocketHelp:'RocketMQ 首批配置仅支持可信网络内的原生 TCP、单 Broker 组、本地一份同步刷盘副本；认证、TLS 和复制配置尚未启用。至少一次投递，下游必须去重。'});
Object.assign(messages.en,{rocketHelp:'Initial RocketMQ profile: trusted-network native TCP, one broker group, one local synchronous disk copy. Authentication, TLS and replicated profiles are not enabled. At-least-once delivery requires consumer deduplication.'});

Object.assign(messages.zh,{ROLE_VIEWER:'只读查看',ROLE_OPERATOR:'任务运维',ROLE_ADMIN:'管理员',FORBIDDEN:'当前账号没有执行此操作的权限。'});
Object.assign(messages.en,{ROLE_VIEWER:'Viewer',ROLE_OPERATOR:'Operator',ROLE_ADMIN:'Administrator',FORBIDDEN:'Your account does not have permission for this operation.'});

Object.assign(messages.zh,{SESSION_CHANGED:'登录状态已改变，已忽略旧请求。'});Object.assign(messages.en,{SESSION_CHANGED:'Session changed; the previous request was ignored.'});

Object.assign(messages.zh,{REQUEST_IDENTITY_CHANGED:'该请求标识属于另一身份或角色，请刷新并使用新的请求标识。'});Object.assign(messages.en,{REQUEST_IDENTITY_CHANGED:'This request ID belongs to a different identity or role; refresh and use a new request ID.'});

Object.assign(messages.zh,{createdBy:'配置创建者'});Object.assign(messages.en,{createdBy:'Configuration created by'});

Object.assign(messages.zh,{PULSAR_ADMIN_TIMEOUT:'Pulsar 管理请求在客户端超时，请检查网络、Broker 负载和资源；这不表示收到 HTTP 500。',PULSAR_ADMIN_CONNECT_RETRY:'Pulsar 管理连接未建立，正在重试，请检查地址、网络和服务状态。'});
Object.assign(messages.en,{PULSAR_ADMIN_TIMEOUT:'Pulsar admin request timed out at the client. Check network, broker load and resources; this does not imply an HTTP 500 response.',PULSAR_ADMIN_CONNECT_RETRY:'Pulsar admin connection failed and is retrying. Check endpoint, network and service state.'});
Object.assign(messages.zh,{PUBLICATION_TRANSPORT_UPGRADE_REQUIRES_PLAN:'已投递目标缺少原生确认回执，需要迁移或重建计划；不能清空回执后继续。',ROCKETMQ_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:'已确认消息不存在，请核验目标备份/保留策略并制定重建计划。',ROCKETMQ_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED:'已确认消息的位置或内容改变，目标可能回退或被替换，需要重建核验。',ROCKETMQ_PROGRESS_INVALID:'原生回执格式或目标绑定不符，请核验版本和元数据。',PUBLICATION_TRANSPORT_PROGRESS_CHANGED:'原生回执发生并发改变，请核验 Worker 租约和元数据。'});
Object.assign(messages.en,{PUBLICATION_TRANSPORT_UPGRADE_REQUIRES_PLAN:'An established destination lacks native confirmation receipts. A migration/rebuild plan is required; do not clear receipts to continue.',ROCKETMQ_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:'A confirmed message is missing. Verify backup/retention and create a rebuild plan.',ROCKETMQ_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED:'Confirmed message position or content changed. Verify destination rollback/replacement before rebuilding.',ROCKETMQ_PROGRESS_INVALID:'Native receipt format or resource binding differs; verify version and metadata.',PUBLICATION_TRANSPORT_PROGRESS_CHANGED:'Native receipts changed concurrently; verify worker leases and metadata.'});

Object.assign(messages.zh,{artemisQueuePrefix:'保留历史队列前缀（自动追加任务代次标识）',artemisBrokerName:'Broker 名称（可选校验）',artemisCopies:'本地同步存储副本数',artemisHelp:'首批 Artemis 配置：单入口 Core 协议、持久非破坏队列、同步提交；消费确认仍保留历史。需要管理权限并使用临时管理回复通道。下游需去重并按协议核对完成；集群/TLS 尚待验收。',ARTEMIS_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:'最后确认消息缺失，目标可能还原了旧备份；请核验并制定重建计划。',ARTEMIS_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED:'最后确认消息内容改变，请核验目标历史并制定重建计划。',ARTEMIS_PROGRESS_INVALID:'Artemis 原生回执格式或队列绑定不符，请核验元数据。',ARTEMIS_QUEUE_CONFIGURATION_REQUIRES_PLAN:'队列持久化、非破坏、路由或保留设置不符，需要配置/重建计划。',ARTEMIS_SECURITY_EXCEPTION:'Artemis 认证或权限被拒绝，请检查目标账号及临时管理通道权限。',ARTEMIS_AUTOMATIC_RESOURCE_CHANGES_MUST_BE_DISABLED:'请关闭业务地址的自动创建/删除，使用专用前缀策略。'});
Object.assign(messages.en,{artemisQueuePrefix:'Retained queue prefix (generation suffix added)',artemisBrokerName:'Broker name (optional check)',artemisCopies:'Local synchronous storage copies',artemisHelp:'Initial Artemis profile: single Core endpoint, persistent non-destructive queue and synchronous commit. Consumer ACK retains history. Management permission and a temporary reply channel are required. Consumers must deduplicate and verify completion; cluster/TLS acceptance is pending.',ARTEMIS_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:'The last confirmed record is missing. Verify an older backup/target history and create a rebuild plan.',ARTEMIS_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED:'The last confirmed record changed. Verify target history before rebuilding.',ARTEMIS_PROGRESS_INVALID:'Artemis native receipt format or queue binding differs; verify metadata.',ARTEMIS_QUEUE_CONFIGURATION_REQUIRES_PLAN:'Queue durability, non-destructive mode, routing or retention differs; review a configuration/rebuild plan.',ARTEMIS_SECURITY_EXCEPTION:'Artemis rejected authentication or permissions; check credentials and temporary management-channel grants.',ARTEMIS_AUTOMATIC_RESOURCE_CHANGES_MUST_BE_DISABLED:'Disable business-address auto-creation/deletion using a dedicated prefix policy.'});

Object.assign(messages.zh,{VERSIONED_MATERIALIZATION:'版本化数据物化',UNCONFIRMED_IN_FLIGHT_MATERIALIZATION:'暂停不能撤回已进入目标的物化请求；业务接管前需要确认在途请求已结束。',operationDetails:'检查结果与操作详情',CLICKHOUSE_INDEX_CONVERSION_REQUIRES_PLAN:'源索引不能安全自动转换；请展开检查结果中的索引计划。可选择明确省略索引的 NONE 模式，新旧任务切换模式需要迁移计划。',CLICKHOUSE_FINAL_INDEX_SAFETY_UNSUPPORTED:'ClickHouse 缺少 FINAL 精确索引模式；请使用已验收版本或制定兼容计划。',CLICKHOUSE_DATA_DEFINITION_CHANGED:'ClickHouse 物理表或索引与预期布局不一致，已停止确认。',metadataSchema:'元数据 schema / database',clickHouseHelp:'ClickHouse 使用版本化物化和 FINAL 查询视图，事务内各行可能分批可见。JSON/日期时间保留为无损文本，业务 UNIQUE 不建约束；普通 BTREE/UNIQUE/GSI 转为非唯一跳数索引，检查结果列出差异。目标仅由同步服务写。恢复会核对原生回执，不能用空表替换旧断点。',CLICKHOUSE_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:'ClickHouse 已确认回执缺失，可能恢复了旧备份；请核验并制定重建计划。',CLICKHOUSE_RESOURCE_ID_CHANGED:'ClickHouse 对象身份改变，不能沿用原断点。',CLICKHOUSE_TABLE_DEFINITION_CHANGED:'ClickHouse 目标结构或查询视图改变，需要迁移/修复计划。',CLICKHOUSE_CONNECTION_RETRY:'ClickHouse 连接或响应中断，正在按持久断点重试。'});
Object.assign(messages.en,{VERSIONED_MATERIALIZATION:'Versioned materialization',UNCONFIRMED_IN_FLIGHT_MATERIALIZATION:'Pause cannot retract target requests already in flight; confirm quiescence before business takeover.',operationDetails:'Check results and operation details',CLICKHOUSE_INDEX_CONVERSION_REQUIRES_PLAN:'A source index needs an explicit conversion plan. Expand check results to review it. NONE explicitly omits indexes; switching an initialized task needs a migration plan.',CLICKHOUSE_FINAL_INDEX_SAFETY_UNSUPPORTED:'ClickHouse lacks FINAL exact index mode. Use an accepted version or create a compatibility plan.',CLICKHOUSE_DATA_DEFINITION_CHANGED:'ClickHouse physical table/indexes differ from the expected layout; confirmation stopped.',metadataSchema:'Metadata schema / database',clickHouseHelp:'ClickHouse uses versioned rows and FINAL query views. Rows within a transaction may become visible in separate batches. JSON/temporal values remain lossless text; business UNIQUE is not enforced. Plain BTREE/UNIQUE/GSI become non-unique skipping indexes; check results list differences. Only the sync service writes. Recovery verifies native receipts; empty replacement tables cannot reuse old checkpoints.',CLICKHOUSE_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:'A confirmed ClickHouse receipt is missing, possibly after an older backup. Verify history and create a rebuild plan.',CLICKHOUSE_RESOURCE_ID_CHANGED:'ClickHouse object identity changed; the old checkpoint cannot be reused.',CLICKHOUSE_TABLE_DEFINITION_CHANGED:'ClickHouse data or view definition changed; review a migration/repair plan.',CLICKHOUSE_CONNECTION_RETRY:'ClickHouse connection/response was interrupted; retrying from durable checkpoints.'});

Object.assign(messages.zh,{targetHelp:'选择目标产品并逐库显式映射，可添加多个目标实例。版本和验收范围见能力目录；高级连接设置可导入 YAML。',classicBroker:'Classic Broker 名称',classicAdmin:'Classic Jolokia 管理地址',classicHelp:"Classic 使用持久队列和独立保留回执。消费者必须使用选择器 rivomesh_record_type = 'EVENT'，按事件ID去重并按事务重组；禁止消费或清理 OWNER/回执。首次接入仅支持固定单Broker及KahaDB同步刷盘。",CLASSIC_CONSUMER_SELECTOR_REQUIRED:'消费者未使用规定的 EVENT 选择器，或正在消费回执队列；请修复后恢复此目标。',CLASSIC_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:'Classic 已确认回执缺失，可能回退了旧备份；需核验并制定重建计划。',CLASSIC_PUBLICATION_MANIFEST_MISSING_OR_CHANGED:'Classic 控制消息缺失或改变，不能沿旧断点创建空队列继续。'});
Object.assign(messages.en,{targetHelp:'Select products and explicitly map each database; add independent destinations. See the capability catalog for versions and acceptance scope. Import YAML for advanced connection settings.',classicBroker:'Classic broker name',classicAdmin:'Classic Jolokia URL',classicHelp:"Classic uses persistent event queues and separate retained receipts. Consumers must use selector rivomesh_record_type = 'EVENT', deduplicate event IDs and assemble transactions. Do not consume/clear OWNER or receipts. Initial profile: one fixed broker and synchronously flushed KahaDB.",CLASSIC_CONSUMER_SELECTOR_REQUIRED:'A consumer lacks the required EVENT selector or consumes receipts. Repair it before resuming this target.',CLASSIC_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:'A confirmed Classic receipt is missing, possibly after restoring an older backup. Verify history and create a rebuild plan.',CLASSIC_PUBLICATION_MANIFEST_MISSING_OR_CHANGED:'Classic ownership records are missing/changed. Empty queues cannot reuse old checkpoints.'});

window.RivomeshConfiguration={api,can,t,explain,notify,refresh,requestId,render,session:()=>session,reviewEvidence:(task,id)=>openFailureEvidence(task,id),tasks:()=>latest.tasks.map(task=>task.id),credential:signIn,register:(zh,en)=>{Object.assign(messages.zh,zh);Object.assign(messages.en,en);},task:id=>latest.tasks.find(task=>task.id===id)};

Object.assign(messages.zh,{taskScope:"限定任务数",TASK_NOT_FOUND:"任务不存在或当前账号无权访问。"});
Object.assign(messages.en,{taskScope:"Granted tasks",TASK_NOT_FOUND:"Task unavailable or not granted to this account."});

Object.assign(messages.zh,{noGrantedTasks:"暂无可见任务",noGrantedTasksHelp:"当前账号没有已加载的授权任务。管理员可配置任务授权并加载对应任务。"});
Object.assign(messages.en,{noGrantedTasks:"No granted pipelines",noGrantedTasksHelp:"This account has no loaded pipelines within its grants. An administrator can assign and load the required tasks."});

Object.assign(messages.zh,{dorisLoadUrl:"Stream Load HTTP 地址",dorisRedirectOrigins:"允许重定向的 HTTP 源地址（逗号分隔，可空）",dorisReplicas:"Doris 数据副本数",dorisBuckets:"每表分桶数",dorisIndexes:"次级索引策略",dorisHelp:"Doris 使用 Unique Key/MoW、源事件版本号及删除标记，事务可能分批可见。JSON/日期时间保留文本，二进制为base64；业务 UNIQUE/GSI 转为非唯一倒排索引，检查结果列出限制。仅同步服务写入；首批元数据身份校验需要 Doris ADMIN 权限。HTTP地址可直接使用BE，FE重定向需显式允许目标地址。默认3副本，单BE测试必须显式选择1。",DORIS_CONNECTION_RETRY:"Doris HTTP连接暂时中断，断点保留并自动重试。",DORIS_SQL_CONNECTION_RETRY:"Doris SQL入口暂不可用，正在重试。",DORIS_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:"Doris确认回执缺失，可能恢复了旧备份；需核验后制定重建计划。",DORIS_INDEX_CONVERSION_REQUIRES_PLAN:"Doris索引转换需要计划，请查看检查结果中的dorisIndexPlans。",DORIS_RESOURCE_ID_CHANGED:"Doris对象身份改变，不能复用原断点。",DORIS_REDIRECT_REJECTED:"Doris HTTP重定向目标未获允许，请核对同一集群的BE地址。"});
Object.assign(messages.en,{dorisLoadUrl:"Stream Load HTTP origin",dorisRedirectOrigins:"Allowed redirect origins (comma-separated, optional)",dorisReplicas:"Doris data replicas",dorisBuckets:"Buckets per table",dorisIndexes:"Secondary index policy",dorisHelp:"Doris uses Unique Key/MoW, event versions and retained tombstones; transactions can appear in separate batches. JSON/temporal values remain text and binary is base64. Business UNIQUE/GSI become nonunique inverted indexes; check results list limits. Only the sync service writes. Initial identity verification requires Doris ADMIN. Use BE HTTP directly, or explicitly allow FE redirect origins. Default3 replicas; a one-BE test must select1.",DORIS_CONNECTION_RETRY:"Doris HTTP connection was interrupted; retrying with retained checkpoints.",DORIS_SQL_CONNECTION_RETRY:"Doris SQL entry point is temporarily unavailable; retrying.",DORIS_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:"A confirmed Doris receipt is missing, possibly after an older backup. Verify history and plan a rebuild.",DORIS_INDEX_CONVERSION_REQUIRES_PLAN:"A Doris index needs a conversion plan; review dorisIndexPlans in check results.",DORIS_RESOURCE_ID_CHANGED:"Doris object identity changed; the previous checkpoint cannot be reused.",DORIS_REDIRECT_REJECTED:"Doris redirected to an unapproved origin. Verify BE addresses in the same cluster."});

Object.assign(messages.zh,{oceanBaseHelp:"OceanBase 首批支持 MySQL 兼容的业务租户，请使用 用户名@租户名。启动会核验产品、租户及强一致读取；自动创建映射数据库，不创建租户。业务 UNIQUE/GSI 可按策略转为普通索引，接管前需重建约束。Oracle 兼容模式尚未支持。",OCEANBASE_MYSQL_USER_TENANT_REQUIRED:"需要 OceanBase MySQL 兼容的业务租户，请核对目标地址、模式及用户名@租户名。"});
Object.assign(messages.en,{oceanBaseHelp:"The first OceanBase profile requires a MySQL-mode user tenant; use user@tenant. Startup verifies the vendor, tenant and strong reads. Mapped databases are created, tenants are not. Business UNIQUE/GSI can become ordinary indexes under the selected policy; restore constraints before takeover. Oracle mode is pending.",OCEANBASE_MYSQL_USER_TENANT_REQUIRED:"An OceanBase MySQL-mode user tenant is required. Check the endpoint, mode and user@tenant."});

Object.assign(messages.zh,{hiveAcknowledgement:"我已了解：Hive不创建普通BTREE/业务UNIQUE/GSI索引，接管业务前需单独评估",hiveTimeout:"Hive 单次操作超时（秒）",hiveHelp:"Hive 使用 ACID ORC 历史表和最新版本视图，跨表事务可能分批可见。38位以上小数、JSON、日期时间保留文本，二进制为base64。保留源行身份，但不创建业务索引或唯一约束。需要Hive事务管理器及持久仓库；首批支持直接二进制HiveServer2连接。服务端默认及所有读取会话必须关闭查询结果缓存（hive.query.results.cache.enabled=false），避免读取旧结果。此目标用于数仓，不自动当作OLTP接管库。",HIVE_INDEX_PLAN_REQUIRED:"请先确认Hive索引省略计划，业务索引不会自动创建。",HIVE_ACID_CONFIGURATION_REQUIRED:"Hive需要启用DbTxnManager和并发事务，请由数据库管理员配置后重试。",HIVE_CONNECTION_RETRY:"Hive连接暂时不可用，保留断点并重试。",HIVE_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:"Hive持久回执缺失，需核验仓库/元数据备份后制定重建计划。",HIVE_IDENTIFIER_PLAN_REQUIRED:"当前Hive目标名称需要转换计划，请使用兼容名称或先调整映射。"});
Object.assign(messages.en,{hiveAcknowledgement:"I understand: no ordinary BTREE/business UNIQUE/GSI indexes are created; application takeover needs separate assessment",hiveTimeout:"Hive operation timeout (seconds)",hiveHelp:"Hive uses ACID ORC history and latest-version views; cross-table transactions may appear in batches. Decimals wider than38 digits, JSON and temporal values remain text; binary is base64. Source row identity is retained, but business indexes/unique constraints are not created. Requires a transaction manager and durable warehouse. The first profile uses direct binary HiveServer2. Disable query-results cache on the server default and every reader (hive.query.results.cache.enabled=false) to avoid stale reads. This is a warehouse destination, not automatic OLTP takeover.",HIVE_INDEX_PLAN_REQUIRED:"Confirm the Hive index-omission plan first; business indexes are not automatically created.",HIVE_ACID_CONFIGURATION_REQUIRED:"Hive requires DbTxnManager and concurrency. Ask the database administrator to configure them before retrying.",HIVE_CONNECTION_RETRY:"Hive is temporarily unavailable; retaining progress and retrying.",HIVE_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED:"A durable Hive receipt is missing. Verify warehouse/metastore backups and plan a rebuild.",HIVE_IDENTIFIER_PLAN_REQUIRED:"This Hive name needs a conversion plan. Use a compatible name or adjust its mapping first."});

Object.assign(messages.zh,{HIVE_QUERY_CACHE_DISABLED_REQUIRED:"Hive查询结果缓存可能返回旧数据。请管理员将服务端默认和所有读取会话的 hive.query.results.cache.enabled 设为 false，再检查并恢复本目标；不要重置断点或据旧计数手工补数据。"});
Object.assign(messages.en,{HIVE_QUERY_CACHE_DISABLED_REQUIRED:"Hive query-results cache can return stale data. Ask the administrator to set hive.query.results.cache.enabled=false on the server default and every reader, then check and resume this destination. Do not reset checkpoints or add rows based on stale counts."});

Object.assign(messages.zh,{kingbaseHelp:'金仓当前为 PG 兼容模式实验适配。逐连接核验产品、database_mode、UTF8 和可写入口，保留空字符串并开启本地同步提交。其他兼容模式及 RAC/分布式场景需独立验收；业务 UNIQUE 可转普通索引，接管前恢复约束。',KINGBASE_PG_MODE_REQUIRED:'目标必须使用金仓 PG 兼容模式；其他模式需要单独的转换方案。',KINGBASE_PRODUCT_REQUIRED:'目标产品检查失败：请连接真实金仓数据库。',KINGBASE_UTF8_REQUIRED:'金仓目标数据库必须使用 UTF8 编码。',KINGBASE_WRITABLE_PRIMARY_REQUIRED:'金仓入口当前只读，等待可写主库恢复；不要修改同步断点。',KINGBASE_EMPTY_STRING_SEMANTICS_UNSUPPORTED:'目标把空字符串作为 NULL，需要兼容性处理，任务已暂停。'});
Object.assign(messages.en,{kingbaseHelp:'Experimental Kingbase PG-mode profile. Each connection checks product, database_mode, UTF8 and a writable endpoint, preserves empty strings and enables local synchronous commit. Other modes and RAC/distributed profiles need separate acceptance. Business UNIQUE may become ordinary indexes; restore constraints before takeover.',KINGBASE_PG_MODE_REQUIRED:'Kingbase PG mode is required; other modes need a separate conversion plan.',KINGBASE_PRODUCT_REQUIRED:'Product check failed: connect to a real Kingbase database.',KINGBASE_UTF8_REQUIRED:'The Kingbase destination requires UTF8 encoding.',KINGBASE_WRITABLE_PRIMARY_REQUIRED:'The Kingbase endpoint is read-only; waiting for a writable primary. Do not change checkpoints.',KINGBASE_EMPTY_STRING_SEMANTICS_UNSUPPORTED:'The destination treats empty strings as NULL; a compatibility plan is required.'});

Object.assign(messages.zh,{dmAcknowledgement:'我已了解：JSON保存为CLOB、超过38位小数保存为文本；接管前评估SQL类型语义',dmHelp:'达梦首批使用原生兼容模式0、UTF8、大小写敏感和BLANK_PAD_MODE=1，并要求提交等待日志刷盘。JSON和超38位小数按计划保留文本，不静默舍入；不自动修改实例参数。',DM_PRODUCT_REQUIRED:'目标必须为真实达梦数据库。',DM_NATIVE_MODE_REQUIRED:'达梦目标当前要求COMPATIBLE_MODE=0，其他模式需独立适配。',DM_UTF8_REQUIRED:'达梦目标要求UTF8编码（UNICODE_FLAG=1）。',DM_CASE_SENSITIVE_REQUIRED:'达梦目标要求大小写敏感，避免表名或行身份合并。',DM_BLANK_PAD_MODE_REQUIRED:'达梦目标要求BLANK_PAD_MODE=1；此初始化参数由DBA评估，服务不会修改。',DM_DURABLE_COMMIT_REQUIRED:'达梦COMMIT_WRITE必须为等待刷盘的WAIT模式，请DBA检查持久性配置。',DM_TEXT_CONVERSION_ACK_REQUIRED:'需要先确认JSON和超过38位小数的文本存储计划。',DM_LOB_INDEX_REQUIRES_PLAN:'达梦大字段不能直接沿用此索引，需要单独的类型/索引转换方案。'});
Object.assign(messages.en,{dmAcknowledgement:'I understand: JSON uses CLOB and decimals wider than38digits use text; review SQL semantics before takeover',dmHelp:'First DM profile: native mode0, UTF8, case-sensitive, BLANK_PAD_MODE=1 and commit WAIT for log flush. JSON/wide decimals retain planned text without silent rounding. Instance parameters are never changed automatically.',DM_PRODUCT_REQUIRED:'Connect to a real DM database.',DM_NATIVE_MODE_REQUIRED:'This DM profile requires COMPATIBLE_MODE=0; other modes need separate adaptation.',DM_UTF8_REQUIRED:'DM requires UTF8 (UNICODE_FLAG=1).',DM_CASE_SENSITIVE_REQUIRED:'DM requires case sensitivity to avoid merging identifiers or row identities.',DM_BLANK_PAD_MODE_REQUIRED:'DM requires BLANK_PAD_MODE=1. Have the DBA assess this initialization parameter; the service never changes it.',DM_DURABLE_COMMIT_REQUIRED:'DM COMMIT_WRITE must wait for log flush. Have the DBA check WAIT durability.',DM_TEXT_CONVERSION_ACK_REQUIRED:'Acknowledge the JSON/wide-decimal text storage plan first.',DM_LOB_INDEX_REQUIRES_PLAN:'This DM large-object index needs a separate type/index conversion plan.'});

Object.assign(messages.zh,{gbase8cHelp:'GBase 8c 当前为 PG/UTF8 实验适配。逐连接检查真实产品、兼容模式、可写入口和本地同步提交。8a、8s 是独立产品，不能使用本适配。分布式部署、其他模式和版本需单独验收；接管前恢复业务约束。',GBASE8C_PRODUCT_REQUIRED:'请连接真实 GBase 8c；8a、8s、PostgreSQL 和 openGauss 不能替代。',GBASE8C_PG_MODE_REQUIRED:'GBase 8c 当前适配要求 PG 兼容模式。',GBASE8C_UTF8_REQUIRED:'GBase 8c 目标数据库必须使用 UTF8 编码。',GBASE8C_WRITABLE_PRIMARY_REQUIRED:'GBase 8c 入口当前只读，等待可写主库恢复。',GBASE8C_EMPTY_STRING_SEMANTICS_UNSUPPORTED:'目标不能保留空字符串，需要兼容性处理。',GBASE8C_STRING_COMPARISON_REQUIRES_PLAN:'目标大小写或尾空格比较语义需要转换方案。'});
Object.assign(messages.en,{gbase8cHelp:'Experimental GBase 8c PG/UTF8 profile. Every connection verifies the actual product, compatibility mode, writable endpoint and local synchronous commit. 8a and 8s are separate products. Distributed deployments, other modes and versions require separate acceptance; restore business constraints before takeover.',GBASE8C_PRODUCT_REQUIRED:'Connect to actual GBase 8c; 8a/8s/PostgreSQL/openGauss are different products.',GBASE8C_PG_MODE_REQUIRED:'The current GBase 8c profile requires PG mode.',GBASE8C_UTF8_REQUIRED:'The GBase 8c database requires UTF8 encoding.',GBASE8C_WRITABLE_PRIMARY_REQUIRED:'The GBase 8c endpoint is read-only; waiting for a writable primary.',GBASE8C_EMPTY_STRING_SEMANTICS_UNSUPPORTED:'The destination cannot preserve empty strings; a compatibility plan is required.',GBASE8C_STRING_COMPARISON_REQUIRES_PLAN:'Destination case or trailing-space comparison semantics require a plan.'});

Object.assign(messages.zh,{loginHelp:'输入访问令牌以建立短期安全会话，刷新页面可继续使用。',SESSION_LOGOUT_FAILED:'服务器未确认退出。请重试退出，或关闭页面并联系管理员撤销会话。',CSRF_REJECTED:'页面安全凭证已失效，请刷新后重试。',ORIGIN_REJECTED:'站点地址与会话安全配置不符，请管理员检查 publicOrigin 和 HTTPS 设置。',AUTHENTICATION_CONFLICT:'浏览器会话与 API 令牌属于不同账号，请先退出当前会话。',SESSION_CAPACITY_REACHED:'当前登录会话已达上限，请稍后重试或联系管理员。',BROWSER_SESSIONS_DISABLED:'此部署关闭了浏览器会话，请管理员启用 sessions.enabled。'});
Object.assign(messages.en,{loginHelp:'Enter an access token to establish a short-lived session that survives page reload.',SESSION_LOGOUT_FAILED:'The server did not confirm sign-out. Retry, or close the page and ask an administrator to revoke the session.',CSRF_REJECTED:'The page security credential is stale. Reload and retry.',ORIGIN_REJECTED:'The site address does not match session security settings. Check publicOrigin and HTTPS.',AUTHENTICATION_CONFLICT:'The browser session and API token belong to different accounts. Sign out first.',SESSION_CAPACITY_REACHED:'The session limit has been reached. Retry later or contact an administrator.',BROWSER_SESSIONS_DISABLED:'Browser sessions are disabled. Ask an administrator to enable sessions.enabled.'});

Object.assign(messages.zh,{SHARED_ACCESS_POLICY_MISMATCH:'共享会话策略不一致，请核对所有节点的期限、容量、Cookie及审计保留设置。',ACCOUNT_DIRECTORY_CAPACITY:'共享账号目录已达容量上限，请减少授权列表或联系管理员评估容量。'});
Object.assign(messages.en,{SHARED_ACCESS_POLICY_MISMATCH:'Shared session policies differ. Align expiry,capacity,cookies and audit retention across nodes.',ACCOUNT_DIRECTORY_CAPACITY:'The shared account directory reached its size limit. Reduce grant lists or review capacity.'});

Object.assign(messages.zh,{WORKER_ENVIRONMENT_DENIED:'任务不能引用管理令牌或共享控制库凭据，请使用任务专用的环境变量。'});
Object.assign(messages.en,{WORKER_ENVIRONMENT_DENIED:'Tasks cannot reference management tokens or control-store credentials. Use task-specific environment variables.'});

Object.assign(messages.zh,{failurePhase:'故障阶段',failedSequence:'失败事务序号',failurePhase_UNKNOWN:'未知（缺少可信阶段记录）',failurePhase_TARGET_ACTIVATE:'目标连接或元数据激活',failurePhase_JOURNAL_READ:'读取持久日志',failurePhase_NORMALIZE:'规范化源事务',failurePhase_PROJECT:'目标映射与历史表结构',failurePhase_RECONCILE:'核对目标提交与日志确认',failurePhase_TARGET_APPLY:'应用目标事务',failurePhase_JOURNAL_ACK:'持久化日志确认',recoveryHint_TARGET_ACTIVATE:'检查目标连接、权限及元数据。修复后恢复会重试原事务。',recoveryHint_TARGET_APPLY:'核对约束、类型和目标数据。修复后恢复会重试完整原事务。',recoveryHint_RECONCILE:'需要核对目标提交凭据与日志确认。恢复会先核对，不要手工推进位点。',recoveryHint_PROJECT:'检查映射和历史表结构；修改配置前先查看预览。恢复仍重试原事务。',recoveryHint_UNKNOWN:'先检查诊断及日志。恢复仅重试原事务，不会跳过数据。',FAILURE_DIAGNOSTIC_UNAVAILABLE:'阶段记录无法验证；请检查日志存储，不能据此批准跳过或补偿。'});
Object.assign(messages.en,{failurePhase:'Failure phase',failedSequence:'Failed transaction sequence',failurePhase_UNKNOWN:'Unknown (no trusted phase record)',failurePhase_TARGET_ACTIVATE:'Destination connection or metadata activation',failurePhase_JOURNAL_READ:'Durable journal read',failurePhase_NORMALIZE:'Source transaction normalization',failurePhase_PROJECT:'Destination mapping and historical schema',failurePhase_RECONCILE:'Destination commit and journal ACK reconciliation',failurePhase_TARGET_APPLY:'Destination transaction apply',failurePhase_JOURNAL_ACK:'Durable journal acknowledgement',recoveryHint_TARGET_ACTIVATE:'Check destination connectivity,permissions and metadata. Resume retries the original transaction.',recoveryHint_TARGET_APPLY:'Check constraints,types and destination data. Resume retries the complete original transaction.',recoveryHint_RECONCILE:'Destination receipts and journal ACKs require reconciliation. Resume checks them first; do not advance positions manually.',recoveryHint_PROJECT:'Check mappings and historical schemas; preview configuration edits first. Resume still retries the original transaction.',recoveryHint_UNKNOWN:'Inspect diagnostics and logs first. Resume only retries the original transaction; it does not skip data.',FAILURE_DIAGNOSTIC_UNAVAILABLE:'The phase record could not be verified. Check journal storage; this cannot authorize skip or compensation.'});

Object.assign(messages.zh,{failurePhase_TARGET_PREPARE:'目标表结构准备',failurePhase_TARGET_SNAPSHOT:'目标全量快照投递',recoveryHint_TARGET_PREPARE:'检查目标建表权限、既有表结构和索引。修复后恢复会继续准备，不会跳过增量事务。',recoveryHint_TARGET_SNAPSHOT:'检查快照写入约束和目标状态。修复后恢复会核对快照分块断点，不会直接推进增量位点。'});
Object.assign(messages.en,{failurePhase_TARGET_PREPARE:'Destination schema preparation',failurePhase_TARGET_SNAPSHOT:'Destination snapshot delivery',recoveryHint_TARGET_PREPARE:'Check DDL permissions,existing schemas and indexes. Resume retries preparation; it does not skip incremental transactions.',recoveryHint_TARGET_SNAPSHOT:'Check snapshot write constraints and destination state. Resume reconciles snapshot part checkpoints; it does not advance incremental positions.'});

function closeFailureEvidence(){failureTicket++;if(failureDialog){const events=failureDialog.querySelector('.failure-event-list');if(events){events._failureEvents=null;events.replaceChildren();}failureDialog.replaceChildren();failureDialog.remove();failureDialog=null;}}
function failureLabel(tag,key){const element=text(tag,t(key));element.dataset.failureLabel=key;return element;}
function renderFailureActions(card,task,sub,pending){
 if(!can('TASK_CONFIG'))return;
 if(!session?.recoveryReview){card.append(text('p',t('failureReviewUnavailable'),'subtle'));return;}
 const review=text('button',t('createFailureEvidence'),'quiet');review.disabled=pending||['TARGET_PREPARE','TARGET_SNAPSHOT'].includes(sub.failurePhase);review.onclick=()=>command(task,'FAILURE_REVIEW',sub);card.append(review);
}
async function openFailureEvidence(task,id){
 closeFailureEvidence();const ticket=++failureTicket,dialog=document.createElement('dialog');failureDialog=dialog;dialog.className='failure-review-dialog';dialog.setAttribute('aria-label',t('viewFailureEvidence'));const heading=failureLabel('h2','viewFailureEvidence'),close=failureLabel('button','closeFailureEvidence'),content=text('div',t('loading'));close.onclick=closeFailureEvidence;dialog.addEventListener('cancel',event=>{event.preventDefault();closeFailureEvidence();});dialog.append(heading,close,content);document.body.append(dialog);dialog.showModal();
 const current=()=>ticket===failureTicket&&dialog.isConnected;
 try{
  const base='/tasks/'+encodeURIComponent(task.id)+'/failure-evidence/'+encodeURIComponent(id),summary=await api(base);if(!current())return;
  if(summary.id!==id||!/^[0-9a-f]{64}$/.test(summary.hash||''))throw new Error(t('FAILURE_EVIDENCE_CONTENT_CHANGED'));
  content.replaceChildren(failureLabel('p','failureEvidenceReadOnly'),text('pre',JSON.stringify(summary,null,2)));renderFailureCheckpoint(content,task,id,summary.hash,summary);const controls=text('div','','detail-actions'),raw=failureLabel('button','rawFailureEvents'),normalized=failureLabel('button','normalizedFailureEvents'),previous=failureLabel('button','previousFailureEvents'),next=failureLabel('button','nextFailureEvents'),rows=text('div','','failure-event-list'),range=text('p','','subtle');controls.append(raw,normalized,previous,next);content.append(controls,range,rows);previous.disabled=true;next.disabled=true;normalized.disabled=summary.normalizedEvents==null;let view='RAW',from=0,nextFrom=null,history=[],pageGeneration=0;
  const page=async(selected,start,back=false)=>{
   const generation=++pageGeneration;raw.disabled=true;normalized.disabled=true;previous.disabled=true;next.disabled=true;rows.textContent=t('loading');
   try{const response=await api(base+'/events',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({view:selected,from:start,limit:20})});if(!current()||generation!==pageGeneration)return;if(response.id!==id||response.hash!==summary.hash)throw new Error(t('FAILURE_EVIDENCE_CONTENT_CHANGED'));
    if(selected!==view){history=[];}else if(back){history.pop();}else if(start!==from){history.push(from);}view=selected;from=start;nextFrom=Number.isSafeInteger(response.next)&&response.next>from?response.next:null;renderFailureEvents(rows,response.events||[]);range.textContent=((response.events||[]).length?from+1:0)+' / '+Number(response.total||0);
   }catch(error){if(current()&&generation===pageGeneration)rows.textContent=error.message;}
   finally{if(current()&&generation===pageGeneration){raw.disabled=false;normalized.disabled=summary.normalizedEvents==null;previous.disabled=!history.length;next.disabled=nextFrom==null;}}
  };
  raw.onclick=()=>{history=[];from=0;page('RAW',0);};normalized.onclick=()=>{history=[];from=0;page('NORMALIZED',0);};previous.onclick=()=>{if(history.length)page(view,history[history.length-1],true);};next.onclick=()=>{if(nextFrom!=null)page(view,nextFrom);};
 }catch(error){if(current())content.textContent=error.message;}
}
Object.assign(messages.zh,{createFailureEvidence:'保存事务审阅证据',viewFailureEvidence:'查看事务证据',closeFailureEvidence:'关闭审阅',rawFailureEvents:'显示原始事件',normalizedFailureEvents:'显示规范化事件',previousFailureEvents:'上一页事件',nextFailureEvents:'下一页事件',failureEvidenceReadOnly:'这是已保存的历史观察。查看事件不会跳过、补偿或恢复任务，也不能据此判断目标是否已提交。',failureReviewUnavailable:'此部署尚未启用事务审阅，请联系管理员。',failure_review:'事务审阅',FAILURE_EVIDENCE_READY:'事务证据已保存，可以打开审阅。',FAILURE_REVIEW_DISABLED:'此集群尚未启用事务审阅。',FAILURE_REVIEW_REQUIRES_PAUSED:'请先定位已暂停的目标及其错误版本。',FAILURE_REVIEW_REQUIRES_INCREMENTAL_FAILURE:'结构准备或全量快照失败不能作为增量事务审阅。',FAILURE_TRANSACTION_UNAVAILABLE:'目前没有可审阅的完整增量事务。',FAILURE_EVIDENCE_CONTENT_CHANGED:'证据内容无法验证，请检查原执行节点的证据目录。',FAILURE_EVIDENCE_CAPACITY:'证据目录已满，旧证据已保留。请管理员检查容量。',FAILURE_EVIDENCE_EVENT_TOO_LARGE:'单个事件超过本页读取限额，未返回截断镜像。',FAILURE_EVIDENCE_NOT_NORMALIZED:'此事务尚无已保存的规范化内容。'});
Object.assign(messages.en,{createFailureEvidence:'Retain transaction evidence',viewFailureEvidence:'Review transaction evidence',closeFailureEvidence:'Close review',rawFailureEvents:'Show raw events',normalizedFailureEvents:'Show normalized events',previousFailureEvents:'Previous events',nextFailureEvents:'Next events',failureEvidenceReadOnly:'This is a retained historical observation. Reading events does not skip,compensate or resume and cannot establish destination commit status.',failureReviewUnavailable:'Transaction review is not enabled for this deployment. Contact an administrator.',failure_review:'Transaction review',FAILURE_EVIDENCE_READY:'Transaction evidence is retained and ready to review.',FAILURE_REVIEW_DISABLED:'Transaction review is not enabled for this cluster.',FAILURE_REVIEW_REQUIRES_PAUSED:'Select a paused destination and its error generation.',FAILURE_REVIEW_REQUIRES_INCREMENTAL_FAILURE:'Preparation or snapshot faults cannot be reviewed as incremental transactions.',FAILURE_TRANSACTION_UNAVAILABLE:'No complete incremental transaction is currently available for review.',FAILURE_EVIDENCE_CONTENT_CHANGED:'Evidence verification failed. Check the original execution node evidence directory.',FAILURE_EVIDENCE_CAPACITY:'Evidence storage is full. Existing records were preserved;ask an administrator to review capacity.',FAILURE_EVIDENCE_EVENT_TOO_LARGE:'One event exceeds the page budget. No truncated image was returned.',FAILURE_EVIDENCE_NOT_NORMALIZED:'This transaction has no retained normalized content.'});

function renderFailureCheckpoint(root,task,id,hash,summary){
 const operation=(task.operations||[]).find(op=>op.failureEvidenceId===id&&op.status==='SUCCEEDED'),result=operation?.result,observation=result?.checkpointObservation,section=text('section','','failure-checkpoint');section.append(failureLabel('h3','failureCheckpointTitle'),failureLabel('p','failureCheckpointHistorical'));
 if(observation){
  if(observation.evidenceId!==id||observation.evidenceHash!==hash||!['CHECKPOINT_NOT_ADVANCED','COMMITTED_UNACKNOWLEDGED'].includes(observation.verdict)){section.append(failureLabel('p','FAILURE_EVIDENCE_CONTENT_CHANGED'));}
  else{section.append(failureLabel('p','checkpoint_'+observation.verdict),text('pre',JSON.stringify(observation,null,2)));if(observation.targetFenceRequired)section.append(failureLabel('p','failureCheckpointNeedsFence'));renderCommitConfirmation(section,task,observation);window.RivomeshRecovery?.renderSkipApproval(section,task,id,hash,summary,observation,closeFailureEvidence);window.RivomeshRecovery?.renderCompensationPreview(section,task,id,hash,summary,observation,closeFailureEvidence);window.RivomeshRecovery?.renderCorrection(section,task,id,hash,summary,observation,closeFailureEvidence);}
 }else{section.append(failureLabel('p','failureCheckpointUnavailable'));if(result?.checkpointProblem)section.append(text('code',String(result.checkpointProblem)));}
 root.append(section);
}
Object.assign(messages.zh,{failureCheckpointTitle:'目标断点核对',failureCheckpointHistorical:'以下为保存事务证据时的观察，不代表当前状态，也不是跳过或补偿授权。重新保存事务证据可重新核对。',checkpoint_CHECKPOINT_NOT_ADVANCED:'目标原生断点未前进。此结果不能证明业务数据不存在。',checkpoint_COMMITTED_UNACKNOWLEDGED:'目标已提交该事务，日志尚未确认。应先恢复确认，不能当作未应用事务跳过或再次补偿。',failureCheckpointNeedsFence:'目标 epoch 落后，后续写操作前必须先完成围栏。',failureCheckpointUnavailable:'本次没有可用的断点核对结果。目标可能不支持核对、日志已经变化，或执行节点版本较旧。'});
Object.assign(messages.en,{failureCheckpointTitle:'Destination checkpoint comparison',failureCheckpointHistorical:'This observation was made while retaining evidence. It is not current status or skip/compensation approval. Retain new evidence to compare again.',checkpoint_CHECKPOINT_NOT_ADVANCED:'The native destination checkpoint has not advanced. This does not prove business data is absent.',checkpoint_COMMITTED_UNACKNOWLEDGED:'The destination committed this transaction but the journal has not acknowledged it. Reconcile acknowledgment before any skip or compensation.',failureCheckpointNeedsFence:'The destination epoch lags. Fence it before a later mutation.',failureCheckpointUnavailable:'No checkpoint comparison is available. The target may be unsupported, the journal may have changed, or the execution member may use an older version.'});

function renderCommitConfirmation(root,task,observation){
 if(!session?.recoveryCommit||!can('TASK_CONFIG')||observation.verdict!=='COMMITTED_UNACKNOWLEDGED')return;
 const sub=task.status?.subscriptions?.find(item=>item.id===observation.subscriptionId);
 if(task.desired!=='PAUSED'||task.status?.run_state!=='PAUSED'||task.status?.epoch!==observation.journalEpoch||sub?.state!=='PAUSED'||sub?.errorGeneration!==observation.errorGeneration||sub?.acknowledged!==observation.acknowledged||observation.targetFenceRequired){root.append(failureLabel('p','failureCommitPauseFirst'));return;}
 const reason=document.createElement('textarea');reason.maxLength=1000;reason.rows=3;reason.setAttribute('aria-label',t('failureCommitReason'));const label=text('label');label.append(failureLabel('span','failureCommitReason'),reason);const approval=document.createElement('input');approval.type='checkbox';const acknowledgment=text('label');acknowledgment.append(approval,failureLabel('span','failureCommitApproval'));const button=failureLabel('button','confirmCommittedTransaction');button.disabled=true;const update=()=>button.disabled=!approval.checked||reason.value.trim().length<8;reason.oninput=update;approval.onchange=update;
 button.onclick=async()=>{if(button.disabled)return;button.disabled=true;try{await api('/tasks/'+encodeURIComponent(task.id)+'/actions',{method:'POST',headers:{'Content-Type':'application/json','X-Request-ID':requestId()},body:JSON.stringify({action:'CONFIRM_COMMIT',expectedRevision:task.revision,errorCount:observation.errorGeneration,subscriptionId:observation.subscriptionId,commit:{evidenceId:observation.evidenceId,evidenceHash:observation.evidenceHash,reason:reason.value}})});closeFailureEvidence();notify(t('accepted'));await refresh();}catch(error){notify(error.message);update();}};
 root.append(failureLabel('p','failureCommitLeavesPaused'),label,acknowledgment,button);
}
Object.assign(messages.zh,{failureCommitPauseFirst:'请先暂停整个任务、确认目标围栏，并重新保存事务证据。当前观察或暂停状态不满足确认条件。',failureCommitReason:'确认原因（至少8个字符）',failureCommitApproval:'我已核对事务和目标提交结果，确认仅恢复这一事务的日志确认。',confirmCommittedTransaction:'确认已提交事务',failureCommitLeavesPaused:'此操作只确认已提交事务的日志 ACK，保持任务和目标暂停。不会重写业务行，也不会跳过未提交事务。',confirm_commit:'确认已提交事务',FAILURE_ACK_CONFIRMED:'已恢复该事务的日志确认；任务和目标保持暂停。',FAILURE_ACK_DISABLED:'集群尚未启用人工提交确认。',FAILURE_ACK_REQUIRES_PIPELINE_PAUSED:'请先暂停整个任务并保存新证据。',FAILURE_ACK_TARGET_FENCE_REQUIRED:'请重试暂停流程以确认目标围栏，再保存新证据。',FAILURE_ACK_COMMIT_NOT_PROVEN:'目标提交内容无法证明与证据一致，已拒绝推进 ACK。'});
Object.assign(messages.en,{failureCommitPauseFirst:'Pause the whole task,confirm destination fencing,and retain fresh evidence. The current observation or pause state cannot authorize confirmation.',failureCommitReason:'Confirmation reason (at least 8 characters)',failureCommitApproval:'I reviewed the transaction and destination commit,and confirm restoring only this transaction acknowledgment.',confirmCommittedTransaction:'Confirm committed transaction',failureCommitLeavesPaused:'Only restore journal ACK for this proven commit. Keep the task and destination paused;do not rewrite rows or skip an uncommitted transaction.',confirm_commit:'Confirm committed transaction',FAILURE_ACK_CONFIRMED:'Journal acknowledgment restored;task and destination remain paused.',FAILURE_ACK_DISABLED:'Manual commit confirmation is not enabled for this cluster.',FAILURE_ACK_REQUIRES_PIPELINE_PAUSED:'Pause the whole task and retain fresh evidence first.',FAILURE_ACK_TARGET_FENCE_REQUIRED:'Retry pause to confirm destination fencing,then retain fresh evidence.',FAILURE_ACK_COMMIT_NOT_PROVEN:'The destination commit does not match the evidence. ACK was not advanced.'});

function renderFailureEvents(root,events){
 root._failureEvents=events;root.replaceChildren();if(!events.length){root.append(text('p',t('noFailureEvents'),'subtle'));return;}
 const cell=(image,index)=>image==null?t('imageNotApplicable'):!image[index]?.present?t('imageNotProvided'):image[index].value===null?'SQL NULL':String(image[index].value);
 for(const item of events){const event=item.event||{},article=text('article'),heading=text('h3',String(Number(item.ordinal||0)+1)+' · '+(item.table||event.tableId||'')+' · '+t('failureOperation_'+event.operation));article.append(heading,text('p',String(event.eventId||''),'subtle'));
  if(Array.isArray(item.columns)&&item.columns.length){const table=text('table'),header=text('tr');for(const key of ['failureColumn','failureType','failureBefore','failureAfter'])header.append(text('th',t(key)));table.append(header);item.columns.forEach((column,index)=>{const row=text('tr');row.append(text('td',column.name+(column.generationKind?' · '+column.generationKind:'')),text('td',column.sourceType||column.type+(column.type==='DECIMAL'&&column.precision?'('+column.precision+','+column.scale+')':'')),text('td',cell(event.before,index)),text('td',cell(event.after,index)));table.append(row);});article.append(table);}
  const detail=text('details');detail.append(text('summary',t('failureRawDetails')),text('pre',JSON.stringify(item,null,2)));article.append(detail);root.append(article);
 }
}
Object.assign(messages.zh,{failureColumn:'源列',failureType:'类型',failureBefore:'前镜像',failureAfter:'后镜像',imageNotApplicable:'不适用',imageNotProvided:'未提供',noFailureEvents:'本页没有事件',failureRawDetails:'原始事件详情',failureOperation_INSERT:'插入',failureOperation_UPDATE:'更新',failureOperation_DELETE:'删除',failureOperation_SNAPSHOT:'全量行',failureOperation_DDL:'结构事件',failureOperation_BARRIER:'屏障'});
Object.assign(messages.en,{failureColumn:'Source column',failureType:'Type',failureBefore:'Before image',failureAfter:'After image',imageNotApplicable:'Not applicable',imageNotProvided:'Not provided',noFailureEvents:'No events on this page',failureRawDetails:'Raw event details',failureOperation_INSERT:'Insert',failureOperation_UPDATE:'Update',failureOperation_DELETE:'Delete',failureOperation_SNAPSHOT:'Snapshot row',failureOperation_DDL:'Schema event',failureOperation_BARRIER:'Barrier'});

Object.assign(messages.zh,{safeCoveredSequence:'安全覆盖序号（含显式跳过）',contiguousAppliedSequence:'连续已应用序号（遇跳过停止）',contiguousDeliveredSequence:'连续未跳过投递序号',firstSkippedSequence:'首次显式跳过序号',progress_SQL_COMMITTED:'SQL目标已提交；共同点核验不会改写原事务为已应用。',progress_BROKER_ACCEPTED:'Broker已确认接收，不代表下游消费者已完成处理。',progress_MATERIALIZER_ACKNOWLEDGED:'物化协议已确认，不代表所有副本已对外可见。'});
Object.assign(messages.en,{safeCoveredSequence:'Safe coverage (includes explicit skips)',contiguousAppliedSequence:'Contiguous applied sequence (stops before a skip)',contiguousDeliveredSequence:'Contiguous delivery without skips',firstSkippedSequence:'First explicitly skipped sequence',progress_SQL_COMMITTED:'SQL destination committed;common-cut verification does not relabel the original transaction as applied.',progress_BROKER_ACCEPTED:'Broker acknowledged receipt,not downstream consumer completion.',progress_MATERIALIZER_ACKNOWLEDGED:'Materialization protocol acknowledged,not proof of visibility on every replica.'});
