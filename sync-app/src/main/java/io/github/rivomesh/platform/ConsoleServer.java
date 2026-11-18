package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sun.net.httpserver.*;
import io.github.rivomesh.Model;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/** Java 8 console with explicit Bearer authentication and revocable browser sessions. */
public final class ConsoleServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor;
    private final TaskService tasks;
    private final TaskControlPlane control;
    private final NodeDirectory nodes;
    private final GlobalControl global;
    private final PlatformConfig.Sessions sessionConfig;
    private final BrowserSessions sessions;
    private final boolean agentOnly;
    private static final class AuditScope {final AuditStore.Event begin;boolean done;AuditScope(AuditStore.Event begin){this.begin=begin;}}
    private final ThreadLocal<AuditScope> auditScope=new ThreadLocal<>();
    private final java.util.concurrent.atomic.AtomicBoolean closed=new java.util.concurrent.atomic.AtomicBoolean();
    public ConsoleServer(PlatformConfig config,TaskService tasks) throws Exception {this(config,tasks,null);}
    ConsoleServer(PlatformConfig config,TaskService tasks,TaskControlPlane control) throws Exception {
        this.tasks=tasks;
        agentOnly=config.cluster!=null&&config.cluster.role==ClusterConfig.Role.AGENT;
        sessionConfig=config.sessions;sessions=agentOnly?null:new BrowserSessions(sessionConfig,tasks.sessionStore(),java.time.Clock.systemUTC());
        server=HttpServer.create(new InetSocketAddress(config.bind,config.port),32);
        executor=new ThreadPoolExecutor(2,8,30,TimeUnit.SECONDS,new ArrayBlockingQueue<>(64),r->{Thread t=new Thread(r,"rivomesh-http");t.setDaemon(true);return t;},new ThreadPoolExecutor.CallerRunsPolicy());
        server.setExecutor(executor);server.createContext("/",this::handle);
        NodeDirectory selected=null;GlobalControl shared=null;
        try{selected=new NodeDirectory(config,tasks);if(config.cluster!=null&&config.cluster.sharedTasks){if(control!=null)throw new IllegalArgumentException("GLOBAL_CONTROL_FACTORY_REQUIRED");shared=new GlobalControl(config,tasks,selected);}nodes=selected;global=shared;this.control=control!=null?control:shared!=null?shared.plane:new LocalTaskPlane(tasks);}
        catch(Exception failure){if(shared!=null)shared.close();if(selected!=null)selected.close();server.stop(0);executor.shutdownNow();if(sessions!=null)sessions.close();throw failure;}
    }
    public void start()throws Exception{try{nodes.start();if(global!=null)global.start();server.start();}catch(Exception failure){close();throw failure;}}
    public int port(){return server.getAddress().getPort();}
    private void handle(HttpExchange exchange) throws IOException {
        try {
            Headers headers=exchange.getResponseHeaders();
            headers.set("X-Content-Type-Options","nosniff");headers.set("X-Frame-Options","DENY");
            headers.set("Cache-Control","no-store");headers.set("Referrer-Policy","no-referrer");
            headers.set("Content-Security-Policy","default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
            String path=exchange.getRequestURI().getPath(),method=exchange.getRequestMethod();
            if((path.equals("/healthz")||path.equals("/readyz"))&&method.equals("GET")){boolean ready=!path.equals("/readyz")||nodes.ready()&&tasks.accessReady()&&(global==null||global.ready());json(exchange,ready?200:503,Collections.singletonMap("status",ready?"UP":"CONTROL_STORE_UNAVAILABLE"));return;}
            if(agentOnly){error(exchange,404,"AGENT_PROBE_ONLY");return;}
            if(!path.startsWith("/api/")) {
                if(!method.equals("GET")){error(exchange,405,"METHOD_NOT_ALLOWED");return;}
                String resource=switch(path) {
                    case "/" -> "index.html";
                    case "/app.js","/theme.js","/configuration-editor.js","/accounts.js","/validation.js",
                         "/recovery.js","/handoff.js","/audit.js","/app.css","/brand/favicon.svg","/brand/wordmark.svg",
                         "/brand/wordmark-light.svg" -> path.substring(1);
                    default -> null;
                };
                if(resource==null){error(exchange,404,"NOT_FOUND");return;}
                try(InputStream in=getClass().getResourceAsStream("/console/"+resource)) {
                    if(in==null){error(exchange,404,"NOT_FOUND");return;}
                    byte[] bytes=read(in,2*1024*1024);
                    String contentType=resource.endsWith(".js")?"application/javascript; charset=utf-8"
                            :resource.endsWith(".css")?"text/css; charset=utf-8"
                            :resource.endsWith(".svg")?"image/svg+xml":"text/html; charset=utf-8";
                    headers.set("Content-Type",contentType);
                    exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);return;
                }
            }
            String authorization=singleHeader(exchange,"Authorization"),cookieId=sessions.cookieId(exchange.getRequestHeaders().get("Cookie"));
            BrowserSessions.Session browser=sessionConfig.enabled?sessions.find(cookieId):null;
            if(browser!=null&&!tasks.sessionCurrent(browser.principal)){sessions.revoke(cookieId);browser=null;}
            boolean cookieAuth=authorization==null;
            AccessControl.Principal principal=cookieAuth?(browser==null?null:browser.principal):tasks.authenticate(authorization);
            String[] auditParts=path.split("/");String auditTask=auditParts.length>=4&&auditParts[2].equals("tasks")?auditParts[3]:null;
            auditScope.set(new AuditScope(tasks.auditBegin(auditAction(method,path),auditTask,principal,exchange.getRemoteAddress().getAddress().getHostAddress(),exchange.getRequestHeaders().getFirst("X-Request-ID"))));
            if(principal==null) {
                if(cookieAuth&&cookieId!=null)headers.set("Set-Cookie",sessions.cookie(null));
                headers.set("WWW-Authenticate","Bearer");error(exchange,401,"UNAUTHORIZED");return;
            }
            if(!cookieAuth&&browser!=null&&!browser.principal.id.equals(principal.id)){error(exchange,409,"AUTHENTICATION_CONFLICT");return;}
            String origin=singleHeader(exchange,"Origin");
            if(origin!=null) {
                URI uri=URI.create(origin);String host=exchange.getRequestHeaders().getFirst("Host");
                if((cookieAuth||path.equals("/api/session")&&method.equals("POST"))?!sessionOrigin(exchange,origin):!Arrays.asList("http","https").contains(uri.getScheme())||!Objects.equals(uri.getRawAuthority(),host)){error(exchange,403,"ORIGIN_REJECTED");return;}
            }
            if(cookieAuth&&(!method.equals("GET")||origin!=null)){
                if(!sessionOrigin(exchange,origin)){error(exchange,403,"ORIGIN_REJECTED");return;}
                if(!method.equals("GET")&&!BrowserSessions.csrf(browser,singleHeader(exchange,"X-CSRF-Token"))){error(exchange,403,"CSRF_REJECTED");return;}
            }
            if(path.equals("/api/session")&&method.equals("POST")){
                if(!sessionConfig.enabled){error(exchange,409,"BROWSER_SESSIONS_DISABLED");return;}
                if(cookieAuth){error(exchange,403,"SESSION_BEARER_REQUIRED");return;}
                if(origin!=null&&!sessionOrigin(exchange,origin)){error(exchange,403,"ORIGIN_REJECTED");return;}
                if(!tasks.sessionCurrent(principal)){error(exchange,401,"UNAUTHORIZED");return;}
                BrowserSessions.Issued issued=sessions.create(principal);sessions.revoke(cookieId);
                headers.set("Set-Cookie",sessions.cookie(issued.id));Map<String,Object> identity=issued.session.view();identity.put("nodeId",tasks.nodeId());identity.put("nodeRole",tasks.nodeRole());identity.put("taskControlScope",control.localOnly()?"LOCAL":"GLOBAL");identity.put("recoveryReview",tasks.failureReviewEnabled());identity.put("recoveryCommit",tasks.commitRecoveryEnabled());identity.put("recoverySkip",tasks.skipRecoveryEnabled());json(exchange,200,identity);return;
            }
            if(path.equals("/api/session/logout")&&method.equals("POST")){
                // A Bearer token never authorizes revoking a different browser's session.
                if(!cookieAuth){error(exchange,403,"SESSION_COOKIE_REQUIRED");return;}
                sessions.revoke(cookieId);headers.set("Set-Cookie",sessions.cookie(null));json(exchange,200,Collections.singletonMap("status","SIGNED_OUT"));return;
            }
            if(cookieAuth)sessions.touch(cookieId);
            if(path.equals("/api/nodes")&&method.equals("GET")){
                if(!principal.administers()){error(exchange,403,"FORBIDDEN");return;}
                Map<String,String> query=nodeQuery(exchange.getRequestURI().getRawQuery());Map<String,Object> page=nodes.page(query.getOrDefault("after",""),Integer.parseInt(query.getOrDefault("limit","20")));page.put("taskControlScope",control.localOnly()?"LOCAL":"GLOBAL");json(exchange,200,page);return;
            }
            if((path.equals("/api/audit")||path.equals("/api/audit/export"))&&method.equals("GET")){
                if(!principal.administers()){error(exchange,403,"FORBIDDEN");return;}
                Map<String,Long> query=auditQuery(exchange.getRequestURI().getRawQuery());long after=query.getOrDefault("after",0L),through=query.getOrDefault("through",0L),limit=query.getOrDefault("limit",100L);if(limit<1||limit>1000)throw new IllegalArgumentException("AUDIT_QUERY_INVALID");
                if(path.endsWith("/export"))headers.set("Content-Disposition","attachment; filename=rivomesh-audit.json");
                json(exchange,200,tasks.auditPage(after,through,(int)limit));return;
            }
            if(path.equals("/api/session")&&method.equals("GET")){Map<String,Object> identity=cookieAuth?browser.view():principal.view();identity.put("nodeId",tasks.nodeId());identity.put("nodeRole",tasks.nodeRole());identity.put("taskControlScope",control.localOnly()?"LOCAL":"GLOBAL");identity.put("recoveryReview",tasks.failureReviewEnabled());identity.put("recoveryCommit",tasks.commitRecoveryEnabled());identity.put("recoverySkip",tasks.skipRecoveryEnabled());json(exchange,200,identity);return;}
            if(path.equals("/api/accounts")||path.startsWith("/api/accounts/")){
                if(!principal.administers()){error(exchange,403,"FORBIDDEN");return;}
                if(path.equals("/api/accounts")&&method.equals("GET")){json(exchange,200,tasks.accountView(principal));return;}
                if(path.equals("/api/accounts/activate")&&method.equals("POST")){json(exchange,200,tasks.mutateAccounts("ACTIVATE",null,body(exchange),exchange.getRequestHeaders().getFirst("X-Request-ID"),principal));return;}
                if(path.equals("/api/accounts")&&method.equals("POST")){json(exchange,201,tasks.mutateAccounts("CREATE",null,body(exchange),exchange.getRequestHeaders().getFirst("X-Request-ID"),principal));return;}
                String[] accountPath=path.split("/");
                if(accountPath.length>=4&&accountPath[3].matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")){
                    String action=accountPath.length==4&&method.equals("PUT")?"UPDATE":accountPath.length==4&&method.equals("DELETE")?"DELETE":accountPath.length==5&&accountPath[4].equals("token")&&method.equals("POST")?"TOKEN":null;
                    if(action!=null){json(exchange,200,tasks.mutateAccounts(action,accountPath[3],body(exchange),exchange.getRequestHeaders().getFirst("X-Request-ID"),principal));return;}
                }
                error(exchange,404,"NOT_FOUND");return;
            }
            if(path.startsWith("/api/tasks/")&&control.localOnly()&&!tasks.nodeAllowed(principal)){error(exchange,404,"TASK_NOT_FOUND");return;}
            if(!method.equals("GET")&&!principal.administers()){
                if(!principal.operates()||!method.equals("POST")||!path.matches("/api/tasks/[A-Za-z0-9][A-Za-z0-9._-]{0,127}/actions")){error(exchange,403,"FORBIDDEN");return;}
            }
            if(path.equals("/api/tasks")&&method.equals("GET")){
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.LIST,null);
                if(!control.localOnly()){
                    Map<String,String> query=pageQuery(exchange.getRequestURI().getRawQuery(),true);
                    request.cursor=query.getOrDefault("after","");
                    request.limit=Integer.parseInt(query.getOrDefault("limit","20"));
                    request.ownerNodeId=query.get("ownerNodeId");
                    request.taskId=query.get("taskId");
                }
                json(exchange,200,control.invoke(request,principal));return;
            }
            if(path.equals("/api/capabilities")&&method.equals("GET")){json(exchange,200,AdapterCatalog.entries());return;}
            if(path.equals("/api/tasks")&&method.equals("POST")) {
                JsonNode body=body(exchange);String yaml;
                if(body.hasNonNull("yaml")&&body.get("yaml").isTextual())yaml=body.get("yaml").asText();
                else if(body.has("config")&&body.get("config").isObject())yaml=new ObjectMapper(new YAMLFactory()).writeValueAsString(body.get("config"));
                else throw new IllegalArgumentException("CONFIG_REQUIRED");
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.CREATE,null);request.yaml=yaml;request.requestId=exchange.getRequestHeaders().getFirst("X-Request-ID");request.ownerNodeId=body.hasNonNull("ownerNodeId")?body.path("ownerNodeId").asText():null;
                if(body.hasNonNull("placement")){
                    request.placement=body.path("placement").textValue();
                    if(request.placement==null)throw new IllegalArgumentException("TASK_PLACEMENT_INVALID");
                }
                json(exchange,control.localOnly()?201:202,control.invoke(request,principal));return;
            }
            if(path.matches("/api/tasks/[A-Za-z0-9][A-Za-z0-9._-]{0,127}/configuration(?:/.*)?")&&!principal.administers()){error(exchange,403,"FORBIDDEN");return;}
            String[] parts=path.split("/");
            if(parts.length>=4&&parts[1].equals("api")&&parts[2].equals("tasks")&&!principal.task(parts[3])){error(exchange,404,"TASK_NOT_FOUND");return;}
            if(parts.length>=5&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("configuration")){
                String id=parts[3];
                if(parts.length==5&&method.equals("GET")){json(exchange,200,invoke(TaskRequest.Kind.CONFIGURATION,id,principal));return;}
                if(parts.length==6&&parts[5].equals("history")&&method.equals("GET")){String query=exchange.getRequestURI().getRawQuery(),cursor=null;if(query!=null){if(!query.startsWith("cursor=")||query.contains("&")||query.length()>256)throw new IllegalArgumentException("CURSOR_INVALID");cursor=URLDecoder.decode(query.substring(7),"UTF-8");}TaskRequest request=TaskRequest.of(TaskRequest.Kind.CONFIG_HISTORY,id);request.cursor=cursor;json(exchange,200,control.invoke(request,principal));return;}
                if(parts.length==7&&parts[5].equals("restore")&&parts[6].equals("validate")&&method.equals("POST")){JsonNode body=body(exchange);TaskRequest request=TaskRequest.of(TaskRequest.Kind.RESTORE_PREVIEW,id);request.reference=body.path("reference").asText(null);request.revision=body.path("expectedRevision").asText("");json(exchange,200,control.invoke(request,principal));return;}
                if(parts.length==6&&parts[5].equals("restore")&&method.equals("POST")){JsonNode body=body(exchange);TaskRequest request=TaskRequest.of(TaskRequest.Kind.RESTORE,id);request.reference=body.path("reference").asText(null);request.revision=body.path("expectedRevision").asText("");request.requestId=exchange.getRequestHeaders().getFirst("X-Request-ID");json(exchange,control.localOnly()?200:202,control.invoke(request,principal));return;}
                if(parts.length==5&&method.equals("PUT")||parts.length==6&&parts[5].equals("validate")&&method.equals("POST")){JsonNode body=body(exchange),document=configurationDocument(body);String revision=body.path("expectedRevision").asText("");TaskRequest request=TaskRequest.of(parts.length==6?TaskRequest.Kind.CONFIG_PREVIEW:TaskRequest.Kind.CONFIG_UPDATE,id);request.document=document;request.revision=revision;request.requestId=exchange.getRequestHeaders().getFirst("X-Request-ID");json(exchange,!control.localOnly()&&request.mutation()?202:200,control.invoke(request,principal));return;}
            }
            if(parts.length==4&&parts[1].equals("api")&&parts[2].equals("tasks")&&method.equals("GET")){json(exchange,200,invoke(TaskRequest.Kind.VIEW,parts[3],principal));return;}
            if(parts.length==6&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("recovery-receipts")&&method.equals("GET")){
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.RECOVERY_RECEIPT,parts[3]);request.reference=parts[5];
                try{json(exchange,200,control.invoke(request,principal));}
                catch(NoSuchElementException missing){
                    if(!"RECOVERY_RECEIPT_NOT_FOUND".equals(missing.getMessage()))throw missing;
                    error(exchange,404,"RECOVERY_RECEIPT_NOT_FOUND");
                }
                return;
            }
            if(parts.length==5&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("handoff")){
                if(method.equals("GET")){
                    try{json(exchange,200,invoke(TaskRequest.Kind.HANDOFF_LATEST,parts[3],principal));}
                    catch(NoSuchElementException missing){
                        if(!"HANDOFF_ASSESSMENT_NOT_FOUND".equals(missing.getMessage()))throw missing;
                        error(exchange,404,"HANDOFF_ASSESSMENT_NOT_FOUND");
                    }
                    return;
                }
                if(method.equals("POST")){
                    if(!principal.administers())throw new SecurityException("FORBIDDEN");
                    JsonNode input=body(exchange);
                    TaskRequest request=TaskRequest.of(TaskRequest.Kind.HANDOFF_ASSESS,parts[3]);
                    request.requestId=exchange.getRequestHeaders().getFirst("X-Request-ID");
                    request.ownerNodeId=input.path("candidateNodeId").textValue();
                    request.revision=input.path("expectedRevision").textValue();
                    request.reference=input.path("expectedAssessment").textValue();
                    if(input.hasNonNull("expectedAssessment")&&request.reference==null)
                        throw new IllegalArgumentException("HANDOFF_REQUEST_INVALID");
                    json(exchange,200,control.invoke(request,principal));return;
                }
            }
            if(parts.length==6&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("handoff")
                    &&parts[5].equals("probe")&&method.equals("POST")){
                if(!principal.administers())throw new SecurityException("FORBIDDEN");
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.HANDOFF_PROBE,parts[3]);
                request.reference=body(exchange).path("expectedAssessment").textValue();
                json(exchange,200,control.invoke(request,principal));return;
            }
            if(parts.length==5&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("diagnostics")&&method.equals("GET")){
                Map<String,String> query=pageQuery(exchange.getRequestURI().getRawQuery(),512,"DIAGNOSTICS_PAGE_INVALID",
                        Arrays.asList("after","limit","subscriptionId"));
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.DIAGNOSTICS,parts[3]);
                request.cursor=query.getOrDefault("after","");
                request.limit=Integer.parseInt(query.getOrDefault("limit","20"));
                request.subscriptionId=query.get("subscriptionId");
                json(exchange,200,control.invoke(request,principal));return;
            }
            if(parts.length==5&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("capture-recovery")&&method.equals("GET")){
                json(exchange,200,invoke(TaskRequest.Kind.CAPTURE_RECOVERY,parts[3],principal));return;
            }
            if(parts.length==5&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("capture-exclusions")&&method.equals("GET")){
                Map<String,String> query=nodeQuery(exchange.getRequestURI().getRawQuery());
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.CAPTURE_EXCLUSIONS,parts[3]);
                request.cursor=query.getOrDefault("after","");
                request.limit=Integer.parseInt(query.getOrDefault("limit","20"));
                json(exchange,200,control.invoke(request,principal));return;
            }
            if(parts.length>=5&&parts[1].equals("api")&&parts[2].equals("tasks")&&Arrays.asList("recovery-plans","gaps","gap-verifications").contains(parts[4])){
                if(!principal.administers()){error(exchange,403,"FORBIDDEN");return;}
                if(parts.length==6&&parts[4].equals("gap-verifications")&&method.equals("GET")){TaskRequest request=TaskRequest.of(TaskRequest.Kind.GAP_VERIFICATION,parts[3]);request.reference=parts[5];json(exchange,200,control.invoke(request,principal));return;}
                if(parts.length==6&&parts[4].equals("recovery-plans")&&method.equals("GET")){TaskRequest request=TaskRequest.of(TaskRequest.Kind.SKIP_PLAN,parts[3]);request.reference=parts[5];json(exchange,200,control.invoke(request,principal));return;}
                if(parts.length==7&&parts[4].equals("recovery-plans")&&parts[6].equals("inspect")&&method.equals("POST")){TaskRequest request=TaskRequest.of(TaskRequest.Kind.SKIP_PLAN,parts[3]);request.reference=parts[5];request.action="NATIVE";json(exchange,200,control.invoke(request,principal));return;}
                if(parts.length==5&&!parts[4].equals("gap-verifications")&&method.equals("POST")){JsonNode input=body(exchange);if(input.has("limit")&&(!input.get("limit").isIntegralNumber()||!input.get("limit").canConvertToInt()))throw new IllegalArgumentException("SKIP_PAGE_INVALID");TaskRequest request=TaskRequest.of(parts[4].equals("gaps")?TaskRequest.Kind.GAPS:TaskRequest.Kind.SKIP_PLANS,parts[3]);request.target=input.path("subscriptionId").asText(null);request.cursor=input.path("after").asText(parts[4].equals("gaps")?"0":"");request.limit=input.path("limit").asInt(20);json(exchange,200,control.invoke(request,principal));return;}
            }
            if(parts.length>=6&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("failure-evidence")){
                if(!principal.administers()){error(exchange,403,"FORBIDDEN");return;}
                if(parts.length==6&&method.equals("GET")){TaskRequest request=TaskRequest.of(TaskRequest.Kind.FAILURE_EVIDENCE_SUMMARY,parts[3]);request.reference=parts[5];json(exchange,200,control.invoke(request,principal));return;}
                if(parts.length==7&&parts[6].equals("events")&&method.equals("POST")){
                    JsonNode input=body(exchange);if(input.has("limit")&&(!input.get("limit").isIntegralNumber()||!input.get("limit").canConvertToInt()))throw new IllegalArgumentException("FAILURE_EVIDENCE_PAGE_INVALID");
                    TaskRequest request=TaskRequest.of(TaskRequest.Kind.FAILURE_EVIDENCE_EVENTS,parts[3]);request.reference=parts[5];request.action=input.path("view").asText("RAW");request.cursor=input.path("from").asText("0");request.limit=input.path("limit").asInt(20);json(exchange,200,control.invoke(request,principal));return;
                }
            }
            if(parts.length==5&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("validations")&&method.equals("GET")){
                Map<String,String> query=nodeQuery(exchange.getRequestURI().getRawQuery());
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.VALIDATION_REPORTS,parts[3]);
                request.cursor=query.getOrDefault("after","");
                request.limit=Integer.parseInt(query.getOrDefault("limit","20"));
                json(exchange,200,control.invoke(request,principal));return;
            }
            if(parts.length==6&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("validations")&&method.equals("GET")){TaskRequest request=TaskRequest.of(TaskRequest.Kind.VALIDATION_REPORT,parts[3]);request.reportId=parts[5];json(exchange,200,control.invoke(request,principal));return;}
            if(parts.length==7&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("validations")&&parts[6].equals("storage")&&method.equals("GET")){
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.VALIDATION_STORAGE,parts[3]);request.reportId=parts[5];
                json(exchange,200,control.invoke(request,principal));return;
            }
            if(parts.length==7&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("validations")&&Arrays.asList("differences","values").contains(parts[6])&&method.equals("POST")){
                if(!principal.administers()){error(exchange,403,"FORBIDDEN");return;}JsonNode input=body(exchange);boolean values=parts[6].equals("values");String identity=values?input.path("identityHash").asText(null):null;if(values&&(identity==null||!identity.matches("[0-9a-f]{64}")))throw new IllegalArgumentException("VALIDATION_IDENTITY_INVALID");
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.VALIDATION_DIFFERENCES,parts[3]);request.reportId=parts[5];request.target=input.path("target").asText(null);request.tableId=input.path("tableId").asText(null);request.after=input.path("after").asText("");request.limit=input.path("limit").asInt(50);request.identityHash=identity;json(exchange,200,control.invoke(request,principal));return;
            }
            if(parts.length==8&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("validations")&&parts[6].equals("repairs")&&method.equals("GET")){TaskRequest request=TaskRequest.of(TaskRequest.Kind.REPAIR_PLAN,parts[3]);request.reportId=parts[5];request.planId=parts[7];json(exchange,200,control.invoke(request,principal));return;}
            if(parts.length==6&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("corrections")
                    &&Arrays.asList("schema","preview").contains(parts[5])&&method.equals("POST")){
                JsonNode input=body(exchange);
                TaskRequest request=TaskRequest.of(parts[5].equals("schema")?TaskRequest.Kind.CORRECTION_SCHEMA:TaskRequest.Kind.CORRECTION_PREVIEW,parts[3]);
                correctionInput(request,input);
                json(exchange,200,control.invoke(request,principal));return;
            }
            if(parts.length==5&&parts[1].equals("api")&&parts[2].equals("tasks")&&parts[4].equals("actions")&&method.equals("POST")) {
                JsonNode body=body(exchange);
                String action=body.path("action").asText(""),revision=body.path("expectedRevision").asText("");
                if(!principal.administers()&&!Arrays.asList("CHECK","START","STOP","PAUSE","RESUME","RESUME_TARGET").contains(action)){error(exchange,403,"FORBIDDEN");return;}
                long generation=body.has("errorCount")&&body.get("errorCount").isIntegralNumber()?body.get("errorCount").asLong():-1;
                ValidationRequest validation=body.hasNonNull("validation")?Model.JSON.treeToValue(body.get("validation"),ValidationRequest.class):null;
                if(body.hasNonNull("skip")&&(!body.get("skip").isObject()||!body.get("skip").path("acceptDataLoss").isBoolean()))throw new IllegalArgumentException("SKIP_DATA_LOSS_APPROVAL_REQUIRED");
                TaskRequest request=TaskRequest.of(TaskRequest.Kind.ACTION,parts[3]);request.action=action;if(request.recoveryCancel())request.reference=body.path("parentRequestId").textValue();if(request.captureRetention())request.reference=body.path("boundaryHash").textValue();request.revision=revision;request.requestId=exchange.getRequestHeaders().getFirst("X-Request-ID");request.errorCount=generation;request.subscriptionId=body.hasNonNull("subscriptionId")?body.get("subscriptionId").asText():null;request.validation=validation;request.repair=body.hasNonNull("repair")?Model.JSON.treeToValue(body.get("repair"),RepairRequest.class):null;request.commit=body.hasNonNull("commit")?Model.JSON.treeToValue(body.get("commit"),CommitRequest.class):null;request.skip=body.hasNonNull("skip")?Model.JSON.treeToValue(body.get("skip"),SkipRequest.class):null;request.gap=body.hasNonNull("gap")?Model.JSON.treeToValue(body.get("gap"),GapVerificationRequest.class):null;request.compensation=body.hasNonNull("compensation")?Model.JSON.treeToValue(body.get("compensation"),CompensationRequest.class):null;if(body.has("correction")||request.correctionAction())correctionInput(request,body);
                if(body.hasNonNull("captureExclusion"))request.captureExclusion=Model.JSON.treeToValue(body.get("captureExclusion"),CaptureExclusionRequest.class);json(exchange,202,control.invoke(request,principal));return;
            }
            error(exchange,404,"NOT_FOUND");
        }catch(ControlStore.Unavailable e){error(exchange,503,"CONTROL_STORE_UNAVAILABLE");}
        catch(AuditStore.Unavailable e){exchange.getResponseHeaders().remove("Set-Cookie");rawJson(exchange,503,Collections.singletonMap("code","AUDIT_UNAVAILABLE"));}
        catch(TaskService.Conflict e){
            if("DIAGNOSTICS_RATE_LIMITED".equals(e.getMessage())){
                exchange.getResponseHeaders().set("Retry-After","1");
                error(exchange,429,e.getMessage());
            }else error(exchange,409,e.getMessage());
        }
        catch(NoSuchElementException e){error(exchange,404,exchange.getRequestURI().getPath().startsWith("/api/accounts")?"ACCOUNT_NOT_FOUND":"TASK_NOT_FOUND");}
        catch(SecurityException e){error(exchange,403,"FORBIDDEN");}
        catch(IllegalArgumentException e){
            String message=e.getMessage();
            boolean known=message!=null&&("TASK_PLACEMENT_INVALID".equals(message)||message.matches("(CORRECTION|HANDOFF)_[A-Z_]+")
                    ||exchange.getRequestURI().getPath().startsWith("/api/accounts")&&message.matches("ACCOUNT_[A-Z_]+"));
            error(exchange,400,known?message:"REQUEST_INVALID");
        }
        catch(com.fasterxml.jackson.core.JsonProcessingException e){error(exchange,400,"CONFIG_INVALID");}
        catch(Exception e){error(exchange,500,TaskService.safeCode(e));}
        finally{auditScope.remove();exchange.close();}
    }
    private static void correctionInput(TaskRequest request,JsonNode input)throws Exception{
        JsonNode error=input.path("errorCount");
        if(!error.isIntegralNumber()||!error.canConvertToLong())throw new IllegalArgumentException("CORRECTION_ERROR_GENERATION_INVALID");
        request.errorCount=error.longValue();
        request.subscriptionId=input.path("subscriptionId").textValue();
        request.revision=input.path("expectedRevision").textValue();
        request.correction=Model.JSON.treeToValue(input.get("correction"),CorrectionRequest.class);
    }

    private Map<String,Object> invoke(TaskRequest.Kind kind,String task,AccessControl.Principal actor)throws Exception{return control.invoke(TaskRequest.of(kind,task),actor);}
    private static Map<String,String> nodeQuery(String raw)throws Exception{return pageQuery(raw,false);}
    private static Map<String,String> pageQuery(String raw,boolean tasks)throws Exception{
        return tasks?pageQuery(raw,1024,"TASK_CURSOR_INVALID",Arrays.asList("after","limit","ownerNodeId","taskId"))
                :pageQuery(raw,160,"NODE_CURSOR_INVALID",Arrays.asList("after","limit"));
    }
    private static Map<String,String> pageQuery(String raw,int limit,String invalid,List<String> fields)throws Exception{
        Map<String,String> result=new HashMap<>();
        if(raw==null)return result;
        if(raw.length()>limit)throw new IllegalArgumentException(invalid);
        for(String field:raw.split("&",-1)){
            String[] pair=field.split("=",-1);
            if(pair.length!=2||!fields.contains(pair[0])||result.put(pair[0],URLDecoder.decode(pair[1],"UTF-8"))!=null)
                throw new IllegalArgumentException(invalid);
        }
        return result;
    }
    private static String singleHeader(HttpExchange exchange,String name){
        List<String> values=exchange.getRequestHeaders().get(name);if(values==null)return null;
        if(values.size()!=1)throw new IllegalArgumentException("HEADER_AMBIGUOUS");return values.get(0);
    }
    private boolean sessionOrigin(HttpExchange exchange,String origin){
        if(origin==null)return false;
        String expected=sessionConfig.publicOrigin!=null?sessionConfig.publicOrigin:(sessionConfig.secureCookie?"https://":"http://")+singleHeader(exchange,"Host");
        return expected.equals(origin);
    }
    private static String auditAction(String method,String path){
        String action=(path.equals("/api/accounts")||path.startsWith("/api/accounts/"))?"ACCOUNTS":path.equals("/api/nodes")?"NODES":path.equals("/api/audit/export")?"AUDIT_EXPORT":path.equals("/api/audit")?"AUDIT_READ":(path.equals("/api/session")||path.equals("/api/session/logout"))?"SESSION":path.equals("/api/capabilities")?"CAPABILITIES":path.equals("/api/tasks")?"TASKS":path.matches("/api/tasks/[A-Za-z0-9][A-Za-z0-9._-]{0,127}/configuration(?:/.*)?")?"CONFIGURATION":path.matches("/api/tasks/[A-Za-z0-9][A-Za-z0-9._-]{0,127}/actions")?"TASK_ACTION":path.matches("/api/tasks/[A-Za-z0-9][A-Za-z0-9._-]{0,127}/validations(?:/.*)?")?"VALIDATION_READ":path.matches("/api/tasks/[A-Za-z0-9][A-Za-z0-9._-]{0,127}/recovery-receipts/[A-Za-z0-9_-]{8,128}")?"RECOVERY_RECEIPT":path.matches("/api/tasks/[A-Za-z0-9][A-Za-z0-9._-]{0,127}/diagnostics")?"TASK_DIAGNOSTICS":path.matches("/api/tasks/[A-Za-z0-9][A-Za-z0-9._-]{0,127}")?"TASK_READ":"UNKNOWN_API";
        return (Arrays.asList("GET","POST","PUT","DELETE","PATCH","HEAD","OPTIONS").contains(method)?method:"OTHER")+"_"+action;
    }
    private static Map<String,Long> auditQuery(String query){
        Map<String,Long> result=new LinkedHashMap<>();if(query==null)return result;if(query.length()>160)throw new IllegalArgumentException("AUDIT_QUERY_INVALID");
        for(String item:query.split("&",-1)){String[] pair=item.split("=",-1);if(pair.length!=2||!Arrays.asList("after","through","limit").contains(pair[0])||!pair[1].matches("[0-9]{1,18}")||result.put(pair[0],Long.parseLong(pair[1]))!=null)throw new IllegalArgumentException("AUDIT_QUERY_INVALID");}return result;
    }
    private static JsonNode configurationDocument(JsonNode body)throws IOException{JsonNode document=body.get("config");if(document==null&&body.hasNonNull("yaml")&&body.get("yaml").isTextual())document=new ObjectMapper(new YAMLFactory()).enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(body.get("yaml").asText());if(document==null||!document.isObject())throw new IllegalArgumentException("CONFIG_REQUIRED");ConfigurationDocuments.validate(document);return document;}
    private static JsonNode body(HttpExchange e) throws IOException {
        String type=e.getRequestHeaders().getFirst("Content-Type");
        if(type==null||!type.toLowerCase(Locale.ROOT).startsWith("application/json"))throw new IllegalArgumentException("JSON_REQUIRED");
        JsonNode value=new ObjectMapper().enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION).enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(read(e.getRequestBody(),1024*1024));
        if(value==null||!value.isObject())throw new IllegalArgumentException("JSON_OBJECT_REQUIRED");return value;
    }
    private static byte[] read(InputStream in,int limit) throws IOException {
        ByteArrayOutputStream out=new ByteArrayOutputStream();byte[] buffer=new byte[8192];int n;
        while((n=in.read(buffer))!=-1){if(out.size()+n>limit)throw new IllegalArgumentException("BODY_TOO_LARGE");out.write(buffer,0,n);}return out.toByteArray();
    }
    private void error(HttpExchange e,int status,String code) throws IOException {json(e,status,Collections.singletonMap("code",code));}
    private void json(HttpExchange e,int status,Object value) throws IOException {
        AuditScope scope=auditScope.get();if(scope!=null&&!scope.done){String code=status>=400&&value instanceof Map?String.valueOf(((Map<?,?>)value).get("code")):null;tasks.auditComplete(scope.begin,status,code);scope.done=true;}
        rawJson(e,status,value);
    }
    private static void rawJson(HttpExchange e,int status,Object value) throws IOException {
        byte[] bytes=Model.JSON.writeValueAsBytes(value);e.getResponseHeaders().set("Content-Type","application/json; charset=utf-8");
        e.sendResponseHeaders(status,bytes.length);e.getResponseBody().write(bytes);
    }
    @Override public void close(){if(closed.compareAndSet(false,true)){server.stop(1);executor.shutdownNow();if(global!=null)global.close();nodes.close();if(sessions!=null)sessions.close();}}
}
