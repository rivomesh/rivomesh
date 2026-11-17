package io.github.rivomesh.acceptance;

import io.github.rivomesh.*;
import io.github.rivomesh.cdc.*;
import io.github.rivomesh.validation.ValidationStore;
import io.github.rivomesh.repair.*;
import io.github.rivomesh.platform.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Native lock, MVCC, metadata, process-loss and GTID/SQL-fence verification. */
public final class ValidationBarrierAcceptance {
    static final String JAR=Paths.get(System.getenv().getOrDefault("RIVOMESH_TEST_ARTIFACT","/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar")).toAbsolutePath().normalize().toString();
    static final long WAIT_SECONDS=Long.parseLong(System.getenv().getOrDefault("RIVOMESH_VALIDATION_WAIT_SECONDS","90"));
    String lastObservedState="";
    final Path directory=Paths.get(System.getenv("RIVOMESH_VALIDATION_DIR"));
    final String id="vb_"+UUID.randomUUID().toString().replace("-","").substring(0,18);
    final PipelineConfig config=new PipelineConfig();Path configuration;Process worker;int starts;
    String managedRepairReport,managedRepairPlan;
    final ExecutorService writers=Executors.newFixedThreadPool(2);
    interface Condition{boolean ready()throws Exception;}
    static void require(boolean value,String why){if(!value)throw new AssertionError(why);}
    void await(Condition condition)throws Exception{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(WAIT_SECONDS);String last="";while(System.nanoTime()<deadline){try{if(condition.ready())return;}catch(SQLException e){last=e.getSQLState();}Thread.sleep(50);}throw new AssertionError("Native barrier condition not met; SQLState="+last+"; lastObserved="+lastObservedState);}
    Connection mysql(String host)throws Exception{return JdbcConnections.open("com.mysql.cj.jdbc.Driver","jdbc:mysql://"+host+":3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000","root","Sync@Test123");}
    Connection pg()throws Exception{return JdbcConnections.open("org.postgresql.Driver","jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10","sync_test","Sync@Test123");}
    void source(String sql)throws Exception{try(Connection c=mysql("mysql");Statement s=c.createStatement()){s.execute(sql);}}
    void sql(PipelineConfig.Target target,String query)throws Exception{try(Connection c=target.product==TargetProduct.MYSQL?mysql("mysql-target"):pg();Statement s=c.createStatement()){s.execute(query);}}
    long count(PipelineConfig.Target target,String sourceDb,String table)throws Exception{try(Connection c=target.product==TargetProduct.MYSQL?mysql("mysql-target"):pg();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT count(*) FROM "+q(target,target.mappings.get(sourceDb))+"."+q(target,table))){r.next();return r.getLong(1);}}
    static String q(PipelineConfig.Target t,String s){return t.product==TargetProduct.MYSQL?Sql.mysql(s):Sql.pg(s);}
    static JsonNode http(String base,String path,Object body,String request,int expected)throws Exception{
        return httpToken(base,path,body,request,expected,"native-managed-validation-fixture-token");
    }
    static JsonNode httpToken(String base,String path,Object body,String request,int expected,String token)throws Exception{
        java.net.HttpURLConnection connection=(java.net.HttpURLConnection)new java.net.URL(base+path).openConnection();connection.setConnectTimeout(3000);connection.setReadTimeout(10000);connection.setRequestProperty("Authorization","Bearer "+token);
        try{if(body!=null){connection.setRequestMethod("POST");connection.setDoOutput(true);connection.setRequestProperty("Content-Type","application/json");connection.setRequestProperty("X-Request-ID",request);try(java.io.OutputStream out=connection.getOutputStream()){out.write(Codec.encode(body).getBytes(java.nio.charset.StandardCharsets.UTF_8));}}
            int status=connection.getResponseCode();try(java.io.InputStream in=status>=400?connection.getErrorStream():connection.getInputStream()){JsonNode result=Codec.JSON.readTree(in);require(status==expected,"Managed validation HTTP status="+status+",code="+result.path("code").asText());return result;}
        }finally{connection.disconnect();}
    }
    static Map<String,Object> action(JsonNode task,String action){Map<String,Object> body=new LinkedHashMap<>();body.put("action",action);body.put("expectedRevision",task.path("revision").asText());return body;}
    static boolean operation(JsonNode task,String request,String code){for(JsonNode op:task.path("operations"))if(op.path("id").asText().equals(request)){String actual=op.path("code").asText();if(actual.equals(code))return true;if(Arrays.asList("FAILED","SUCCEEDED").contains(op.path("status").asText()))throw new AssertionError("Operation "+request+" finished "+actual+"; expected "+code);}return false;}
    static final class TracingBackend implements TaskBackend {
        final TaskBackend.Database database=new TaskBackend.Database();
        public Map<String,Object> status(Config c)throws Exception{return database.status(c);}public void pause(Config c)throws Exception{database.pause(c);}public void resume(Config c,long g)throws Exception{database.resume(c,g);}public Map<String,Object> check(Config c)throws Exception{return database.check(c);}
        public Map<String,Object> repair(TaskDefinition task,Path reports,RepairRequest request,boolean execute,String actor,String operation,String createdAt)throws Exception{
            try{return TaskBackend.super.repair(task,reports,request,execute,actor,operation,createdAt);}catch(Exception failure){if("CONTROL_FAILED".equals(TaskService.safeCode(failure))){System.err.println("NATIVE_UNEXPECTED_REPAIR_FAILURE");failure.printStackTrace();}throw failure;}
        }
    }
    void managedValidation()throws Exception{
        PipelineConfig managed=Codec.decode(Codec.encode(config),PipelineConfig.class);managed.pipelineId=id+"_web";managed.source.serverId++;managed.journal.namespace=id+"_web_journal";
        for(PipelineConfig.Target target:managed.targets){target.metadataSchema+="_web";for(Map.Entry<String,String> mapping:target.mappings.entrySet())mapping.setValue(mapping.getValue()+"_web");}
        com.fasterxml.jackson.databind.node.ObjectNode document=(com.fasterxml.jackson.databind.node.ObjectNode)Codec.JSON.valueToTree(managed);((com.fasterxml.jackson.databind.node.ObjectNode)document.get("journal")).remove("path");
        PlatformConfig platform=new PlatformConfig();platform.port=0;platform.pollMillis=100;platform.authToken="native-managed-validation-fixture-token";platform.tasksDirectory=directory.resolve("managed/tasks").toString();platform.dataDirectory=directory.resolve("managed/data").toString();String route="/api/tasks/"+managed.pipelineId,report;
        PlatformConfig.User viewer=new PlatformConfig.User();viewer.id="detail-viewer";viewer.role=PlatformConfig.Role.VIEWER;viewer.token="native-detail-viewer-fixture-token-at-least-32";viewer.taskIds=Collections.singletonList(managed.pipelineId);platform.users.add(viewer);
        try(TaskService tasks=new TaskService(platform,new TracingBackend(),TaskWorker.Jvm::new);ConsoleServer server=new ConsoleServer(platform,tasks)){
            server.start();String base="http://127.0.0.1:"+server.port();JsonNode created=http(base,"/api/tasks",Collections.singletonMap("config",document),"managed_create",201);
            await(()->http(base,route,null,null,200).path("statusAvailable").asBoolean());
            if("EXTERNAL".equals(managed.journal.mode)&&managed.journal.product==TargetProduct.MYSQL)try(Connection c=mysql("mysql-journal");PreparedStatement query=c.prepareStatement("SELECT count(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?")){query.setString(1,managed.journal.namespace);try(ResultSet result=query.executeQuery()){result.next();require(result.getLong(1)==0,"Status inspection created the MySQL journal namespace before START");}}
            http(base,route+"/actions",action(created,"START"),"managed_start",202);
            await(()->{JsonNode task=http(base,route,null,null,200);lastObservedState="worker="+task.path("workerState").asText()+", phase="+task.path("status").path("phase").asText()+", error="+task.path("status").path("error_code").asText()+", snapshotTables="+task.path("status").path("completed_tables").asLong()+"/"+task.path("status").path("table_count").asLong()+", targetAcks="+task.path("status").path("snapshotAcknowledgments");return task.path("workerState").asText().equals("RUNNING")&&task.path("status").path("phase").asText().equals("STREAMING");});
            Map<String,Object> start=action(http(base,route,null,null,200),"VALIDATE");ValidationRequest options=new ValidationRequest();options.targets.add("postgres");options.gateMillis=15000;options.sourceMillis=60000;options.targetMillis=30000;start.put("validation",options);
            JsonNode accepted=http(base,route+"/actions",start,"managed_validate",202);report=accepted.path("operations").get(accepted.path("operations").size()-1).path("validation").path("reportId").asText();require(!report.isEmpty(),"Managed report ID was not persisted before acceptance");http(base,route+"/actions",start,"managed_validate",202);
            await(()->operation(http(base,route,null,null,200),"managed_validate","VALIDATION_MATCH"));require(http(base,route+"/validations/"+report,null,null,200).path("code").asText().equals("VALIDATION_MATCH"),"Managed report did not match");
            await(()->http(base,route,null,null,200).path("status").path("run_state").asText().equals("PAUSED"));JsonNode paused=http(base,route,null,null,200);Map<String,Object> resume=action(paused,"RESUME");resume.put("errorCount",paused.path("status").path("error_count").asLong());http(base,route+"/actions",resume,"managed_resume",202);
            await(()->{JsonNode task=http(base,route,null,null,200);return task.path("workerState").asText().equals("RUNNING")&&task.path("status").path("run_state").asText().equals("RUNNING");});
            insert(5);await(()->count(managed.targets.get(0),id+"_sales","items")==5&&count(managed.targets.get(1),id+"_sales","items")==5);
            String blockedReport;
            try(Connection block=pg();Statement lock=block.createStatement()){
                block.setAutoCommit(false);String targetTable=Sql.pg(managed.targets.get(0).mappings.get(id+"_sales"))+".items";lock.execute("LOCK TABLE "+targetTable+" IN ACCESS EXCLUSIVE MODE");
                Map<String,Object> second=action(http(base,route,null,null,200),"VALIDATE");second.put("validation",options);JsonNode started=http(base,route+"/actions",second,"managed_cancel_scan",202);blockedReport=started.path("operations").get(started.path("operations").size()-1).path("validation").path("reportId").asText();
                await(()->{try(Connection c=pg();Statement query=c.createStatement();ResultSet r=query.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE state='active' AND wait_event_type='Lock' AND query LIKE '%_web%' AND query LIKE '%items%'")){r.next();return r.getLong(1)>0;}});
                http(base,route+"/actions",action(http(base,route,null,null,200),"VALIDATE_CANCEL"),"managed_cancel",202);await(()->operation(http(base,route,null,null,200),"managed_cancel_scan","VALIDATION_CANCELLED"));block.rollback();
            }
            JsonNode interrupted=http(base,route+"/validations/"+blockedReport,null,null,200);require(interrupted.path("sourceComplete").asBoolean(),"Cancellation discarded the sealed source");
            Map<String,Object> retry=action(http(base,route,null,null,200),"VALIDATE_RETRY");retry.put("validation",Collections.singletonMap("reportId",blockedReport));http(base,route+"/actions",retry,"managed_retry",202);await(()->operation(http(base,route,null,null,200),"managed_retry","VALIDATION_MATCH"));report=blockedReport;
            await(()->http(base,route,null,null,200).path("status").path("run_state").asText().equals("PAUSED"));JsonNode cut=http(base,route,null,null,200);Map<String,Object> again=action(cut,"RESUME");again.put("errorCount",cut.path("status").path("error_count").asLong());http(base,route+"/actions",again,"details_resume",202);
            await(()->{JsonNode task=http(base,route,null,null,200);return task.path("workerState").asText().equals("RUNNING")&&task.path("status").path("run_state").asText().equals("RUNNING");});insert(6);await(()->count(managed.targets.get(0),id+"_sales","items")==6&&count(managed.targets.get(1),id+"_sales","items")==6);
            PipelineConfig.Target pgTarget=managed.targets.get(0);String items=Sql.pg(pgTarget.mappings.get(id+"_sales"))+".items",duplicates=Sql.pg(pgTarget.mappings.get(id+"_sales"))+".duplicates";
            sql(pgTarget,"UPDATE "+items+" SET note='<img src=x onerror=alert(1)>' WHERE id=1");sql(pgTarget,"DELETE FROM "+items+" WHERE id=2");sql(pgTarget,"INSERT INTO "+items+"(id,note) VALUES(99,'public-extra')");sql(pgTarget,"UPDATE "+duplicates+" SET \"_rivomesh_row_id\"='"+Codec.sha256("public-wrong-occurrence")+"' WHERE \"_rivomesh_row_id\"=(SELECT min(\"_rivomesh_row_id\") FROM "+duplicates+")");
            PipelineConfig.Target myTarget=managed.targets.get(1);String myItems=q(myTarget,myTarget.mappings.get(id+"_sales"))+".items",myDuplicates=q(myTarget,myTarget.mappings.get(id+"_sales"))+".duplicates";
            sql(myTarget,"UPDATE "+myItems+" SET note='mysql-difference' WHERE id=1");sql(myTarget,"DELETE FROM "+myItems+" WHERE id=2");sql(myTarget,"INSERT INTO "+myItems+"(id,note) VALUES(99,'mysql-extra')");sql(myTarget,"UPDATE "+myDuplicates+" SET `_rivomesh_row_id`='"+Codec.sha256("mysql-wrong-occurrence")+"' ORDER BY `_rivomesh_row_id` LIMIT 1");options.targets.add("shadow");
            Map<String,Object> different=action(http(base,route,null,null,200),"VALIDATE");different.put("validation",options);JsonNode submitted=http(base,route+"/actions",different,"managed_details",202);String detailReport=submitted.path("operations").get(submitted.path("operations").size()-1).path("validation").path("reportId").asText();await(()->operation(http(base,route,null,null,200),"managed_details","VALIDATION_DIFFERENCES"));
            String endpoint=route+"/validations/"+detailReport;require(httpToken(base,endpoint,null,null,200,viewer.token).path("code").asText().equals("VALIDATION_DIFFERENCES"),"Scoped viewer summary unavailable");
            for(String tableName:Arrays.asList("items","duplicates")){
                Map<String,Object> pageRequest=new LinkedHashMap<>();pageRequest.put("target","postgres");pageRequest.put("tableId",TableSchema.identity(id+"_sales","",tableName));pageRequest.put("limit",1);String cursor="";int seen=0;
                do{pageRequest.put("after",cursor);JsonNode page=http(base,endpoint+"/differences",pageRequest,"detail_page_"+tableName+seen,200);require(page.path("items").size()==1,"Public difference keyset page missing");JsonNode difference=page.path("items").get(0);Map<String,Object> rowRequest=new LinkedHashMap<>(pageRequest);rowRequest.put("identityHash",difference.path("identityHash").asText());
                    httpToken(base,endpoint+"/values",rowRequest,"denied_value_"+tableName+seen,403,viewer.token);httpToken(base,endpoint+"/differences",pageRequest,"denied_page_"+tableName+seen,403,viewer.token);
                    JsonNode values=http(base,endpoint+"/values",rowRequest,"raw_values_"+tableName+seen,200);String kind=difference.path("kind").asText();
                    if(kind.equals("MISSING"))require(values.path("values").get("actual").isNull()&&values.path("values").path("expected").isArray(),"Missing row confused with SQL NULL");else if(kind.equals("EXTRA"))require(values.path("values").get("expected").isNull()&&values.path("values").path("actual").isArray(),"Extra row confused with SQL NULL");else require(values.path("values").path("actual").get(1).path("value").asText().contains("<img"),"Changed raw row not preserved");
                    seen++;cursor=page.hasNonNull("next")?page.path("next").asText():null;
                }while(cursor!=null);require(seen==(tableName.equals("items")?3:2),"Public difference page omitted rows");
            }
            System.out.println("MANAGED_DIFFERENCE_DETAILS_ACCEPTED: actual SQL missing/extra/changed and keyless identity details, one-item keyset pages, raw values/null/absent distinctions and viewer refusal; no repair performed");
            Path projected=Paths.get(platform.dataDirectory).resolve("worker-configs").resolve(managed.pipelineId+".yml");phase("stop-repair-source");try{repairAcceptance(PipelineConfig.load(projected),Paths.get(platform.dataDirectory).resolve("validations").resolve(managed.pipelineId),detailReport,projected);}finally{phase("start-repair-source");}managedRepairAcceptance(base,route,platform,PipelineConfig.load(projected),viewer.token);
        }
        try(TaskService tasks=new TaskService(platform,new TracingBackend(),TaskWorker.Jvm::new);ConsoleServer server=new ConsoleServer(platform,tasks)){
            server.start();String base="http://127.0.0.1:"+server.port();require(http(base,route+"/validations/"+report,null,null,200).path("code").asText().equals("VALIDATION_MATCH"),"Controller restart lost its report");require(!http(base,route,null,null,200).path("desired").asText().equals("RUNNING"),"Controller restart resumed a validation pause");require(http(base,route+"/validations/"+managedRepairReport+"/repairs/"+managedRepairPlan,null,null,200).path("id").asText().equals(managedRepairPlan),"Controller restart lost reviewed plan");require(operation(http(base,route,null,null,200),"managed_repair_retry","REPAIR_COMPLETED"),"Controller restart lost repair completion");
            long after=0,through=0;boolean audited=false;while(true){JsonNode page=http(base,"/api/audit?limit=500&after="+after+"&through="+through,null,null,200);if(through==0)through=page.path("through").asLong();for(JsonNode event:page.path("events"))if(event.path("action").asText().equals("VALIDATE_CANCEL")&&event.path("subjectId").asText().equals(report))audited=true;if(!page.hasNonNull("next"))break;after=page.path("next").asLong();}require(audited,"Managed cancellation report audit missing");
        }
        System.out.println("MANAGED_VALIDATION_HTTP_ACCEPTED: actual controller/worker start, request replay, SQL comparison, physical table-lock blocked scan cancellation, sealed-source retry, report persistence and audit across controller restart");
    }
    String submitRepair(String base,String route,String action,String request,RepairRequest repair)throws Exception{
        Map<String,Object> body=action(http(base,route,null,null,200),action);if(repair!=null)body.put("repair",repair);JsonNode accepted=http(base,route+"/actions",body,request,202);return repair==null?null:accepted.path("operations").get(accepted.path("operations").size()-1).path("repair").path("planId").asText();
    }
    void managedRepairAcceptance(String base,String route,PlatformConfig platform,PipelineConfig managed,String viewer)throws Exception{
        JsonNode paused=http(base,route,null,null,200);Map<String,Object> resume=action(paused,"RESUME");resume.put("errorCount",paused.path("status").path("error_count").asLong());http(base,route+"/actions",resume,"repair_new_cut_resume",202);await(()->http(base,route,null,null,200).path("status").path("run_state").asText().equals("RUNNING"));
        for(PipelineConfig.Target target:managed.targets){String items=q(target,target.mappings.get(id+"_sales"))+".items",duplicates=q(target,target.mappings.get(id+"_sales"))+".duplicates";sql(target,"UPDATE "+items+" SET note='managed-repair-drift' WHERE id=1");if(target.product==TargetProduct.MYSQL)sql(target,"UPDATE "+duplicates+" SET `_rivomesh_row_id`='"+Codec.sha256("managed-wrong-my")+"' ORDER BY `_rivomesh_row_id` LIMIT 1");else sql(target,"UPDATE "+duplicates+" SET \"_rivomesh_row_id\"='"+Codec.sha256("managed-wrong-pg")+"' WHERE \"_rivomesh_row_id\"=(SELECT min(\"_rivomesh_row_id\") FROM "+duplicates+")");}
        ValidationRequest options=new ValidationRequest();options.targets.addAll(Arrays.asList("postgres","shadow"));options.gateMillis=15000;options.sourceMillis=60000;options.targetMillis=30000;Map<String,Object> validate=action(http(base,route,null,null,200),"VALIDATE");validate.put("validation",options);JsonNode accepted=http(base,route+"/actions",validate,"managed_repair_validate",202);String report=accepted.path("operations").get(accepted.path("operations").size()-1).path("validation").path("reportId").asText();await(()->operation(http(base,route,null,null,200),"managed_repair_validate","VALIDATION_DIFFERENCES"));managedRepairReport=report;
        JsonNode summary=http(base,route+"/validations/"+report,null,null,200);RepairRequest request=new RepairRequest();request.reportId=report;summary.path("results").fields().forEachRemaining(entry->{JsonNode row=entry.getValue();if(row.path("missing").asLong()+row.path("extra").asLong()+row.path("changed").asLong()>0)request.scope.add(entry.getKey());});require(request.scope.size()==4,"Managed repair scope lost a keyed/keyless destination");
        PipelineConfig.Target pg=managed.targets.get(0),my=managed.targets.get(1);String items=q(pg,pg.mappings.get(id+"_sales"))+".items",function=q(pg,pg.mappings.get(id+"_sales"))+".repair_noop",child=q(pg,pg.mappings.get(id+"_sales"))+".repair_child";
        sql(pg,"CREATE FUNCTION "+function+"() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN OLD; END;$$");sql(pg,"CREATE TRIGGER repair_trigger BEFORE DELETE ON "+items+" FOR EACH ROW EXECUTE FUNCTION "+function+"()");submitRepair(base,route,"REPAIR_PLAN","reject_pg_trigger",request);await(()->operation(http(base,route,null,null,200),"reject_pg_trigger","REPAIR_DEPENDENCY_PLAN_REQUIRED"));sql(pg,"DROP TRIGGER repair_trigger ON "+items);sql(pg,"DROP FUNCTION "+function+"()");
        sql(pg,"CREATE TABLE "+child+"(id BIGINT PRIMARY KEY,parent BIGINT REFERENCES "+items+"(id) ON DELETE CASCADE)");sql(pg,"INSERT INTO "+child+" VALUES(1,1)");submitRepair(base,route,"REPAIR_PLAN","reject_cascade_fk",request);await(()->operation(http(base,route,null,null,200),"reject_cascade_fk","REPAIR_DEPENDENCY_PLAN_REQUIRED"));require(count(pg,id+"_sales","repair_child")==1,"Dependency check changed cascaded data");sql(pg,"DROP TABLE "+child);
        String myItems=q(my,my.mappings.get(id+"_sales"))+".items",trigger=q(my,my.mappings.get(id+"_sales"))+".repair_trigger";sql(my,"CREATE TRIGGER "+trigger+" BEFORE DELETE ON "+myItems+" FOR EACH ROW SET @rivomesh_fixture_trigger=1");submitRepair(base,route,"REPAIR_PLAN","reject_my_trigger",request);await(()->operation(http(base,route,null,null,200),"reject_my_trigger","REPAIR_DEPENDENCY_PLAN_REQUIRED"));
        Path reports=Paths.get(platform.dataDirectory).resolve("validations").resolve(managed.pipelineId);PipelineValidationBarrier.Certificate cut;TableSchema itemSchema;try(ValidationStore store=ValidationStore.open(reports,report)){cut=store.manifest().certificate;itemSchema=store.manifest().schemas.values().stream().filter(t->t.name.equals("items")).findFirst().get();}
        String limited=id+"_repair";sql(my,"CREATE USER '"+limited+"'@'%' IDENTIFIED BY 'FixtureOnly-Test123!'");sql(my,"GRANT SELECT ON *.* TO '"+limited+"'@'%'");sql(my,"GRANT ALL ON "+q(my,my.metadataSchema)+".* TO '"+limited+"'@'%'");for(String schema:my.mappings.values())sql(my,"GRANT SELECT,INSERT,UPDATE,DELETE ON "+q(my,schema)+".* TO '"+limited+"'@'%'");JdbcSink.Settings restricted=managed.targetSettings(my,cut.generation);restricted.username=limited;restricted.password="FixtureOnly-Test123!";
        try(JdbcSink sink=JdbcSink.existing(restricted)){try{sink.checkRepairDependencies(itemSchema,cut.targetCheckpoints.get(my.id));throw new AssertionError("Invisible trigger metadata was accepted");}catch(IllegalStateException expected){require(expected.getMessage().equals("REPAIR_METADATA_VISIBILITY_REQUIRED"),"Wrong metadata visibility guard");}}finally{sql(my,"DROP USER '"+limited+"'@'%'");sql(my,"DROP TRIGGER "+trigger);}
        String stale=http(base,route,null,null,200).path("revision").asText();String planId=submitRepair(base,route,"REPAIR_PLAN","managed_repair_plan",request);managedRepairPlan=planId;await(()->operation(http(base,route,null,null,200),"managed_repair_plan","REPAIR_PLAN_READY"));String endpoint=route+"/validations/"+report+"/repairs/"+planId;httpToken(base,endpoint,null,null,403,viewer);JsonNode review=http(base,endpoint,null,null,200);String hash=review.path("hash").asText();require(review.path("createdBy").asText().equals("legacy-admin"),"Plan actor not derived from authentication");
        RepairRequest execute=new RepairRequest();execute.reportId=report;execute.planId=planId;execute.approvedHash=Codec.sha256("wrong");submitRepair(base,route,"REPAIR_EXECUTE","managed_repair_wrong_hash",execute);await(()->operation(http(base,route,null,null,200),"managed_repair_wrong_hash","REPAIR_APPROVAL_HASH_MISMATCH"));execute.approvedHash=hash;
        Map<String,Object> staleBody=new LinkedHashMap<>();staleBody.put("action","REPAIR_EXECUTE");staleBody.put("expectedRevision",stale);staleBody.put("repair",execute);http(base,route+"/actions",staleBody,"managed_repair_stale",409);httpToken(base,route+"/actions",staleBody,"managed_repair_viewer",403,viewer);
        phase("stop-managed-repair-source");try{
            try(Connection block=pg();Statement lock=block.createStatement()){block.setAutoCommit(false);lock.execute("LOCK TABLE "+items+" IN ACCESS EXCLUSIVE MODE");submitRepair(base,route,"REPAIR_EXECUTE","managed_repair_blocked",execute);
                await(()->{try(Connection c=pg();Statement st=c.createStatement();ResultSet rows=st.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE state='active' AND wait_event_type='Lock' AND query LIKE '%_web%' AND query LIKE '%items%'")){rows.next();return rows.getLong(1)>0;}});submitRepair(base,route,"REPAIR_CANCEL","managed_repair_cancel",null);await(()->operation(http(base,route,null,null,200),"managed_repair_blocked","REPAIR_CANCELLED"));block.rollback();}
            submitRepair(base,route,"REPAIR_EXECUTE","managed_repair_retry",execute);await(()->operation(http(base,route,null,null,200),"managed_repair_retry","REPAIR_COMPLETED"));RepairPlan plan=RepairPlans.load(reports,report,planId);for(RepairPlan.Table table:plan.tables.values())require(repairRoot(managed,plan,table,directory.resolve("managed-proof-probes")).equals(table.afterRoot),"Managed repair did not restore sealed roots");
            JsonNode task=http(base,route,null,null,200);require(task.path("desired").asText().equals("PAUSED"),"Managed repair resumed replication");JsonNode operation=null;for(JsonNode op:task.path("operations"))if(op.path("id").asText().equals("managed_repair_retry"))operation=op;require(operation!=null&&operation.path("result").path("receipts").size()==4,"Managed repair did not persist receipts");
        }finally{phase("start-managed-repair-source");}
        System.out.println("MANAGED_REPAIR_HTTP_ACCEPTED: authenticated plan review/hash/revision checks, trigger/cascade/metadata-visibility refusal, actual relation-lock cancellation, source-offline repair retry, keyed/keyless equality and four persisted receipts; task stays paused");
    }
    static final class CommitFault {
        static volatile String mode;
        static volatile Path ready;
    }
    /** Native-only wrapper: every statement and commit still executes on the actual vendor connection. */
    public static final class CommitFaultDriver implements java.sql.Driver {
        public Connection connect(String url,Properties properties)throws SQLException{
            Connection real=JdbcConnections.open(url.startsWith("jdbc:mysql:")?"com.mysql.cj.jdbc.Driver":"org.postgresql.Driver",url,properties.getProperty("user"),properties.getProperty("password"));boolean[] repair={false};
            return (Connection)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args)->{
                try{
                    if(method.getName().equals("prepareStatement")&&args!=null&&args[0] instanceof String&&((String)args[0]).startsWith("INSERT INTO ")&&((String)args[0]).contains("rm3_sink_repair"))repair[0]=true;
                    if(method.getName().equals("commit")&&repair[0]&&CommitFault.mode!=null){
                        String mode=CommitFault.mode;CommitFault.mode=null;
                        if(mode.equals("before")){Files.write(CommitFault.ready,new byte[]{1});while(true)Thread.sleep(1000);}
                        Object result=method.invoke(real,args);
                        if(mode.equals("throw"))throw new SQLRecoverableException("NATIVE_LOST_COMMIT_REPLY","08006");
                        Files.write(CommitFault.ready,new byte[]{1});while(true)Thread.sleep(1000);
                    }
                    return method.invoke(real,args);
                }catch(java.lang.reflect.InvocationTargetException error){throw error.getCause();}
            });
        }
        public boolean acceptsURL(String url){return url.startsWith("jdbc:mysql:")||url.startsWith("jdbc:postgresql:");}
        public DriverPropertyInfo[] getPropertyInfo(String url,Properties p){return new DriverPropertyInfo[0];}
        public int getMajorVersion(){return 1;}public int getMinorVersion(){return 0;}public boolean jdbcCompliant(){return false;}public java.util.logging.Logger getParentLogger(){return java.util.logging.Logger.getGlobal();}
    }
    static String repairRoot(PipelineConfig config,RepairPlan plan,RepairPlan.Table selected,Path scratch)throws Exception{
        PipelineConfig.Target target=config.targets.stream().filter(t->t.id.equals(selected.target)).findFirst().get();TableSchema table;
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings())){table=journal.schemas().stream().filter(t->t.id.equals(selected.tableId)).findFirst().get();}
        try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target,plan.certificate.generation));TargetRead reader=sink.validationRead(table,plan.certificate.targetCheckpoints.get(target.id),30000,plan.maximumRowBytes);RepairProbe probe=new RepairProbe(scratch,table,plan.maximumProbeBytes,100000,plan.maximumRowBytes)){
            while(true){List<TargetRead.Row> rows=reader.next(100);if(rows.isEmpty())break;for(TargetRead.Row row:rows)probe.append(row);}return probe.finish().root;
        }
    }
    static JdbcSink.RepairReceipt directRepair(PipelineConfig config,Path reports,RepairPlan plan,RepairPlan.Table selected,boolean faulty)throws Exception{
        PipelineConfig.Target target=config.targets.stream().filter(t->t.id.equals(selected.target)).findFirst().get();JdbcSink.Settings settings=config.targetSettings(target,plan.certificate.generation);if(faulty)settings.driver=CommitFaultDriver.class.getName();
        try(ValidationStore store=ValidationStore.open(reports,plan.reportId);JdbcJournal journal=new JdbcJournal(config.existingJournalSettings());PausedRepairFence fence=new PausedRepairFence(journal,plan.certificate,30000);JdbcSink sink=JdbcSink.existing(settings)){
            sink.prepareRepair();return sink.repair(plan,selected,store,reports.resolve(plan.reportId).resolve("native-probes"),"native-admin",Codec.sha256("native-repair-request"),fence);
        }
    }
    void killRepair(PipelineConfig config,Path configuration,Path reports,RepairPlan plan,RepairPlan.Table table,String mode)throws Exception{
        Path ready=directory.resolve("repair-"+mode+".ready");Process child=new ProcessBuilder("java","-Xmx128m","-cp",JAR+":/project/target/validation-barrier-tests",getClass().getName(),"repair-fault",configuration.toString(),reports.toString(),plan.reportId,plan.id,table.target+"/"+table.tableId,mode,ready.toString()).redirectErrorStream(true).redirectOutput(directory.resolve("repair-"+mode+".log").toFile()).start();
        try{await(()->{if(!child.isAlive())throw new AssertionError("Repair fault child exited before commit boundary");return Files.exists(ready);});child.destroyForcibly();require(child.waitFor(10,TimeUnit.SECONDS),"Repair SIGKILL unconfirmed");}finally{if(child.isAlive()){child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);}}
    }
    void repairAcceptance(PipelineConfig managed,Path reports,String reportId,Path projected)throws Exception{
        Set<String> scope=new TreeSet<>();try(ValidationStore store=ValidationStore.open(reports,reportId)){for(Map.Entry<String,ValidationStore.TableResult> entry:store.manifest().results.entrySet())if(!entry.getValue().matches())scope.add(entry.getKey());}
        String planJson=cli("repair-plan","repair-plan",projected.toString(),reports.toString(),reportId,String.join(",",scope),"native-admin");RepairPlan plan=Codec.decode(planJson.substring(planJson.indexOf('{')),RepairPlan.class);plan.validate();require(plan.tables.size()==4,"Repair plan lost a destination/keyless scope");
        try{PipelineRepair.execute(managed,reports,reportId,plan.id,Codec.sha256("wrong approval"),"native-admin",UUID.randomUUID().toString());throw new AssertionError("Changed approval hash accepted");}catch(IllegalArgumentException expected){require(expected.getMessage().equals("REPAIR_APPROVAL_HASH_MISMATCH"),"Wrong approval guard");}
        RepairPlan.Table pgItems=plan.tables.values().stream().filter(t->t.target.equals("postgres")&&t.tableId.equals(TableSchema.identity(id+"_sales","","items"))).findFirst().get();RepairPlan.Table myItems=plan.tables.values().stream().filter(t->t.target.equals("shadow")&&t.tableId.equals(pgItems.tableId)).findFirst().get();
        PipelineConfig.Target pg=managed.targets.get(0);String items=q(pg,pg.mappings.get(id+"_sales"))+".items";
        sql(pg,"UPDATE "+items+" SET note='later-drift' WHERE id=1");try{directRepair(managed,reports,plan,pgItems,false);throw new AssertionError("Live drift was repaired without a new plan");}catch(IllegalStateException expected){require(expected.getMessage().equals("REPAIR_TARGET_DRIFTED"),"Wrong drift guard");}sql(pg,"UPDATE "+items+" SET note='<img src=x onerror=alert(1)>' WHERE id=1");
        sql(pg,"ALTER TABLE "+items+" ADD CONSTRAINT repair_block CHECK(id<>2)");try{directRepair(managed,reports,plan,pgItems,false);throw new AssertionError("SQL failure was ignored");}catch(SQLException expected){require(expected.getSQLState().equals("23514"),"Wrong SQL repair error");}finally{sql(pg,"ALTER TABLE "+items+" DROP CONSTRAINT repair_block");}require(repairRoot(managed,plan,pgItems,directory.resolve("native-repair-probes")).equals(pgItems.beforeRoot),"Failed repair left partial business writes");
        CommitFault.mode="throw";try{directRepair(managed,reports,plan,pgItems,true);throw new AssertionError("Lost commit reply not injected");}catch(SQLRecoverableException expected){require(expected.getMessage().equals("NATIVE_LOST_COMMIT_REPLY"),"Wrong uncertain commit");}
        require(repairRoot(managed,plan,pgItems,directory.resolve("native-repair-probes")).equals(pgItems.afterRoot),"Lost-reply repair did not actually commit");
        killRepair(managed,projected,reports,plan,myItems,"before");require(repairRoot(managed,plan,myItems,directory.resolve("native-repair-probes")).equals(myItems.beforeRoot),"SIGKILL before commit leaked business writes");
        killRepair(managed,projected,reports,plan,myItems,"after");require(repairRoot(managed,plan,myItems,directory.resolve("native-repair-probes")).equals(myItems.afterRoot),"SIGKILL after commit lost business writes");
        Map<String,JdbcSink.RepairReceipt> receipts=PipelineRepair.execute(managed,reports,reportId,plan.id,plan.hash,"native-admin",UUID.randomUUID().toString());require(receipts.size()==4&&receipts.get("postgres/"+pgItems.tableId).alreadyApplied&&receipts.get("shadow/"+myItems.tableId).alreadyApplied,"Retry did not resolve actual committed receipts");
        for(RepairPlan.Table table:plan.tables.values()){require(repairRoot(managed,plan,table,directory.resolve("native-repair-probes")).equals(table.afterRoot),"Repaired rows do not match sealed source root");PipelineConfig.Target target=managed.targets.stream().filter(t->t.id.equals(table.target)).findFirst().get();try(JdbcSink sink=JdbcSink.existing(managed.targetSettings(target,plan.certificate.generation))){require(Codec.encode(sink.checkpoint()).equals(Codec.encode(plan.certificate.targetCheckpoints.get(table.target))),"Repair advanced CDC checkpoint");}}
        require(cli("repair-replay","repair-execute",projected.toString(),reports.toString(),reportId,plan.id,plan.hash,"native-admin",UUID.randomUUID().toString()).contains("\"alreadyApplied\" : true"),"CLI repair receipt replay failed");
        System.out.println("COMMON_CUT_REPAIR_ACCEPTED: real PostgreSQL/MySQL keyed/keyless roots, reviewed CLI plan/hash, live drift refusal, SQL rollback, lost commit reply, SIGKILL before/after commit, durable receipt replay, exact occurrence restoration and unchanged CDC checkpoints; no automatic resume");
    }
    String business(){return Sql.mysql(id+"_sales")+".items";}
    void configure()throws Exception{
        Files.createDirectories(directory);Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwx------"));config.pipelineId=id;config.nodeId="validation-worker";config.source.clusterId=id;config.source.host="mysql";config.source.username="root";config.source.password="Sync@Test123";config.source.database=id+"_sales";config.source.serverId=992107;config.databases=Arrays.asList(id+"_sales",id+"_billing");config.chunkSize=1;config.pollIntervalMillis=100;config.retryDelayMillis=200;config.leaseMillis=5000;config.heartbeatMillis=300;config.journal.path=directory.resolve("journal").toString();
        PipelineConfig.Target pg=new PipelineConfig.Target();pg.id="postgres";pg.product=TargetProduct.POSTGRESQL;pg.url="jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10";pg.username="sync_test";pg.password="Sync@Test123";pg.metadataSchema=id+"_pg_meta";pg.mappings.put(id+"_sales",id+"_pg_sales");pg.mappings.put(id+"_billing",id+"_pg_billing");config.targets.add(pg);
        PipelineConfig.Target my=new PipelineConfig.Target();my.id="shadow";my.product=TargetProduct.MYSQL;my.url="jdbc:mysql://mysql-target:3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000";my.username="root";my.password="Sync@Test123";my.metadataSchema=id+"_my_meta";my.mappings.put(id+"_sales",id+"_my_sales");my.mappings.put(id+"_billing",id+"_my_billing");config.targets.add(my);
        String mode=System.getenv("RIVOMESH_VALIDATION_JOURNAL");
        if(mode!=null&&!mode.equals("LOCAL")){
            config.journal.mode="EXTERNAL";config.journal.namespace=id+"_journal";config.journal.username=mode.equals("MYSQL")?"root":"sync_test";config.journal.password="Sync@Test123";
            if(mode.equals("MYSQL")){config.journal.product=TargetProduct.MYSQL;config.journal.url="jdbc:mysql://mysql-journal:3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000";}
            else if(mode.equals("POSTGRESQL")){config.journal.product=TargetProduct.POSTGRESQL;config.journal.url="jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10";}
            else if(mode.equals("GAUSS")){config.journal.product=TargetProduct.GAUSS;config.journal.url="jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10";}
            else throw new IllegalArgumentException("Invalid fixture journal mode");
        }
        System.out.println("VALIDATION_JOURNAL_PROFILE: "+config.journal.mode+"/"+(config.journal.mode.equals("LOCAL")?"H2":config.journal.product));
        require(WAIT_SECONDS>=30&&WAIT_SECONDS<=900,"Invalid bounded fixture wait budget");System.out.println("VALIDATION_TIMING_PROFILE: waitSeconds="+WAIT_SECONDS+", leaseMillis="+config.leaseMillis+", heartbeatMillis="+config.heartbeatMillis);
        config.validate();config.prepareLocalDirectory();configuration=directory.resolve("task.yml");Files.write(configuration,new ObjectMapper(new YAMLFactory()).writeValueAsBytes(config));Files.setPosixFilePermissions(configuration,PosixFilePermissions.fromString("rw-------"));
    }
    void start()throws Exception{worker=new ProcessBuilder("java","-Xmx256m","-jar",JAR,"run",configuration.toString()).redirectErrorStream(true).redirectOutput(directory.resolve("worker-"+(++starts)+".log").toFile()).start();}
    void kill()throws Exception{if(worker!=null){worker.destroyForcibly();require(worker.waitFor(15,TimeUnit.SECONDS),"Worker stop unconfirmed");worker=null;}}
    Future<?> write(String statement,AtomicLong connection)throws Exception{return writers.submit(()->{try(Connection c=mysql("mysql");Statement s=c.createStatement()){try(ResultSet r=s.executeQuery("SELECT CONNECTION_ID()")){r.next();connection.set(r.getLong(1));}s.execute(statement);return null;}});}
    void blocked(AtomicLong connection,String state)throws Exception{await(()->{if(connection.get()==0)return false;try(Connection c=mysql("mysql");PreparedStatement p=c.prepareStatement("SELECT STATE FROM information_schema.PROCESSLIST WHERE ID=?")){p.setLong(1,connection.get());try(ResultSet r=p.executeQuery()){return r.next()&&r.getString(1)!=null&&r.getString(1).contains(state);}}});}
    Map<String,List<List<Cell>>> read(SourceAdapter.ConsistentRead view,List<TableSchema> tables)throws Exception{Map<String,List<List<Cell>>> data=new TreeMap<>();for(TableSchema table:tables){List<List<Cell>> rows=new ArrayList<>();try(SourceAdapter.Snapshot cursor=view.table(table)){for(List<List<Cell>> chunk;!(chunk=cursor.next(1)).isEmpty();)rows.addAll(chunk);}data.put(table.name,rows);}return data;}
    void directViews()throws Exception{
        try(MysqlCdcSource source=new MysqlCdcSource(config.legacy(),new TreeSet<>(config.databases),"native-view")){
            List<TableSchema> tables=source.discover();AtomicLong writer=new AtomicLong();Future<?> changed;
            try(SourceAdapter.ConsistentRead view=source.consistentRead(tables,10000,30000)){
                changed=write("UPDATE "+business()+" SET note='after' WHERE id=1",writer);blocked(writer,"global read lock");require(!changed.isDone(),"Source write escaped the gate");view.releaseWrites();changed.get(10,TimeUnit.SECONDS);
                Map<String,List<List<Cell>>> rows=read(view,tables);TableSchema table=tables.stream().filter(t->t.name.equals("items")).findFirst().get();require(rows.get("items").get(0).get(table.column("note")).value.equals("before"),"MVCC view advanced after source write gate release");view.verify();
                AtomicLong ddl=new AtomicLong();Future<?> alter=write("ALTER TABLE "+business()+" ADD COLUMN later INT",ddl);blocked(ddl,"metadata lock");require(!alter.isDone(),"Table definition was not pinned");view.close();alter.get(10,TimeUnit.SECONDS);
            }
            try{source.consistentRead(tables,5000,10000);throw new AssertionError("Old schema accepted");}catch(SyncFailure expected){require("SOURCE_SCHEMA_CHANGED_BEFORE_VALIDATION".equals(expected.code),"Wrong schema guard");}
            tables=source.discover();AtomicLong waiting=new AtomicLong();
            try(SourceAdapter.ConsistentRead timed=source.consistentRead(tables,2000,10000)){
                Future<?> update=write("UPDATE "+business()+" SET note='timeout-released' WHERE id=1",waiting);blocked(waiting,"global read lock");update.get(8,TimeUnit.SECONDS);
                try{timed.verify();throw new AssertionError("Expired source gate accepted");}catch(SyncFailure expected){require("SOURCE_WRITE_GATE_TIMED_OUT".equals(expected.code),"Wrong gate deadline reason");}
            }
            try(SourceAdapter.ConsistentRead timed=source.consistentRead(tables,2000,2500)){
                timed.releaseWrites();Thread.sleep(2800);try{timed.verify();throw new AssertionError("Expired read accepted");}catch(SyncFailure expected){require("SOURCE_CONSISTENT_READ_TIMED_OUT".equals(expected.code),"Wrong snapshot deadline reason");}
            }
            try(SourceAdapter.ConsistentRead partial=source.consistentRead(tables,5000,10000)){
                SourceAdapter.Snapshot cursor=partial.table(tables.stream().filter(t->t.name.equals("items")).findFirst().get());cursor.next(1);cursor.close();
                try{partial.verify();throw new AssertionError("Partial cursor remained eligible");}catch(SyncFailure expected){require("SOURCE_VALIDATION_CURSOR_INCOMPLETE".equals(expected.code),"Wrong partial cursor guard");}
            }
        }
        source("UPDATE "+business()+" SET note='before' WHERE id=1");System.out.println("VALIDATION_SOURCE_VIEWS_ACCEPTED: real global-write blocking/release, shared MVCC, pinned metadata, changed-schema refusal, gate/read deadlines and incomplete-cursor abort");
    }
    void processLoss()throws Exception{
        Path ready=directory.resolve("child.ready");Files.deleteIfExists(ready);
        Process child=new ProcessBuilder("java","-Xmx128m","-cp",JAR+":/project/target/validation-barrier-tests",ValidationBarrierAcceptance.class.getName(),"hold",configuration.toString(),ready.toString()).redirectErrorStream(true).redirectOutput(directory.resolve("holder.log").toFile()).start();
        try{await(()->Files.exists(ready));AtomicLong id=new AtomicLong();Future<?> update=write("UPDATE "+business()+" SET note='process-loss' WHERE id=1",id);blocked(id,"global read lock");child.destroyForcibly();require(child.waitFor(10,TimeUnit.SECONDS),"Holder SIGKILL unconfirmed");update.get(8,TimeUnit.SECONDS);}
        finally{if(child.isAlive()){child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);}}
        source("UPDATE "+business()+" SET note='before' WHERE id=1");System.out.println("VALIDATION_PROCESS_LOSS_ACCEPTED: SIGKILL releases the actual source gate before its configured deadline");
    }
    Journal.Subscription sub(JdbcJournal journal,String id)throws Exception{return journal.subscriptions().stream().filter(s->s.id.equals(id)).findFirst().get();}
    void phase(String name)throws Exception{Files.write(directory.resolve(name+".request"),new byte[]{1});await(()->Files.exists(directory.resolve(name+".done")));}
    void insert(int id)throws Exception{source("INSERT INTO "+business()+" (id,note,payload,clock) VALUES("+id+",'new',JSON_OBJECT('id',"+id+"),'-00:00:00.000001')");}
    String cli(String name,String... arguments)throws Exception{
        Path log=directory.resolve(name+".log");List<String> command=new ArrayList<>(Arrays.asList("java","-Xmx256m","-jar",JAR));command.addAll(Arrays.asList(arguments));Process process=new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try{require(process.waitFor(90,TimeUnit.SECONDS),"Validation CLI timed out");require(process.exitValue()==0,"Validation CLI failed; inspect "+name);return new String(Files.readAllBytes(log),java.nio.charset.StandardCharsets.UTF_8);}
        finally{if(process.isAlive()){process.destroyForcibly();process.waitFor(10,TimeUnit.SECONDS);}}
    }
    void typedReaders()throws Exception{
        for(PipelineConfig.Target target:config.targets){
            JdbcSink.Settings settings=config.targetSettings(target,"typed-reader");settings.pipelineId=id+"_types";settings.id="reader";settings.metadataSchema=id+"_types_meta";settings.fingerprint=Codec.sha256(id+target.id);settings.databases=Collections.singletonMap(id+"_sales",id+"_types_copy");
            TableSchema table=new TableSchema();table.database=id+"_sales";table.name="typed";table.id=TableSchema.identity(table.database,"",table.name);table.primaryKey.add("id");
            TableSchema.Type[] types={TableSchema.Type.INT64,TableSchema.Type.BOOLEAN,TableSchema.Type.FLOAT32,TableSchema.Type.FLOAT64,TableSchema.Type.BYTES,TableSchema.Type.STRING,TableSchema.Type.DATE,TableSchema.Type.DATETIME,TableSchema.Type.TIMESTAMP,TableSchema.Type.TIME,TableSchema.Type.JSON,TableSchema.Type.UINT64,TableSchema.Type.DECIMAL};
            for(int i=0;i<types.length;i++){TableSchema.Column column=new TableSchema.Column();column.name=i==0?"id":"c"+i;column.type=types[i];column.length=100;column.precision=65;column.scale=30;column.nullable=i!=0;table.columns.add(column);}
            List<Cell> values=Arrays.asList(Cell.value("1"),Cell.value("true"),Cell.value("0.1"),Cell.value("1.2345678901234567"),Cell.value("AP+A"),Cell.value("报价'☃😀"),Cell.value("2024-02-29"),Cell.value("2024-02-29 12:34:56.123456"),Cell.value("2024-02-29 12:34:56.123456"),Cell.value("-00:00:00.000001"),Cell.value("{\"n\":18446744073709551615,\"a\":[true,null]}"),Cell.value("18446744073709551615"),Cell.value("12345678901234567890123456789012345.123456789012345678901234567890"));
            Journal.SnapshotState snapshot=new Journal.SnapshotState();snapshot.tableId=table.id;snapshot.schemaVersion=table.version;snapshot.snapshotId=UUID.randomUUID().toString();snapshot.state="COMPLETE";snapshot.nextPart=1;snapshot.rows=2;Journal.SnapshotPart part=new Journal.SnapshotPart();List<Cell> nullable=new ArrayList<>();nullable.add(Cell.value("2"));for(int i=1;i<types.length;i++)nullable.add(Cell.value(null));part.rows=Arrays.asList(values,nullable);
            try(JdbcSink sink=new JdbcSink(settings)){
                sink.activate(1);sink.prepare(1,table);sink.beginSnapshot(1,table,snapshot);sink.snapshotPart(1,table,snapshot.snapshotId,part);sink.finishSnapshot(1,table,snapshot);Set<String> actual=new HashSet<>();
                try(TargetRead read=sink.validationRead(table,sink.checkpoint(),15000,65536)){for(List<TargetRead.Row> page;!(page=read.next(1)).isEmpty();)for(TargetRead.Row row:page)actual.add(RowValues.canonical(table,row.values));read.verify();}
                require(actual.equals(new HashSet<>(Arrays.asList(RowValues.canonical(table,values),RowValues.canonical(table,nullable)))),"Typed target read changed values: "+target.product+" fixtureExpected="+RowValues.canonical(table,values)+" fixtureActual="+actual);
                try(TargetRead partial=sink.validationRead(table,sink.checkpoint(),15000,65536)){require(partial.next(1).size()==1,"Partial reader missing");}require(sink.activate(2).epoch==2,"Partial read left the physical checkpoint locked");
            }
        }
        System.out.println("VALIDATION_TYPED_TARGET_READ_ACCEPTED: native PostgreSQL/MySQL bool/float32/float64/bytes/Unicode/date/timestamp/microsecond TIME/JSON/uint64/65digit decimal/NULL and partial-close fence release");
    }
    void reportProcessLoss(String reportId)throws Exception{
        Path ready=directory.resolve("store.ready");Files.deleteIfExists(ready);Path reports=directory.resolve("reports");
        Process child=new ProcessBuilder("java","-Xmx128m","-cp",JAR+":/project/target/validation-barrier-tests",ValidationBarrierAcceptance.class.getName(),"hold-store",reports.toString(),reportId,ready.toString(),configuration.toString()).redirectErrorStream(true).redirectOutput(directory.resolve("store-holder.log").toFile()).start();
        try{await(()->Files.exists(ready));child.destroyForcibly();require(child.waitFor(10,TimeUnit.SECONDS),"Report holder SIGKILL unconfirmed");}
        finally{if(child.isAlive()){child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);}}
        try(ValidationStore recovered=ValidationStore.open(reports,reportId)){require(recovered.manifest().sourceComplete&&recovered.manifest().rows==7&&recovered.manifest().state.equals("SCANNING_TARGETS"),"Sealed source or partial target staging did not survive process loss");}
        System.out.println("VALIDATION_SOURCE_STORE_PROCESS_LOSS_ACCEPTED: actual target scan holder SIGKILL releases its SQL checkpoint lock; private H2 source seal and partial target row reopen for discard/retry");
    }
    void pipeline()throws Exception{
        PipelineConfig.Target pg=config.targets.get(0),my=config.targets.get(1);
        try(JdbcJournal journal=new JdbcJournal(config.journalSettings())){
            start();await(()->count(pg,id+"_sales","items")==2&&count(my,id+"_sales","items")==2&&count(pg,id+"_billing","ledger")==1);
            source("UPDATE "+Sql.mysql(id+"_sales")+".duplicates SET value=8 WHERE value=7 LIMIT 1");
            await(()->{try(Connection c=pg();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT count(*) FROM "+q(pg,pg.mappings.get(id+"_sales"))+".duplicates WHERE value=8")){r.next();return r.getLong(1)==1;}});
            source("UPDATE "+Sql.mysql(id+"_sales")+".duplicates SET value=7 WHERE value=8");
            await(()->{try(Connection c=pg();Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT count(*) FROM "+q(pg,pg.mappings.get(id+"_sales"))+".duplicates WHERE value=7")){r.next();return r.getLong(1)==2;}});
            sql(my,"ALTER TABLE "+q(my,my.mappings.get(id+"_sales"))+".items ADD CONSTRAINT block_next CHECK(id<3)");insert(3);await(()->count(pg,id+"_sales","items")==3&&"PAUSED".equals(sub(journal,"shadow").state));
            phase("stop");PipelineValidationBarrier.Certificate saved;
            try(PipelineValidationBarrier.Session cut=PipelineValidationBarrier.open(config,Collections.singleton("postgres"),15000,60000)){
                saved=cut.certificate();require(saved.selectedTargets.equals(Collections.singleton("postgres"))&&saved.targetCheckpoints.size()==1,"Unselected target was presented as validated");require("PAUSED".equals(journal.status().get("run_state")),"Task did not quiesce");require(saved.sequence==sub(journal,"postgres").acknowledged,"Target did not reach declared sequence");
                insert(4);require(count(pg,id+"_sales","items")==3,"Paused target advanced");
                try(ValidationStore store=ValidationStore.create(directory.resolve("reports"),saved.id,saved,cut.tables(),Collections.singletonMap("postgres",new TreeSet<>(saved.schemas.keySet())),new ValidationStore.Limits())){
                    for(TableSchema t:cut.tables()){
                        if(t.keyless()){
                            String after="";while(true){List<JdbcJournal.IdentityRow> page=journal.identities(saved.generation,saved.epoch,saved.sequence,t.id,after,1);if(page.isEmpty())break;for(JdbcJournal.IdentityRow identity:page){store.identity(t.id,identity);after=identity.rowId;}}
                        }
                        long rows=0;try(SourceAdapter.Snapshot cursor=cut.sourceRows(t)){for(List<List<Cell>> batch;!(batch=cursor.next(1)).isEmpty();){for(List<Cell> row:batch)store.source(t.id,row);rows+=batch.size();}}
                        if(t.name.equals("items"))require(rows==3,"Pinned cut included a later source insert");store.finishSourceTable(t.id);
                    }
                    cut.verify();store.completeSource();require(store.manifest().rows==6,"Source staging omitted rows or occurrences");
                }
                try(ValidationStore reopened=ValidationStore.open(directory.resolve("reports"),saved.id)){require(reopened.manifest().sourceComplete&&reopened.manifest().state.equals("SOURCE_READY"),"Sealed source staging did not survive reopen");}
                reportProcessLoss(saved.id);
                Map<String,Object> match=PipelineReconciliation.compare(config,directory.resolve("reports"),saved.id,15000);require("VALIDATION_MATCH".equals(match.get("code")),"Real target rows did not match the pinned source cut");
                System.out.println("VALIDATION_SOURCE_STAGING_ACCEPTED: paused journal identity pages, keyless multiset/occurrence matching, typed source rows and durable sealed reopen");
                System.out.println("VALIDATION_TARGET_MATCH_ACCEPTED: actual PostgreSQL rows match the pinned source, including JSON/Unicode/wide decimals/signed TIME/keyless IDs, with an offline unselected target");
                cut.verify();
                try(JdbcSink sink=JdbcSink.existing(config.targetSettings(pg,saved.generation))){try{sink.activate(saved.epoch-1);throw new AssertionError("Old target writer was not fenced");}catch(IllegalStateException expected){require("SINK_FENCED".equals(expected.getMessage()),"Wrong fence guard");}}
                journal.resumePipeline(saved.errorGeneration);try{cut.verify();throw new AssertionError("Concurrent resume preserved eligibility");}catch(SyncFailure expected){require("VALIDATION_BARRIER_MOVED".equals(expected.code),"Wrong concurrent-control guard");}
                try{PipelineReconciliation.compare(config,directory.resolve("reports"),saved.id,15000);throw new AssertionError("Old completed report authorized a new scan after resume");}catch(SyncFailure expected){require("VALIDATION_BARRIER_MOVED".equals(expected.code),"Wrong persisted-certificate guard");}
            }
            await(()->count(pg,id+"_sales","items")==4);phase("start");sql(my,"ALTER TABLE "+q(my,my.mappings.get(id+"_sales"))+".items DROP CHECK block_next");journal.resume("shadow",sub(journal,"shadow").errorGeneration);await(()->count(my,id+"_sales","items")==4);
            String output=cli("stage-cli","validate-stage",configuration.toString(),directory.resolve("reports").toString(),"postgres,shadow");String report=Arrays.stream(output.split("\\R")).filter(line->line.startsWith("VALIDATION_SOURCE_SEALED ")).findFirst().get().substring("VALIDATION_SOURCE_SEALED ".length());
            sql(pg,"DELETE FROM "+q(pg,pg.mappings.get(id+"_sales"))+".items WHERE id=2");sql(pg,"UPDATE "+q(pg,pg.mappings.get(id+"_sales"))+".items SET note='deliberate-difference' WHERE id=1");sql(pg,"INSERT INTO "+q(pg,pg.mappings.get(id+"_sales"))+".items(id,note) VALUES(99,'extra')");
            sql(pg,"UPDATE "+q(pg,pg.mappings.get(id+"_sales"))+".duplicates SET \"_rivomesh_row_id\"='"+Codec.sha256("deliberately-wrong-occurrence")+"' WHERE \"_rivomesh_row_id\"=(SELECT min(\"_rivomesh_row_id\") FROM "+q(pg,pg.mappings.get(id+"_sales"))+".duplicates)");
            Map<String,Object> compared;phase("stop-source");try{compared=PipelineReconciliation.compare(config,directory.resolve("reports"),report,15000);require("VALIDATION_DIFFERENCES".equals(compared.get("code")),"Actual target divergence was accepted as equal");}finally{phase("start-source");}
            require(cli("report-cli","validation-report",configuration.toString(),directory.resolve("reports").toString(),report).contains("VALIDATION_DIFFERENCES"),"CLI historical report missing");
            require(cli("compare-cli","validate-compare",configuration.toString(),directory.resolve("reports").toString(),report).contains("VALIDATION_DIFFERENCES"),"CLI comparison replay changed its report");
            try(ValidationStore store=ValidationStore.open(directory.resolve("reports"),report)){
                ValidationStore.Manifest m=store.manifest();require(m.certificate.targetCheckpoints.size()==2,"Second SQL target was not fenced");
                for(TableSchema t:m.schemas.values()){
                    ValidationStore.TableResult clean=m.results.get("shadow/"+t.id);require(clean!=null&&clean.matches(),"Unmodified MySQL target did not match");ValidationStore.TableResult changed=m.results.get("postgres/"+t.id);
                    if(t.name.equals("items")){require(changed.missing==1&&changed.extra==1&&changed.changed==1,"Wrong keyed difference counts");String after="";int pages=0;for(List<ValidationStore.Difference> page;!(page=store.differences("postgres",t.id,after,1)).isEmpty();){require(page.size()==1,"Difference page limit ignored");after=page.get(0).identityHash;ValidationStore.RowPair values=store.values("postgres",t.id,after);require(values.expected!=null||values.actual!=null,"Difference values unavailable");pages++;}require(pages==3,"Difference paging omitted rows");}
                    else if(t.keyless())require(changed.missing==1&&changed.extra==1&&changed.changed==0,"Keyless occurrence corruption was ignored");else require(changed.matches(),"Unmodified ledger differed");
                }
            }
            System.out.println("VALIDATION_TARGET_DIFFERENCES_ACCEPTED: source actually stopped after sealing; PostgreSQL missing/extra/changed rows and wrong keyless identity detected; untouched MySQL target matches; persisted row detail/pagination and CLI stage/report/replay verified");
            kill();start();await(()->"PAUSED".equals(journal.status().get("run_state")));require(count(pg,id+"_sales","items")==4,"Restart bypassed barrier pause");
            try{PipelineValidationBarrier.open(config,Collections.singleton("postgres"),5000,10000);throw new AssertionError("Paused task silently resumed for a new barrier");}catch(SyncFailure expected){require("VALIDATION_REQUIRES_RUNNING_PIPELINE".equals(expected.code),"Wrong paused-task guard");}
            String missing=id+"_lost_metadata";sql(pg,"CREATE SCHEMA "+Sql.pg(missing));JdbcSink.Settings absent=config.targetSettings(pg,saved.generation);absent.metadataSchema=missing;
            try{JdbcSink.existing(absent);throw new AssertionError("Missing checkpoint was recreated");}catch(SQLException expected){require("42P01".equals(expected.getSQLState()),"Wrong missing-metadata refusal");}
            try(Connection c=pg();PreparedStatement p=c.prepareStatement("SELECT count(*) FROM information_schema.tables WHERE table_schema=?")){p.setString(1,missing);try(ResultSet r=p.executeQuery()){r.next();require(r.getLong(1)==0,"Administrative fence created missing metadata");}}
        }
        System.out.println("VALIDATION_PIPELINE_BARRIER_ACCEPTED: actual GTID/normalized/SQL-ACK cut, selected targets with offline peer, source MVCC after new writes, physical epoch fences, concurrent-control refusal, resumed CDC catch-up, persisted pause and missing-metadata refusal");
    }
    void run()throws Exception{
        configure();typedReaders();source("CREATE DATABASE "+Sql.mysql(id+"_sales"));source("CREATE DATABASE "+Sql.mysql(id+"_billing"));
        try{source("CREATE TABLE "+business()+" (id BIGINT PRIMARY KEY,note VARCHAR(50),payload JSON,clock TIME(6),derived_clock TIME(6) GENERATED ALWAYS AS (clock) VIRTUAL) ENGINE=InnoDB");source("INSERT INTO "+business()+"(id,note,payload,clock) VALUES(1,'before',JSON_OBJECT('n',18446744073709551615),'-25:01:02.123456'),(2,'second',NULL,'-00:00:00.000001')");source("CREATE TABLE "+Sql.mysql(id+"_sales")+".duplicates(value INT,payload JSON,clock TIME(6)) ENGINE=InnoDB");source("INSERT INTO "+Sql.mysql(id+"_sales")+".duplicates VALUES(7,JSON_OBJECT('same',true,'unicode',CONVERT(0xe4b8adf09f9880 USING utf8mb4)),'-00:00:00.000001'),(7,JSON_OBJECT('same',true,'unicode',CONVERT(0xe4b8adf09f9880 USING utf8mb4)),'-00:00:00.000001')");source("CREATE TABLE "+Sql.mysql(id+"_billing")+".ledger(id BIGINT PRIMARY KEY,amount DECIMAL(65,30)) ENGINE=InnoDB");source("INSERT INTO "+Sql.mysql(id+"_billing")+".ledger VALUES(1,12345678901234567890123456789012345.123456789012345678901234567890)");if("true".equals(System.getenv("RIVOMESH_MANAGED_ONLY"))){insert(3);insert(4);managedValidation();return;}directViews();processLoss();pipeline();managedValidation();System.out.println("VALIDATION_BARRIER_ACCEPTED: source locks/MVCC/faults, actual SQL cut, sealed source process recovery and real PostgreSQL/MySQL row comparison; repair remains separate");}
        finally{kill();writers.shutdownNow();source("DROP DATABASE IF EXISTS "+Sql.mysql(id+"_sales"));source("DROP DATABASE IF EXISTS "+Sql.mysql(id+"_billing"));}
    }
    public static void main(String[] args)throws Exception{
        if(args.length>0&&args[0].equals("repair-fault")){
            PipelineConfig config=PipelineConfig.load(Paths.get(args[1]));Path reports=Paths.get(args[2]);RepairPlan plan=RepairPlans.load(reports,args[3],args[4]);CommitFault.mode=args[6];CommitFault.ready=Paths.get(args[7]);directRepair(config,reports,plan,plan.tables.get(args[5]),true);return;
        }

        if(args.length>0&&args[0].equals("hold-store")){
            PipelineConfig config=PipelineConfig.load(Paths.get(args[4]));try(ValidationStore store=ValidationStore.open(Paths.get(args[1]),args[2])){
                ValidationStore.Manifest manifest=store.manifest();PipelineConfig.Target target=config.targets.stream().filter(t->manifest.certificate.selectedTargets.contains(t.id)).findFirst().get();TableSchema table=manifest.schemas.values().stream().filter(t->t.name.equals("items")).findFirst().get();
                try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target,manifest.certificate.generation));TargetRead read=sink.validationRead(table,manifest.certificate.targetCheckpoints.get(target.id),30000,65536)){
                    TargetRead.Row row=read.next(1).get(0);store.target(target.id,table.id,row.rowId,row.values);store.flush();Files.write(Paths.get(args[3]),new byte[]{1});new CountDownLatch(1).await();
                }
            }return;
        }
        if(args.length>0&&args[0].equals("hold")){PipelineConfig c=PipelineConfig.load(Paths.get(args[1]));try(MysqlCdcSource source=new MysqlCdcSource(c.legacy(),new TreeSet<>(c.databases),"holder");SourceAdapter.ConsistentRead view=source.consistentRead(source.discover(),20000,30000)){Files.write(Paths.get(args[2]),new byte[]{1});new CountDownLatch(1).await();}return;}
        new ValidationBarrierAcceptance().run();
    }
}
