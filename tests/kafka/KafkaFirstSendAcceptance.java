package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.platform.ClusterConfig;
import io.github.rivomesh.platform.ConsoleServer;
import io.github.rivomesh.platform.PlatformConfig;
import io.github.rivomesh.platform.TaskService;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.Resource;
import org.apache.kafka.common.resource.ResourceType;
import static io.github.rivomesh.cdc.KafkaPhysicalOwnershipAcceptance.*;
import static org.apache.kafka.common.acl.AclOperation.*;

/** Normal managed Worker, native MySQL capture and two independent Kafka business destinations. */
public final class KafkaFirstSendAcceptance {
    private static final String TASK="kafka-first-send",SOURCE="fixture_sales",OWNER="first-send-agent";
    private static final String CONTROL="fixture_control",SELECTED="selected",PEER="peer";
    private static final String SELECTED_TOPIC="first.send.selected",PEER_TOPIC="first.send.peer";
    private static final String TASK_ROUTE="/api/tasks/"+TASK;
    private static final String MYSQL_DRIVER="com.mysql.cj.jdbc.Driver";
    private static final String JOURNAL_SCHEMA="\"rivomesh_v3\".";
    private static final String CONTROL_PREFIX="`fixture_control`.`rmc_";
    private static final String MYSQL_OPTIONS="?sslMode=DISABLED&allowPublicKeyRetrieval=true&connectTimeout=5000&socketTimeout=10000";
    private static final long WAIT_SECONDS=75;
    private static final String CODE="[A-Z][A-Z0-9_]{1,95}";
    private static final Pattern PIPELINE_LOG=Pattern.compile("^PIPELINE_DIAGNOSTIC ("+CODE+")(?: ("+CODE+"))?$");
    private static final Pattern TARGET_LOG=Pattern.compile("^TARGET_(PAUSED|RETRYING) (selected|peer) ("+CODE+")(?: .*)?$");
    private static final Pattern WORKER_EXCEPTION=Pattern.compile("^Exception in thread \"main\" [a-zA-Z][a-zA-Z0-9_.]+: ("+CODE+")$");

    public static void main(String[] args)throws Exception{
        require(args.length==4,"Expected Kafka address, two MySQL ports and private working directory");
        String secret=System.getenv("RIVOMESH_KAFKA_FIXTURE_SECRET");
        require(secret!=null&&secret.matches("[a-f0-9]{48}"),"Fixture secret is required");
        Path home=Path.of(args[3]).toRealPath();
        PipelineConfig pipeline=pipeline(args[0],Integer.parseInt(args[1]),secret);
        pipeline.journal.path=home.resolve(OWNER).resolve("data").resolve("journals").resolve(TASK).toString();
        pipeline.validate();
        require(pipeline.kafkaHistoryFromFirstSend,"Candidate does not contain enrolled Worker support");
        if("--control-preflight".equals(args[0]))return;
        String sourceMysql="jdbc:mysql://127.0.0.1:"+args[1]+"/";
        String controlMysql="jdbc:mysql://127.0.0.1:"+args[2]+"/";
        verifySourcePermissions(sourceMysql+SOURCE+MYSQL_OPTIONS,pipeline);
        String key=Base64.getEncoder().encodeToString(java.security.SecureRandom.getSeed(32));
        String token=secret+"-management";
        PlatformConfig controllerA=control(home,"first-send-controller-a",ClusterConfig.Role.CONTROLLER,controlMysql,key,token,secret);
        PlatformConfig controllerB=control(home,"first-send-controller-b",ClusterConfig.Role.CONTROLLER,controlMysql,key,token,secret);
        PlatformConfig owner=control(home,OWNER,ClusterConfig.Role.AGENT,controlMysql,key,token,secret);
        owner.kafkaPhysicalOwnershipEvidence=true;
        owner.cluster.username="fixture_agent";
        owner.cluster.password=secret+"-agent";
        try(Connection source=JdbcConnections.open(MYSQL_DRIVER,sourceMysql+SOURCE+MYSQL_OPTIONS,"root",secret+"-mysql");
            Connection control=JdbcConnections.open(MYSQL_DRIVER,controlMysql+CONTROL+MYSQL_OPTIONS,CONTROL,secret+"-control");
            Connection controlAdmin=JdbcConnections.open(MYSQL_DRIVER,controlMysql+CONTROL+MYSQL_OPTIONS,"root",secret+"-mysql");
            Admin broker=Admin.create(adminProperties(pipeline.targets.getFirst(),secret));
            TaskService a=new TaskService(controllerA);TaskService b=new TaskService(controllerB);
            ConsoleServer consoleA=new ConsoleServer(controllerA,a);ConsoleServer consoleB=new ConsoleServer(controllerB,b)){
            broker.createTopics(List.of(new NewTopic(SELECTED_TOPIC,1,(short)1).configs(KAFKA_CONFIG),
                    new NewTopic(PEER_TOPIC,1,(short)1).configs(KAFKA_CONFIG))).all().get(20,TimeUnit.SECONDS);
            var routes=new ArrayList<>(List.of(acl(ResourceType.CLUSTER,Resource.CLUSTER_NAME,DESCRIBE_CONFIGS),
                    acl(ResourceType.CLUSTER,Resource.CLUSTER_NAME,IDEMPOTENT_WRITE)));
            for(String topic:List.of(SELECTED_TOPIC,PEER_TOPIC)){
                routes.add(acl(ResourceType.TOPIC,topic,READ));
                routes.add(acl(ResourceType.TOPIC,topic,DESCRIBE_CONFIGS));
            }
            routes.add(acl(ResourceType.TOPIC,PEER_TOPIC,WRITE));
            broker.createAcls(routes).all().get(15,TimeUnit.SECONDS);
            Properties business=KafkaMessagePublisher.connectionProperties(pipeline.targets.getFirst());
            awaitTopicAccess(business,false);
            // START and topic preparation still have all the metadata permissions they require.
            KafkaMessagePublisher.check(pipeline.targets.getFirst());
            try(KafkaMessagePublisher metadata=new KafkaMessagePublisher(pipeline.targets.getFirst())){metadata.require(Set.of());}
            execute(source,"CREATE TABLE items (id INT PRIMARY KEY, label VARCHAR(40)) ENGINE=InnoDB");
            insert(source,1);

            consoleA.start();consoleB.start();
            var first=new Management(consoleA,token);
            var second=new Management(consoleB,token);
            Origin original; Intent selected,peer; String oldIncarnation;
            try(TaskService agent=new TaskService(owner);ConsoleServer member=new ConsoleServer(owner,agent)){
                member.start();
                first.create(pipeline);
                Origin pending=origin(control);
                oldIncarnation=incarnation(control);
                second.action("START","first_send_start",null,0);
                Journal.Subscription failed=awaitWithDiagnostics(second,home,"native Produce denial",()->subscription(pipeline,SELECTED),
                        s->s!=null&&"PAUSED".equals(s.state));
                require("KAFKA_REQUEST_REJECTED".equals(failed.errorCode),"Selected must pause on native Kafka Produce rejection");
                original=origin(control);
                require(pending.generation().equals(original.generation())&&!pending.phase().equals(original.phase()),
                        "Existing control origin must advance from pending to ready");
                selected=intent(pipeline,SELECTED);
                require(pending.initialVersion().equals(selected.payload().path("scope").path("configurationVersion").asText()),
                        "First send must bind the active initial version");
                require(original.generation().equals(selected.payload().path("generation").asText()),"Original generation mismatch");
                require(selected.payload().path("kinds").get(0).asText().equals("SCHEMA"),"First selected message must be SCHEMA");
                await("peer snapshot",()->eventSequence(broker,business,PEER_TOPIC,"SNAPSHOT_ROW",1),sequence->sequence>=0);
                System.out.println("PASS Kafka denied the selected first SCHEMA attempt and the peer received its native snapshot row.");
                insert(source,2);
                awaitDelivery(first,home,pipeline,broker,business,PEER,PEER_TOPIC,2);
                require(offset(broker,SELECTED_TOPIC)==0&&subscription(pipeline,SELECTED).acknowledged==0,
                        "Kafka rejection must leave the selected broker and journal without progress");
                peer=intent(pipeline,PEER);
                require(!selected.signature().equals(peer.signature()),"Subscriptions must keep distinct signed first intents");
                System.out.println("PASS genuine Kafka Produce refusal preserves UNKNOWN and keeps the peer snapshot and MySQL CDC running.");

                broker.createAcls(List.of(acl(ResourceType.TOPIC,SELECTED_TOPIC,WRITE))).all().get(15,TimeUnit.SECONDS);
                awaitTopicAccess(business,true);
                first.action("RESUME_TARGET","first_send_resume",SELECTED,failed.errorGeneration);
                await("resumed target snapshot",()->eventSequence(broker,business,SELECTED_TOPIC,"SNAPSHOT_ROW",1),sequence->sequence>=0);
                awaitDelivery(first,home,pipeline,broker,business,SELECTED,SELECTED_TOPIC,2);
                checkUnchanged(pipeline,control,original,selected,peer);
                second.action("STOP","first_send_stop",null,0);
                await("normal Worker stop",()->worker(home),p->p==null);
                long selectedBefore=offset(broker,SELECTED_TOPIC),peerBefore=offset(broker,PEER_TOPIC);
                insert(source,3);
                require(offset(broker,SELECTED_TOPIC)==selectedBefore&&offset(broker,PEER_TOPIC)==peerBefore,
                        "Stopped Worker must leave both Kafka Topic offsets unchanged");
            }

            boolean fencingVerified=false;
            try(TaskService agent=new TaskService(owner);ConsoleServer member=new ConsoleServer(owner,agent)){
                member.start();
                require(!oldIncarnation.equals(incarnation(control)),"Restart must change the Agent incarnation");
                first.action("START","first_send_restart",null,0);
                awaitDelivery(first,home,pipeline,broker,business,SELECTED,SELECTED_TOPIC,3);
                awaitDelivery(first,home,pipeline,broker,business,PEER,PEER_TOPIC,3);
                checkUnchanged(pipeline,control,original,selected,peer);
                System.out.println("PASS both Kafka targets resume MySQL CDC with the original signed first attempts after Agent restart.");
                ProcessHandle prior=await("actual Worker child",()->worker(home),handle->handle!=null);
                execute(controlAdmin,"ALTER USER 'fixture_agent'@'%' ACCOUNT LOCK");
                try(var unexpectedlyAllowed=JdbcConnections.open(MYSQL_DRIVER,controlMysql+CONTROL+MYSQL_OPTIONS,
                        owner.cluster.username,owner.cluster.password)){
                    throw new AssertionError("Revoked Agent can still open the control store");
                }catch(java.sql.SQLException locked){
                    require(locked.getErrorCode()==3118,"Fixture Agent was not denied by the native account lock");
                }
                try(var statement=control.prepareStatement("UPDATE "+CONTROL_PREFIX+"node` SET lease_until=0"
                        +" WHERE cluster_id=? AND node_id=?")){
                    statement.setString(1,TASK);statement.setString(2,OWNER);
                    require(statement.executeUpdate()==1,"Fixture must revoke the exact owning Agent lease");
                }
                await("revoked Worker process stopped",prior::isAlive,Boolean.FALSE::equals);
                await("Controller observes revoked Agent",second::task,
                        observed->!Set.of("RUNNING","STARTING").contains(observed.path("workerState").asText()));
                long selectedAfter=offset(broker,SELECTED_TOPIC),peerAfter=offset(broker,PEER_TOPIC);
                long selectedAck=subscription(pipeline,SELECTED).acknowledged,peerAck=subscription(pipeline,PEER).acknowledged;
                insert(source,4);
                // A row introduced after the old process actually exited cannot be part of its in-flight send.
                Thread.sleep(600);
                require(worker(home)==null&&offset(broker,SELECTED_TOPIC)==selectedAfter&&offset(broker,PEER_TOPIC)==peerAfter
                        &&subscription(pipeline,SELECTED).acknowledged==selectedAck&&subscription(pipeline,PEER).acknowledged==peerAck,
                        "A fenced Worker must not publish or ACK a newly introduced MySQL row");
                checkUnchanged(pipeline,control,original,selected,peer);
                fencingVerified=true;
            }catch(java.io.IOException stoppedWithoutAudit){
                if(!fencingVerified||!"io.github.rivomesh.platform.AuditStore$Unavailable".equals(stoppedWithoutAudit.getClass().getName())
                        ||!"AUDIT_UNAVAILABLE".equals(stoppedWithoutAudit.getMessage()))throw stoppedWithoutAudit;
                System.out.println("The locked Agent also refused the graceful shutdown audit, as expected after credential revocation.");
            }
            require(fencingVerified,"Native Worker fencing was not verified");
            require(broker.listConsumerGroups().all().get(10,TimeUnit.SECONDS).isEmpty(),"Fixture must not create Kafka consumer groups");
            System.out.println("PASS the revoked Agent loses the old Worker and neither target sends or ACKs a newly introduced MySQL row.");
        }
    }

    private static PipelineConfig pipeline(String address,int port,String secret){
        String endpoint="--control-preflight".equals(address)?"127.0.0.1:1":address;
        var pipeline=new PipelineConfig();
        pipeline.pipelineId=TASK;pipeline.kafkaHistoryFromFirstSend=true;pipeline.databases=List.of(SOURCE);
        pipeline.source.host="127.0.0.1";pipeline.source.port=port;pipeline.source.topology="ASYNC";
        pipeline.source.clusterId="fixture-first-send";pipeline.source.username="fixture_source";
        pipeline.source.password=secret+"-source";pipeline.source.serverId=99421;
        for(String id:List.of(SELECTED,PEER)){
            var route=target(endpoint,secret,id.equals(SELECTED)?SELECTED_TOPIC:PEER_TOPIC);
            route.id=id;route.mappings=Map.of(SOURCE,"fixture_orders_"+id);
            pipeline.targets.add(route);
        }
        return pipeline;
    }

    private static void verifySourcePermissions(String url,PipelineConfig pipeline){
        String operation="CONNECT";
        try(var source=JdbcConnections.open(MYSQL_DRIVER,url,pipeline.source.username,pipeline.source.password);
            var statement=source.createStatement()){
            statement.setQueryTimeout(10);
            operation="GROUP_MEMBERS";
            try(var groups=statement.executeQuery("SELECT MEMBER_ROLE,MEMBER_STATE "
                    +"FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@global.server_uuid")){
                require(!groups.next(),"Fixture source must be outside MySQL Group Replication");
            }
            operation="GLOBAL_READ_LOCK";
            statement.execute("FLUSH TABLES WITH READ LOCK");
            operation="UNLOCK";
            statement.execute("UNLOCK TABLES");
        }catch(SQLException denied){
            String state=denied.getSQLState();
            System.err.println("MySQL source permission check "+operation+" SQLState="
                    +(state!=null&&state.matches("[0-9A-Z]{5}")?state:"UNAVAILABLE")+" code="+denied.getErrorCode());
            throw new IllegalStateException("MYSQL_SOURCE_PERMISSION_CHECK_FAILED");
        }
        System.out.println("PASS MySQL source account can read group membership, acquire the global read lock and release it.");
    }

    private static PlatformConfig control(Path base,String id,ClusterConfig.Role role,String mysql,String key,String token,String secret){
        var config=new PlatformConfig();config.port=0;config.pollMillis=100;config.authToken=token;config.workerHeapMiB=256;
        config.tasksDirectory=base.resolve(id).resolve("tasks").toString();
        config.dataDirectory=base.resolve(id).resolve("data").toString();
        var cluster=new ClusterConfig();
        cluster.id=TASK;cluster.nodeId=id;cluster.driver=MYSQL_DRIVER;cluster.namespace=CONTROL;
        cluster.url=mysql+CONTROL+MYSQL_OPTIONS;cluster.username=CONTROL;cluster.password=secret+"-control";cluster.taskKey=key;
        cluster.sharedAccess=true;cluster.sharedTasks=true;cluster.separateExecution=true;cluster.role=role;
        cluster.heartbeatMillis=250;cluster.leaseMillis=12000;
        config.cluster=cluster;return config;
    }

    private record Origin(String initialVersion,String generation,String phase){}
    private record Intent(JsonNode payload,String signature){}
    private static Origin origin(Connection c)throws Exception{
        try(var p=c.prepareStatement("SELECT initial_version,generation,phase_signature FROM "+CONTROL_PREFIX
                +"kafka_history_origin` WHERE cluster_id=? AND task_id=?")){
            p.setString(1,TASK);p.setString(2,TASK);
            try(var row=p.executeQuery()){
                require(row.next(),"Missing shared origin");
                return new Origin(row.getString(1),row.getString(2),row.getString(3));
            }
        }
    }
    private static String incarnation(Connection c)throws Exception{
        try(var p=c.prepareStatement("SELECT incarnation FROM "+CONTROL_PREFIX+"node` WHERE cluster_id=? AND node_id=?")){
            p.setString(1,TASK);p.setString(2,OWNER);
            try(var row=p.executeQuery()){require(row.next(),"Missing Agent registration");return row.getString(1);}
        }
    }
    private static Intent intent(PipelineConfig pipeline,String id)throws Exception{
        var settings=pipeline.existingJournalSettings();
        try(var c=JdbcConnections.open(settings.driver,settings.url,settings.username,settings.password);
            var p=c.prepareStatement("SELECT payload,signature FROM "+JOURNAL_SCHEMA
                    +"\"rm3_kafka_history_first_send\" WHERE pipeline_id=? AND subscription_id=?")){
            p.setString(1,TASK);p.setString(2,id);
            try(var row=p.executeQuery()){
                require(row.next(),"Missing per-target intent");
                JsonNode contents=Codec.JSON.readTree(row.getString(1));
                String signature=row.getString(2);
                require("UNKNOWN".equals(contents.path("result").asText())&&id.equals(contents.path("target").asText())
                        &&contents.path("wireSha256").asText().matches("[a-f0-9]{64}")
                        &&signature.matches("[A-Za-z0-9_-]{43}"),"First-send intent was incomplete");
                return new Intent(contents,signature);
            }
        }
    }
    private static void checkUnchanged(PipelineConfig pipeline,Connection control,Origin origin,Intent selected,Intent peer)throws Exception{
        require(origin.equals(origin(control))&&selected.equals(intent(pipeline,SELECTED))&&peer.equals(intent(pipeline,PEER)),
                "Restart or normal publication advanced or replaced a first-send receipt");
    }
    private static Journal.Subscription subscription(PipelineConfig pipeline,String id)throws Exception{
        try(JdbcJournal journal=new JdbcJournal(pipeline.existingJournalSettings())){
            return journal.subscriptions().stream().filter(s->s.id.equals(id)).findFirst().orElseThrow();
        }
    }
    private static void awaitTopicAccess(Properties props,boolean selectedWrite)throws Exception{
        try(Admin observed=Admin.create(props)){
            await("business ACL propagation",()->{
                var metadata=observed.describeTopics(List.of(SELECTED_TOPIC,PEER_TOPIC),new DescribeTopicsOptions().includeAuthorizedOperations(true))
                        .allTopicNames().get(5,TimeUnit.SECONDS);
                Set<AclOperation> selected=metadata.get(SELECTED_TOPIC).authorizedOperations(),peer=metadata.get(PEER_TOPIC).authorizedOperations();
                return selected!=null&&peer!=null&&selected.containsAll(Set.of(READ,DESCRIBE,DESCRIBE_CONFIGS))
                        &&selected.contains(WRITE)==selectedWrite&&peer.containsAll(Set.of(READ,WRITE,DESCRIBE_CONFIGS));
            },Boolean.TRUE::equals);
        }
    }
    private static long offset(Admin broker,String topic)throws Exception{
        TopicPartition partition=new TopicPartition(topic,0);
        return broker.listOffsets(Map.of(partition,OffsetSpec.latest())).partitionResult(partition).get(5,TimeUnit.SECONDS).offset();
    }
    private static long eventSequence(Admin broker,Properties business,String topic,String kind,int id)throws Exception{
        long latest=offset(broker,topic);
        if(latest==0)return -1;
        Properties settings=new Properties();settings.putAll(business);
        settings.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        settings.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        settings.put("enable.auto.commit","false");
        TopicPartition partition=new TopicPartition(topic,0);
        try(KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<>(settings)){
            consumer.assign(List.of(partition));consumer.seek(partition,0);
            long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while(System.nanoTime()<end){
                for(var record:consumer.poll(Duration.ofMillis(120))){
                    var event=Codec.JSON.readValue(record.value(),MessageEvent.class);
                    if(kind.equals(event.kind)&&event.change!=null&&event.change.after!=null
                            &&Integer.toString(id).equals(event.change.after.getFirst().value))return event.sequence;
                }
                if(consumer.position(partition)>=latest)return -1;
            }
            return -1;
        }
    }
    private static void awaitDelivery(Management management,Path home,PipelineConfig pipeline,Admin broker,
                                      Properties business,String target,String topic,int id)throws Exception{
        long[] lastSequence={-1};
        try{
            awaitWithDiagnostics(management,home,"native CDC row and journal ACK "+id+" for "+target,()->{
                lastSequence[0]=eventSequence(broker,business,topic,"ROW",id);
                return lastSequence[0]>0&&subscription(pipeline,target).acknowledged>=lastSequence[0];
            },Boolean.TRUE::equals);
        }catch(AssertionError timeout){
            printDeliveryDiagnostics(pipeline,broker,target,topic,lastSequence[0]);
            throw timeout;
        }
    }

    private static void printDeliveryDiagnostics(PipelineConfig pipeline,Admin broker,String target,String topic,long sequence){
        try{
            System.err.println("Delivery runtime target="+target+" kafkaEnd="+offset(broker,topic)+" matchingRowSequence="+sequence);
        }catch(Exception unavailable){
            System.err.println("Delivery broker diagnostics unavailable "+unavailable.getClass().getSimpleName());
        }
        try(JdbcJournal journal=new JdbcJournal(pipeline.existingJournalSettings())){
            var status=journal.status();
            System.err.println("Delivery journal last="+status.get("lastSequence")+" normalized="+status.get("normalizedSequence")
                    +" state="+status.get("run_state")+" code="+status.get("errorCode"));
            for(Journal.Subscription sub:journal.subscriptions()){
                System.err.println("Delivery subscription "+sub.id+" ack="+sub.acknowledged+" state="+sub.state+" code="+sub.errorCode);
            }
        }catch(Exception unavailable){
            System.err.println("Delivery journal diagnostics unavailable "+unavailable.getClass().getSimpleName());
        }
    }
    private static ProcessHandle worker(Path home){
        String directory=home.resolve(OWNER).toAbsolutePath().toString();
        return ProcessHandle.current().descendants().filter(p->p.info().arguments().stream()
                .anyMatch(args->java.util.Arrays.stream(args).anyMatch(arg->arg.startsWith(directory)&&arg.endsWith(".yml"))))
                .findFirst().orElse(null);
    }
    private static void execute(Connection c,String sql)throws Exception{
        try(var s=c.createStatement()){s.execute(sql);}
    }
    private static void insert(Connection c,int id)throws Exception{
        try(var p=c.prepareStatement("INSERT INTO items (id,label) VALUES (?,?)")){
            p.setInt(1,id);p.setString(2,"fixture-"+id);p.executeUpdate();
        }
    }

    @FunctionalInterface private interface Read<T>{T get()throws Exception;}
    private static <T>T await(String label,Read<T> read,Predicate<T> accepted)throws Exception{
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        String lastError="";
        do{
            try{T value=read.get();if(accepted.test(value))return value;}
            catch(Exception unavailable){lastError=unavailable.getClass().getSimpleName();}
            Thread.sleep(140);
        }while(System.nanoTime()<deadline);
        throw new AssertionError("Timed out waiting for "+label+" "+lastError);
    }
    private static <T>T awaitWithDiagnostics(Management management,Path home,String label,Read<T> read,
                                             Predicate<T> accepted)throws Exception{
        try{return await(label,read,accepted);}
        catch(AssertionError timeout){
            printTimeoutDiagnostics(management,home);
            throw timeout;
        }
    }
    private static void printTimeoutDiagnostics(Management management,Path home){
        try{
            JsonNode task=management.task(),status=task.path("status");
            System.err.println("Controller task desired="+code(task,"desired")+" workerState="+code(task,"workerState")
                    +" ownerState="+code(task,"ownerState")+" ownerControlState="+code(task,"ownerControlState")
                    +" problem="+code(task,"problem")+" captureLeaseState="+code(task,"captureLeaseState")
                    +" journalState="+code(status,"run_state")+" workerErrorStage="+code(status,"worker_error_stage")
                    +" workerErrorCode="+code(status,"worker_error_code"));
        }catch(Exception unavailable){
            System.err.println("Controller task diagnosis unavailable");
        }
        Path log=home.resolve(OWNER).resolve("data").resolve("logs").resolve(TASK+".log");
        var codes=new ArrayDeque<String>();
        for(Path saved:List.of(log.resolveSibling(log.getFileName()+".1"),log)){
            if(!Files.isRegularFile(saved))continue;
            try(var lines=Files.lines(saved)){
                lines.forEachOrdered(line->{
                    String diagnostic=workerCode(line);
                    if(diagnostic==null)return;
                    if(codes.size()==6)codes.removeFirst();
                    codes.addLast(diagnostic);
                });
            }catch(Exception unavailable){
                System.err.println("Worker diagnostic read unavailable");
            }
        }
        if(codes.isEmpty())System.err.println("Worker has no recognized diagnostic code");
        else codes.forEach(diagnostic->System.err.println("Worker "+diagnostic));
    }
    private static String code(JsonNode object,String field){
        String value=object.path(field).asText("");
        return value.matches(CODE)?value:"UNAVAILABLE";
    }
    private static String workerCode(String line){
        if(line.length()>2048)return null;
        var pipeline=PIPELINE_LOG.matcher(line);
        if(pipeline.matches())return pipeline.group(2)==null?pipeline.group(1):pipeline.group(1)+" "+pipeline.group(2);
        var target=TARGET_LOG.matcher(line);
        if(target.matches())return target.group(1)+" "+target.group(2)+" "+target.group(3);
        var exception=WORKER_EXCEPTION.matcher(line);
        return exception.matches()?exception.group(1):null;
    }

    private static void require(boolean valid,String message){if(!valid)throw new AssertionError(message);}

    private static final class Management {
        final HttpClient client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        final String base,token;
        Management(ConsoleServer console,String token){base="http://127.0.0.1:"+console.port();this.token=token;}
        JsonNode task()throws Exception{return request("GET",TASK_ROUTE,null,null,200);}
        JsonNode request(String method,String route,String id,JsonNode body,int status)throws Exception{
            var builder=HttpRequest.newBuilder(URI.create(base+route)).timeout(Duration.ofSeconds(15)).header("Authorization","Bearer "+token);
            if(id!=null)builder.header("X-Request-ID",id);
            if(body!=null)builder.header("Content-Type","application/json");
            var reply=client.send(builder.method(method,body==null?HttpRequest.BodyPublishers.noBody():
                    HttpRequest.BodyPublishers.ofString(body.toString())).build(),HttpResponse.BodyHandlers.ofString());
            JsonNode json=Codec.JSON.readTree(reply.body());
            require(reply.statusCode()==status,"Unexpected "+method+" status "+reply.statusCode()+" "+json.path("code").asText());
            return json;
        }
        void create(PipelineConfig pipeline)throws Exception{
            ObjectNode config=Codec.JSON.valueToTree(pipeline);((ObjectNode)config.path("journal")).remove("path");
            ObjectNode body=Codec.JSON.createObjectNode().put("ownerNodeId",OWNER);body.set("config",config);
            request("POST","/api/tasks","first_send_create",body,202);
            operation("first_send_create");
        }
        void action(String action,String id,String target,long generation)throws Exception{
            ObjectNode body=Codec.JSON.createObjectNode().put("action",action).put("expectedRevision",task().path("revision").asText());
            if(target!=null)body.put("subscriptionId",target).put("errorCount",generation);
            request("POST",TASK_ROUTE+"/actions",id,body,202);
            operation(id);
        }
        void operation(String id)throws Exception{
            JsonNode op=await("management command "+id,()->{
                for(JsonNode operation:task().path("operations"))if(id.equals(operation.path("id").asText()))return operation;
                return Codec.JSON.nullNode();
            },state->Set.of("SUCCEEDED","FAILED","REJECTED").contains(state.path("status").asText()));
            require("SUCCEEDED".equals(op.path("status").asText()),"Management command "+id+" failed "+op.path("code").asText());
        }
    }
}
