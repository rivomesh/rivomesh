package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.node.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Native MoW/version columns and durable destination receipts, independent of short-lived load labels. */
public final class DorisMaterializer implements ConfirmedPublisher {
    public static final class Settings {
        public DorisHttpClient.Settings http;
        public String jdbcUrl,username,password,metadataDatabase="rivomesh_v3",pipelineId,subscriptionId,generation,fingerprint;
        public Map<String,String> mappings=new TreeMap<>();
        public int replicas=3,buckets=4;
        public boolean autoCreate=true;
        public DorisIndexPlan.Mode secondaryIndexes=DorisIndexPlan.Mode.INVERTED;
    }
    public static final class SchemaRecord {
        public TableSchema schema;
        public String namespace,channel,dataName,dataIdentity,dataDefinitionHash,viewDefinitionHash;
        public DorisIndexPlan indexPlan;
    }
    private static final String KEY="_rivomesh_key",VERSION="_rivomesh_version",DELETED="_rivomesh_deleted";
    private final Settings settings;
    private final DorisSqlClient sql;
    private final DorisHttpClient http;
    private final String binding,rootName,root,owner;
    private final Map<String,SchemaRecord> schemas=new HashMap<>();
    private String rootIdentity,receiptId,receiptHash;
    public DorisMaterializer(Settings s){
        if(s==null||s.http==null||s.secondaryIndexes==null||s.fingerprint==null||!s.fingerprint.matches("[a-f0-9]{64}")||s.generation==null||s.generation.isEmpty()||s.generation.length()>128||s.mappings==null||s.mappings.isEmpty()||s.replicas<1||s.replicas>5||s.buckets<1||s.buckets>64)throw new IllegalArgumentException("DORIS_CONFIG_INVALID");
        for(String id:Arrays.asList(s.pipelineId,s.subscriptionId))if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw new IllegalArgumentException("DORIS_ID_INVALID");
        databaseName(s.metadataDatabase);Set<String> unique=new HashSet<>();for(String value:s.mappings.values()){databaseName(value);if(value.equalsIgnoreCase(s.metadataDatabase)||!unique.add(value.toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("DORIS_MAPPING_COLLISION");}
        settings=s;binding="rivomesh:"+Codec.sha256(s.pipelineId+":"+s.subscriptionId+":"+s.generation);rootName="rm3_doris_"+Codec.sha256(s.pipelineId+":"+s.subscriptionId).substring(0,48);root=q(s.metadataDatabase)+"."+q(rootName);owner=binding+":"+s.fingerprint;
        http=new DorisHttpClient(s.http);sql=new DorisSqlClient(s.jdbcUrl,s.username,s.password,s.http.requestTimeoutMillis);
    }
    public static String dataName(Settings s,TableSchema table){return "rm3_data_"+Codec.sha256("rivomesh:"+Codec.sha256(s.pipelineId+":"+s.subscriptionId+":"+s.generation)+":"+table.id).substring(0,48);}
    public static void databaseName(String name){if(name==null||!name.matches("[A-Za-z][A-Za-z0-9_]{0,63}"))throw new IllegalArgumentException("DORIS_DATABASE_NAME_INVALID");}
    private static String q(String name){return DorisRows.q(name);}
    // DDL comments contain internal identifiers only. SQL values use binary prepared parameters.
    private static String commentLiteral(String value){if(value==null||!value.matches("[A-Za-z0-9:._-]+"))throw new IllegalArgumentException("DORIS_COMMENT_INVALID");return "'"+value+"'";}
    public static Map<String,Object> check(Settings settings)throws Exception{
        try(DorisMaterializer materializer=new DorisMaterializer(settings)){
            List<Map<String,String>> nodes=materializer.sql.query("SHOW FRONTENDS");Set<String> versions=new TreeSet<>();for(Map<String,String> node:nodes){String version=node.get("Version");if(version==null||!version.toLowerCase(Locale.ROOT).contains("doris"))throw new IllegalStateException("DORIS_PROFILE_UNSUPPORTED");versions.add(version);}
            if(versions.isEmpty())throw new IllegalStateException("DORIS_PROFILE_UNSUPPORTED");materializer.http.check();Map<String,Object> result=new LinkedHashMap<>();result.put("versions",versions);result.put("consistency",SinkAdapter.Consistency.VERSIONED_MATERIALIZATION.name());result.put("profile","UNIQUE_MOW_SEQUENCE_V1");result.put("httpCheck","health only; LOAD/DDL privileges checked on startup");result.put("temporalJsonBinaryStorage","lossless text; binary/geometry base64");return result;
        }
    }
    private Map<String,String> object(String database,String name)throws Exception{
        List<Map<String,String>> rows=sql.query("SELECT TABLE_TYPE,TABLE_COMMENT FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",database,name);if(rows.size()>1)throw new IllegalStateException("DORIS_CATALOG_AMBIGUOUS");return rows.isEmpty()?null:rows.get(0);
    }
    private String definition(String database,String name,boolean view)throws Exception{
        List<Map<String,String>> rows=sql.query("SHOW CREATE "+(view?"VIEW ":"TABLE ")+q(database)+"."+q(name));String result=rows.size()==1?rows.get(0).get(view?"Create View":"Create Table"):null;if(result==null)throw new IllegalStateException("DORIS_OBJECT_DEFINITION_MISSING");return result;
    }
    private String physicalIdentity(String name)throws Exception{
        List<Map<String,String>> tablets=sql.query("SHOW TABLETS FROM "+q(settings.metadataDatabase)+"."+q(name));Set<String> ids=new TreeSet<>();for(Map<String,String> row:tablets){String id=row.get("TabletId");if(id==null||!id.matches("[0-9]+"))throw new IllegalStateException("DORIS_TABLE_ID_UNAVAILABLE");ids.add(id);}if(ids.isEmpty())throw new IllegalStateException("DORIS_TABLE_ID_UNAVAILABLE");return Codec.sha256(String.join(",",ids));
    }
    private void database(String database,boolean create)throws Exception{
        List<Map<String,String>> rows=sql.query("SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME=?",database);if(rows.isEmpty()){if(!create)throw new IllegalStateException("DORIS_DATABASE_MISSING_REBUILD_REQUIRED");sql.execute("CREATE DATABASE IF NOT EXISTS "+q(database));}
    }
    private String properties(boolean sequence){return " PROPERTIES ('replication_num'='"+settings.replicas+"'"+(sequence?",'enable_unique_key_merge_on_write'='true','function_column.sequence_col'='"+VERSION+"'":"")+")";}
    private void root(boolean create)throws Exception{
        database(settings.metadataDatabase,create&&receiptId==null);Map<String,String> object=object(settings.metadataDatabase,rootName);
        if(object==null){if(!create||receiptId!=null)throw new IllegalStateException("DORIS_DESTINATION_MISSING_REBUILD_REQUIRED");sql.execute("CREATE TABLE IF NOT EXISTS "+root+" (kind VARCHAR(32) NOT NULL,id VARCHAR(64) NOT NULL,hash VARCHAR(64) NOT NULL,payload STRING NOT NULL) DUPLICATE KEY(kind,id) COMMENT "+commentLiteral(owner)+" DISTRIBUTED BY HASH(id) BUCKETS 1"+properties(false));object=object(settings.metadataDatabase,rootName);}
        if(object==null||!"BASE TABLE".equals(object.get("TABLE_TYPE"))||!owner.equals(object.get("TABLE_COMMENT")))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");
        String identity=physicalIdentity(rootName);if(rootIdentity!=null&&!rootIdentity.equals(identity))throw new IllegalStateException("DORIS_RESOURCE_ID_CHANGED");rootIdentity=identity;
        columns(rootName,Arrays.asList("kind","id","hash","payload"),Arrays.asList("varchar(32)","varchar(64)","varchar(64)","text"),Arrays.asList(false,false,false,false));
    }
    private void columns(String name,List<String> expectedNames,List<String> expectedTypes,List<Boolean> nullable)throws Exception{
        List<Map<String,String>> columns=sql.query("DESC "+q(settings.metadataDatabase)+"."+q(name));if(columns.size()!=expectedNames.size())throw new IllegalStateException("DORIS_COLUMNS_CHANGED");
        for(int i=0;i<columns.size();i++){Map<String,String> column=columns.get(i);if(!expectedNames.get(i).equals(column.get("Field"))||!expectedTypes.get(i).replace(" ","").equalsIgnoreCase(column.get("Type").replace(" ",""))||nullable.get(i)!=column.get("Null").equalsIgnoreCase("YES"))throw new IllegalStateException("DORIS_COLUMNS_CHANGED");}
    }
    // Idempotency belongs to versioned rows and durable receipts, not cancelled/expired load labels.
    private String label(String content){return "rm_"+Codec.sha256(binding+":"+content).substring(0,48)+"_"+UUID.randomUUID().toString().replace("-","");}
    private void save(String kind,String id,String hash,String payload)throws Exception{
        ObjectNode row=Codec.JSON.createObjectNode();row.put("kind",kind);row.put("id",id);row.put("hash",hash);row.put("payload",payload);byte[] body=Codec.JSON.writeValueAsBytes(Collections.singletonList(row));http.load(settings.metadataDatabase,rootName,label(kind+":"+id+":"+hash),body,1);if(!Objects.equals(payload,load(kind,id,hash)))throw new IllegalStateException("DORIS_METADATA_NOT_OBSERVED");
    }
    private String load(String kind,String id,String expectedHash)throws Exception{
        List<Map<String,String>> rows=sql.query("SELECT hash,payload FROM "+root+" WHERE kind=? AND id=? GROUP BY hash,payload LIMIT 2",kind,id);if(rows.isEmpty())return null;if(rows.size()!=1)throw new IllegalStateException("DORIS_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED");Map<String,String> row=rows.get(0);if(expectedHash!=null&&!expectedHash.equals(row.get("hash"))||expectedHash==null&&!Codec.sha256(row.get("payload")).equals(row.get("hash")))throw new IllegalStateException("DORIS_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED");return row.get("payload");
    }
    private void verifyReceipt()throws Exception{if(receiptId!=null&&load("RECEIPT",receiptId,receiptHash)==null)throw new IllegalStateException("DORIS_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED");}
    @Override public boolean requiresProgress(){return true;}
    @Override public void resumeProgress(String progress){if(rootIdentity!=null||receiptId!=null)throw new IllegalStateException("DORIS_PROGRESS_ALREADY_ACTIVE");if(progress==null)return;String[] fields=progress.split(":",-1);if(fields.length!=3||!fields[0].equals("DORIS_RECEIPT_V1")||!fields[1].matches("[a-f0-9]{64}")||!fields[2].matches("[a-f0-9]{64}"))throw new IllegalArgumentException("DORIS_PROGRESS_INVALID");receiptId=fields[1];receiptHash=fields[2];}
    @Override public String confirmedProgress(){return receiptId==null?null:"DORIS_RECEIPT_V1:"+receiptId+":"+receiptHash;}
    @Override public String identity(){return rootIdentity==null?null:"DORIS_TABLE_V1:"+rootIdentity;}
    private static boolean control(String channel){return channel.endsWith(".__schema")||channel.endsWith(".__transactions");}
    @Override public void prepare(Set<String> destinations)throws Exception{root(settings.autoCreate);verifyReceipt();for(String channel:destinations)if(!control(channel)){SchemaRecord schema=schema(channel,false);if(schema!=null)verify(schema);}}
    @Override public void require(Set<String> destinations)throws Exception{root(false);verifyReceipt();for(String channel:destinations)if(!control(channel))verify(schema(channel,true));}
    private SchemaRecord schema(String channel,boolean required)throws Exception{
        SchemaRecord record=schemas.get(channel);if(record==null){String payload=load("SCHEMA",Codec.sha256(channel),null);if(payload!=null){record=Codec.decode(payload,SchemaRecord.class);if(record.schema==null||!channel.equals(record.channel))throw new IllegalStateException("DORIS_SCHEMA_METADATA_INVALID");schemas.put(channel,record);}}
        if(record==null&&required)throw new IllegalStateException("DORIS_SCHEMA_MISSING_REBUILD_REQUIRED");return record;
    }
    private void verify(SchemaRecord record)throws Exception{
        if(object(settings.metadataDatabase,record.dataName)==null||object(record.namespace,record.schema.name)==null)throw new IllegalStateException("DORIS_TABLE_MISSING_REBUILD_REQUIRED");
        if(!record.dataIdentity.equals(physicalIdentity(record.dataName)))throw new IllegalStateException("DORIS_RESOURCE_ID_CHANGED");
        if(!record.dataDefinitionHash.equals(Codec.sha256(definition(settings.metadataDatabase,record.dataName,false)))||!record.viewDefinitionHash.equals(Codec.sha256(definition(record.namespace,record.schema.name,true))))throw new IllegalStateException("DORIS_TABLE_DEFINITION_CHANGED");
    }
    private void create(MessageEvent event)throws Exception{
        TableSchema table=event.schema;if(table==null||!settings.mappings.containsKey(table.database)||!Objects.equals(settings.mappings.get(table.database),event.namespace))throw new IllegalArgumentException("DORIS_MAPPING_MISMATCH");table.validate();String channel=MessageSink.channel(event.namespace,table);SchemaRecord known=schema(channel,false);if(known!=null){if(!Codec.encode(known.schema).equals(Codec.encode(table)))throw new IllegalStateException("DORIS_SCHEMA_MIGRATION_REQUIRED");verify(known);return;}
        DorisIndexPlan plan=DorisIndexPlan.plan(table,settings.secondaryIndexes);plan.requireSupported();database(event.namespace,settings.autoCreate);String dataName=dataName(settings,table),data=q(settings.metadataDatabase)+"."+q(dataName),view=q(event.namespace)+"."+q(table.name),comment=binding+":"+table.id+":"+Codec.sha256(Codec.encode(table))+":"+plan.profile;
        List<String> definitions=new ArrayList<>(Arrays.asList(q(KEY)+" VARCHAR(64) NOT NULL",q(VERSION)+" LARGEINT NOT NULL",q(DELETED)+" TINYINT NOT NULL")),names=new ArrayList<>(Arrays.asList(KEY,VERSION,DELETED)),types=new ArrayList<>(Arrays.asList("varchar(64)","largeint","tinyint")),selected=new ArrayList<>();List<Boolean> nullable=new ArrayList<>(Arrays.asList(false,false,false));
        for(TableSchema.Column column:table.columns){if(Arrays.asList(KEY,VERSION,DELETED).contains(column.name.toLowerCase(Locale.ROOT))||column.name.toUpperCase(Locale.ROOT).startsWith("__DORIS_"))throw new IllegalArgumentException("DORIS_RESERVED_COLUMN");String type=DorisRows.type(column);definitions.add(q(column.name)+" "+type+(column.nullable?" NULL":" NOT NULL"));names.add(column.name);types.add(type.equals("STRING")?"text":type.toLowerCase(Locale.ROOT));nullable.add(column.nullable);selected.add(q(column.name));}
        for(DorisIndexPlan.Index index:plan.indexes)definitions.add(index.definition());Map<String,String> existing=object(settings.metadataDatabase,dataName);if(existing!=null&&!comment.equals(existing.get("TABLE_COMMENT")))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");
        if(existing==null){if(!settings.autoCreate)throw new IllegalStateException("DORIS_TABLE_CREATE_DISABLED");sql.execute("CREATE TABLE IF NOT EXISTS "+data+" ("+String.join(",",definitions)+") UNIQUE KEY("+q(KEY)+") COMMENT "+commentLiteral(comment)+" DISTRIBUTED BY HASH("+q(KEY)+") BUCKETS "+settings.buckets+properties(true));}
        columns(dataName,names,types,nullable);String dataDefinition=definition(settings.metadataDatabase,dataName,false);if(!dataDefinition.contains("UNIQUE KEY(`"+KEY+"`)")||!dataDefinition.contains("\"function_column.sequence_col\" = \""+VERSION+"\"")||!dataDefinition.contains("\"enable_unique_key_merge_on_write\" = \"true\""))throw new IllegalStateException("DORIS_MOW_SEQUENCE_REQUIRED");
        existing=object(event.namespace,table.name);if(existing!=null&&!comment.equals(existing.get("TABLE_COMMENT")))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");
        if(existing==null){if(!settings.autoCreate)throw new IllegalStateException("DORIS_TABLE_CREATE_DISABLED");sql.execute("CREATE VIEW "+view+" COMMENT "+commentLiteral(comment)+" AS SELECT "+String.join(",",selected)+" FROM "+data+" WHERE "+q(DELETED)+"=0");}
        SchemaRecord record=new SchemaRecord();record.schema=table;record.namespace=event.namespace;record.channel=channel;record.dataName=dataName;record.indexPlan=plan;record.dataIdentity=physicalIdentity(dataName);record.dataDefinitionHash=Codec.sha256(dataDefinition);record.viewDefinitionHash=Codec.sha256(definition(event.namespace,table.name,true));save("SCHEMA",Codec.sha256(channel),Codec.sha256(Codec.encode(record)),Codec.encode(record));schemas.put(channel,record);verify(record);
    }
    private static String batchHash(List<Message> messages)throws Exception{MessageDigest digest=MessageDigest.getInstance("SHA-256");for(Message message:messages)for(byte[] bytes:Arrays.asList(message.id.getBytes(StandardCharsets.UTF_8),message.destination.getBytes(StandardCharsets.UTF_8),message.body)){digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());digest.update(bytes);}return Base64.getEncoder().encodeToString(digest.digest());}
    @Override public void publish(List<Message> messages)throws Exception{
        if(rootIdentity==null)throw new IllegalStateException("PUBLICATION_NOT_PREPARED");if(messages==null||messages.isEmpty())throw new IllegalArgumentException("DORIS_EMPTY_BATCH");root(false);verifyReceipt();List<MessageEvent> events=new ArrayList<>();Set<String> verified=new HashSet<>();Map<String,List<ObjectNode>> rows=new LinkedHashMap<>();
        for(Message message:messages){MessageEvent event=Codec.decode(new String(message.body,StandardCharsets.UTF_8),MessageEvent.class);if(event.version!=1||!settings.pipelineId.equals(event.pipelineId)||!settings.subscriptionId.equals(event.subscriptionId)||!settings.generation.equals(event.generation)||!Objects.equals(message.id,event.id))throw new IllegalArgumentException("DORIS_EVENT_IDENTITY_MISMATCH");events.add(event);
            if(Arrays.asList("ROW","SNAPSHOT_ROW").contains(event.kind)){
                SchemaRecord schema=schema(message.destination,true);if(!schema.schema.id.equals(event.tableId)||schema.schema.version!=event.schemaVersion||event.change==null||event.ordinal<0||event.sequence<0)throw new IllegalStateException("DORIS_ROW_SCHEMA_CHANGED");event.change.validate();if(verified.add(schema.channel))verify(schema);BigInteger version=event.kind.equals("SNAPSHOT_ROW")?BigInteger.ZERO:BigInteger.valueOf(event.sequence).shiftLeft(32).add(BigInteger.valueOf(event.ordinal).shiftLeft(1));List<ObjectNode> table=rows.computeIfAbsent(schema.channel,key->new ArrayList<>());Change change=event.change;
                if(!Objects.equals(change.tableId,schema.schema.id)||change.schemaVersion!=schema.schema.version)throw new IllegalStateException("DORIS_ROW_SCHEMA_CHANGED");
                ObjectNode before=change.before==null?null:DorisRows.row(schema.schema,change.before,event.beforeKey,version,true,settings.http.maximumRequestBytes),after=change.after==null?null:DorisRows.row(schema.schema,change.after,event.afterKey,event.kind.equals("SNAPSHOT_ROW")?version:version.add(BigInteger.ONE),false,settings.http.maximumRequestBytes);
                if(before!=null&&(after==null||!Objects.equals(event.beforeKey,event.afterKey)))table.add(before);if(after!=null)table.add(after);
            }else if(!Arrays.asList("SCHEMA","BEGIN","COMMIT","SNAPSHOT_BEGIN","SNAPSHOT_END").contains(event.kind))throw new IllegalArgumentException("DORIS_EVENT_KIND_UNSUPPORTED");
        }
        for(MessageEvent event:events)if(event.kind.equals("SCHEMA"))create(event);
        for(Map.Entry<String,List<ObjectNode>> table:rows.entrySet()){
            SchemaRecord schema=schema(table.getKey(),true);List<ObjectNode> batch=new ArrayList<>();int bytes=2;
            for(ObjectNode row:table.getValue()){int size=Codec.JSON.writeValueAsBytes(row).length;if(bytes+size+(batch.isEmpty()?0:1)>settings.http.maximumRequestBytes&&!batch.isEmpty()){write(schema,batch);batch.clear();bytes=2;}batch.add(row);bytes+=size+(batch.size()==1?0:1);}if(!batch.isEmpty())write(schema,batch);verify(schema);
        }
        String id=Codec.sha256(messages.get(messages.size()-1).id),hash=Codec.sha256(batchHash(messages));save("RECEIPT",id,hash,"");if(load("RECEIPT",id,hash)==null)throw new IllegalStateException("DORIS_RECEIPT_NOT_OBSERVED");root(false);receiptId=id;receiptHash=hash;
    }
    private void write(SchemaRecord schema,List<ObjectNode> rows)throws Exception{
        byte[] body=Codec.JSON.writeValueAsBytes(rows);http.load(settings.metadataDatabase,schema.dataName,label(schema.channel+":"+Base64.getEncoder().encodeToString(body)),body,rows.size());
        Map<String,ObjectNode> expected=new LinkedHashMap<>();for(ObjectNode row:rows){String key=row.path(KEY).asText();ObjectNode before=expected.get(key);if(before==null||row.path(VERSION).bigIntegerValue().compareTo(before.path(VERSION).bigIntegerValue())>=0)expected.put(key,row);}
        List<String> selected=new ArrayList<>(Arrays.asList(q(KEY)+" AS rm_key","CAST("+q(VERSION)+" AS STRING) AS rm_version",q(DELETED)+" AS rm_deleted"));for(int i=0;i<schema.schema.columns.size();i++)selected.add("CAST("+q(schema.schema.columns.get(i).name)+" AS STRING) AS rm_c"+i);
        List<String> keys=new ArrayList<>(Collections.nCopies(expected.size(),"?"));List<Map<String,String>> found=sql.query("SELECT "+String.join(",",selected)+" FROM "+q(settings.metadataDatabase)+"."+q(schema.dataName)+" WHERE "+q(KEY)+" IN ("+String.join(",",keys)+")",expected.keySet().toArray(new String[0]));Set<String> seen=new HashSet<>();
        for(Map<String,String> actual:found){String key=actual.get("rm_key");ObjectNode row=expected.get(key);if(row==null||!seen.add(key))throw new IllegalStateException("DORIS_ROW_IDENTITY_INVALID");BigInteger version=new BigInteger(actual.get("rm_version"));int comparison=version.compareTo(row.path(VERSION).bigIntegerValue());if(comparison<0)throw new IllegalStateException("DORIS_ROWS_NOT_OBSERVED");if(comparison==0){if(!actual.get("rm_deleted").equals(row.path(DELETED).asText()))throw new IllegalStateException("DORIS_ROWS_NOT_OBSERVED");for(int i=0;i<schema.schema.columns.size();i++)if(!matches(schema.schema.columns.get(i),actual.get("rm_c"+i),row.get(schema.schema.columns.get(i).name)))throw new IllegalStateException("DORIS_ROWS_NOT_OBSERVED");}}
        if(seen.size()!=expected.size())throw new IllegalStateException("DORIS_ROWS_NOT_OBSERVED");
    }
    private static boolean matches(TableSchema.Column column,String actual,com.fasterxml.jackson.databind.JsonNode expected){
        if(expected.isNull())return actual==null;if(actual==null)return false;
        switch(column.type){case BOOLEAN:return expected.asBoolean()?actual.equals("1")||actual.equalsIgnoreCase("true"):actual.equals("0")||actual.equalsIgnoreCase("false");case INT64:case UINT64:return new BigInteger(actual).equals(expected.bigIntegerValue());case DECIMAL:return new java.math.BigDecimal(actual).compareTo(expected.decimalValue())==0;case FLOAT32:return Float.floatToIntBits(Float.parseFloat(actual))==Float.floatToIntBits(expected.floatValue());case FLOAT64:return Double.doubleToLongBits(Double.parseDouble(actual))==Double.doubleToLongBits(expected.doubleValue());default:return actual.equals(expected.asText());}
    }
    @Override public void close(){http.close();sql.close();}
}
