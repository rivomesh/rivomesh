package io.github.rivomesh.cdc;

import java.math.*;
import java.util.*;

/** Native ACID INSERT history and latest-version views. Source cross-table visibility is not atomic. */
public final class HiveMaterializer implements ConfirmedPublisher {
    public static final class Settings {
        public String jdbcUrl,username,password,metadataDatabase="rivomesh_v3",pipelineId,subscriptionId,generation,fingerprint;
        public Map<String,String> mappings=new TreeMap<>();
        public int queryTimeoutSeconds=300,maximumSqlBytes=4*1024*1024,maximumResponseBytes=16*1024*1024;
        public boolean autoCreate=true,acknowledgeIndexOmission=false;
    }
    public static final class SchemaRecord {
        public TableSchema schema;
        public String namespace,channel,dataName,dataIdentity,viewDefinitionHash;
    }
    private static final String KEY="_rivomesh_key",VERSION="_rivomesh_version",DELETED="_rivomesh_deleted";
    private final Settings settings;
    private final HiveSqlClient sql;
    private final String binding,rootName,root,owner;
    private final Map<String,SchemaRecord> schemas=new HashMap<>(),schemaIds=new HashMap<>();
    private String rootIdentity,receiptId,receiptHash;
    public HiveMaterializer(Settings s){
        if(s==null||s.fingerprint==null||!s.fingerprint.matches("[a-f0-9]{64}")||s.generation==null||s.generation.isEmpty()||s.generation.length()>128||s.mappings==null||s.mappings.isEmpty())throw new IllegalArgumentException("HIVE_CONFIG_INVALID");
        for(String id:Arrays.asList(s.pipelineId,s.subscriptionId))if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw new IllegalArgumentException("HIVE_ID_INVALID");
        HiveRows.name(s.metadataDatabase);Set<String> unique=new HashSet<>();for(String name:s.mappings.values()){HiveRows.name(name);if(name.equalsIgnoreCase(s.metadataDatabase)||!unique.add(name.toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("HIVE_MAPPING_COLLISION");}
        settings=s;binding="rivomesh:"+Codec.sha256(s.pipelineId+":"+s.subscriptionId+":"+s.generation);rootName="rm3_hive_"+Codec.sha256(s.pipelineId+":"+s.subscriptionId).substring(0,48);root=q(s.metadataDatabase)+"."+q(rootName);owner=binding+":"+s.fingerprint;sql=new HiveSqlClient(s.jdbcUrl,s.username,s.password,s.queryTimeoutSeconds,s.maximumSqlBytes,s.maximumResponseBytes);
    }
    private static String q(String name){return HiveRows.q(name);}
    private static String safe(String value){if(value==null||!value.matches("[A-Za-z0-9:._-]+"))throw new IllegalArgumentException("HIVE_INTERNAL_LITERAL_INVALID");return "'"+value+"'";}
    public static Map<String,Object> check(Settings s)throws Exception{
        try(HiveMaterializer client=new HiveMaterializer(s)){List<Map<String,String>> rows=client.sql.query("SELECT version() AS version");String version=rows.size()==1?rows.get(0).get("version"):null;if(version==null||!version.matches("[0-9]+\\..*"))throw new IllegalStateException("HIVE_PROFILE_UNSUPPORTED");
            client.transactionSettings();Map<String,Object> result=new LinkedHashMap<>();result.put("version",version);result.put("profile","ACID_HISTORY_V1");result.put("queryResultsCache","disabled required for writer and readers");result.put("consistency",SinkAdapter.Consistency.VERSIONED_MATERIALIZATION.name());result.put("widerDecimals","precision>38 stored as STRING");result.put("secondaryIndexes","not created; Hive has no ordinary BTREE equivalent");return result;}
    }
    private void transactionSettings()throws Exception{
        for(String[] rule:new String[][]{{"hive.txn.manager","org.apache.hadoop.hive.ql.lockmgr.DbTxnManager","HIVE_ACID_CONFIGURATION_REQUIRED"},{"hive.support.concurrency","true","HIVE_ACID_CONFIGURATION_REQUIRED"},{"hive.query.results.cache.enabled","false","HIVE_QUERY_CACHE_DISABLED_REQUIRED"}}){List<Map<String,String>> rows=sql.query("SET "+rule[0]);String actual=rows.size()==1?rows.get(0).values().iterator().next():null;if(!Objects.equals(actual,rule[0]+"="+rule[1]))throw new IllegalStateException(rule[2]);}
    }
    private boolean databaseExists(String name)throws Exception{return !sql.query("SHOW DATABASES LIKE "+safe(name)).isEmpty();}
    private void database(String name,boolean create)throws Exception{if(!databaseExists(name)){if(!create)throw new IllegalStateException("HIVE_DATABASE_MISSING_REBUILD_REQUIRED");sql.execute("CREATE DATABASE IF NOT EXISTS "+q(name));}}
    private boolean exists(String database,String table)throws Exception{if(!databaseExists(database))return false;return !sql.query("SHOW TABLES IN "+q(database)+" LIKE "+safe(table)).isEmpty();}
    private Map<String,String> properties(String table)throws Exception{
        Map<String,String> result=new HashMap<>();for(Map<String,String> row:sql.query("SHOW TBLPROPERTIES "+table)){String key=row.get("prpt_name"),value=row.get("prpt_value");if(key==null||value==null)throw new IllegalStateException("HIVE_PROPERTY_METADATA_INVALID");if(result.put(key,value)!=null)throw new IllegalStateException("HIVE_PROPERTY_METADATA_AMBIGUOUS");}return result;
    }
    private String verifyTable(String table,String expectedOwner,List<String> names,List<String> types)throws Exception{
        Map<String,String> properties=properties(table);if(!expectedOwner.equals(properties.get("rivomesh.owner")))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");String identity=properties.get("rivomesh.resource");if(identity==null||!identity.matches("[a-f0-9]{32}")||!"true".equals(properties.get("transactional"))||!"default".equals(properties.get("transactional_properties")))throw new IllegalStateException("HIVE_ACID_RESOURCE_CHANGED");
        List<Map<String,String>> found=sql.query("DESCRIBE "+table);List<String> actualNames=new ArrayList<>(),actualTypes=new ArrayList<>();for(Map<String,String> row:found){String name=row.get("col_name");if(name!=null&&!name.trim().isEmpty()&&!name.startsWith("#")){actualNames.add(name.toLowerCase(Locale.ROOT));actualTypes.add(row.get("data_type").replace(" ","").toLowerCase(Locale.ROOT));}}
        if(!actualNames.equals(names)||!actualTypes.equals(types))throw new IllegalStateException("HIVE_COLUMNS_CHANGED");
        boolean orc=false,managed=false;for(Map<String,String> row:sql.query("DESCRIBE FORMATTED "+table)){String key=row.get("col_name"),value=row.get("data_type");if(key!=null&&value!=null){if(key.trim().equals("InputFormat:"))orc=value.trim().equals("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");if(key.trim().equals("Table Type:"))managed=value.trim().equals("MANAGED_TABLE");}}
        if(!orc||!managed)throw new IllegalStateException("HIVE_MANAGED_ACID_ORC_REQUIRED");return identity;
    }
    private String tableProperties(String owner,String identity){return " TBLPROPERTIES ('transactional'='true','transactional_properties'='default','rivomesh.owner'="+safe(owner)+",'rivomesh.resource'="+safe(identity)+")";}
    private void root(boolean create)throws Exception{
        transactionSettings();database(settings.metadataDatabase,create&&receiptId==null);
        if(!exists(settings.metadataDatabase,rootName)){if(!create||receiptId!=null)throw new IllegalStateException("HIVE_DESTINATION_MISSING_REBUILD_REQUIRED");sql.execute("CREATE TABLE IF NOT EXISTS "+root+" (kind STRING,id STRING,hash STRING,payload STRING) CLUSTERED BY (id) INTO 1 BUCKETS STORED AS ORC"+tableProperties(owner,UUID.randomUUID().toString().replace("-","")));}
        String actual=verifyTable(root,owner,Arrays.asList("kind","id","hash","payload"),Arrays.asList("string","string","string","string"));if(rootIdentity!=null&&!rootIdentity.equals(actual))throw new IllegalStateException("HIVE_RESOURCE_ID_CHANGED");rootIdentity=actual;verifyReceipt();
    }
    private String load(String kind,String id,String hash)throws Exception{
        List<Map<String,String>> found=sql.query("SELECT hash,payload FROM "+root+" WHERE kind="+safe(kind)+" AND id="+safe(id)+" LIMIT 10001");if(found.isEmpty())return null;String payload=null;boolean first=true;
        for(Map<String,String> row:found){String value=row.get("payload"),actual=row.get("hash");if(value==null||!Objects.equals(hash==null?Codec.sha256(value):hash,actual)||!first&&!Objects.equals(payload,value))throw new IllegalStateException("HIVE_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED");payload=value;first=false;}return payload;
    }
    private void save(String kind,String id,String hash,String payload)throws Exception{
        sql.execute("INSERT INTO "+root+" SELECT "+safe(kind)+","+safe(id)+","+safe(hash)+","+HiveRows.text(payload));if(!Objects.equals(payload,load(kind,id,hash)))throw new IllegalStateException("HIVE_METADATA_NOT_OBSERVED");
    }
    private void verifyReceipt()throws Exception{if(receiptId!=null&&load("RECEIPT",receiptId,receiptHash)==null)throw new IllegalStateException("HIVE_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED");}
    @Override public boolean requiresProgress(){return true;}
    @Override public void resumeProgress(String progress){if(rootIdentity!=null||receiptId!=null)throw new IllegalStateException("HIVE_PROGRESS_ALREADY_ACTIVE");if(progress==null)return;String[] parts=progress.split(":",-1);if(parts.length!=4||!parts[0].equals("HIVE_RECEIPT_V1")||!parts[1].matches("[a-f0-9]{32}")||!parts[2].matches("[a-f0-9]{64}")||!parts[3].matches("[a-f0-9]{64}"))throw new IllegalArgumentException("HIVE_PROGRESS_INVALID");rootIdentity=parts[1];receiptId=parts[2];receiptHash=parts[3];}
    @Override public String confirmedProgress(){return receiptId==null?null:"HIVE_RECEIPT_V1:"+rootIdentity+":"+receiptId+":"+receiptHash;}
    @Override public String identity(){return rootIdentity==null?null:"HIVE_RESOURCE_V1:"+rootIdentity;}
    private static boolean control(String channel){return channel.endsWith(".__schema")||channel.endsWith(".__transactions");}
    @Override public void prepare(Set<String> destinations)throws Exception{root(settings.autoCreate);for(String channel:destinations)if(!control(channel)){SchemaRecord record=schema(channel,false);if(record!=null)verify(record);}}
    @Override public void require(Set<String> destinations)throws Exception{root(false);for(String channel:destinations)if(!control(channel))verify(schema(channel,true));}
    private SchemaRecord schema(String channel,boolean required)throws Exception{
        SchemaRecord record=schemas.get(channel);if(record==null){String encoded=load("SCHEMA",Codec.sha256(channel),null);if(encoded!=null){record=Codec.decode(encoded,SchemaRecord.class);if(record.schema==null||!channel.equals(record.channel))throw new IllegalStateException("HIVE_SCHEMA_METADATA_INVALID");cache(record);}}
        if(record==null&&required)throw new IllegalStateException("HIVE_SCHEMA_MISSING_REBUILD_REQUIRED");return record;
    }
    private void cache(SchemaRecord record){schemas.put(record.channel,record);schemaIds.put(record.namespace+"\u0000"+record.schema.id,record);}
    private String owner(TableSchema table){return binding+":"+table.id+":"+Codec.sha256(Codec.encode(table));}
    private List<String> names(TableSchema table){List<String> result=new ArrayList<>(Arrays.asList(KEY,VERSION,DELETED));for(TableSchema.Column c:table.columns)result.add(c.name.toLowerCase(Locale.ROOT));return result;}
    private List<String> types(TableSchema table){List<String> result=new ArrayList<>(Arrays.asList("string","decimal(38,0)","tinyint"));for(TableSchema.Column c:table.columns)result.add(HiveRows.type(c).toLowerCase(Locale.ROOT));return result;}
    private String definition(String table)throws Exception{StringBuilder result=new StringBuilder();for(Map<String,String> row:sql.query("SHOW CREATE TABLE "+table)){String line=row.get("createtab_stmt");if(line==null)throw new IllegalStateException("HIVE_DEFINITION_MISSING");result.append(line).append('\n');}return result.toString();}
    private void verify(SchemaRecord record)throws Exception{
        if(!exists(settings.metadataDatabase,record.dataName)||!exists(record.namespace,record.schema.name))throw new IllegalStateException("HIVE_TABLE_MISSING_REBUILD_REQUIRED");String id=verifyTable(q(settings.metadataDatabase)+"."+q(record.dataName),owner(record.schema),names(record.schema),types(record.schema));if(!Objects.equals(id,record.dataIdentity))throw new IllegalStateException("HIVE_RESOURCE_ID_CHANGED");if(!Objects.equals(record.viewDefinitionHash,Codec.sha256(definition(q(record.namespace)+"."+q(record.schema.name)))))throw new IllegalStateException("HIVE_VIEW_DEFINITION_CHANGED");
    }
    private void create(MessageEvent event)throws Exception{
        TableSchema table=event.schema;if(table==null||!settings.mappings.containsKey(table.database)||!Objects.equals(settings.mappings.get(table.database),event.namespace))throw new IllegalArgumentException("HIVE_MAPPING_MISMATCH");table.validate();HiveIndexPlan.plan(table,settings.acknowledgeIndexOmission).requireAcknowledged();HiveRows.storage(table);if(!table.id.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("HIVE_TABLE_ID_INVALID");String channel=MessageSink.channel(event.namespace,table);SchemaRecord known=schema(channel,false);if(known!=null){if(!Codec.encode(known.schema).equals(Codec.encode(table)))throw new IllegalStateException("HIVE_SCHEMA_MIGRATION_REQUIRED");verify(known);return;}
        database(event.namespace,settings.autoCreate);String dataName="rm3_data_"+Codec.sha256(binding+":"+table.id).substring(0,48),data=q(settings.metadataDatabase)+"."+q(dataName),view=q(event.namespace)+"."+q(table.name),owner=owner(table);List<String> definitions=new ArrayList<>(Arrays.asList(q(KEY)+" STRING",q(VERSION)+" DECIMAL(38,0)",q(DELETED)+" TINYINT")),selected=new ArrayList<>();
        for(TableSchema.Column c:table.columns){q(c.name);if(c.name.toLowerCase(Locale.ROOT).startsWith("_rivomesh_"))throw new IllegalArgumentException("HIVE_RESERVED_COLUMN");definitions.add(q(c.name)+" "+HiveRows.type(c));selected.add(q(c.name));}
        if(!exists(settings.metadataDatabase,dataName)){if(!settings.autoCreate)throw new IllegalStateException("HIVE_CREATE_DISABLED");sql.execute("CREATE TABLE IF NOT EXISTS "+data+" ("+String.join(",",definitions)+") CLUSTERED BY ("+q(KEY)+") INTO 1 BUCKETS STORED AS ORC"+tableProperties(owner,UUID.randomUUID().toString().replace("-","")));}
        String identity=verifyTable(data,owner,names(table),types(table));
        if(!exists(event.namespace,table.name)){if(!settings.autoCreate)throw new IllegalStateException("HIVE_CREATE_DISABLED");sql.execute("CREATE VIEW "+view+" TBLPROPERTIES ('rivomesh.owner'="+safe(owner)+") AS SELECT "+String.join(",",selected)+" FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY "+q(KEY)+" ORDER BY "+q(VERSION)+" DESC) AS "+q("_rivomesh_rank")+" FROM "+data+") rm WHERE "+q("_rivomesh_rank")+"=1 AND "+q(DELETED)+"=0");}
        if(!owner.equals(properties(view).get("rivomesh.owner")))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");SchemaRecord record=new SchemaRecord();record.schema=table;record.namespace=event.namespace;record.channel=channel;record.dataName=dataName;record.dataIdentity=identity;record.viewDefinitionHash=Codec.sha256(definition(view));String encoded=Codec.encode(record);save("SCHEMA",Codec.sha256(channel),Codec.sha256(encoded),encoded);cache(record);verify(record);
    }
    private static BigInteger version(MessageEvent event,int stage){return BigInteger.valueOf(event.sequence).shiftLeft(32).add(BigInteger.valueOf((long)event.ordinal*2+stage));}
    private static String batchHash(List<Message> messages)throws Exception{java.security.MessageDigest hash=java.security.MessageDigest.getInstance("SHA-256");for(Message message:messages)for(byte[] value:Arrays.asList(message.id.getBytes(java.nio.charset.StandardCharsets.UTF_8),message.destination.getBytes(java.nio.charset.StandardCharsets.UTF_8),message.body)){hash.update(java.nio.ByteBuffer.allocate(4).putInt(value.length).array());hash.update(value);}return Codec.sha256(Base64.getEncoder().encodeToString(hash.digest()));}
    private static final class Row {List<Cell> cells;String key;BigInteger version;boolean deleted;}
    private Row row(MessageEvent event,boolean before){Row row=new Row();row.cells=before?event.change.before:event.change.after;row.key=before?event.beforeKey:event.afterKey;row.version=event.kind.equals("SNAPSHOT_ROW")?BigInteger.ZERO:version(event,before?0:1);row.deleted=before;return row;}
    @Override public void publish(List<Message> messages)throws Exception{
        if(messages==null||messages.isEmpty()||messages.size()>100)throw new IllegalArgumentException("HIVE_BATCH_INVALID");root(false);String receipt=Codec.sha256(messages.get(messages.size()-1).id),content=batchHash(messages);boolean receipted=load("RECEIPT",receipt,content)!=null;List<MessageEvent> events=new ArrayList<>();
        for(Message message:messages){MessageEvent event=Codec.decode(new String(message.body,java.nio.charset.StandardCharsets.UTF_8),MessageEvent.class);if(event.version!=1||!settings.pipelineId.equals(event.pipelineId)||!settings.subscriptionId.equals(event.subscriptionId)||!settings.generation.equals(event.generation)||!Objects.equals(message.id,event.id))throw new IllegalArgumentException("HIVE_EVENT_IDENTITY_MISMATCH");events.add(event);}
        for(MessageEvent event:events)if(event.kind.equals("SCHEMA"))create(event);
        Map<String,List<Row>> rows=new LinkedHashMap<>();
        for(MessageEvent event:events){if(event.kind.equals("ROW")||event.kind.equals("SNAPSHOT_ROW")){SchemaRecord record=schema(MessageSink.channel(event.namespace,schemaFor(event)),true);if(!record.schema.id.equals(event.tableId)||record.schema.version!=event.schemaVersion)throw new IllegalStateException("HIVE_SCHEMA_MISMATCH");List<Row> batch=rows.computeIfAbsent(record.channel,k->new ArrayList<>());
                if(event.beforeKey!=null&&(event.afterKey==null||!event.beforeKey.equals(event.afterKey)))batch.add(row(event,true));if(event.afterKey!=null)batch.add(row(event,false));
            }else if(!Arrays.asList("SCHEMA","BEGIN","COMMIT","SNAPSHOT_BEGIN","SNAPSHOT_END").contains(event.kind))throw new IllegalArgumentException("HIVE_EVENT_KIND_INVALID");}
        for(Map.Entry<String,List<Row>> batch:rows.entrySet()){SchemaRecord record=schema(batch.getKey(),true);verify(record);write(record,batch.getValue());}
        if(!receipted)save("RECEIPT",receipt,content,"");receiptId=receipt;receiptHash=content;
    }
    private TableSchema schemaFor(MessageEvent event){SchemaRecord record=schemaIds.get(event.namespace+"\u0000"+event.tableId);if(record==null)throw new IllegalStateException("HIVE_SCHEMA_NOT_LOADED");return record.schema;}
    private void write(SchemaRecord record,List<Row> rows)throws Exception{
        String prefix="INSERT INTO "+q(settings.metadataDatabase)+"."+q(record.dataName)+" ";List<String> selects=new ArrayList<>();int bytes=prefix.length();for(Row row:rows){String select="SELECT "+String.join(",",HiveRows.row(record.schema,row.cells,row.key,row.version,row.deleted));int size=select.getBytes(java.nio.charset.StandardCharsets.UTF_8).length+11;if(size+prefix.length()>settings.maximumSqlBytes)throw new IllegalArgumentException("HIVE_ROW_LIMIT");if(bytes+size>settings.maximumSqlBytes&&!selects.isEmpty()){sql.execute(prefix+String.join(" UNION ALL ",selects));selects.clear();bytes=prefix.length();}selects.add(select);bytes+=size;}if(!selects.isEmpty())sql.execute(prefix+String.join(" UNION ALL ",selects));
        Map<String,Row> expected=new LinkedHashMap<>();for(Row row:rows){String key=Codec.sha256(row.key);Row previous=expected.get(key);if(previous==null||row.version.compareTo(previous.version)>=0)expected.put(key,row);}
        List<String> keys=new ArrayList<>();for(String key:expected.keySet())keys.add(safe(key));List<String> selected=new ArrayList<>(Arrays.asList(q(KEY)+" AS rmkey","CAST("+q(VERSION)+" AS STRING) AS rmversion",q(DELETED)+" AS rmdeleted"));for(int i=0;i<record.schema.columns.size();i++)selected.add("CAST("+q(record.schema.columns.get(i).name)+" AS STRING) AS c"+i);
        List<Map<String,String>> actual=sql.query("SELECT "+String.join(",",selected)+" FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY "+q(KEY)+" ORDER BY "+q(VERSION)+" DESC) AS rmrank FROM "+q(settings.metadataDatabase)+"."+q(record.dataName)+" WHERE "+q(KEY)+" IN ("+String.join(",",keys)+")) rm WHERE rmrank=1");Set<String> seen=new HashSet<>();
        for(Map<String,String> found:actual){Row row=expected.get(found.get("rmkey"));if(row==null||!seen.add(found.get("rmkey")))throw new IllegalStateException("HIVE_ROWS_NOT_OBSERVED");int compared=new BigInteger(found.get("rmversion")).compareTo(row.version);if(compared<0)throw new IllegalStateException("HIVE_ROWS_NOT_OBSERVED");if(compared==0){if(!Objects.equals(found.get("rmdeleted"),row.deleted?"1":"0"))throw new IllegalStateException("HIVE_ROWS_NOT_OBSERVED");for(int i=0;i<row.cells.size();i++)if(!matches(record.schema.columns.get(i),row.cells.get(i).value,found.get("c"+i)))throw new IllegalStateException("HIVE_ROWS_NOT_OBSERVED");}}
        if(seen.size()!=expected.size())throw new IllegalStateException("HIVE_ROWS_NOT_OBSERVED");
    }
    private static boolean matches(TableSchema.Column column,String expected,String actual){if(expected==null)return actual==null;if(actual==null)return false;switch(column.type){case BOOLEAN:return (expected.equals("1")||expected.equals("true"))==Boolean.parseBoolean(actual);case INT64:case UINT64:return new BigInteger(expected).equals(new BigInteger(actual));case DECIMAL:return new BigDecimal(expected).compareTo(new BigDecimal(actual))==0;case FLOAT32:return Float.floatToIntBits(Float.parseFloat(expected))==Float.floatToIntBits(Float.parseFloat(actual));case FLOAT64:return Double.doubleToLongBits(Double.parseDouble(expected))==Double.doubleToLongBits(Double.parseDouble(actual));default:return expected.equals(actual);}}
    @Override public void close(){sql.close();}
}
