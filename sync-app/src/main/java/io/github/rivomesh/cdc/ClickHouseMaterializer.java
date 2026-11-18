package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Native versioned rows with confirmed batch receipts, sharing the publication checkpoint protocol. */
public final class ClickHouseMaterializer implements ConfirmedPublisher {
    public static final class Settings {
        public ClickHouseHttpClient.Settings http;
        public String metadataDatabase="rivomesh_v3",pipelineId,subscriptionId,generation,fingerprint;
        public Map<String,String> mappings=new TreeMap<>();
        public boolean autoCreate=true;
        public ClickHouseIndexPlan.Mode secondaryIndexes=ClickHouseIndexPlan.Mode.DATA_SKIPPING;
    }
    public static final class SchemaRecord {
        public TableSchema schema;
        public String namespace,channel,dataName,dataUuid,viewUuid,dataDefinitionHash,viewDefinitionHash;
        public ClickHouseIndexPlan indexPlan;
    }
    private static final String KEY="_rivomesh_key",VERSION="_rivomesh_version",DELETED="_rivomesh_deleted";
    private final Settings settings;
    private final ClickHouseHttpClient client;
    private final String binding,rootName,root,comment;
    private final Map<String,SchemaRecord> schemas=new HashMap<>();
    private String rootUuid,receiptId,receiptHash;

    public ClickHouseMaterializer(Settings s)throws Exception{
        if(s==null||s.secondaryIndexes==null||s.http==null||s.fingerprint==null||!s.fingerprint.matches("[a-f0-9]{64}")||s.generation==null||s.generation.isEmpty()||s.generation.length()>128||s.mappings==null||s.mappings.isEmpty())throw new IllegalArgumentException("CLICKHOUSE_SINK_CONFIG_INVALID");
        for(String id:Arrays.asList(s.pipelineId,s.subscriptionId))if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}"))throw new IllegalArgumentException("CLICKHOUSE_SINK_ID_INVALID");
        q(s.metadataDatabase);Set<String> destinations=new HashSet<>();for(Map.Entry<String,String> entry:s.mappings.entrySet()){q(entry.getValue());if(entry.getKey()==null||entry.getKey().isEmpty()||entry.getValue().equalsIgnoreCase(s.metadataDatabase)||!destinations.add(entry.getValue().toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("CLICKHOUSE_MAPPING_COLLISION");}
        settings=s;binding="rivomesh:"+Codec.sha256(s.pipelineId+":"+s.subscriptionId+":"+s.generation);
        rootName="rm3_ch_"+Codec.sha256(s.pipelineId+":"+s.subscriptionId);root=q(s.metadataDatabase)+"."+q(rootName);comment=binding+":"+s.fingerprint;client=new ClickHouseHttpClient(s.http);
    }
    public static Map<String,Object> check(ClickHouseHttpClient.Settings settings)throws Exception{
        try(ClickHouseHttpClient client=new ClickHouseHttpClient(settings)){
            String version=client.query("SELECT version()").path("data").get(0).get(0).asText();
            JsonNode features=client.query("SELECT name FROM system.merge_tree_settings WHERE name IN ('fsync_after_insert','fsync_part_directory','min_rows_to_fsync_after_merge')").path("data");
            if(features.size()!=3||!client.query("SELECT toTypeName(toUInt128(0)),formatQuerySingleLine('SELECT 1')").path("data").get(0).get(0).asText().equals("UInt128"))throw new IllegalStateException("CLICKHOUSE_PROFILE_UNSUPPORTED");
            if(client.query("SELECT count() FROM system.settings WHERE name IN (\'use_skip_indexes_if_final\',\'use_skip_indexes_if_final_exact_mode\')").path("data").get(0).get(0).asInt()!=2)throw new IllegalStateException("CLICKHOUSE_FINAL_INDEX_SAFETY_UNSUPPORTED");
            Map<String,Object> result=new LinkedHashMap<>();result.put("version",version);result.put("consistency",SinkAdapter.Consistency.VERSIONED_MATERIALIZATION.name());result.put("temporalAndJsonStorage","lossless String");result.put("secondaryIndexProfile","DATA_SKIPPING_V1 or explicit NONE");return result;
        }
    }
    public static String q(String identifier){if(identifier==null||identifier.isEmpty()||identifier.indexOf('\0')>=0||identifier.getBytes(StandardCharsets.UTF_8).length>512)throw new IllegalArgumentException("CLICKHOUSE_IDENTIFIER_INVALID");return "`"+identifier.replace("\\","\\\\").replace("`","\\`")+"`";}
    private static String literal(String text){return "'"+text.replace("\\","\\\\").replace("'","\\'")+"'";}
    private JsonNode rows(String query)throws Exception{return client.query(query).path("data");}
    private JsonNode object(String database,String name)throws Exception{
        JsonNode values=rows("SELECT engine,toString(uuid),comment,create_table_query FROM system.tables WHERE database="+literal(database)+" AND name="+literal(name));
        if(values.size()>1)throw new IllegalStateException("CLICKHOUSE_CATALOG_AMBIGUOUS");return values.size()==0?null:values.get(0);
    }
    private void database(String name,boolean create)throws Exception{
        JsonNode before=rows("SELECT engine FROM system.databases WHERE name="+literal(name));
        if(before.size()==0){if(!create)throw new IllegalStateException("CLICKHOUSE_DATABASE_MISSING_REBUILD_REQUIRED");client.execute("CREATE DATABASE IF NOT EXISTS "+q(name)+" ENGINE=Atomic");before=rows("SELECT engine FROM system.databases WHERE name="+literal(name));}
        if(before.size()!=1||!"Atomic".equals(before.get(0).get(0).asText()))throw new IllegalStateException("CLICKHOUSE_ATOMIC_DATABASE_REQUIRED");
    }
    private void root(boolean create)throws Exception{
        database(settings.metadataDatabase,create&&receiptId==null);JsonNode existing=object(settings.metadataDatabase,rootName);
        if(existing==null){if(!create||receiptId!=null)throw new IllegalStateException("MESSAGE_DESTINATION_MISSING_REBUILD_REQUIRED");
            client.execute("CREATE TABLE IF NOT EXISTS "+root+" (kind String,id String,hash FixedString(64),payload String) ENGINE=MergeTree ORDER BY (kind,id) SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1 COMMENT "+literal(comment)+"");existing=object(settings.metadataDatabase,rootName);}
        if(existing==null||!"MergeTree".equals(existing.get(0).asText())||!comment.equals(existing.get(2).asText()))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");
        String found=existing.get(1).asText();if(found.isEmpty()||rootUuid!=null&&!rootUuid.equals(found))throw new IllegalStateException("CLICKHOUSE_RESOURCE_ID_CHANGED");rootUuid=found;
        checkColumns(settings.metadataDatabase,rootName,Arrays.asList("kind","id","hash","payload"),Arrays.asList("String","String","FixedString(64)","String"));
        String definition=existing.get(3).asText();if(!definition.contains("fsync_after_insert = 1")||!definition.contains("fsync_part_directory = 1")||!definition.contains("min_rows_to_fsync_after_merge = 1")||!definition.contains("ORDER BY (kind, id)"))throw new IllegalStateException("CLICKHOUSE_RECEIPT_CONFIGURATION_REQUIRES_PLAN");
    }
    private String fence(){
        return "throwIf((SELECT any(toString(uuid)) FROM system.tables WHERE database="+literal(settings.metadataDatabase)+" AND name="+literal(rootName)+") != "+literal(rootUuid)+", 'RIVOMESH_RESOURCE_CHANGED') = 0";
    }
    private void save(String kind,String id,String hash,String payload)throws Exception{
        client.execute("INSERT INTO "+root+" SELECT "+literal(kind)+","+literal(id)+","+literal(hash)+","+literal(payload)+" WHERE "+fence());
    }
    private String load(String kind,String id,String expectedHash)throws Exception{
        JsonNode values=rows("SELECT uniqExact(hash),any(hash),uniqExact(payload),any(payload) FROM "+root+" WHERE kind="+literal(kind)+" AND id="+literal(id));JsonNode record=values.get(0);
        if(record.get(0).asLong()==0)return null;
        if(record.get(0).asLong()!=1||record.get(2).asLong()!=1||expectedHash!=null&&!expectedHash.equals(record.get(1).asText()))throw new IllegalStateException("CLICKHOUSE_CONFIRMED_HISTORY_CHANGED_REBUILD_REQUIRED");
        String payload=record.get(3).asText();if(expectedHash==null&&!Codec.sha256(payload).equals(record.get(1).asText()))throw new IllegalStateException("CLICKHOUSE_METADATA_CONTENT_CHANGED");return payload;
    }
    private void verifyReceipt()throws Exception{if(receiptId!=null&&load("RECEIPT",receiptId,receiptHash)==null)throw new IllegalStateException("CLICKHOUSE_CONFIRMED_HISTORY_MISSING_REBUILD_REQUIRED");}
    @Override public boolean requiresProgress(){return true;}
    @Override public void resumeProgress(String progress){
        if(rootUuid!=null||receiptId!=null)throw new IllegalStateException("CLICKHOUSE_PROGRESS_ALREADY_ACTIVE");if(progress==null)return;
        try{String[] fields=progress.split(":",-1);if(fields.length!=3||!fields[0].equals("CLICKHOUSE_RECEIPT_V1")||!fields[2].matches("[a-f0-9]{64}"))throw new IllegalArgumentException();String id=new String(Base64.getUrlDecoder().decode(fields[1]),StandardCharsets.UTF_8);if(id.isEmpty()||id.length()>512||!Base64.getUrlEncoder().withoutPadding().encodeToString(id.getBytes(StandardCharsets.UTF_8)).equals(fields[1]))throw new IllegalArgumentException();receiptId=id;receiptHash=fields[2];}catch(Exception invalid){throw new IllegalArgumentException("CLICKHOUSE_PROGRESS_INVALID");}
    }
    @Override public String confirmedProgress(){return receiptId==null?null:"CLICKHOUSE_RECEIPT_V1:"+Base64.getUrlEncoder().withoutPadding().encodeToString(receiptId.getBytes(StandardCharsets.UTF_8))+":"+receiptHash;}
    @Override public String identity(){return rootUuid==null?null:"CLICKHOUSE_TABLE_V1:"+rootUuid;}
    /** Evidence for the saved predecessor only. Does not activate or materialize any resource. */
    void verifyPrecedingCommit(String progress,String expectedIdentityHash,String expectedEventId)throws Exception{
        resumeProgress(progress);
        if(!expectedEventId.equals(receiptId))throw new IllegalStateException("CLICKHOUSE_RECOVERY_PUBLICATION_UNCERTAIN");
        root(false);
        if(!Codec.sha256(identity()).equals(expectedIdentityHash))throw new IllegalStateException("CLICKHOUSE_RESOURCE_ID_CHANGED");
        verifyReceipt();
        root(false);
    }
    @Override public void prepare(Set<String> destinations)throws Exception{root(settings.autoCreate);verifyReceipt();for(String channel:destinations)if(!control(channel)){SchemaRecord record=schema(channel,false);if(record!=null)verify(record);}}
    @Override public void require(Set<String> destinations)throws Exception{root(false);verifyReceipt();for(String channel:destinations)if(!control(channel))verify(schema(channel,true));}
    private static boolean control(String channel){return channel.endsWith(".__schema")||channel.endsWith(".__transactions");}
    private SchemaRecord schema(String channel,boolean required)throws Exception{
        SchemaRecord record=schemas.get(channel);if(record==null){String payload=load("SCHEMA",Codec.sha256(channel),null);if(payload!=null){record=Codec.decode(payload,SchemaRecord.class);if(!channel.equals(record.channel)||record.schema==null)throw new IllegalStateException("CLICKHOUSE_SCHEMA_METADATA_INVALID");schemas.put(channel,record);}}
        if(record==null&&required)throw new IllegalStateException("CLICKHOUSE_SCHEMA_MISSING_REBUILD_REQUIRED");return record;
    }
    private void checkColumns(String database,String table,List<String> names,List<String> types)throws Exception{
        JsonNode found=rows("SELECT name,type FROM system.columns WHERE database="+literal(database)+" AND table="+literal(table)+" ORDER BY position");
        if(found.size()!=names.size())throw new IllegalStateException("CLICKHOUSE_COLUMN_SHAPE_CHANGED");for(int i=0;i<names.size();i++)if(!names.get(i).equals(found.get(i).get(0).asText())||!types.get(i).replace(" ","").equals(found.get(i).get(1).asText().replace(" ","")))throw new IllegalStateException("CLICKHOUSE_COLUMN_SHAPE_CHANGED");
    }
    private void verify(SchemaRecord record)throws Exception{
        JsonNode data=object(settings.metadataDatabase,record.dataName),view=object(record.namespace,record.schema.name);
        if(data==null||view==null)throw new IllegalStateException("CLICKHOUSE_TABLE_MISSING_REBUILD_REQUIRED");
        if(!record.dataUuid.equals(data.get(1).asText())||!record.viewUuid.equals(view.get(1).asText()))throw new IllegalStateException("CLICKHOUSE_RESOURCE_ID_CHANGED");
        if(!record.dataDefinitionHash.equals(Codec.sha256(data.get(3).asText()))||!record.viewDefinitionHash.equals(Codec.sha256(view.get(3).asText())))throw new IllegalStateException("CLICKHOUSE_TABLE_DEFINITION_CHANGED");
    }
    private String canonical(String sql)throws Exception{return rows("SELECT formatQuerySingleLine("+literal(sql)+")").get(0).get(0).asText();}
    private void create(MessageEvent event)throws Exception{
        TableSchema table=event.schema;if(table==null||!settings.mappings.containsKey(table.database)||!event.namespace.equals(settings.mappings.get(table.database)))throw new IllegalArgumentException("CLICKHOUSE_MAPPING_MISMATCH");table.validate();String channel=MessageSink.channel(event.namespace,table);SchemaRecord known=schema(channel,false);
        if(known!=null){if(!Codec.encode(known.schema).equals(Codec.encode(table)))throw new IllegalStateException("CLICKHOUSE_SCHEMA_MIGRATION_REQUIRED");verify(known);return;}
        ClickHouseIndexPlan indexPlan=ClickHouseIndexPlan.plan(table,settings.secondaryIndexes);indexPlan.requireSupported();
        database(event.namespace,settings.autoCreate);String dataName="rm3_data_"+Codec.sha256(binding+":"+table.id),data=q(settings.metadataDatabase)+"."+q(dataName),view=q(event.namespace)+"."+q(table.name);
        String owner=binding+":"+table.id+":"+Codec.sha256(Codec.encode(table))+(settings.secondaryIndexes==ClickHouseIndexPlan.Mode.DATA_SKIPPING?":DATA_SKIPPING_V1":"");List<String> definitions=new ArrayList<>(Arrays.asList(q(KEY)+" FixedString(64)",q(VERSION)+" UInt128",q(DELETED)+" UInt8")),names=new ArrayList<>(Arrays.asList(KEY,VERSION,DELETED)),types=new ArrayList<>(Arrays.asList("FixedString(64)","UInt128","UInt8")),columns=new ArrayList<>();
        for(TableSchema.Column c:table.columns){if(c.name.equalsIgnoreCase(KEY)||c.name.equalsIgnoreCase(VERSION)||c.name.equalsIgnoreCase(DELETED))throw new IllegalArgumentException("CLICKHOUSE_RESERVED_COLUMN");String type=ClickHouseRowBinary.type(c);definitions.add(q(c.name)+" "+type);names.add(c.name);types.add(type);columns.add(q(c.name));}
        JsonNode existingData=object(settings.metadataDatabase,dataName);if(existingData!=null&&!owner.equals(existingData.get(2).asText()))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");
        List<String> physicalDefinitions=new ArrayList<>(definitions);for(ClickHouseIndexPlan.SkippingIndex index:indexPlan.indexes)physicalDefinitions.add(index.definition());
        String dataSql="CREATE TABLE "+data+" ("+String.join(",",physicalDefinitions)+") ENGINE=ReplacingMergeTree("+q(VERSION)+") ORDER BY "+q(KEY)+" SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1,index_granularity=8192 COMMENT "+literal(owner);
        if(existingData==null){if(!settings.autoCreate)throw new IllegalStateException("CLICKHOUSE_TABLE_CREATE_DISABLED");client.execute(dataSql.replaceFirst("CREATE TABLE ","CREATE TABLE IF NOT EXISTS "));}
        existingData=object(settings.metadataDatabase,dataName);if(existingData==null||!"ReplacingMergeTree".equals(existingData.get(0).asText())||!owner.equals(existingData.get(2).asText()))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");checkColumns(settings.metadataDatabase,dataName,names,types);
        if(!canonical(dataSql).equals(canonical(existingData.get(3).asText())))throw new IllegalStateException("CLICKHOUSE_DATA_DEFINITION_CHANGED");
        String select="SELECT "+String.join(",",columns)+" FROM "+data+" FINAL WHERE "+q(DELETED)+"=0"+(settings.secondaryIndexes==ClickHouseIndexPlan.Mode.DATA_SKIPPING?" SETTINGS use_skip_indexes_if_final=1,use_skip_indexes_if_final_exact_mode=1":""),viewSql="CREATE VIEW "+view+" ("+String.join(",",definitions.subList(3,definitions.size()))+") SQL SECURITY INVOKER COMMENT "+literal(owner)+" AS "+select;
        JsonNode existingView=object(event.namespace,table.name);if(existingView!=null&&!owner.equals(existingView.get(2).asText()))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");
        if(existingView==null){if(!settings.autoCreate)throw new IllegalStateException("CLICKHOUSE_TABLE_CREATE_DISABLED");client.execute(viewSql.replaceFirst("CREATE VIEW ","CREATE VIEW IF NOT EXISTS "));existingView=object(event.namespace,table.name);}
        if(existingView==null||!"View".equals(existingView.get(0).asText())||!owner.equals(existingView.get(2).asText()))throw new IllegalStateException("DESTINATION_ALREADY_OWNED");
        String expected=rows("SELECT formatQuerySingleLine("+literal(viewSql)+")").get(0).get(0).asText(),actual=rows("SELECT formatQuerySingleLine("+literal(existingView.get(3).asText())+")").get(0).get(0).asText();
        if(!expected.equals(actual))throw new IllegalStateException("CLICKHOUSE_VIEW_DEFINITION_CHANGED");
        SchemaRecord record=new SchemaRecord();record.indexPlan=indexPlan;record.schema=table;record.namespace=event.namespace;record.channel=channel;record.dataName=dataName;record.dataUuid=existingData.get(1).asText();record.viewUuid=existingView.get(1).asText();record.dataDefinitionHash=Codec.sha256(existingData.get(3).asText());record.viewDefinitionHash=Codec.sha256(existingView.get(3).asText());String payload=Codec.encode(record);save("SCHEMA",Codec.sha256(channel),Codec.sha256(payload),payload);schemas.put(channel,record);verify(record);
    }
    private static String batchHash(List<Message> messages)throws Exception{
        MessageDigest digest=MessageDigest.getInstance("SHA-256");for(Message message:messages)for(byte[] part:Arrays.asList(message.id.getBytes(StandardCharsets.UTF_8),message.destination.getBytes(StandardCharsets.UTF_8),message.body)){digest.update(java.nio.ByteBuffer.allocate(4).putInt(part.length).array());digest.update(part);}StringBuilder result=new StringBuilder();for(byte b:digest.digest())result.append(String.format(Locale.ROOT,"%02x",b&255));return result.toString();
    }
    private static final class Mutation {
        SchemaRecord schema;String key;BigInteger version;boolean deleted;List<Cell> values;
        Mutation(SchemaRecord schema,String key,BigInteger version,boolean deleted,List<Cell> values){this.schema=schema;this.key=key;this.version=version;this.deleted=deleted;this.values=values;}
    }
    private ClickHouseRowBinary.Buffer body(SchemaRecord schema)throws Exception{
        ClickHouseRowBinary.Buffer body=new ClickHouseRowBinary.Buffer(settings.http.maximumRequestBytes);List<String> definitions=new ArrayList<>(Arrays.asList(q(KEY)+" FixedString(64)",q(VERSION)+" UInt128",q(DELETED)+" UInt8"));for(TableSchema.Column c:schema.schema.columns)definitions.add(q(c.name)+" "+ClickHouseRowBinary.type(c));
        String sql="INSERT INTO "+q(settings.metadataDatabase)+"."+q(schema.dataName)+" SELECT * FROM input("+literal(String.join(",",definitions))+") WHERE "+fence()+" FORMAT RowBinary\n";byte[] prefix=sql.getBytes(StandardCharsets.UTF_8);body.write(prefix,0,prefix.length);return body;
    }
    private byte[] encoded(Mutation mutation)throws Exception{
        if(mutation.key==null)throw new IllegalArgumentException("CLICKHOUSE_ROW_IDENTITY_REQUIRED");ClickHouseRowBinary.Buffer row=new ClickHouseRowBinary.Buffer(settings.http.maximumRequestBytes);byte[] key=Codec.sha256(mutation.key).getBytes(StandardCharsets.UTF_8);row.write(key,0,key.length);ClickHouseRowBinary.unsigned(row,mutation.version,16);row.write(mutation.deleted?1:0);ClickHouseRowBinary.writeRow(row,mutation.schema.schema.columns,mutation.values);return row.toByteArray();
    }
    private void write(List<Mutation> mutations)throws Exception{
        Map<String,List<Mutation>> tables=new LinkedHashMap<>();for(Mutation mutation:mutations)tables.computeIfAbsent(mutation.schema.channel,k->new ArrayList<>()).add(mutation);
        // Size/type validation precedes the first row write, including SQL/identity overhead.
        for(List<Mutation> table:tables.values()){int overhead=body(table.get(0).schema).size();for(Mutation mutation:table)if(encoded(mutation).length>settings.http.maximumRequestBytes-overhead)throw new IllegalArgumentException("CLICKHOUSE_ROW_LIMIT");}
        for(List<Mutation> table:tables.values()){
            SchemaRecord schema=table.get(0).schema;ClickHouseRowBinary.Buffer batch=body(schema);int count=0;
            for(Mutation mutation:table){byte[] row=encoded(mutation);if(row.length>batch.remaining()&&count>0){client.execute(batch.toByteArray());batch=body(schema);count=0;}batch.write(row,0,row.length);count++;}
            if(count>0)client.execute(batch.toByteArray());verify(schema);
        }
    }
    @Override public void publish(List<Message> messages)throws Exception{
        if(rootUuid==null)throw new IllegalStateException("PUBLICATION_NOT_PREPARED");if(messages==null||messages.isEmpty())throw new IllegalArgumentException("CLICKHOUSE_EMPTY_BATCH");root(false);verifyReceipt();List<MessageEvent> events=new ArrayList<>();Set<String> verifiedSchemas=new HashSet<>();
        // Validate every row in this batch before issuing its first data write.
        for(Message message:messages){MessageEvent event=Codec.decode(new String(message.body,StandardCharsets.UTF_8),MessageEvent.class);if(event.version!=1||!settings.pipelineId.equals(event.pipelineId)||!settings.subscriptionId.equals(event.subscriptionId)||!settings.generation.equals(event.generation)||!Objects.equals(message.id,event.id))throw new IllegalArgumentException("CLICKHOUSE_EVENT_IDENTITY_MISMATCH");
            if(Arrays.asList("ROW","SNAPSHOT_ROW").contains(event.kind)){SchemaRecord schema=schema(message.destination,true);if(!schema.schema.id.equals(event.tableId)||schema.schema.version!=event.schemaVersion||event.change==null)throw new IllegalStateException("CLICKHOUSE_ROW_SCHEMA_CHANGED");event.change.validate();if(event.sequence<0||event.ordinal<0)throw new IllegalArgumentException("CLICKHOUSE_ROW_VERSION_INVALID");for(List<Cell> image:Arrays.asList(event.change.before,event.change.after))if(image!=null)ClickHouseRowBinary.row(schema.schema.columns,image,settings.http.maximumRequestBytes);if(verifiedSchemas.add(schema.channel))verify(schema);}
            else if(!Arrays.asList("SCHEMA","BEGIN","COMMIT","SNAPSHOT_BEGIN","SNAPSHOT_END").contains(event.kind))throw new IllegalArgumentException("CLICKHOUSE_EVENT_KIND_UNSUPPORTED");events.add(event);
        }
        List<Mutation> mutations=new ArrayList<>();
        for(int i=0;i<events.size();i++){MessageEvent event=events.get(i);if(event.kind.equals("SCHEMA")){create(event);continue;}if(!event.kind.equals("ROW")&&!event.kind.equals("SNAPSHOT_ROW"))continue;
            SchemaRecord schema=schema(messages.get(i).destination,true);BigInteger version=event.kind.equals("SNAPSHOT_ROW")?BigInteger.ZERO:BigInteger.valueOf(event.sequence).shiftLeft(32).add(BigInteger.valueOf(event.ordinal).shiftLeft(1));Change change=event.change;
            if(change.before!=null&&(change.after==null||!Objects.equals(event.beforeKey,event.afterKey)))mutations.add(new Mutation(schema,event.beforeKey,version,true,change.before));
            if(change.after!=null)mutations.add(new Mutation(schema,event.afterKey,event.kind.equals("SNAPSHOT_ROW")?version:version.add(BigInteger.ONE),false,change.after));
        }
        write(mutations);
        String id=messages.get(messages.size()-1).id,hash=batchHash(messages);save("RECEIPT",id,hash,"");if(load("RECEIPT",id,hash)==null)throw new IllegalStateException("CLICKHOUSE_RECEIPT_NOT_OBSERVED");root(false);receiptId=id;receiptHash=hash;
    }
    @Override public void close(){client.close();}
}
