package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.*;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/** MySQL GTID adapter for explicit databases. Uses complete commit boundaries and MVCC snapshot artifacts. */
public final class MysqlCdcSource implements SourceAdapter {
    private final Config config;
    private final Set<String> databases;
    private final String generation;
    private final Map<String,TableSchema> schemas=new java.util.concurrent.ConcurrentHashMap<>();
    private MysqlSource selected;
    private Connection evaluator;
    private String sqlMode;
    public MysqlCdcSource(Config config,Set<String> databases,String generation)throws Exception{
        if(databases==null||databases.isEmpty()||generation==null||generation.isEmpty())throw new IllegalArgumentException("MYSQL_SOURCE_CONFIG_INVALID");
        this.config=config;this.databases=new TreeSet<>(databases);this.generation=generation;validateFailover(null,Collections.emptyList());
    }
    @Override public String type(){return "MYSQL";}
    public String physicalIdentity(){return selected.identity();}
    public void checkTopology(SourcePosition captured,List<SourcePosition> dependencies)throws Exception{
        String required=position(captured).gtidSet;for(SourcePosition p:dependencies)required=Gtids.union(required,position(p).gtidSet);
        MysqlSource candidate=new SourceSelector(config).select(position(captured),required);
        if(!selected.identity().equals(candidate.identity()))throw new SQLTransientConnectionException("SOURCE_PRIMARY_CHANGED","08006");
    }
    private SourcePosition position(Model.Position p){Map<String,String> values=new TreeMap<>();values.put("gtidSet",p.gtidSet);values.put("file",p.file);values.put("offset",Long.toString(p.offset));values.put("serverUuid",p.sourceUuid);return new SourcePosition(1,"MYSQL",config.source.clusterId,values);}
    private Model.Position position(SourcePosition p){
        if(!"MYSQL".equals(p.getSourceType())||!config.source.clusterId.equals(p.getSourceId())||p.getFormatVersion()!=1)throw new IllegalArgumentException("MYSQL_POSITION_IDENTITY_MISMATCH");
        Model.Position result=new Model.Position(p.getPayload().get("file"),Long.parseLong(p.getPayload().get("offset")));result.gtidSet=Gtids.parse(p.getPayload().get("gtidSet")).toString();result.sourceUuid=p.getPayload().get("serverUuid");return result;
    }
    @Override public SourcePosition position()throws Exception{return position(selected.position());}
    SourcePosition position(Connection connection)throws Exception{return position(selected.position(connection));}
    Connection readConnection()throws SQLException{return selected.connect();}
    @Override public boolean covers(SourcePosition captured,SourcePosition required){return captured!=null&&required!=null&&Gtids.contains(position(captured).gtidSet,position(required).gtidSet);}
    @Override public ConsistentRead consistentRead(List<TableSchema> tables,long gateMillis,long readMillis)throws Exception{return new MysqlConsistentRead(this,tables,gateMillis,readMillis);}
    @Override public void validateFailover(SourcePosition captured,List<SourcePosition> dependencies)throws Exception{
        String required=captured==null?"":position(captured).gtidSet;for(SourcePosition p:dependencies)required=Gtids.union(required,position(p).gtidSet);
        selected=new SourceSelector(config).select(captured==null?null:position(captured),required);
        if(evaluator!=null){evaluator.close();evaluator=null;}
        try(Connection c=selected.connect();Statement st=c.createStatement();ResultSet rs=st.executeQuery("SELECT @@SESSION.sql_mode")){rs.next();sqlMode=rs.getString(1);}
    }
    @Override public boolean includes(SourcePosition baseline,String id){return Gtids.contains(position(baseline).gtidSet,id);}
    public void useSchema(TableSchema table){table.validate();schemas.put(table.database+"\u0000"+table.name,table);}
    @Override public List<TableSchema> discover()throws Exception{
        List<TableSchema> result=new ArrayList<>();try(Connection c=selected.connect()){
            for(String database:databases){
                try(PreparedStatement check=c.prepareStatement("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE BINARY SCHEMA_NAME=?")){
                    check.setString(1,database);try(ResultSet rs=check.executeQuery()){if(!rs.next())throw new IllegalArgumentException("SOURCE_DATABASE_NOT_VISIBLE_OR_CASE_MISMATCH");}
                }
                try(PreparedStatement p=c.prepareStatement("SELECT TABLE_NAME,ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")){
                p.setString(1,database);try(ResultSet rs=p.executeQuery()){while(rs.next()){if(!"InnoDB".equalsIgnoreCase(rs.getString(2)))throw new IllegalArgumentException("SOURCE_ENGINE_UNSUPPORTED");TableSchema schema=describe(c,database,rs.getString(1));useSchema(schema);result.add(schema);}}
            }}
        }if(result.isEmpty())throw new IllegalArgumentException("SOURCE_NO_VISIBLE_TABLES");return result;
    }
    TableSchema describe(Connection c,String database,String name)throws Exception{
        TableSchema table=new TableSchema();table.id=TableSchema.identity(database,"",name);table.database=database;table.name=name;table.sourceEngine="InnoDB";table.properties.put("mysql.sqlMode",sqlMode);
        try(PreparedStatement p=c.prepareStatement("SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE,CHARACTER_SET_NAME,COLLATION_NAME,IS_NULLABLE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,EXTRA,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION")){
            p.setString(1,database);p.setString(2,name);try(ResultSet rs=p.executeQuery()){while(rs.next()){
                TableSchema.Column col=new TableSchema.Column();col.name=rs.getString(1);col.sourceType=rs.getString(3);col.charset=rs.getString(4);col.collation=rs.getString(5);col.nullable="YES".equals(rs.getString(6));col.length=rs.getLong(7);col.precision=rs.getInt(8);col.scale=rs.getInt(9);
                String extra=rs.getString(10).toUpperCase(Locale.ROOT);col.generationKind=extra.contains("VIRTUAL GENERATED")?"VIRTUAL":extra.contains("STORED GENERATED")?"STORED":null;col.generationExpression=rs.getString(11);
                if(col.generationKind!=null)validateExpression(col.generationExpression);
                String type=rs.getString(2);table.properties.put("mysql.type."+col.name,type);
                switch(type){
                    case "tinyint":case "smallint":case "mediumint":case "int":case "bigint":case "year":col.type=col.sourceType.toLowerCase(Locale.ROOT).contains("unsigned")?TableSchema.Type.UINT64:TableSchema.Type.INT64;break;
                    case "decimal":col.type=TableSchema.Type.DECIMAL;break;
                    case "float":col.type=TableSchema.Type.FLOAT32;break;case "double":col.type=TableSchema.Type.FLOAT64;break;
                    case "char":case "varchar":case "tinytext":case "text":case "mediumtext":case "longtext":col.type=TableSchema.Type.STRING;Model.charset(col.charset);break;
                    case "binary":case "varbinary":case "tinyblob":case "blob":case "mediumblob":case "longblob":col.type=TableSchema.Type.BYTES;break;
                    case "date":col.type=TableSchema.Type.DATE;break;case "time":col.type=TableSchema.Type.TIME;break;case "datetime":col.type=TableSchema.Type.DATETIME;break;case "timestamp":col.type=TableSchema.Type.TIMESTAMP;break;
                    case "json":col.type=TableSchema.Type.JSON;break;
                    default:throw new IllegalArgumentException("SOURCE_TYPE_REQUIRES_PROFILE: "+type);
                }
                table.columns.add(col);
            }}
        }
        Map<String,TableSchema.Index> indexes=new LinkedHashMap<>();
        try(PreparedStatement p=c.prepareStatement("SELECT INDEX_NAME,NON_UNIQUE,COLUMN_NAME,SUB_PART,COLLATION,INDEX_TYPE FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX")){
            p.setString(1,database);p.setString(2,name);try(ResultSet rs=p.executeQuery()){while(rs.next()){
                String indexName=rs.getString(1);TableSchema.Index index=indexes.computeIfAbsent(indexName,k->{TableSchema.Index i=new TableSchema.Index();i.name=k;return i;});index.unique=!rs.getBoolean(2);index.primary="PRIMARY".equals(indexName);index.method=rs.getString(6);
                String column=rs.getString(3);if(column==null)index.expression="MYSQL_FUNCTIONAL_INDEX_REQUIRES_PLAN";else index.columns.add(column);index.prefixLengths.add(rs.getInt(4));index.descending.add("D".equals(rs.getString(5)));if(index.primary)table.primaryKey.add(column);
            }}
        }
        table.indexes.addAll(indexes.values());table.validate();return table;
    }
    private Model.Column legacy(TableSchema table,TableSchema.Column col){Model.Column c=new Model.Column();c.name=col.name;c.mysqlType=table.properties.get("mysql.type."+col.name);c.definition=col.sourceType;c.charset=col.charset;c.targetType=MysqlSource.mapType(c.mysqlType,c.definition,col.length,col.precision,col.scale);return c;}
    Cell jdbc(TableSchema table,int column,ResultSet rs,int resultColumn)throws Exception{
        if(table.columns.get(column).type==TableSchema.Type.TIME)return Cell.value(MysqlDuration.canonical(rs.getString(resultColumn)));
        TableSchema.Column col=table.columns.get(column);if(table.properties.get("mysql.type."+col.name).equals("year")){int year=rs.getInt(resultColumn);return Cell.value(rs.wasNull()?null:Integer.toString(year));}
        return Cell.value(legacy(table,col).fromJdbc(rs,resultColumn));
    }
    private void context(Connection c,TableSchema table)throws Exception{try(PreparedStatement p=c.prepareStatement("SET SESSION sql_mode=?")){p.setString(1,table.properties.get("mysql.sqlMode"));p.execute();}}
    static String readColumns(TableSchema table){return table.columns.stream().map(col->readExpression(Sql.mysql(col.name),col.type)).collect(Collectors.joining(","));}
    private static String readExpression(String expression,TableSchema.Type type){return type==TableSchema.Type.TIME?"CAST(("+expression+") AS CHAR)":expression;}
    @Override public Snapshot snapshot(TableSchema table)throws Exception{
        Connection lock=selected.connect(),read=null;PreparedStatement query=null;ResultSet rows=null;boolean locked=false;
        try{
            try(Statement st=lock.createStatement()){st.setQueryTimeout(10);st.execute("FLUSH TABLES WITH READ LOCK");locked=true;}
            TableSchema current=describe(lock,table.database,table.name);
            if(!Codec.encode(current).equals(Codec.encode(table)))throw new IllegalStateException("SOURCE_SCHEMA_CHANGED_BEFORE_SNAPSHOT");
            read=selected.connect();context(read,table);read.setAutoCommit(false);
            try(Statement st=read.createStatement()){st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");st.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT");}
            SourcePosition baseline=position(selected.position());
            String columns=readColumns(table);
            query=read.prepareStatement("SELECT "+columns+" FROM "+Sql.mysql(table.database)+"."+Sql.mysql(table.name),ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);query.setFetchSize(Integer.MIN_VALUE);rows=query.executeQuery();
            try(Statement st=lock.createStatement()){st.execute("UNLOCK TABLES");locked=false;}
            final Connection connection=read;final PreparedStatement statement=query;final ResultSet result=rows;
            return new Snapshot(){boolean closed;
                public SourcePosition baseline(){return baseline;}
                public List<List<Cell>> next(int maximum)throws Exception{if(closed||maximum<1||maximum>10000)throw new IllegalArgumentException("SNAPSHOT_READ_INVALID");List<List<Cell>> chunk=new ArrayList<>();while(chunk.size()<maximum&&result.next()){List<Cell> row=new ArrayList<>();for(int i=0;i<table.columns.size();i++)row.add(jdbc(table,i,result,i+1));chunk.add(row);}return chunk;}
                public void close()throws Exception{closed=true;try{result.close();}finally{try{statement.close();}finally{try{connection.rollback();}finally{connection.close();}}}}
            };
        }catch(Exception failure){if(rows!=null)rows.close();if(query!=null)query.close();if(read!=null)read.close();throw failure;}
        finally{if(locked)try(Statement st=lock.createStatement()){st.execute("UNLOCK TABLES");}finally{lock.close();}else lock.close();}
    }
    private static void validateExpression(String expression){
        if(expression==null||expression.isEmpty()||expression.indexOf(';')>=0||expression.indexOf('@')>=0)throw new IllegalArgumentException("GENERATED_EXPRESSION_REQUIRES_PLAN");
        Set<String> allowed=new HashSet<>(Arrays.asList("abs","ceil","ceiling","floor","round","truncate","mod","pow","power","sqrt","sign","concat","concat_ws","lower","lcase","upper","ucase","length","char_length","character_length","substring","substr","left","right","trim","ltrim","rtrim","replace","reverse","coalesce","ifnull","nullif","if","cast","convert","json_extract","json_unquote","json_length","json_type","json_valid","json_object","json_array"));
        java.util.regex.Matcher functions=java.util.regex.Pattern.compile("(?i)([a-z_][a-z0-9_]*)\\s*\\(").matcher(expression);while(functions.find())if(!allowed.contains(functions.group(1).toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("GENERATED_FUNCTION_REQUIRES_PLAN");
    }
    private String cast(TableSchema.Column col){
        switch(col.type){case INT64:return "CAST(? AS SIGNED)";case UINT64:return "CAST(? AS UNSIGNED)";case DECIMAL:return "CAST(? AS DECIMAL("+col.precision+","+col.scale+"))";case FLOAT32:case FLOAT64:return "(? + 0e0)";
            case BYTES:return "CAST(? AS BINARY)";case DATE:return "CAST(? AS DATE)";case TIME:return "CAST(? AS TIME(6))";case DATETIME:case TIMESTAMP:return "CAST(? AS DATETIME(6))";case JSON:return "CAST(? AS JSON)";
            default:if(col.charset==null||col.collation==null||!col.charset.matches("[A-Za-z0-9_]+")||!col.collation.matches("[A-Za-z0-9_]+"))throw new IllegalArgumentException("GENERATED_CHARSET_REQUIRES_PLAN");return "CAST(? AS CHAR CHARACTER SET "+col.charset+") COLLATE "+col.collation;}
    }
    private List<Cell> image(TableSchema table,Object[] values,BitSet included)throws Exception{
        if(values.length!=table.columns.size())throw new IllegalStateException("BINLOG_SCHEMA_WIDTH_CHANGED");List<Cell> row=new ArrayList<>();
        for(int i=0;i<values.length;i++){
            if(!included.get(i)){row.add(Cell.missing());continue;}
            if(table.columns.get(i).type==TableSchema.Type.TIME){if(values[i]!=null&&!(values[i] instanceof Long))throw new IllegalArgumentException("MYSQL_DURATION_ROW_IMAGE_INVALID");row.add(Cell.value(values[i]==null?null:MysqlDuration.format((Long)values[i])));}
            else row.add(Cell.value(legacy(table,table.columns.get(i)).fromBinlog(values[i])));
        }
        for(int i=0;i<row.size();i++)if(!row.get(i).present){
            TableSchema.Column generated=table.columns.get(i);if(!"VIRTUAL".equals(generated.generationKind))throw new IllegalStateException("BINLOG_BASE_OR_STORED_IMAGE_INCOMPLETE");
            List<Integer> fields=new ArrayList<>();List<String> columns=new ArrayList<>();for(int n=0;n<row.size();n++)if(row.get(n).present){fields.add(n);columns.add(cast(table.columns.get(n))+" AS "+Sql.mysql(table.columns.get(n).name));}
            if(columns.isEmpty())throw new IllegalStateException("GENERATED_INPUTS_MISSING");
            if(evaluator==null)evaluator=selected.connect();context(evaluator,table);
            // Historical literals only. This query never fetches a current source row.
            try(PreparedStatement p=evaluator.prepareStatement("SELECT "+readExpression(generated.generationExpression,generated.type)+" FROM (SELECT "+String.join(",",columns)+") AS `_rivomesh_values`")){
                p.setQueryTimeout(10);for(int n=0;n<fields.size();n++){int field=fields.get(n);if(table.columns.get(field).type==TableSchema.Type.BYTES)p.setBytes(n+1,row.get(field).value==null?null:Base64.getDecoder().decode(row.get(field).value));else p.setString(n+1,row.get(field).value);}
                try(ResultSet rs=p.executeQuery()){if(!rs.next())throw new IllegalStateException("GENERATED_VALUE_MISSING");row.set(i,jdbc(table,i,rs,1));}
            }
        }
        return row;
    }
    @Override public Stream stream(SourcePosition captured,Receiver receiver)throws Exception{return new Capture(captured,receiver);}
    private final class Capture implements Stream {
        final BinaryLogClient client;final Receiver receiver;final Map<Long,TableMapEventData> tableIds=new HashMap<>();final List<Change> changes=new ArrayList<>();final AtomicReference<Throwable> failure=new AtomicReference<>();
        volatile boolean closing,delivering;volatile long lastEvent=System.nanoTime();String gtid,committed,file;
        Capture(SourcePosition captured,Receiver receiver)throws Exception{
            this.receiver=receiver;Model.Position p=position(captured);committed=p.gtidSet;file=p.file;
            client=new BinaryLogClient(selected.endpoint().host,selected.endpoint().port,config.source.username,config.source.password);client.setServerId(config.source.serverId);client.setGtidSet(committed);client.setGtidSetFallbackToPurged(false);client.setUseBinlogFilenamePositionInGtidMode(false);client.setSSLMode(SSLMode.valueOf(config.source.sslMode));client.setKeepAlive(false);client.setHeartbeatInterval(1000);client.setConnectTimeout(10000);
            client.setEventDeserializer(MysqlEventDeserializer.create(tableIds));
            client.registerEventListener(e->{
                if(closing||failure.get()!=null)return;
                lastEvent=System.nanoTime();
                try{
                    if(selected.serverId()!=client.getMasterServerId())
                        throw new PipelineFailure(PipelineFailure.Phase.SOURCE_READ,new IllegalStateException("SOURCE_BACKEND_CHANGED"));
                    accept(e);
                }catch(Throwable problem){recordFailure(PipelineFailure.Phase.SOURCE_DECODE,problem);}
            });
            client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener(){
                public void onCommunicationFailure(BinaryLogClient c,Exception e){recordFailure(PipelineFailure.Phase.SOURCE_READ,e);}
                public void onEventDeserializationFailure(BinaryLogClient c,Exception e){
                    recordFailure(PipelineFailure.Phase.SOURCE_DECODE,
                            new SyncFailure("BINLOG_EVENT_DECODE_FAILED","SOURCE_DECODE",false,"BINLOG_EVENT_DECODE_FAILED",e,null,0,null));
                }
                public void onDisconnect(BinaryLogClient c){recordFailure(PipelineFailure.Phase.SOURCE_READ,new java.io.IOException("MYSQL_STREAM_DISCONNECTED"));}
            });
            try{client.connect(10000);}catch(Exception e){closing=true;receiver.cancelled();client.disconnect();throw e;}
        }
        private void recordFailure(PipelineFailure.Phase phase,Throwable problem){
            if(!closing)failure.compareAndSet(null,problem instanceof PipelineFailure?problem:new PipelineFailure(phase,problem));
        }
        TableSchema table(long id){TableMapEventData map=tableIds.get(id);if(map==null)throw new IllegalStateException("BINLOG_TABLE_MAP_MISSING");if(!databases.contains(map.getDatabase()))return null;TableSchema table=schemas.get(map.getDatabase()+"\u0000"+map.getTable());if(table==null||table.columns.size()!=map.getColumnTypes().length)throw new IllegalStateException("BINLOG_SCHEMA_CHANGED");return table;}
        void row(TableSchema t,Change.Operation operation,Object[] before,BitSet beforeMask,Object[] after,BitSet afterMask)throws Exception{
            if(t==null)return;if(gtid==null)throw new IllegalStateException("BINLOG_GTID_MISSING");if(changes.size()>=config.maxTransactionRows)throw new IllegalStateException("TRANSACTION_ROW_LIMIT");Change ch=new Change();ch.tableId=t.id;ch.schemaVersion=t.version;ch.operation=operation;ch.eventId=gtid+":"+changes.size();if(before!=null)ch.before=image(t,before,beforeMask);if(after!=null)ch.after=image(t,after,afterMask);changes.add(ch);
        }
        void commit(EventHeaderV4 header)throws Exception{
            if(gtid==null)throw new IllegalStateException("BINLOG_GTID_MISSING");String next=Gtids.union(committed,gtid);Model.Position p=new Model.Position(file,header.getNextPosition());p.gtidSet=next;p.sourceUuid=selected.identity();SourceTransaction tx=new SourceTransaction();tx.id=gtid;tx.streamGeneration=generation;tx.position=position(p);tx.changes.addAll(changes);tx.validate();delivering=true;try{receiver.committed(tx);}finally{lastEvent=System.nanoTime();delivering=false;}committed=next;changes.clear();gtid=null;
        }
        void accept(Event e)throws Exception{
            EventData data=e.getData();EventHeaderV4 h=e.getHeader();String event=h.getEventType().name();
            if(event.contains("ANONYMOUS")||event.contains("TAGGED")||event.equals("TRANSACTION_PAYLOAD"))throw new IllegalStateException("BINLOG_EVENT_PROFILE_UNSUPPORTED");
            if(data instanceof RotateEventData){if(gtid!=null||!changes.isEmpty())throw new IllegalStateException("BINLOG_ROTATE_INSIDE_TRANSACTION");file=((RotateEventData)data).getBinlogFilename();tableIds.clear();}
            else if(data instanceof GtidEventData){if(gtid!=null)throw new IllegalStateException("BINLOG_TRANSACTION_UNFINISHED");gtid=((GtidEventData)data).getGtid();Gtids.parse(gtid);}
            else if(data instanceof TableMapEventData){TableMapEventData map=(TableMapEventData)data;tableIds.put(map.getTableId(),map);table(map.getTableId());}
            else if(data instanceof WriteRowsEventData){WriteRowsEventData r=(WriteRowsEventData)data;TableSchema t=table(r.getTableId());for(Object[] a:r.getRows())row(t,Change.Operation.INSERT,null,null,a,r.getIncludedColumns());}
            else if(data instanceof DeleteRowsEventData){DeleteRowsEventData r=(DeleteRowsEventData)data;TableSchema t=table(r.getTableId());for(Object[] b:r.getRows())row(t,Change.Operation.DELETE,b,r.getIncludedColumns(),null,null);}
            else if(data instanceof UpdateRowsEventData){UpdateRowsEventData r=(UpdateRowsEventData)data;TableSchema t=table(r.getTableId());for(Map.Entry<java.io.Serializable[],java.io.Serializable[]> pair:r.getRows())row(t,Change.Operation.UPDATE,pair.getKey(),r.getIncludedColumnsBeforeUpdate(),pair.getValue(),r.getIncludedColumns());}
            else if(data instanceof XidEventData)commit(h);
            else if(data instanceof QueryEventData){String query=((QueryEventData)data).getSql().trim();if(query.equalsIgnoreCase("BEGIN"))return;if(query.equalsIgnoreCase("ROLLBACK")){changes.clear();commit(h);}else if(query.equalsIgnoreCase("COMMIT"))commit(h);else throw new PipelineFailure(PipelineFailure.Phase.DDL,new IllegalStateException("DDL_PLAN_REQUIRED"));}
        }
        public void check()throws Exception{Throwable e=failure.get();if(e instanceof Exception)throw (Exception)e;if(e!=null)throw new IllegalStateException("MYSQL_CAPTURE_FAILED",e);if(!client.isConnected()||(!delivering&&System.nanoTime()-lastEvent>java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(config.source.binlogIdleTimeoutMillis)))throw new PipelineFailure(PipelineFailure.Phase.SOURCE_READ,new java.io.IOException("MYSQL_STREAM_IDLE_OR_DISCONNECTED"));}
        public void close()throws Exception{closing=true;receiver.cancelled();client.disconnect();}
    }
    @Override public void close()throws Exception{if(evaluator!=null){evaluator.close();evaluator=null;}}
}
