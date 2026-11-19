package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.PipelineFailure;
import io.github.rivomesh.cdc.SourceAdapter;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.TableSchema;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/** PostgreSQL single-primary CDC using an isolated logical slot and an exported MVCC snapshot. */
final class PostgresqlCdcSource implements SourceAdapter {
    private static final String DRIVER="org.postgresql.Driver";
    private static final Pattern NAME=Pattern.compile("[a-z_][a-z0-9_]{0,30}");
    private final Config.Source config;
    private final Config.PostgresqlSource options;
    private final Set<String> namespaces;
    private final String generation,slot;
    private final int maxRows,majorVersion;
    private final String systemId,timeline;
    private final PostgresSourcePosition positions;
    private final Map<Integer,TableSchema> tables=new LinkedHashMap<>();

    static void validate(PipelineConfig pipeline){
        Config.Source source=pipeline.source;
        Config.PostgresqlSource postgres=source.postgresql;
        if(postgres==null||postgres.url==null||!postgres.url.startsWith("jdbc:postgresql://")
                ||postgres.url.contains(",")||postgres.publication==null||!NAME.matcher(postgres.publication).matches()
                ||postgres.slotPrefix==null||!NAME.matcher(postgres.slotPrefix).matches()
                ||source.database==null||source.database.isEmpty()||source.username==null||source.username.isEmpty()
                ||source.clusterId==null||!source.clusterId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                ||pipeline.maxTransactionRows<1)
            throw invalid("POSTGRESQL_SOURCE_CONFIG_INVALID");
        Properties parsed=org.postgresql.Driver.parseURL(postgres.url,new Properties());
        if(parsed==null||parsed.containsKey("replication")||parsed.containsKey("assumeMinServerVersion")
                ||parsed.containsKey("preferQueryMode")||parsed.containsKey("user")||parsed.containsKey("password"))
            throw invalid("POSTGRESQL_SOURCE_URL_INVALID");
        if(source.nodes==null||!source.nodes.isEmpty()||!Arrays.asList("AUTO","SINGLE").contains(source.topology))
            throw invalid("POSTGRESQL_SOURCE_SINGLE_PRIMARY_REQUIRED");
        if(source.expectedVersion!=null&&!source.expectedVersion.matches("(17|18)(\\.[0-9]{1,3})?"))
            throw invalid("SOURCE_VERSION_PROFILE_INVALID");
        for(PipelineConfig.Target target:pipeline.targets){
            if(target!=null&&target.product!=io.github.rivomesh.cdc.TargetProduct.POSTGRESQL
                    &&target.product!=io.github.rivomesh.cdc.TargetProduct.MYSQL)
                throw invalid("POSTGRESQL_SOURCE_TARGET_PROFILE_UNAVAILABLE");
        }
    }

    PostgresqlCdcSource(PipelineConfig pipeline,String generation)throws Exception{
        validate(pipeline);
        this.config=pipeline.source;
        this.options=config.postgresql;
        this.namespaces=new TreeSet<>(pipeline.databases);
        this.generation=generation;
        this.maxRows=pipeline.maxTransactionRows;
        this.slot=options.slotPrefix+"_"+Codec.sha256(pipeline.pipelineId+"\n"+generation).substring(0,16);
        try(Connection c=sql();Statement st=c.createStatement();
            ResultSet r=st.executeQuery("SELECT current_database(),current_setting('server_version_num'),"
                    +"current_setting('wal_level'),current_setting('server_encoding'),pg_is_in_recovery()")){
            if(!r.next()||!config.database.equals(r.getString(1)))throw invalid("POSTGRESQL_SOURCE_DATABASE_CHANGED");
            int version=r.getInt(2);
            majorVersion=version/10000;
            String major=Integer.toString(majorVersion);
            if((majorVersion!=17&&majorVersion!=18)||(config.expectedVersion!=null
                    &&!major.equals(config.expectedVersion)&&!(major+"."+version%10000).equals(config.expectedVersion)))
                throw invalid("POSTGRESQL_SOURCE_VERSION_UNSUPPORTED");
            if(r.getBoolean(5)&&options.failoverSlot)
                throw new SQLTransientConnectionException("POSTGRESQL_PRIMARY_NOT_READY","08006");
            if(!"logical".equals(r.getString(3))||!"UTF8".equals(r.getString(4))||r.getBoolean(5))
                throw invalid("POSTGRESQL_LOGICAL_PRIMARY_REQUIRED");
        }
        try(Connection c=replication()){
            String[] identity=identify(c);
            systemId=identity[0];
            timeline=identity[1];
            Map<String,Long> ancestors=options.failoverSlot&&!"1".equals(timeline)?
                    timelineHistory(c,timeline):Collections.emptyMap();
            positions=new PostgresSourcePosition(config.clusterId,systemId,timeline,slot,ancestors);
        }
    }

    private Connection sql()throws Exception{
        return JdbcConnections.open(DRIVER,options.url,config.username,config.password);
    }

    private Connection replication()throws Exception{
        Properties properties=new Properties();
        properties.setProperty("user",config.username);
        if(config.password!=null)properties.setProperty("password",config.password);
        properties.setProperty("assumeMinServerVersion","17");
        properties.setProperty("replication","database");
        properties.setProperty("preferQueryMode","simple");
        Connection connection=new org.postgresql.Driver().connect(options.url,properties);
        if(connection==null)throw new SQLException("POSTGRESQL_REPLICATION_URL_MISMATCH","08001");
        return connection;
    }

    private String[] identify(Connection c)throws Exception{
        try(Statement st=c.createStatement();ResultSet r=st.executeQuery("IDENTIFY_SYSTEM")){
            if(!r.next()||!config.database.equals(r.getString("dbname")))throw invalid("POSTGRESQL_SOURCE_DATABASE_CHANGED");
            return new String[]{r.getString("systemid"),r.getString("timeline")};
        }
    }

    private static Map<String,Long> timelineHistory(Connection c,String timeline)throws Exception{
        if(!timeline.matches("[1-9][0-9]{0,9}"))throw invalid("POSTGRESQL_TIMELINE_HISTORY_INVALID");
        try(Statement statement=c.createStatement();ResultSet result=statement.executeQuery("TIMELINE_HISTORY "+timeline)){
            if(!result.next())throw invalid("POSTGRESQL_TIMELINE_HISTORY_INVALID");
            byte[] contents=result.getBytes("content");
            if(contents==null||contents.length>65536)throw invalid("POSTGRESQL_TIMELINE_HISTORY_INVALID");
            return PostgresSourcePosition.history(new String(contents,StandardCharsets.US_ASCII),timeline);
        }
    }

    private void requireIdentity(Connection c)throws Exception{
        String[] observed=identify(c);
        if(!systemId.equals(observed[0]))throw invalid("POSTGRESQL_SYSTEM_ID_CHANGED");
        if(!timeline.equals(observed[1])){
            if(options.failoverSlot)
                throw new SQLTransientConnectionException("POSTGRESQL_RECHECK_PROMOTED_PRIMARY","08006");
            throw invalid("POSTGRESQL_TIMELINE_CHANGED");
        }
    }

    @Override public String type(){return PostgresSourcePosition.TYPE;}

    @Override public void requireSeparateDatabase(String driver,String url,String username,String password)throws Exception{
        if(!DRIVER.equals(driver))return;
        try(Connection c=JdbcConnections.open(driver,url,username,password);Statement st=c.createStatement();
            ResultSet r=st.executeQuery("SELECT current_database(),system_identifier::text FROM pg_control_system()")){
            if(r.next()&&config.database.equals(r.getString(1))&&systemId.equals(r.getString(2)))
                throw invalid("SOURCE_JOURNAL_OR_TARGET_FEEDBACK_LOOP");
        }
    }

    private long[] slotPosition()throws Exception{
        try(Connection c=sql();PreparedStatement p=c.prepareStatement("SELECT database,plugin,restart_lsn::text,confirmed_flush_lsn::text,wal_status,failover,two_phase "
                +"FROM pg_replication_slots WHERE slot_name=?")){
            p.setString(1,slot);
            try(ResultSet r=p.executeQuery()){
                if(!r.next())return null;
                if(!config.database.equals(r.getString(1))||!"pgoutput".equals(r.getString(2))
                        ||r.getBoolean(6)!=options.failoverSlot||r.getBoolean(7))
                    throw invalid("POSTGRESQL_SLOT_SCOPE_CHANGED");
                String wal=r.getString(5);
                if(!Arrays.asList("reserved","extended").contains(wal)||r.getString(3)==null||r.getString(4)==null)
                    throw invalid("POSTGRESQL_REQUIRED_WAL_UNAVAILABLE");
                return new long[]{PostgresSourcePosition.parse(r.getString(3)),PostgresSourcePosition.parse(r.getString(4))};
            }
        }
    }

    @Override public SourcePosition position()throws Exception{
        if(options.failoverSlot){
            try(Connection c=sql();Statement s=c.createStatement();
                ResultSet r=s.executeQuery("SHOW synchronized_standby_slots")){
                if(!r.next()||r.getString(1).trim().isEmpty())
                    throw invalid("POSTGRESQL_SYNCHRONIZED_STANDBY_SLOT_REQUIRED");
            }
        }
        long[] existing=slotPosition();
        if(existing!=null)return positions.at(existing[1]);
        try(Connection c=replication()){
            requireIdentity(c);
            String flags=options.failoverSlot?"SNAPSHOT 'nothing', FAILOVER true":"SNAPSHOT 'nothing'";
            try(Statement st=c.createStatement();ResultSet r=st.executeQuery("CREATE_REPLICATION_SLOT "+slot
                    +" LOGICAL pgoutput ("+flags+")")){
                if(!r.next())throw invalid("POSTGRESQL_SLOT_CREATE_FAILED");
                return positions.at(PostgresSourcePosition.parse(r.getString("consistent_point")));
            }
        }
    }

    @Override public void validateFailover(SourcePosition captured,List<SourcePosition> dependencies)throws Exception{
        try(Connection c=replication()){requireIdentity(c);}
        if(captured==null)return;
        long durable=positions.lsn(captured);
        for(SourcePosition dependency:dependencies)positions.lsn(dependency);
        long[] available=slotPosition();
        if(available==null||Long.compareUnsigned(available[0],durable)>0)throw invalid("POSTGRESQL_REQUIRED_WAL_UNAVAILABLE");
        if(Long.compareUnsigned(available[1],durable)>0)throw invalid("POSTGRESQL_SLOT_AHEAD_OF_JOURNAL");
    }

    @Override public void checkTopology(SourcePosition captured,List<SourcePosition> dependencies)throws Exception{
        try(Connection c=replication()){requireIdentity(c);}
        positions.lsn(captured);
        for(SourcePosition dependency:dependencies)positions.lsn(dependency);
        // The capture thread can persist and confirm a newer commit while this check is running.
        // Compare confirmed LSN to the journal only before starting replication.
        if(slotPosition()==null)throw invalid("POSTGRESQL_REQUIRED_WAL_UNAVAILABLE");
    }

    @Override public boolean includes(SourcePosition baseline,String transaction){return positions.includes(baseline,transaction);}
    @Override public boolean covers(SourcePosition captured,SourcePosition required){return positions.covers(captured,required);}

    @Override public List<TableSchema> discover()throws Exception{
        Map<Integer,TableSchema> found=new LinkedHashMap<>();
        Set<String> observed=new TreeSet<>();
        try(Connection c=sql()){
            c.setReadOnly(true);
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);
            try(PreparedStatement p=c.prepareStatement("SELECT puballtables,pubinsert,pubupdate,pubdelete,pubviaroot FROM pg_publication WHERE pubname=?")){
                p.setString(1,options.publication);
                try(ResultSet r=p.executeQuery()){
                    if(!r.next()||r.getBoolean(1)||!r.getBoolean(2)||!r.getBoolean(3)||!r.getBoolean(4)||r.getBoolean(5))
                        throw invalid("POSTGRESQL_EXPLICIT_PUBLICATION_REQUIRED");
                }
            }
            try(PreparedStatement p=c.prepareStatement("SELECT t.schemaname,t.tablename,t.attnames,t.rowfilter,r.oid::text,r.relkind,r.relreplident "
                    +"FROM pg_publication_tables t JOIN pg_namespace n ON n.nspname=t.schemaname "
                    +"JOIN pg_class r ON r.relnamespace=n.oid AND r.relname=t.tablename "
                    +"WHERE t.pubname=? ORDER BY t.schemaname,t.tablename")){
                p.setString(1,options.publication);
                try(ResultSet r=p.executeQuery()){
                    while(r.next()){
                        String schema=r.getString(1),name=r.getString(2);
                        if(!namespaces.contains(schema)||r.getString(4)!=null||!"r".equals(r.getString(6)))
                            throw invalid("POSTGRESQL_PUBLICATION_SCOPE_UNSUPPORTED");
                        if(!"f".equals(r.getString(7)))throw invalid("POSTGRESQL_FULL_REPLICA_IDENTITY_REQUIRED");
                        String oid=r.getString(5);
                        TableSchema table=describe(c,schema,name,oid);
                        String[] published=(String[])r.getArray(3).getArray();
                        if(published.length!=table.columns.size()||!new TreeSet<>(Arrays.asList(published))
                                .equals(columnNames(table)))throw invalid("POSTGRESQL_PUBLICATION_COLUMN_FILTER_UNSUPPORTED");
                        found.put(Integer.parseUnsignedInt(oid),table);
                        observed.add(schema);
                    }
                }
            }
            c.commit();
        }
        if(found.isEmpty()||!observed.equals(namespaces))throw invalid("POSTGRESQL_PUBLICATION_SCOPE_UNSUPPORTED");
        tables.clear();
        tables.putAll(found);
        return new ArrayList<>(found.values());
    }

    private static Set<String> columnNames(TableSchema table){
        Set<String> result=new TreeSet<>();
        for(TableSchema.Column column:table.columns)result.add(column.name);
        return result;
    }

    private TableSchema describe(Connection c,String schema,String name,String oid)throws Exception{
        TableSchema table=new TableSchema();
        table.id=TableSchema.identity(config.database,schema,name);
        table.database=schema;
        table.name=name;
        table.sourceEngine="POSTGRESQL";
        try(PreparedStatement p=c.prepareStatement("SELECT a.attname,a.atttypid::text,a.atttypmod,a.attnotnull,"
                +"a.attgenerated,pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a "
                +"LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
                +"WHERE a.attrelid=?::oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum")){
            p.setString(1,oid);
            try(ResultSet r=p.executeQuery()){
                while(r.next()){
                    TableSchema.Column column=new TableSchema.Column();
                    String generated=r.getString(5);
                    if(!generated.isEmpty()){
                        if(majorVersion<18||!"s".equals(generated)||r.getString(6)==null)
                            throw invalid("POSTGRESQL_GENERATED_COLUMN_REQUIRES_PLAN");
                        column.generationKind="STORED";
                        column.generationExpression=r.getString(6);
                    }
                    column.name=r.getString(1);
                    column.nullable=!r.getBoolean(4);
                    String type=r.getString(2);
                    int modifier=r.getInt(3);
                    column.sourceType=type;
                    configureType(column,Integer.parseUnsignedInt(type),modifier);
                    table.columns.add(column);
                    table.properties.put("pg.oid."+column.name,type);
                    table.properties.put("pg.modifier."+column.name,Integer.toString(modifier));
                }
            }
        }
        try(PreparedStatement p=c.prepareStatement("SELECT a.attname FROM pg_index i "
                +"CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum,position) "
                +"JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum "
                +"WHERE i.indrelid=?::oid AND i.indisprimary AND k.position<=i.indnkeyatts ORDER BY k.position")){
            p.setString(1,oid);
            try(ResultSet r=p.executeQuery()){while(r.next())table.primaryKey.add(r.getString(1));}
        }
        table.validate();
        return table;
    }

    private static void configureType(TableSchema.Column column,int oid,int modifier){
        switch(oid){
            case 16:column.type=TableSchema.Type.BOOLEAN;break;
            case 17:column.type=TableSchema.Type.BYTES;break;
            case 20:case 21:case 23:column.type=TableSchema.Type.INT64;break;
            case 25:case 1043:
                column.type=TableSchema.Type.STRING;
                if(oid==1043&&modifier>=4)column.length=modifier-4;
                break;
            case 2950:
                column.type=TableSchema.Type.STRING;
                column.length=36;
                break;
            case 114:case 3802:column.type=TableSchema.Type.JSON;break;
            case PostgresDate.OID:column.type=TableSchema.Type.DATE;break;
            case PostgresClock.OID:column.type=TableSchema.Type.TIME;break;
            case PostgresTimestamp.CIVIL_OID:column.type=TableSchema.Type.DATETIME;break;
            case PostgresTimestamp.INSTANT_OID:column.type=TableSchema.Type.TIMESTAMP;break;
            case 1700:
                if(modifier<4)throw invalid("POSTGRESQL_NUMERIC_PRECISION_REQUIRED");
                int numeric=modifier-4;
                column.precision=numeric>>>16;
                column.scale=numeric&2047;
                if(column.precision<1||column.precision>65||column.scale>Math.min(column.precision,30))
                    throw invalid("POSTGRESQL_NUMERIC_PROFILE_UNSUPPORTED");
                column.type=TableSchema.Type.DECIMAL;
                break;
            default:throw invalid("POSTGRESQL_TYPE_UNSUPPORTED");
        }
    }

    @Override public Snapshot snapshot(TableSchema table)throws Exception{
        String temporary="rm_pg_snapshot_"+UUID.randomUUID().toString().replace("-","");
        try(Connection export=replication()){
            requireIdentity(export);
            try(Statement st=export.createStatement();ResultSet cut=st.executeQuery("CREATE_REPLICATION_SLOT "+temporary
                    +" TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')")){
                if(!cut.next())throw invalid("POSTGRESQL_EXPORT_SNAPSHOT_FAILED");
                SourcePosition baseline=positions.at(PostgresSourcePosition.parse(cut.getString("consistent_point")));
                String snapshot=cut.getString("snapshot_name");
                if(snapshot==null||!snapshot.matches("[0-9A-Fa-f-]{8,80}"))throw invalid("POSTGRESQL_EXPORTED_SNAPSHOT_INVALID");
                return openSnapshot(table,baseline,snapshot);
            }
        }
    }

    private Snapshot openSnapshot(TableSchema table,SourcePosition baseline,String snapshot)throws Exception{
        Connection read=sql();
        try{
            read.setReadOnly(true);
            read.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            read.setAutoCommit(false);
            try(Statement st=read.createStatement()){
                st.execute("SET TRANSACTION SNAPSHOT '"+snapshot+"'");
                st.execute("SET LOCAL bytea_output='hex'");
                st.execute("SET LOCAL DateStyle TO 'ISO, YMD'");
                st.execute("SET LOCAL TimeZone TO 'UTC'");
            }
            StringBuilder query=new StringBuilder("SELECT ");
            for(TableSchema.Column column:table.columns){
                if(query.length()>7)query.append(',');
                query.append(Sql.pg(column.name)).append("::text");
            }
            query.append(" FROM ").append(Sql.pg(table.database)).append('.').append(Sql.pg(table.name));
            PreparedStatement select=read.prepareStatement(query.toString());
            select.setFetchSize(1000);
            ResultSet rows=select.executeQuery();
            return new Snapshot(){
                public SourcePosition baseline(){return baseline;}
                public List<List<Cell>> next(int maximum)throws Exception{
                    if(maximum<1||maximum>10000)throw invalid("SNAPSHOT_READ_INVALID");
                    List<List<Cell>> result=new ArrayList<>();
                    while(result.size()<maximum&&rows.next()){
                        List<Cell> values=new ArrayList<>();
                        for(int i=0;i<table.columns.size();i++)values.add(PgOutput.value(table.columns.get(i),rows.getString(i+1)));
                        result.add(values);
                    }
                    return result;
                }
                public void close()throws Exception{
                    try{rows.close();}finally{try{select.close();}finally{read.close();}}
                }
            };
        }catch(Exception failure){
            try{read.close();}catch(Exception close){failure.addSuppressed(close);}
            throw failure;
        }
    }

    @Override public Stream stream(SourcePosition captured,Receiver receiver)throws Exception{
        return new Capture(positions.lsn(captured),receiver);
    }

    private final class Capture implements Stream {
        private final Connection connection;
        private final PGReplicationStream stream;
        private final Receiver receiver;
        private final PgOutput decoder;
        private final Thread reader;
        private final AtomicReference<Throwable> failure=new AtomicReference<>();
        private volatile boolean closing;

        Capture(long start,Receiver receiver)throws Exception{
            this.receiver=receiver;
            this.decoder=new PgOutput(positions,generation,maxRows,new LinkedHashMap<>(tables),start);
            connection=replication();
            try{
                requireIdentity(connection);
                try(Statement session=connection.createStatement()){
                    session.setQueryTimeout(15);
                    session.execute("SET DateStyle TO 'ISO, YMD'");
                    session.execute("SET TimeZone TO 'UTC'");
                }
                stream=connection.unwrap(PGConnection.class).getReplicationAPI().replicationStream().logical()
                        .withSlotName(slot).withStartPosition(LogSequenceNumber.valueOf(start))
                        .withSlotOption("proto_version",1).withSlotOption("publication_names",options.publication)
                        .withAutomaticFlush(false)
                        .withStatusInterval(1,TimeUnit.SECONDS).start();
            }catch(Exception error){
                try{connection.close();}catch(Exception close){error.addSuppressed(close);}
                if(error instanceof SQLException&&"55006".equals(((SQLException)error).getSQLState()))
                    throw new SQLTransientConnectionException("POSTGRESQL_SLOT_ACTIVE","08006",error);
                throw error;
            }
            reader=new Thread(this::read,"rivomesh-postgresql-capture");
            reader.setDaemon(true);
            reader.start();
        }

        private void read(){
            try{
                while(!closing){
                    ByteBuffer frame;
                    try{frame=stream.readPending();}
                    catch(Exception error){throw new PipelineFailure(PipelineFailure.Phase.SOURCE_READ,error);}
                    if(frame==null){
                        if(stream.isClosed())throw new PipelineFailure(PipelineFailure.Phase.SOURCE_READ,
                                new java.io.IOException("POSTGRESQL_REPLICATION_DISCONNECTED"));
                        Thread.sleep(20);
                        continue;
                    }
                    SourceTransaction transaction;
                    try{transaction=decoder.accept(frame);}
                    catch(PipelineFailure error){throw error;}
                    catch(Exception error){throw new PipelineFailure(PipelineFailure.Phase.SOURCE_DECODE,error);}
                    if(transaction!=null){
                        receiver.committed(transaction);
                        LogSequenceNumber durable=LogSequenceNumber.valueOf(positions.lsn(transaction.position));
                        stream.setAppliedLSN(durable);
                        stream.setFlushedLSN(durable);
                        try{stream.forceUpdateStatus();}
                        catch(Exception error){throw new PipelineFailure(PipelineFailure.Phase.SOURCE_READ,error);}
                    }
                }
            }catch(Throwable error){
                if(!closing)failure.compareAndSet(null,error);
            }
        }

        public void check()throws Exception{
            Throwable error=failure.get();
            if(error instanceof Exception)throw (Exception)error;
            if(error!=null)throw new IllegalStateException("POSTGRESQL_CAPTURE_FAILED",error);
        }

        public void close()throws Exception{
            closing=true;
            receiver.cancelled();
            try{connection.close();}
            finally{reader.join(5000);}
        }
    }

    @Override public void close(){/* each replication and snapshot connection has its own lifetime */}

    private static IllegalStateException invalid(String code){return new IllegalStateException(code);}
}
