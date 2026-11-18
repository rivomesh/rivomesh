package io.github.rivomesh;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.EventHeaderV4Deserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.NullEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.SSLMode;

import java.io.EOFException;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32;

/** Verifies the bytes of a bounded row transaction without invoking a business row decoder. */
final class MysqlBinlogEvidence {
    private static final int HEADER_BYTES=19;
    private static final int MAX_BYTES=4*1024*1024;

    private MysqlBinlogEvidence() {}

    static Map<String,Object> verify(Config config,MysqlSource source,Map<String,Object> boundary,
                                     ByteArrayOutputStream retained)throws Exception {
        String file=(String)boundary.get("logFile"),gtid=(String)boundary.get("transactionId");
        long start=Long.parseLong((String)boundary.get("startOffset")),end=Long.parseLong((String)boundary.get("endOffset"));
        int expectedEvents=(int)boundary.get("eventCount");
        if(end-start<=0||end-start>MAX_BYTES)throw invalid("MYSQL_BINLOG_EVIDENCE_RANGE_TOO_LARGE");
        MessageDigest digest=MessageDigest.getInstance("SHA-256");
        CompletableFuture<Map<String,Object>> checked=new CompletableFuture<>();
        FrameReader frames=new FrameReader(checked::isDone);
        BinaryLogClient client=new BinaryLogClient(source.endpoint().host,source.endpoint().port,config.source.username,config.source.password);
        client.setSSLMode(SSLMode.valueOf(config.source.sslMode));
        client.setBlocking(false);
        client.setKeepAlive(false);
        client.setBinlogFilename(file);
        client.setBinlogPosition(start);
        client.setConnectTimeout(4000);
        client.setEventDeserializer(frames);
        client.registerEventListener(new BinaryLogClient.EventListener(){
            private long offset=start;
            private int eventCount;

            @Override public void onEvent(Event event){
                if(checked.isDone())return;
                try{
                    if(source.serverId()!=client.getMasterServerId())throw invalid("SOURCE_BACKEND_CHANGED");
                    EventHeaderV4 header=event.getHeader();
                    EventType type=header.getEventType();
                    if(eventCount==0&&type==EventType.ROTATE){
                        RotateEventData rotate=event.getData();
                        if(!file.equals(rotate.getBinlogFilename())||rotate.getBinlogPosition()!=start)
                            throw invalid("MYSQL_BINLOG_EVIDENCE_BOUNDARY_CHANGED");
                        return;
                    }
                    if(eventCount==0&&type==EventType.FORMAT_DESCRIPTION)return;
                    if(header.getPosition()!=offset||header.getNextPosition()<=offset||header.getNextPosition()>end
                            ||header.getEventLength()!=frames.latest.length||++eventCount>expectedEvents)
                        throw invalid("MYSQL_BINLOG_EVIDENCE_BOUNDARY_CHANGED");
                    if(eventCount==1&&(type!=EventType.GTID||!gtid.equalsIgnoreCase(((GtidEventData)event.getData()).getGtid())))
                        throw invalid("MYSQL_BINLOG_EVIDENCE_GTID_CHANGED");
                    if(eventCount==2&&(type!=EventType.QUERY||!"BEGIN".equalsIgnoreCase(((QueryEventData)event.getData()).getSql()))
                            ||eventCount>2&&type!=EventType.TABLE_MAP&&type!=EventType.XID&&!EventType.isRowMutation(type))
                        throw invalid("MYSQL_BINLOG_EVIDENCE_BOUNDARY_CHANGED");
                    requireCrc32(frames.latest);
                    digest.update(frames.latest);
                    if(retained!=null)retained.write(frames.latest);
                    offset=header.getNextPosition();
                    if(type==EventType.XID||offset==end){
                        if(type!=EventType.XID||offset!=end||eventCount!=expectedEvents)
                            throw invalid("MYSQL_BINLOG_EVIDENCE_BOUNDARY_CHANGED");
                        Map<String,Object> proof=new LinkedHashMap<>();
                        proof.put("scope","BINLOG_WIRE_SHA256");
                        proof.put("contentSha256",hex(digest.digest()));
                        proof.put("bytes",Long.toString(end-start));
                        proof.put("boundaryHash",boundary.get("boundaryHash"));
                        checked.complete(proof);
                    }
                }catch(Exception failure){checked.completeExceptionally(failure);}
            }
        });
        client.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener(){
            @Override public void onDisconnect(BinaryLogClient unused){checked.completeExceptionally(invalid("MYSQL_BINLOG_EVIDENCE_EOF"));}
            @Override public void onCommunicationFailure(BinaryLogClient unused,Exception error){checked.completeExceptionally(invalid("MYSQL_BINLOG_EVIDENCE_UNAVAILABLE"));}
            @Override public void onEventDeserializationFailure(BinaryLogClient unused,Exception error){checked.completeExceptionally(invalid("MYSQL_BINLOG_EVIDENCE_FRAME_INVALID"));}
        });
        try{
            client.connect(4000);
            return checked.get(5,TimeUnit.SECONDS);
        }finally{client.disconnect();}
    }

    private static final class FrameReader extends EventDeserializer {
        private final BooleanSupplier finished;
        private final EventHeaderV4Deserializer headers=new EventHeaderV4Deserializer();
        byte[] latest;
        FrameReader(BooleanSupplier finished){
            this.finished=finished;
            for(EventType type:EventType.values())if(EventType.isRowMutation(type))
                setEventDataDeserializer(type,new NullEventDataDeserializer());
        }
        @Override public Event nextEvent(ByteArrayInputStream data)throws IOException {
            if(finished.getAsBoolean())throw new EOFException("MYSQL_BINLOG_EVIDENCE_FINISHED");
            if(data.peek()==-1)return null;
            byte[] prefix=data.read(HEADER_BYTES);
            EventHeaderV4 header=headers.deserialize(new ByteArrayInputStream(prefix));
            EventType type=header.getEventType();
            if(type!=EventType.ROTATE&&type!=EventType.FORMAT_DESCRIPTION&&type!=EventType.GTID&&type!=EventType.QUERY
                    &&type!=EventType.TABLE_MAP&&type!=EventType.XID&&!EventType.isRowMutation(type))
                throw new IOException("MYSQL_BINLOG_EVIDENCE_FRAME_INVALID");
            long length=header.getEventLength();
            if(length<HEADER_BYTES||length>MAX_BYTES)throw new IOException("MYSQL_BINLOG_EVIDENCE_FRAME_INVALID");
            latest=Arrays.copyOf(prefix,(int)length);
            byte[] tail=data.read((int)length-HEADER_BYTES);
            System.arraycopy(tail,0,latest,HEADER_BYTES,tail.length);
            return super.nextEvent(new ByteArrayInputStream(latest));
        }
    }

    private static void requireCrc32(byte[] frame){
        if(frame.length<HEADER_BYTES+4)throw invalid("MYSQL_BINLOG_EVIDENCE_CHECKSUM_REQUIRED");
        CRC32 crc=new CRC32();crc.update(frame,0,frame.length-4);
        if(crc.getValue()!=unsignedInt(frame,frame.length-4))throw invalid("MYSQL_BINLOG_EVIDENCE_CHECKSUM_REQUIRED");
    }

    private static long unsignedInt(byte[] data,int offset){
        return (data[offset]&255L)|((data[offset+1]&255L)<<8)|((data[offset+2]&255L)<<16)|((data[offset+3]&255L)<<24);
    }
    private static String hex(byte[] bytes){
        StringBuilder value=new StringBuilder();for(byte b:bytes)value.append(String.format(Locale.ROOT,"%02x",b&255));return value.toString();
    }
    private static IllegalStateException invalid(String message){return new IllegalStateException(message);}
}
