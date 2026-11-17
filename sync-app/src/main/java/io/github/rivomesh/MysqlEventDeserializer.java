package io.github.rivomesh;

import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.*;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import java.io.*;
import java.util.Map;

/** Preserve signed TIME/TIME2 values while retaining the pinned SDK's other event codecs. */
final class MysqlEventDeserializer {
    private MysqlEventDeserializer(){}
    static EventDeserializer create(Map<Long,TableMapEventData> tables){
        EventDeserializer result=new EventDeserializer();
        result.setCompatibilityMode(EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY,EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,EventDeserializer.CompatibilityMode.INVALID_DATE_AND_TIME_AS_MIN_VALUE);
        for(boolean extended:new boolean[]{false,true}){
            result.setEventDataDeserializer(extended?EventType.EXT_WRITE_ROWS:EventType.WRITE_ROWS,new WriteRowsEventDataDeserializer(tables){
                @Override protected Serializable deserializeTime(ByteArrayInputStream in)throws IOException{return MysqlDuration.legacy(in);}
                @Override protected Serializable deserializeTimeV2(int precision,ByteArrayInputStream in)throws IOException{return MysqlDuration.time2(precision,in);}
            }.setMayContainExtraInformation(extended));
            result.setEventDataDeserializer(extended?EventType.EXT_UPDATE_ROWS:EventType.UPDATE_ROWS,new UpdateRowsEventDataDeserializer(tables){
                @Override protected Serializable deserializeTime(ByteArrayInputStream in)throws IOException{return MysqlDuration.legacy(in);}
                @Override protected Serializable deserializeTimeV2(int precision,ByteArrayInputStream in)throws IOException{return MysqlDuration.time2(precision,in);}
            }.setMayContainExtraInformation(extended));
            result.setEventDataDeserializer(extended?EventType.EXT_DELETE_ROWS:EventType.DELETE_ROWS,new DeleteRowsEventDataDeserializer(tables){
                @Override protected Serializable deserializeTime(ByteArrayInputStream in)throws IOException{return MysqlDuration.legacy(in);}
                @Override protected Serializable deserializeTimeV2(int precision,ByteArrayInputStream in)throws IOException{return MysqlDuration.time2(precision,in);}
            }.setMayContainExtraInformation(extended));
        }
        return result;
    }
}
