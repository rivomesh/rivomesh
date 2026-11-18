package io.github.rivomesh;

import io.github.rivomesh.cdc.Codec;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Read-only inspection of the next same-file MySQL row transaction's native event headers. */
final class MysqlDecodeBoundary {
    private static final int EVENT_LIMIT = 256;
    private static final Pattern LOG_FILE = Pattern.compile("[A-Za-z0-9_.-]{1,255}");
    private static final Pattern GTID = Pattern.compile("SET @@SESSION\\.GTID_NEXT\\s*=\\s*'([0-9a-fA-F-]{36}:[1-9][0-9]*)'", Pattern.CASE_INSENSITIVE);
    private static final Pattern TABLE = Pattern.compile("table_id: (\\d{1,16}) \\(([A-Za-z0-9_$]+)\\.([A-Za-z0-9_$]+)\\)");
    private static final List<String> ROW_EVENTS = Arrays.asList(
            "Write_rows","Update_rows","Delete_rows","Write_rows_v1","Update_rows_v1","Delete_rows_v1");
    private static final Pattern ROW = Pattern.compile("table_id: (\\d{1,16})(?: flags: [A-Z0-9_, ]+)?");

    private MysqlDecodeBoundary() {}

    static Map<String,Object> read(Connection connection,Model.Position checkpoint)throws SQLException {
        if(checkpoint.file==null||!LOG_FILE.matcher(checkpoint.file).matches()||checkpoint.offset<4)return null;
        // The file and position come from a durable checkpoint, with a restricted SQL literal grammar.
        String sql="SHOW BINLOG EVENTS IN '"+checkpoint.file+"' FROM "+checkpoint.offset+" LIMIT "+EVENT_LIMIT;
        try(Statement statement=connection.createStatement()){
            statement.setQueryTimeout(5);
            statement.setFetchSize(Integer.MIN_VALUE);
            try(ResultSet events=statement.executeQuery(sql)){
                return boundary(events,checkpoint);
            }
        }
    }

    private static Map<String,Object> boundary(ResultSet events,Model.Position checkpoint)throws SQLException {
        if(!events.next()||!checkpoint.file.equals(events.getString(1))||events.getLong(2)!=checkpoint.offset
                ||!"Gtid".equals(events.getString(3)))return null;
        String gtidInfo=events.getString(6);
        if(gtidInfo==null)return null;
        Matcher id=GTID.matcher(gtidInfo);
        if(!id.matches())return null;
        String gtid=id.group(1);
        if(Gtids.contains(checkpoint.gtidSet,gtid))return null;
        long next=events.getLong(5);
        if(next<=checkpoint.offset)return null;
        Map<String,String> tableIds=new LinkedHashMap<>();
        Set<String> changedTables=new TreeSet<>();
        boolean begun=false;
        int count=1;
        while(events.next()){
            long start=events.getLong(2),end=events.getLong(5);
            if(!checkpoint.file.equals(events.getString(1))||start!=next||end<=start)return null;
            next=end;
            count++;
            String type=events.getString(3),info=events.getString(6);
            if(!begun){
                if(!"Query".equals(type)||!"BEGIN".equalsIgnoreCase(info))return null;
                begun=true;
            }else if("Table_map".equals(type)){
                if(info==null)return null;
                Matcher table=TABLE.matcher(info);
                if(!table.matches())return null;
                tableIds.put(table.group(1),table.group(2)+"."+table.group(3));
            }else if(ROW_EVENTS.contains(type)){
                if(info==null)return null;
                Matcher row=ROW.matcher(info);
                if(!row.matches())return null;
                String table=tableIds.get(row.group(1));
                if(table==null)return null;
                changedTables.add(table);
            }else if("Xid".equals(type)&&!changedTables.isEmpty()){
                Map<String,Object> result=new LinkedHashMap<>();
                result.put("scope","BINLOG_HEADERS_ONLY");
                result.put("transactionId",gtid);
                result.put("logFile",checkpoint.file);
                result.put("startOffset",Long.toString(checkpoint.offset));
                result.put("endOffset",Long.toString(end));
                result.put("eventCount",count);
                result.put("tables",changedTables);
                result.put("boundaryHash",Codec.sha256(Codec.encode(result)+"\n"+checkpoint.sourceUuid));
                return result;
            }else return null;
        }
        return null;
    }
}
