package io.github.rivomesh;

import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.TableSchema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Inspects a closed archive range using a dedicated, read-only application session. */
final class OracleArchiveRead {
    private static final int QUERY_SECONDS=30;
    private static final int MAX_PREVIEW_ROWS=10000;

    private OracleArchiveRead(){}

    static Map<String,Object> inspect(Connection root,String pdb,OracleArchiveWindow.Coverage range,
                                     Map<String,TableSchema> tables)throws Exception{
        if(tables.isEmpty()||tables.size()>200)throw invalid("ORACLE_ARCHIVE_TABLE_LIMIT");
        boolean first=true;
        for(String file:range.files){
            String mode=first?"NEW":"ADDFILE";
            try(CallableStatement add=root.prepareCall("BEGIN DBMS_LOGMNR.ADD_LOGFILE(?, DBMS_LOGMNR."+mode+"); END;")){
                add.setQueryTimeout(QUERY_SECONDS);
                add.setString(1,file);
                add.execute();
            }
            first=false;
        }
        try(CallableStatement start=root.prepareCall("BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN=>?, ENDSCN=>?, "
                +"OPTIONS=>DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG+DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;")){
            start.setQueryTimeout(QUERY_SECONDS);
            start.setBigDecimal(1,new BigDecimal(range.start));
            start.setBigDecimal(2,new BigDecimal(range.end.subtract(BigInteger.ONE)));
            start.execute();
        }
        try(CallableStatement end=root.prepareCall("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")){
            end.setQueryTimeout(QUERY_SECONDS);
            try(AutoCloseable cleanup=()->end.execute()){
                return read(root,pdb,range,tables);
            }
        }
    }

    private static Map<String,Object> read(Connection root,String pdb,OracleArchiveWindow.Coverage range,
                                           Map<String,TableSchema> tables)throws Exception{
        String scope=String.join(" OR ",Collections.nCopies(tables.size(),"(seg_owner=? AND table_name=?)"));
        String sql="SELECT operation,seg_owner,table_name,start_scn,commit_scn,RAWTOHEX(xid),csf,status,sql_redo,sql_undo "
                +"FROM v$logmnr_contents WHERE operation='MISSING_SCN' OR (src_con_name=? AND ("+scope+")) "
                +"ORDER BY commit_scn,RAWTOHEX(xid),scn,rs_id,ssn";
        MessageDigest digest=MessageDigest.getInstance("SHA-256");
        int transactions=0,count=0;
        String prior=null;
        Map<String,Integer> operations=new LinkedHashMap<>();
        try(PreparedStatement query=root.prepareStatement(sql)){
            query.setQueryTimeout(QUERY_SECONDS);
            query.setFetchSize(32);
            query.setString(1,pdb);
            int index=2;
            for(TableSchema table:tables.values()){
                query.setString(index++,table.database);
                query.setString(index++,table.name);
            }
            try(ResultSet rows=query.executeQuery()){
                while(rows.next()){
                    String operation=rows.getString(1);
                    if("MISSING_SCN".equals(operation))throw invalid("ORACLE_ARCHIVE_LOG_GAP");
                    if(!Arrays.asList("INSERT","UPDATE","DELETE").contains(operation))
                        throw invalid("ORACLE_REDO_OPERATION_UNSUPPORTED");
                    if(rows.getInt(7)!=0||rows.getInt(8)!=0)throw invalid("ORACLE_REDO_ROW_UNSUPPORTED");
                    BigInteger start=scn(rows.getString(4)),commit=scn(rows.getString(5));
                    if(start.compareTo(range.start)<0||commit.compareTo(range.end)>=0||commit.compareTo(start)<0)
                        throw invalid("ORACLE_ARCHIVE_TRANSACTION_CROSSES_RANGE");
                    String xid=rows.getString(6);
                    if(xid==null||!xid.matches("[0-9A-F]{16}"))throw invalid("ORACLE_REDO_XID_INVALID");
                    String transaction=commit+":"+xid;
                    if(!transaction.equals(prior)){transactions++;prior=transaction;}
                    if(++count>MAX_PREVIEW_ROWS)throw invalid("ORACLE_ARCHIVE_PREVIEW_ROW_LIMIT");
                    TableSchema table=tables.get(rows.getString(2)+"."+rows.getString(3));
                    if(table==null)throw invalid("ORACLE_REDO_TABLE_OR_OPERATION_MISMATCH");
                    Change event=OracleRedoDml.decode(table,operation,rows.getString(9),rows.getString(10));
                    event.eventId=transaction+":"+count;
                    digest.update(Codec.encode(event).getBytes(StandardCharsets.UTF_8));
                    digest.update((byte)'\n');
                    operations.merge(operation,1,Integer::sum);
                }
            }
        }
        StringBuilder checksum=new StringBuilder();
        for(byte value:digest.digest())checksum.append(String.format("%02x",value&255));
        Map<String,Object> report=new LinkedHashMap<>();
        report.put("committedTransactions",transactions);
        report.put("operations",operations);
        report.put("contentSha256",checksum.toString());
        return report;
    }

    private static BigInteger scn(String value){
        if(value==null||!value.matches("[1-9][0-9]{0,19}"))throw invalid("ORACLE_REDO_SCN_INVALID");
        return new BigInteger(value);
    }

    private static IllegalStateException invalid(String code){return new IllegalStateException(code);}
}
