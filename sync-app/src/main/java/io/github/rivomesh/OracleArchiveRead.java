package io.github.rivomesh;

import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.Codec;
import io.github.rivomesh.cdc.SourceTransaction;
import io.github.rivomesh.cdc.TableSchema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Inspects a closed archive range using a dedicated, read-only application session. */
final class OracleArchiveRead {
    private static final int QUERY_SECONDS=30;
    private static final int MAX_PREVIEW_ROWS=10000;

    private OracleArchiveRead(){}

    @FunctionalInterface
    interface Receiver { void committed(SourceTransaction transaction)throws Exception; }

    static Map<String,Object> inspect(Connection root,String pdb,OracleArchiveWindow.Coverage range,
                                     Map<String,TableSchema> tables,SourcePosition anchor)throws Exception{
        Preview preview=new Preview();
        visit(root,pdb,range,tables,anchor,null,"oracle-preview",MAX_PREVIEW_ROWS,preview::record);
        return preview.result();
    }

    static void visit(Connection root,String pdb,OracleArchiveWindow.Coverage range,Map<String,TableSchema> tables,
                      SourcePosition anchor,BigInteger after,String generation,int maxTransactionRows,Receiver receiver)throws Exception{
        if(tables.isEmpty()||tables.size()>200)throw invalid("ORACLE_ARCHIVE_TABLE_LIMIT");
        if(maxTransactionRows<1)throw invalid("ORACLE_ARCHIVE_ROW_LIMIT");
        if(range.files.isEmpty())throw invalid("ORACLE_ARCHIVE_LOG_GAP");
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
                read(root,pdb,range,tables,anchor,after,generation,maxTransactionRows,receiver);
            }
        }
    }

    private static void read(Connection root,String pdb,OracleArchiveWindow.Coverage range,
                             Map<String,TableSchema> tables,SourcePosition anchor,BigInteger after,String generation,
                             int maxTransactionRows,Receiver receiver)throws Exception{
        String scope=String.join(" OR ",Collections.nCopies(tables.size(),"(seg_owner=? AND table_name=?)"));
        String sql="SELECT operation,seg_owner,table_name,start_scn,commit_scn,RAWTOHEX(xid),csf,status,sql_redo,sql_undo,rollback,RAWTOHEX(src_con_guid),src_con_uid "
                +"FROM v$logmnr_contents WHERE operation='MISSING_SCN' OR ((src_con_name=? OR RAWTOHEX(src_con_guid)=? OR src_con_uid=?) AND ("
                +(after==null?scope:"(operation='DDL' AND scn>?) OR (commit_scn>? AND ("+scope+"))") +")) "
                +"ORDER BY CASE WHEN operation IN ('MISSING_SCN','DDL') THEN 0 ELSE 1 END,commit_scn,RAWTOHEX(xid),scn,rs_id,ssn";
        Commit group=null;
        try(PreparedStatement query=root.prepareStatement(sql)){
            query.setQueryTimeout(QUERY_SECONDS);
            query.setFetchSize(32);
            query.setString(1,pdb);
            query.setString(2,anchor.getPayload().get("pdbGuid"));
            query.setBigDecimal(3,new BigDecimal(anchor.getPayload().get("pdbUid")));
            int index=4;
            if(after!=null){
                query.setBigDecimal(index++,new BigDecimal(after));
                query.setBigDecimal(index++,new BigDecimal(after));
            }
            for(TableSchema table:tables.values()){
                query.setString(index++,table.database);
                query.setString(index++,table.name);
            }
            try(ResultSet rows=query.executeQuery()){
                while(rows.next()){
                    String operation=rows.getString(1);
                    if("MISSING_SCN".equals(operation))throw invalid("ORACLE_ARCHIVE_LOG_GAP");
                    if("DDL".equals(operation))throw invalid("ORACLE_REDO_DDL_REQUIRES_PLAN");
                    requirePdb(anchor,rows.getString(12),rows.getString(13));
                    if(!Arrays.asList("INSERT","UPDATE","DELETE").contains(operation))
                        throw invalid("ORACLE_REDO_OPERATION_UNSUPPORTED");
                    if(rows.getInt(7)!=0||rows.getInt(8)!=0)throw invalid("ORACLE_REDO_ROW_UNSUPPORTED");
                    if(rows.getInt(11)!=0)throw invalid("ORACLE_REDO_PARTIAL_ROLLBACK_UNSUPPORTED");
                    BigInteger start=scn(rows.getString(4)),commit=scn(rows.getString(5));
                    if(start.compareTo(range.start)<0||commit.compareTo(range.end)>=0||commit.compareTo(start)<0)
                        throw invalid("ORACLE_ARCHIVE_TRANSACTION_CROSSES_RANGE");
                    String xid=rows.getString(6);
                    if(xid==null||!xid.matches("[0-9A-F]{16}"))throw invalid("ORACLE_REDO_XID_INVALID");
                    if(group==null||!group.commit.equals(commit)){
                        if(group!=null)receiver.committed(group.transaction(anchor,generation));
                        group=new Commit(start,commit,xid);
                    }else if(!group.xid.equals(xid)||!group.start.equals(start)){
                        throw invalid("ORACLE_REDO_COMMIT_ORDER_UNPROVEN");
                    }
                    if(group.changes.size()>=maxTransactionRows)throw invalid("TRANSACTION_ROW_LIMIT");
                    TableSchema table=tables.get(rows.getString(2)+"."+rows.getString(3));
                    if(table==null)throw invalid("ORACLE_REDO_TABLE_OR_OPERATION_MISMATCH");
                    String redo=rows.getString(9),undo=rows.getString(10);
                    group.textBytes=Math.addExact(group.textBytes,3L*((redo==null?0:redo.length())+(undo==null?0:undo.length())));
                    if(group.textBytes>16L*1024*1024)throw invalid("ORACLE_TRANSACTION_BYTE_LIMIT");
                    Change event=OracleRedoDml.decode(table,operation,redo,undo);
                    event.eventId=group.id+":"+group.changes.size();
                    group.changes.add(event);
                }
            }
        }
        if(group!=null)receiver.committed(group.transaction(anchor,generation));
    }

    private static final class Commit {
        final BigInteger start,commit;
        final String xid,id;
        final List<Change> changes=new ArrayList<>();
        long textBytes;

        Commit(BigInteger start,BigInteger commit,String xid){
            this.start=start;this.commit=commit;this.xid=xid;
            this.id="oracle:"+commit+":"+xid;
        }

        SourceTransaction transaction(SourcePosition anchor,String generation){
            Map<String,String> fields=new LinkedHashMap<>(anchor.getPayload());
            fields.put("scn",commit.toString());
            fields.put("xid",xid);
            SourceTransaction out=new SourceTransaction();
            out.id=id;
            out.position=new SourcePosition(1,"ORACLE",anchor.getSourceId(),fields);
            out.attributes.put("startScn",start.toString());
            out.streamGeneration=generation;
            out.changes=changes;
            out.validate();
            return out;
        }
    }

    private static final class Preview {
        final MessageDigest digest=MessageDigest.getInstance("SHA-256");
        final Map<String,Integer> operations=new LinkedHashMap<>();
        int transactions,rows;
        SourcePosition lastCommit;

        Preview()throws Exception{}

        void record(SourceTransaction transaction){
            rows=Math.addExact(rows,transaction.changes.size());
            if(rows>MAX_PREVIEW_ROWS)throw invalid("ORACLE_ARCHIVE_PREVIEW_ROW_LIMIT");
            transactions++;
            lastCommit=transaction.position;
            for(Change event:transaction.changes)operations.merge(event.operation.name(),1,Integer::sum);
            digest.update(Codec.encode(transaction).getBytes(StandardCharsets.UTF_8));
            digest.update((byte)'\n');
        }

        Map<String,Object> result(){
            StringBuilder checksum=new StringBuilder();
            for(byte value:digest.digest())checksum.append(String.format("%02x",value&255));
            Map<String,Object> report=new LinkedHashMap<>();
            report.put("committedTransactions",transactions);
            report.put("operations",operations);
            report.put("contentSha256",checksum.toString());
            if(lastCommit!=null)report.put("lastCommit",lastCommit);
            return report;
        }
    }

    private static void requirePdb(SourcePosition anchor,String guid,String uid){
        boolean guidPresent=guid!=null&&!guid.matches("0{32}");
        boolean uidPresent=uid!=null&&!"0".equals(uid);
        if(!guidPresent&&!uidPresent
                ||guidPresent&&!anchor.getPayload().get("pdbGuid").equals(guid)
                ||uidPresent&&!anchor.getPayload().get("pdbUid").equals(uid))
            throw invalid("ORACLE_REDO_PDB_IDENTITY_CHANGED");
    }

    private static BigInteger scn(String value){
        if(value==null||!value.matches("[1-9][0-9]{0,19}"))throw invalid("ORACLE_REDO_SCN_INVALID");
        return new BigInteger(value);
    }

    private static IllegalStateException invalid(String code){return new IllegalStateException(code);}
}
