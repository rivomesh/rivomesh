package io.github.rivomesh.cdc;

import io.github.rivomesh.recovery.*;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/** Uses the caller's source job fence and transaction; it never advances an ACK. */
final class JdbcGapVerificationLedger {
    private JdbcGapVerificationLedger(){}
    static GapVerificationReceipt replay(JdbcJournal j,Connection c,GapVerificationPlan plan)throws Exception{
        GapVerificationReceipt receipt=load(j,c,plan.id);
        if(receipt==null)return null;
        if(!receipt.plan.hash().equals(plan.hash()))throw new IllegalStateException("GAP_VERIFICATION_REQUEST_CONFLICT");
        for(String id:plan.gaps.keySet()){
            JdbcSkipJournal.Stored stored=JdbcSkipJournal.load(j,c,id);GapVerificationPlan.Gap expected=plan.gaps.get(id);
            if(stored==null||!"COMPLETED".equals(stored.state)||!expected.planHash.equals(stored.hash)||!expected.receiptHash.equals(stored.receipt.hash()))throw new IllegalStateException("GAP_VERIFICATION_GAP_CHANGED");
            try(PreparedStatement p=c.prepareStatement("SELECT verification_id,verification_hash FROM "+j.table("gap")+" WHERE pipeline_id=? AND operation_id=?")){
                p.setString(1,plan.pipelineId);p.setString(2,id);try(ResultSet r=p.executeQuery()){if(!r.next()||!plan.id.equals(r.getString(1))||!receipt.hash().equals(r.getString(2)))throw new IllegalStateException("GAP_VERIFICATION_RECORD_CHANGED");}
            }
        }
        return receipt;
    }
    static GapVerificationReceipt load(JdbcJournal j,Connection c,String operation)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT plan_hash,receipt_hash,payload FROM "+j.table("gap_verification")+" WHERE pipeline_id=? AND operation_id=?")){
            p.setString(1,j.settings.pipelineId);p.setString(2,operation);try(ResultSet r=p.executeQuery()){
                if(!r.next())return null;String text=bounded(r.getCharacterStream(3));
                if(!Codec.sha256(text).equals(r.getString(2)))throw new IllegalStateException("GAP_VERIFICATION_RECORD_CHANGED");
                GapVerificationReceipt receipt=Codec.decode(text,GapVerificationReceipt.class);receipt.validate();
                if(!j.settings.pipelineId.equals(receipt.plan.pipelineId)||!operation.equals(receipt.plan.id)||!receipt.plan.hash().equals(r.getString(1))||!receipt.hash().equals(r.getString(2)))throw new IllegalStateException("GAP_VERIFICATION_RECORD_CHANGED");return receipt;
            }
        }
    }
    static void requireOpen(JdbcJournal j,Connection c,String operation)throws Exception{
        try(PreparedStatement p=c.prepareStatement("SELECT verification_id,verification_hash FROM "+j.table("gap")+" WHERE pipeline_id=? AND operation_id=?")){
            p.setString(1,j.settings.pipelineId);p.setString(2,operation);try(ResultSet r=p.executeQuery()){if(!r.next())throw new IllegalStateException("SKIP_GAP_MISSING");if(r.getString(1)!=null||r.getString(2)!=null)throw new IllegalStateException("GAP_ALREADY_VERIFIED");}
        }
    }
    static GapVerificationReceipt record(JdbcJournal j,Connection c,GapVerificationPlan plan,JdbcGapVerification.Observation observation)throws Exception{
        plan.validate();
        if(!plan.hash().equals(observation.planHash)||!plan.reportId.equals(observation.reportId)||!plan.sourceSeal.equals(observation.sourceSeal)||plan.epoch!=observation.epoch||plan.sequence!=observation.sequence||!plan.tables.keySet().equals(observation.tables.keySet()))throw new IllegalStateException("GAP_VERIFICATION_OBSERVATION_CHANGED");
        for(String table:plan.tables.keySet())if(plan.tables.get(table).rows!=observation.tables.get(table).rows||!plan.tables.get(table).sourceRoot.equals(observation.tables.get(table).root))throw new IllegalStateException("GAP_VERIFICATION_OBSERVATION_CHANGED");
        for(String id:plan.gaps.keySet())requireOpen(j,c,id);
        GapVerificationReceipt receipt=new GapVerificationReceipt();receipt.plan=Codec.decode(Codec.encode(plan),GapVerificationPlan.class);receipt.verifiedAt=observation.observedAt;receipt.validate();
        JdbcJournal.execute(c,"INSERT INTO "+j.table("gap_verification")+" (pipeline_id,operation_id,plan_hash,receipt_hash,payload,created_at) VALUES (?,?,?,?,?,?)",plan.pipelineId,plan.id,plan.hash(),receipt.hash(),Codec.encode(receipt),receipt.verifiedAt);
        for(String id:plan.gaps.keySet())try(PreparedStatement p=c.prepareStatement("UPDATE "+j.table("gap")+" SET verification_id=?,verification_hash=? WHERE pipeline_id=? AND operation_id=? AND verification_id IS NULL AND verification_hash IS NULL")){
            p.setString(1,plan.id);p.setString(2,receipt.hash());p.setString(3,plan.pipelineId);p.setString(4,id);if(p.executeUpdate()!=1)throw new IllegalStateException("GAP_VERIFICATION_RECORD_CHANGED");
        }
        return receipt;
    }
    private static String bounded(Reader input)throws Exception{
        if(input==null)throw new IllegalStateException("GAP_VERIFICATION_RECORD_CHANGED");try(Reader reader=input){StringBuilder text=new StringBuilder();char[] buffer=new char[4096];int size;while((size=reader.read(buffer))!=-1){if(text.length()+size>1024*1024)throw new IllegalStateException("GAP_VERIFICATION_RECORD_TOO_LARGE");text.append(buffer,0,size);}String value=text.toString();if(value.getBytes(StandardCharsets.UTF_8).length>1024*1024)throw new IllegalStateException("GAP_VERIFICATION_RECORD_TOO_LARGE");return value;}
    }
}
