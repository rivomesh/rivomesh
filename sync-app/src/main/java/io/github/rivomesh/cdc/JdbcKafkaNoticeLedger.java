package io.github.rivomesh.cdc;

import io.github.rivomesh.cdc.KafkaRecoveryNotice.Attempt;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Authority;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Failure;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Plan;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Receipt;
import io.github.rivomesh.cdc.KafkaRecoveryNotice.Topic;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/** Internal ledger for one transactional control notice per failed target sequence. */
final class JdbcKafkaNoticeLedger {
    private enum State { SENDING,COMMIT_INTENT,VERIFIED }
    record Entry(State state,long number,Long offset) {
        boolean hasCommitIntent(){return state!=State.SENDING;}
        boolean verified(){return state==State.VERIFIED;}
    }

    @FunctionalInterface
    private interface Transaction<T> { T run(Connection connection,Map<String,Object> job)throws Exception; }

    private final JdbcJournal journal;
    private final String table;

    JdbcKafkaNoticeLedger(JdbcJournal journal){
        this.journal=journal;
        table=journal.table("kafka_recovery_notice");
    }

    void provision()throws Exception {
        try(Connection c=journal.connect()){
            journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+table+" (pipeline_id VARCHAR(128) NOT NULL,"
                    +"notice_id VARCHAR(64) NOT NULL,plan_hash VARCHAR(64) NOT NULL,run_state VARCHAR(16) NOT NULL,"
                    +"attempt_no BIGINT NOT NULL,native_offset BIGINT,state_hash VARCHAR(64) NOT NULL,"
                    +"PRIMARY KEY(pipeline_id,notice_id))");
        }
    }

    Plan prepare(String target,long errorGeneration,Topic topic,String previousReceiptHash,Authority authority)throws Exception {
        return transact((c,job)->{
            authority.verify();
            return new Plan(failedCut(c,job,target,errorGeneration),topic,previousReceiptHash);
        });
    }

    Entry existing(Plan plan)throws Exception {
        return transact((c,job)->load(c,plan));
    }

    Entry start(Plan plan,Authority authority)throws Exception {
        return transact((c,job)->{
            Entry current=load(c,plan);
            if(current!=null&&current.hasCommitIntent())return current;
            requireCurrent(c,job,plan,authority);
            // The attempt number fences a stale producer before it may record an intent.
            Entry next=new Entry(State.SENDING,current==null?1:Math.addExact(current.number(),1),null);
            save(c,plan,current,next);
            return next;
        });
    }

    void commitIntent(Plan plan,long attempt,long offset,Authority authority)throws Exception {
        if(offset<0)throw new IllegalArgumentException("KAFKA_NOTICE_OFFSET_INVALID");
        transact((c,job)->{
            Entry current=requireAttempt(c,plan,attempt,State.SENDING);
            requireCurrent(c,job,plan,authority);
            save(c,plan,current,new Entry(State.COMMIT_INTENT,attempt,offset));
            return null;
        });
    }

    void commitUnderFence(Plan plan,long attempt,long offset,Authority authority,Attempt producer)throws Exception {
        transact((c,job)->{
            Entry current=requireAttempt(c,plan,attempt,State.COMMIT_INTENT);
            if(current.offset()!=offset)throw changed();
            requireCurrent(c,job,plan,authority);
            producer.commit();
            return null;
        });
    }

    Receipt recordRetained(Plan plan,Receipt receipt,Authority authority)throws Exception {
        return transact((c,job)->{
            Entry current=load(c,plan);
            if(current!=null&&current.hasCommitIntent()){
                authority.verify();
                return confirm(c,plan,current,receipt);
            }
            requireCurrent(c,job,plan,authority);
            long attempt=current==null?1:current.number();
            save(c,plan,current,new Entry(State.VERIFIED,attempt,receipt.offset()));
            return receipt;
        });
    }

    Receipt recordVerified(Plan plan,Receipt receipt,Authority authority)throws Exception {
        return transact((c,job)->{
            authority.verify();
            return confirm(c,plan,load(c,plan),receipt);
        });
    }

    private Receipt confirm(Connection c,Plan plan,Entry current,Receipt receipt)throws SQLException {
        if(current==null||!current.hasCommitIntent()||current.offset()!=receipt.offset())throw changed();
        if(!current.verified())save(c,plan,current,new Entry(State.VERIFIED,current.number(),current.offset()));
        return receipt;
    }

    private void requireCurrent(Connection c,Map<String,Object> job,Plan plan,Authority authority)throws Exception {
        if(!plan.failure().equals(failedCut(c,job,plan.failure().target(),plan.failure().errorGeneration())))
            throw new IllegalStateException("KAFKA_NOTICE_FAILURE_CHANGED");
        authority.verify();
    }

    private Failure failedCut(Connection c,Map<String,Object> job,String target,long errorGeneration)throws Exception {
        if(!"PAUSED".equals(job.get("state"))||job.get("owner")!=null)
            throw new IllegalStateException("KAFKA_NOTICE_REQUIRES_PIPELINE_PAUSED");
        Journal.FailureReview reviewed=new JdbcFailureReview(journal).inspect(c,target,errorGeneration,JdbcFailureReview.MAXIMUM_BYTES);
        String content=KafkaRecoveryReview.failureContentSha256(reviewed);
        if(reviewed.failure.phase!=SubscriberFailure.Phase.TARGET_APPLY||reviewed.failure.sequence!=reviewed.sequence||content==null)
            throw new IllegalStateException("KAFKA_NOTICE_REQUIRES_COMPLETE_APPLY_FAILURE");
        return new Failure(reviewed.pipelineId,target,reviewed.generation,reviewed.epoch,errorGeneration,reviewed.sequence,content);
    }

    private Entry requireAttempt(Connection c,Plan plan,long attempt,State expected)throws SQLException {
        Entry current=load(c,plan);
        if(current==null||current.number()!=attempt||current.state()!=expected)
            throw new IllegalStateException("KAFKA_NOTICE_ATTEMPT_FENCED");
        return current;
    }

    private Entry load(Connection c,Plan plan)throws SQLException {
        if(!journal.settings.pipelineId.equals(plan.failure().pipeline()))throw changed();
        String sql="SELECT plan_hash,run_state,attempt_no,native_offset,state_hash FROM "+table+" WHERE pipeline_id=? AND notice_id=?";
        try(PreparedStatement query=c.prepareStatement(sql)){
            query.setString(1,journal.settings.pipelineId);query.setString(2,plan.id());
            try(ResultSet row=query.executeQuery()){
                if(!row.next())return null;
                if(!plan.hash().equals(row.getString(1)))throw new IllegalStateException("KAFKA_NOTICE_APPROVAL_CHANGED");
                State state;
                try{state=State.valueOf(row.getString(2));}
                catch(RuntimeException bad){throw changed();}
                long number=row.getLong(3);
                long offset=row.getLong(4);
                Entry entry=new Entry(state,number,row.wasNull()?null:offset);
                if(number<1||entry.hasCommitIntent()&&(entry.offset()==null||entry.offset()<0)
                        ||!entry.hasCommitIntent()&&entry.offset()!=null||!checksum(plan,entry).equals(row.getString(5)))throw changed();
                return entry;
            }
        }
    }

    private void save(Connection c,Plan plan,Entry current,Entry next)throws SQLException {
        int affected;
        if(current==null){
            affected=JdbcJournal.execute(c,"INSERT INTO "+table+" (pipeline_id,notice_id,plan_hash,run_state,attempt_no,native_offset,state_hash)"
                    +" VALUES (?,?,?,?,?,?,?)",journal.settings.pipelineId,plan.id(),plan.hash(),next.state().name(),
                    next.number(),next.offset(),checksum(plan,next));
        }else{
            affected=JdbcJournal.execute(c,"UPDATE "+table+" SET run_state=?,attempt_no=?,native_offset=?,state_hash=? "
                    +"WHERE pipeline_id=? AND notice_id=? AND state_hash=?",next.state().name(),next.number(),next.offset(),
                    checksum(plan,next),journal.settings.pipelineId,plan.id(),checksum(plan,current));
        }
        if(affected!=1)throw changed();
    }

    private String checksum(Plan plan,Entry state){
        return Codec.sha256(Codec.encode(List.of(plan.id(),plan.hash(),state.state().name(),state.number(),
                state.offset()==null?"":Long.toString(state.offset()))));
    }

    private <T>T transact(Transaction<T> action)throws Exception {
        try(Connection c=journal.connect();RepairConnectionGuard deadline=new RepairConnectionGuard(c,15000)){
            c.setAutoCommit(false);
            try{
                T outcome=action.run(c,journal.locked(c));
                deadline.verify();
                c.commit();
                return outcome;
            }catch(Exception failed){
                try{c.rollback();}catch(SQLException rollback){failed.addSuppressed(rollback);}
                throw failed;
            }
        }
    }

    private static IllegalStateException changed(){return new IllegalStateException("KAFKA_NOTICE_LEDGER_CHANGED");}
}
