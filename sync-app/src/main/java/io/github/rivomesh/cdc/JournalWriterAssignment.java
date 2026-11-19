package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.Model;
import io.github.rivomesh.Sql;
import io.github.rivomesh.api.SourcePosition;
import java.sql.*;
import java.util.*;

/** Durable write admission for a standalone pipeline using a shared transactional journal. */
public final class JournalWriterAssignment {
    public record Placement(String assignment, String node, String previousAssignment, String generation,
                            long epoch, long sequence, String checkpointSha256, String evidenceSha256) {}
    public record ExpectedCut(long epoch,long sequence,String checkpointSha256){
        public ExpectedCut{
            if(epoch<1||sequence<0||checkpointSha256==null||!checkpointSha256.matches("[0-9a-f]{64}"))
                throw new IllegalArgumentException("HANDOFF_CUT_INVALID");
        }
    }
    public record CutProof(String journalGenerationSha256,String nativeEvidenceSha256,List<String> destinations,
                           boolean writerPlacementEnrolled,
                           @com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
                           ManagedCut managedWriter){}
    public record ManagedCut(String originalNode,String originalAssignmentSha256,long epoch,long sequence,
                             String checkpointSha256){}
    @FunctionalInterface public interface SourceConnection {
        SourceAdapter open(String generation)throws Exception;
    }
    private JournalWriterAssignment(){}

    public static boolean validId(String id){
        try{return id!=null&&UUID.fromString(id).toString().equals(id);}
        catch(IllegalArgumentException invalid){return false;}
    }

    static void initialize(JdbcJournal journal,Connection c)throws SQLException{
        journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+journal.table("writer_assignment")
                +" (pipeline_id VARCHAR(128) PRIMARY KEY,assignment_id VARCHAR(36) NOT NULL)");
        journal.ddl(c,"CREATE TABLE IF NOT EXISTS "+journal.table("writer_assignment_history")
                +" (pipeline_id VARCHAR(128) NOT NULL,assignment_id VARCHAR(36) NOT NULL,"
                +"node_id VARCHAR(128) NOT NULL,previous_assignment VARCHAR(36),generation VARCHAR(128) NOT NULL,"
                +"activated_epoch BIGINT NOT NULL,last_sequence BIGINT NOT NULL,checkpoint_hash VARCHAR(64),"
                +"evidence_hash VARCHAR(64),PRIMARY KEY(pipeline_id,assignment_id))");
    }

    static void initial(JdbcJournal j,Connection c,String generation)throws SQLException{
        Placement first=new Placement(j.settings.writerAssignment,j.settings.writerNode,null,generation,0,0,null,null);
        insert(j,c,first);
        JdbcJournal.execute(c,"INSERT INTO "+j.table("writer_assignment")+" (pipeline_id,assignment_id) VALUES (?,?)",
                j.settings.pipelineId,first.assignment());
    }

    private static void insert(JdbcJournal j,Connection c,Placement p)throws SQLException{
        JdbcJournal.execute(c,"INSERT INTO "+j.table("writer_assignment_history")
                        +" (pipeline_id,assignment_id,node_id,previous_assignment,generation,activated_epoch,"
                        +"last_sequence,checkpoint_hash,evidence_hash) VALUES (?,?,?,?,?,?,?,?,?)",
                j.settings.pipelineId,p.assignment(),p.node(),p.previousAssignment(),p.generation(),p.epoch(),
                p.sequence(),p.checkpointSha256(),p.evidenceSha256());
    }

    static Placement current(JdbcJournal j,Connection c)throws SQLException{
        String sql="SELECT h.assignment_id,h.node_id,h.previous_assignment,h.generation,h.activated_epoch,"
                +"h.last_sequence,h.checkpoint_hash,h.evidence_hash FROM "+j.table("writer_assignment")
                +" p JOIN "+j.table("writer_assignment_history")
                +" h ON h.pipeline_id=p.pipeline_id AND h.assignment_id=p.assignment_id WHERE p.pipeline_id=?";
        try(PreparedStatement statement=c.prepareStatement(sql)){
            statement.setString(1,j.settings.pipelineId);
            try(ResultSet rows=statement.executeQuery()){
                if(!rows.next())throw new IllegalStateException("WRITER_ASSIGNMENT_HISTORY_REQUIRED");
                return new Placement(rows.getString(1),rows.getString(2),rows.getString(3),rows.getString(4),
                        rows.getLong(5),rows.getLong(6),rows.getString(7),rows.getString(8));
            }
        }
    }

    static void requireConfigured(JdbcJournal j,Connection c,Map<String,Object> job)throws SQLException{
        if(JdbcJournal.assignedWriter((int)job.get("format"))){
            if(j.settings.writerAssignment==null||j.settings.managedWriter!=JdbcJournal.managedWriter((int)job.get("format")))
                throw new JdbcJournal.Fenced();
            Placement p=current(j,c);
            if(!j.settings.writerAssignment.equals(p.assignment())||!j.settings.writerNode.equals(p.node())
                    ||!p.generation().equals(job.get("generation"))||p.epoch()>(long)job.get("epoch"))
                throw new JdbcJournal.Fenced();
        }else if(j.settings.writerAssignment!=null)throw new IllegalStateException("WRITER_ASSIGNMENT_NOT_ENROLLED");
    }

    public static Placement initialize(PipelineConfig config,SourceConnection sources)throws Exception{
        config.validate();
        JdbcJournal.Settings settings=config.existingJournalSettings();
        if(settings.writerAssignment==null)throw new IllegalArgumentException("WRITER_ASSIGNMENT_REQUIRED");
        try(SourceAdapter source=sources.open("probe")){
            source.requireSeparateDatabase(settings.driver,settings.url,settings.username,settings.password);
        }
        try(JdbcJournal j=new JdbcJournal(settings)){
            j.initializeStorage(config.fingerprint(),null,config.subscribers());
            try(Connection c=j.connect()){return current(j,c);}
        }
    }

    public static Placement transfer(PipelineConfig candidate,String previous,String generation,long epoch,
                                     SourceConnection sources)throws Exception{
        candidate.validate();
        JdbcJournal.Settings settings=candidate.existingJournalSettings();
        if(settings.managedWriter)throw new IllegalStateException("WRITER_ASSIGNMENT_MANAGED_REQUIRED");
        if(settings.writerAssignment==null||!validId(previous)||!validId(generation)||previous.equals(settings.writerAssignment)
                ||epoch<1||epoch==Long.MAX_VALUE)throw new IllegalArgumentException("WRITER_ASSIGNMENT_REQUEST_INVALID");
        try(JdbcJournal j=new JdbcJournal(settings);Connection c=j.connect();
            RepairConnectionGuard deadline=new RepairConnectionGuard(c,60000)){
            c.setAutoCommit(false);
            Map<String,Object> job=j.assignmentLock(c);
            String fingerprint=candidate.fingerprint();
            if(JdbcJournal.managedWriter((int)job.get("format")))throw new IllegalStateException("WRITER_ASSIGNMENT_MANAGED_REQUIRED");
            if(!JdbcJournal.assignedWriter((int)job.get("format")))
                throw new IllegalStateException("WRITER_ASSIGNMENT_NOT_ENROLLED");
            if(!generation.equals(job.get("generation"))||!fingerprint.equals(job.get("fingerprint")))
                throw new IllegalStateException("WRITER_ASSIGNMENT_JOURNAL_CHANGED");
            Placement current=current(j,c);
            if(settings.writerAssignment.equals(current.assignment())){
                if(!previous.equals(current.previousAssignment())||epoch+1!=current.epoch()
                        ||!settings.writerNode.equals(current.node())||!generation.equals(current.generation()))
                    throw new IllegalStateException("WRITER_ASSIGNMENT_REQUEST_CHANGED");
                c.commit();
                return current;
            }
            if(!previous.equals(current.assignment())||!generation.equals(current.generation())
                    ||epoch!=(long)job.get("epoch"))
                throw new IllegalStateException("WRITER_ASSIGNMENT_CHANGED");
            long sequence=requireReadyCut(candidate,j,c,job);

            String captured=(String)job.get("captured");
            String checkpointHash=Codec.sha256(captured);
            String evidenceHash=verifyOriginalCut(candidate,j,c,job,generation,sequence,captured,sources);
            Placement next=new Placement(settings.writerAssignment,settings.writerNode,previous,generation,epoch+1,
                    sequence,checkpointHash,evidenceHash);
            try{insert(j,c,next);}
            catch(SQLException duplicate){
                if("23505".equals(duplicate.getSQLState()))throw new IllegalStateException("WRITER_ASSIGNMENT_RETIRED");
                throw duplicate;
            }
            JdbcJournal.execute(c,"UPDATE "+j.table("writer_assignment")+" SET assignment_id=? WHERE pipeline_id=?",
                    next.assignment(),settings.pipelineId);
            JdbcJournal.execute(c,"UPDATE "+j.table("job")+" SET epoch=?,owner=NULL,lease_until=0 WHERE pipeline_id=?",
                    next.epoch(),settings.pipelineId);
            deadline.verify();
            c.commit();
            return next;
        }
    }

    /** Checks the exact durable cut through existing metadata, with no placement or data writes. */
    public static CutProof verifyExistingCut(PipelineConfig candidate,ExpectedCut expected,
                                             SourceConnection sources)throws Exception{
        candidate.validate();
        if(!"EXTERNAL".equals(candidate.journal.mode)||candidate.journal.product!=TargetProduct.POSTGRESQL
                ||!"MYSQL".equals(candidate.effectiveSourceType())||candidate.targets.size()>8
                ||candidate.targets.stream().anyMatch(t->t.product!=TargetProduct.POSTGRESQL&&t.product!=TargetProduct.MYSQL))
            throw new IllegalArgumentException("HANDOFF_JOURNAL_PROFILE_UNSUPPORTED");
        try(JdbcJournal j=new JdbcJournal(candidate.existingJournalSettings());Connection c=j.connect();
            RepairConnectionGuard deadline=new RepairConnectionGuard(c,20000)){
            c.setAutoCommit(false);
            Map<String,Object> job=j.assignmentLock(c);
            if(!candidate.fingerprint().equals(job.get("fingerprint"))||expected.epoch()!=(long)job.get("epoch"))
                throw new IllegalStateException("HANDOFF_ORIGINAL_CUT_CHANGED");
            long sequence=requireReadyCut(candidate,j,c,job);
            String captured=(String)job.get("captured");
            String observed=checkpointDigest(captured);
            if(expected.sequence()!=sequence||!expected.checkpointSha256().equals(observed))
                throw new IllegalStateException("HANDOFF_ORIGINAL_CUT_CHANGED");
            String generation=(String)job.get("generation");
            String nativeEvidence=verifyOriginalCut(candidate,j,c,job,generation,sequence,captured,sources);
            deadline.verify();
            int format=(int)job.get("format");
            ManagedCut managed=null;
            if(candidate.journal.managedWriter||JdbcJournal.managedWriter(format)){
                requireConfigured(j,c,job);
                Placement original=current(j,c);
                managed=new ManagedCut(original.node(),Codec.sha256(original.assignment()),expected.epoch(),sequence,observed);
            }
            c.rollback();
            return new CutProof(Codec.sha256(generation),nativeEvidence,candidate.subscribers().stream().sorted().toList(),
                    JdbcJournal.assignedWriter(format),managed);
        }
    }

    private static String checkpointDigest(String captured)throws Exception{
        return Codec.sha256(Model.JSON.writeValueAsString(Codec.decode(captured,SourcePosition.class)));
    }

    private record LockedControlCut(String generation,String captured){}

    /** Lock and check the same managed journal cut using the caller's PostgreSQL control transaction. */
    public static void verifyManagedControlCut(Connection c,String namespace,String task,String assignment,CutProof proof)throws Exception{
        lockManagedControlCut(c,namespace,task,assignment,proof);
    }

    /** Writes the journal half on the caller's control transaction. The caller must commit both halves together. */
    public static Placement writeManagedControlPlacement(Connection c,String namespace,String task,String previous,
                                                         CutProof proof,String next,String candidate)throws Exception{
        if(proof==null||proof.managedWriter()==null||!validId(next)||next.equals(previous)
                ||candidate==null||!candidate.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
                ||candidate.equals(proof.managedWriter().originalNode())||proof.managedWriter().epoch()>=Long.MAX_VALUE
                ||!Objects.toString(proof.nativeEvidenceSha256(),"").matches("[a-f0-9]{64}"))
            throw new IllegalStateException("HANDOFF_MANAGED_PLACEMENT_INVALID");
        LockedControlCut locked=lockManagedControlCut(c,namespace,task,previous,proof);
        ManagedCut cut=proof.managedWriter();
        long nextEpoch=Math.addExact(cut.epoch(),1);
        Placement placement=new Placement(next,candidate,previous,locked.generation,nextEpoch,cut.sequence(),
                Codec.sha256(locked.captured),proof.nativeEvidenceSha256());
        String schema=Sql.pg(namespace);
        JdbcJournal.execute(c,"INSERT INTO "+schema+".rm3_writer_assignment_history"
                        +" (pipeline_id,assignment_id,node_id,previous_assignment,generation,activated_epoch,last_sequence,checkpoint_hash,evidence_hash)"
                        +" VALUES (?,?,?,?,?,?,?,?,?)",task,next,candidate,previous,locked.generation,nextEpoch,cut.sequence(),
                placement.checkpointSha256(),placement.evidenceSha256());
        if(JdbcJournal.execute(c,"UPDATE "+schema+".rm3_writer_assignment SET assignment_id=?"
                +" WHERE pipeline_id=? AND assignment_id=?",next,task,previous)!=1
                ||JdbcJournal.execute(c,"UPDATE "+schema+".rm3_job SET epoch=?,owner=NULL,lease_until=0"
                +" WHERE pipeline_id=? AND epoch=? AND generation=? AND next_seq=?",nextEpoch,task,cut.epoch(),
                locked.generation,Math.addExact(cut.sequence(),1))!=1)
            throw new IllegalStateException("HANDOFF_MANAGED_CONTROL_CUT_CHANGED");
        return placement;
    }

    /** A stopped recipient must see the exact committed native history before writing its local configuration. */
    public static void verifyRecipientControlCut(Connection c,String namespace,String task,String previousAssignment,
                                                 String assignment,CutProof proof)throws Exception{
        if(!validId(previousAssignment)||Objects.equals(previousAssignment,assignment))
            throw new IllegalStateException("HANDOFF_RECIPIENT_NATIVE_HISTORY_CHANGED");
        LockedControlCut locked=lockManagedControlCut(c,namespace,task,assignment,proof);
        ManagedCut cut=proof.managedWriter();
        try(PreparedStatement statement=c.prepareStatement("SELECT previous_assignment,activated_epoch,last_sequence,checkpoint_hash,evidence_hash"
                +" FROM "+Sql.pg(namespace)+".rm3_writer_assignment_history WHERE pipeline_id=? AND assignment_id=?")){
            statement.setQueryTimeout(5);statement.setString(1,task);statement.setString(2,assignment);
            try(ResultSet row=statement.executeQuery()){
                if(!row.next()||!previousAssignment.equals(row.getString(1))||row.getLong(2)!=cut.epoch()
                        ||row.getLong(3)!=cut.sequence()||!Codec.sha256(locked.captured).equals(row.getString(4))
                        ||!proof.nativeEvidenceSha256().equals(row.getString(5)))
                    throw new IllegalStateException("HANDOFF_RECIPIENT_NATIVE_HISTORY_CHANGED");
            }
        }
    }

    private static LockedControlCut lockManagedControlCut(Connection c,String namespace,String task,String assignment,CutProof proof)throws Exception{
        if(c.getAutoCommit()||!"PostgreSQL".equals(c.getMetaData().getDatabaseProductName())||proof==null
                ||proof.managedWriter()==null||!validId(assignment)||!Codec.sha256(assignment).equals(proof.managedWriter().originalAssignmentSha256()))
            throw new IllegalStateException("HANDOFF_MANAGED_CONTROL_CUT_INVALID");
        ManagedCut expected=proof.managedWriter();
        String schema=Sql.pg(namespace);
        String sql="SELECT j.format_version,j.generation,j.epoch,j.next_seq,j.normalized_seq,j.captured,j.owner,j.lease_until,"
                +"j.run_state,h.assignment_id,h.node_id,h.generation,h.activated_epoch,"
                +"CAST(EXTRACT(EPOCH FROM clock_timestamp())*1000 AS BIGINT) FROM "+schema+".rm3_job j JOIN "
                +schema+".rm3_writer_assignment a ON a.pipeline_id=j.pipeline_id JOIN "+schema+".rm3_writer_assignment_history h"
                +" ON h.pipeline_id=a.pipeline_id AND h.assignment_id=a.assignment_id WHERE j.pipeline_id=? FOR UPDATE OF j";
        LockedControlCut locked;
        try(PreparedStatement statement=c.prepareStatement(sql)){
            statement.setQueryTimeout(5);statement.setString(1,task);
            try(ResultSet row=statement.executeQuery()){
                if(!row.next()||!JdbcJournal.managedWriter(row.getInt(1))||!proof.journalGenerationSha256().equals(Codec.sha256(row.getString(2)))
                        ||row.getLong(3)!=expected.epoch()||row.getLong(4)-1!=expected.sequence()||row.getLong(5)!=expected.sequence()
                        ||row.getString(6)==null||!expected.checkpointSha256().equals(checkpointDigest(row.getString(6)))
                        ||row.getString(7)!=null&&row.getLong(8)>row.getLong(14)||!"RUNNING".equals(row.getString(9))
                        ||!assignment.equals(row.getString(10))||!expected.originalNode().equals(row.getString(11))
                        ||!row.getString(2).equals(row.getString(12))||row.getLong(13)>expected.epoch())
                    throw new IllegalStateException("HANDOFF_MANAGED_CONTROL_CUT_CHANGED");
                locked=new LockedControlCut(row.getString(2),row.getString(6));
            }
        }
        Set<String> destinations=new TreeSet<>();
        try(PreparedStatement statement=c.prepareStatement("SELECT subscription_id,acknowledged,run_state FROM "
                +schema+".rm3_subscription WHERE pipeline_id=?")){
            statement.setQueryTimeout(5);statement.setString(1,task);
            try(ResultSet rows=statement.executeQuery()){
                while(rows.next()){
                    if(rows.getLong(2)!=expected.sequence()||!"RUNNING".equals(rows.getString(3)))
                        throw new IllegalStateException("HANDOFF_MANAGED_CONTROL_CUT_CHANGED");
                    destinations.add(rows.getString(1));
                }
            }
        }
        if(!destinations.equals(new TreeSet<>(proof.destinations())))
            throw new IllegalStateException("HANDOFF_MANAGED_CONTROL_CUT_CHANGED");
        return locked;
    }

    private static long requireReadyCut(PipelineConfig candidate,JdbcJournal j,Connection c,Map<String,Object> job)throws Exception{
        if(job.get("owner")!=null&&(long)job.get("lease")>j.now(c))throw new JdbcJournal.Busy();
        if(!"RUNNING".equals(job.get("state"))||job.get("captured")==null)
            throw new IllegalStateException("WRITER_ASSIGNMENT_PIPELINE_NOT_READY");
        if(JdbcJournal.hasTargetGaps((int)job.get("format")))JdbcSkipJournal.requireNoPending(j,c,null);
        long sequence=(long)job.get("next")-1;
        if(sequence!=(long)job.get("normalized"))throw new IllegalStateException("WRITER_ASSIGNMENT_NOT_CAUGHT_UP");
        List<Journal.Subscription> subscriptions=j.subscriptions(c);
        if(!new TreeSet<>(candidate.subscribers()).equals(new TreeSet<>(subscriptions.stream().map(s->s.id).toList())))
            throw new IllegalStateException("WRITER_ASSIGNMENT_JOURNAL_CHANGED");
        for(Journal.Subscription subscription:subscriptions)
            if(!"RUNNING".equals(subscription.state)||subscription.acknowledged!=sequence)
                throw new IllegalStateException("WRITER_ASSIGNMENT_NOT_CAUGHT_UP");
        return sequence;
    }

    private static String verifyOriginalCut(PipelineConfig config,JdbcJournal j,Connection c,Map<String,Object> job,String generation,
                                            long sequence,String captured,SourceConnection sources)throws Exception{
        Map<String,Journal.SnapshotState> snapshots=new TreeMap<>();
        List<TableSchema> schemas;
        try(SourceAdapter source=sources.open(generation)){
            JdbcJournal.Settings storage=j.settings;
            source.requireSeparateDatabase(storage.driver,storage.url,storage.username,storage.password);
            source.validateFailover(Codec.decode(captured,SourcePosition.class),j.dependencies());
            schemas=source.discover();
            for(PipelineConfig.Target target:config.targets){
                JdbcSink.Settings destination=config.targetSettings(target,generation);
                source.requireSeparateDatabase(destination.driver,destination.url,destination.username,destination.password);
            }
        }
        if(schemas.isEmpty()||schemas.size()>256)throw new IllegalStateException("WRITER_ASSIGNMENT_TABLE_LIMIT");
        Map<String,TableSchema> retained=new TreeMap<>();
        for(TableSchema schema:j.schemas())retained.put(schema.id,schema);
        for(TableSchema schema:schemas){
            TableSchema known=retained.get(schema.id);
            Journal.SnapshotState snap=j.snapshot(c,"source",schema.id,false);
            if(snap==null||!"COMPLETE".equals(snap.state)||known==null||!Codec.encode(known).equals(Codec.encode(schema))
                    ||snap.schemaVersion!=schema.version)
                throw new IllegalStateException("WRITER_ASSIGNMENT_SNAPSHOT_CHANGED");
            snapshots.put(schema.id,snap);
        }
        try(PreparedStatement p=c.prepareStatement("SELECT COUNT(*) FROM "+j.table("snapshot")
                +" WHERE pipeline_id=? AND snapshot_scope='source'")){
            p.setString(1,j.settings.pipelineId);
            try(ResultSet r=p.executeQuery()){
                r.next();
                if(r.getLong(1)!=snapshots.size())throw new IllegalStateException("WRITER_ASSIGNMENT_SNAPSHOT_CHANGED");
            }
        }
        Map<String,SinkAdapter.Checkpoint> targetEvidence=new TreeMap<>();
        for(PipelineConfig.Target target:config.targets)try(JdbcSink sink=JdbcSink.existing(config.targetSettings(target,generation))){
            List<TableSchema> mapped=schemas.stream().filter(sink::includes).toList();
            // The dispatcher acknowledges every source manifest and writes only the mapped tables.
            requireSnapshotAcknowledgements(j,c,target.id,snapshots);
            SinkAdapter.Checkpoint checkpoint=sink.verifyWriterCut((long)job.get("epoch"),sequence,mapped,snapshots);
            requireTransactionAt(j,c,sequence,checkpoint.transactionId);
            if(checkpoint.skipOperation!=null)JdbcSkipJournal.verifyCheckpoint(j,c,job,target.id,checkpoint);
            targetEvidence.put(target.id,checkpoint);
        }
        return Codec.sha256(Codec.encode(List.of(snapshots,targetEvidence)));
    }

    private static void requireTransactionAt(JdbcJournal j,Connection c,long sequence,String transaction)throws SQLException{
        if(sequence==0)return;
        try(PreparedStatement p=c.prepareStatement("SELECT stream_seq FROM "+j.table("receipt")
                +" WHERE pipeline_id=? AND transaction_id=?")){
            p.setString(1,j.settings.pipelineId);p.setString(2,transaction);
            try(ResultSet r=p.executeQuery()){
                if(!r.next())throw new IllegalStateException("WRITER_ASSIGNMENT_TRANSACTION_MISSING");
                if(r.getLong(1)!=sequence)throw new IllegalStateException("WRITER_ASSIGNMENT_TRANSACTION_CHANGED");
            }
        }
    }

    private static void requireSnapshotAcknowledgements(JdbcJournal j,Connection c,String target,
                                                         Map<String,Journal.SnapshotState> snapshots)throws SQLException{
        Map<String,String> expected=new TreeMap<>(),actual=new TreeMap<>();
        for(Map.Entry<String,Journal.SnapshotState> table:snapshots.entrySet())
            expected.put(table.getKey(),table.getValue().snapshotId);
        String sql="SELECT table_id,snapshot_id FROM "+j.table("snapshot_ack")
                +" WHERE pipeline_id=? AND snapshot_scope='source' AND subscription_id=?";
        try(PreparedStatement p=c.prepareStatement(sql)){
            p.setString(1,j.settings.pipelineId);p.setString(2,target);
            try(ResultSet r=p.executeQuery()){while(r.next())actual.put(r.getString(1),r.getString(2));}
        }
        if(!expected.equals(actual))throw new IllegalStateException("WRITER_ASSIGNMENT_SNAPSHOT_NOT_ACKNOWLEDGED");
    }
}
