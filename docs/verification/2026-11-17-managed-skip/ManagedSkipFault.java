package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import java.nio.file.Paths;
import java.sql.*;

/** Test-only source ACK failure,installed only after the real task is fully paused. */
public final class ManagedSkipFault {
    public static void main(String[] args)throws Exception{
        if(args.length!=3||!("BLOCK_ACK".equals(args[2])||"CLEAR_ACK".equals(args[2])))throw new IllegalArgumentException("FAULT_ARGUMENTS_INVALID");
        PipelineConfig config=PipelineConfig.load(Paths.get(args[0]));String target=args[1];if(!target.matches("[A-Za-z0-9._-]{1,128}"))throw new IllegalArgumentException("FAULT_TARGET_INVALID");
        try(JdbcJournal journal=new JdbcJournal(config.existingJournalSettings());Connection c=journal.connect()){
            journal.verifyIdentity(config.fingerprint(),config.subscribers());Journal.Subscription sub=journal.subscriptions().stream().filter(s->s.id.equals(target)).findFirst().orElseThrow(()->new IllegalStateException("FAULT_TARGET_MISSING"));
            if(!"PAUSED".equals(journal.status().get("run_state"))||!"PAUSED".equals(sub.state))throw new IllegalStateException("FAULT_PAUSE_REQUIRED");
            String sql="BLOCK_ACK".equals(args[2])?"ALTER TABLE "+journal.table("subscription")+" ADD CONSTRAINT managed_skip_ack_block CHECK (subscription_id <> '"+target+"' OR acknowledged <= "+sub.acknowledged+")":"ALTER TABLE "+journal.table("subscription")+(journal.settings.driver.startsWith("com.mysql")?" DROP CHECK ":" DROP CONSTRAINT ")+"managed_skip_ack_block";
            try(Statement statement=c.createStatement()){statement.execute(sql);}System.out.println("MANAGED_SKIP_"+args[2]+" sequence="+sub.acknowledged);
        }
    }
}
