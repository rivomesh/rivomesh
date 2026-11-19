package io.github.rivomesh.cdc;

import io.github.rivomesh.PipelineConfig;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/** Exercise the transaction-local ACK setting against the disposable journal role. */
public final class ManagedAckIdleTimeoutFixture {
    private ManagedAckIdleTimeoutFixture(){}

    public static void main(String[] args)throws Exception{
        if(args.length!=1)throw new IllegalArgumentException("ACK_TIMEOUT_FIXTURE_ARGUMENTS_INVALID");
        PipelineConfig pipeline=PipelineConfig.load(Path.of(args[0]));
        JdbcJournal.Settings settings=pipeline.existingJournalSettings();
        if(!"global_cdc".equals(pipeline.pipelineId)||!"transfer_journal".equals(settings.namespace)
                ||!settings.managedWriter||!"jdbc:postgresql://store:5432/rivomesh_control".equals(settings.url)
                ||!"sync_journal".equals(settings.username)||settings.leaseMillis!=5000)
            throw new IllegalArgumentException("ACK_TIMEOUT_FIXTURE_REQUIRES_DISPOSABLE_JOURNAL");

        try(JdbcJournal journal=new JdbcJournal(settings);Connection connection=journal.connect()){
            connection.setAutoCommit(false);
            long initial=setting(connection);
            verify(journal,connection,0,5000);
            verify(journal,connection,3000,3000);
            verify(journal,connection,60000,5000);
            if(setting(connection)!=initial)throw new IllegalStateException("ACK_TIMEOUT_LEAKED_AFTER_ROLLBACK");
            connection.rollback();
        }
        System.out.println("MANAGED_ACK_IDLE_TIMEOUT_VERIFIED");
    }

    private static void verify(JdbcJournal journal,Connection connection,long configured,long expected)throws Exception{
        try(PreparedStatement value=connection.prepareStatement(
                "SELECT pg_catalog.set_config('idle_in_transaction_session_timeout', ?::text || 'ms', true)")){
            value.setLong(1,configured);
            value.execute();
        }
        journal.boundAcknowledgementIdle(connection);
        if(setting(connection)!=expected)throw new IllegalStateException("ACK_TIMEOUT_LIMIT_CHANGED");
        connection.rollback();
    }

    private static long setting(Connection connection)throws Exception{
        try(Statement query=connection.createStatement();ResultSet row=query.executeQuery(
                "SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name='idle_in_transaction_session_timeout'")){
            if(!row.next())throw new IllegalStateException("ACK_TIMEOUT_SETTING_MISSING");
            return row.getLong(1);
        }
    }
}
