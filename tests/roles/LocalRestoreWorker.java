package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.PipelineEngine;

import java.io.IOException;
import java.nio.file.Path;

/** Gives the backup test an orderly stop using the release engine's lifecycle. */
public final class LocalRestoreWorker {
    public static void main(String[] args) throws Exception {
        PipelineConfig configuration = PipelineConfig.load(Path.of(args[1]));
        if ("shutdown-h2".equals(args[0])) {
            var storage = configuration.existingJournalSettings();
            try (var connection = JdbcConnections.open(storage.driver, storage.url, storage.username, storage.password);
                 var statement = connection.createStatement()) {
                statement.execute("SHUTDOWN");
                if (!connection.isClosed()) throw new IllegalStateException("H2_SHUTDOWN_NOT_CONFIRMED");
            }
            System.out.println("LOCAL_RESTORE_H2_CLOSED");
            return;
        }
        try (PipelineEngine engine = new PipelineEngine(configuration)) {
            Thread.ofPlatform().name("fixture-stop").daemon().start(() -> {
                try {
                    System.in.read();
                } catch (IOException supervisorClosed) {
                    // Losing the supervisor also requests shutdown.
                } finally {
                    engine.close();
                }
            });
            engine.run();
        }
        System.out.println("LOCAL_RESTORE_WORKER_CLOSED");
    }
}
