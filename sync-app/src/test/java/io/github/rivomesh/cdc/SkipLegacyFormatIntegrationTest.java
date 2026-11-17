package io.github.rivomesh.cdc;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native fixture explicitly selects this actual-older-binary guard; offline suite excludes it. */
@EnabledIfEnvironmentVariable(named="RIVOMESH_LEGACY_ARTIFACT",matches=".+")
class SkipLegacyFormatIntegrationTest {
    @TempDir Path directory;
    @Test void acceptedOlderBinaryRefusesFormatFourBeforeClaimAndCannotResetItsHistory()throws Exception{
        TransactionSkipRecoveryTest setup=new TransactionSkipRecoveryTest();setup.directory=directory;setup.prepare();
        try{
            setup.execute();JdbcJournal journal=setup.fixture.journal;journal.resumePipeline(journal.pausePipeline().errorGeneration);journal.resume("target",2);
            String before;try(JdbcSink sink=JdbcSink.existing(setup.fixture.target)){before=Codec.encode(sink.checkpoint());}
            Path output=directory.resolve("legacy-probe.log");String cp=System.getenv("RIVOMESH_LEGACY_ARTIFACT")+File.pathSeparator+System.getenv("RIVOMESH_LEGACY_TEST_CLASSES");
            Process child=new ProcessBuilder(new File(System.getProperty("java.home"),"bin/java").toString(),"-Xmx128m","-cp",cp,"io.github.rivomesh.cdc.OldJournalFormatProbe",journal.settings.namespace,journal.settings.pipelineId,setup.fixture.target.fingerprint,"generation").redirectErrorStream(true).redirectOutput(output.toFile()).start();
            boolean done=child.waitFor(30,TimeUnit.SECONDS);if(!done)child.destroyForcibly();assertTrue(done,"Legacy probe timed out");String text=new String(Files.readAllBytes(output),java.nio.charset.StandardCharsets.UTF_8);assertEquals(0,child.exitValue(),text);assertTrue(text.contains("LEGACY_FORMAT4_REFUSED_BEFORE_CLAIM"),text);
            assertEquals(4,journal.status().get("dataFormat"));assertEquals(1,journal.status().get("gapCount"));assertNull(journal.status().get("owner"));try(JdbcSink sink=JdbcSink.existing(setup.fixture.target)){assertEquals(before,Codec.encode(sink.checkpoint()));}
        }finally{setup.close();}
    }
}
