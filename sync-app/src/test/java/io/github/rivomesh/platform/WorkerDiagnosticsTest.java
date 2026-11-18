package io.github.rivomesh.platform;

import io.github.rivomesh.cdc.PipelineFailure.Phase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WorkerDiagnosticsTest {
    @TempDir Path directory;

    @Test void pagesOnlyStructuredEventsAndKeepsAnAnchorAcrossNewEvents()throws Exception{
        Path log=directory.resolve("job.log");
        WorkerDiagnostics.append(log,WorkerDiagnostics.Event.CAPTURE_READY,null);
        WorkerDiagnostics.append(log,WorkerDiagnostics.Event.CAPTURE_FAILURE,Phase.SOURCE_DECODE);
        Files.write(log.resolveSibling("job.log.events"),
                "ca32cedd-d7d6-46ca-93c3-5a12e31bed1a 2026-11-18T00:00:00Z CAPTURE_FAILURE BUSINESS_VALUE\n"
                        .getBytes(StandardCharsets.UTF_8),StandardOpenOption.APPEND);

        Map<String,Object> first=WorkerDiagnostics.page(log,"",1);
        assertEquals("CAPTURE_FAILURE",event(first).get("event"));
        assertEquals("SOURCE_DECODE",event(first).get("phase"));
        assertFalse(first.toString().contains("BUSINESS_VALUE"));

        WorkerDiagnostics.append(log,WorkerDiagnostics.Event.RETRY,null);
        Map<String,Object> older=WorkerDiagnostics.page(log,(String)first.get("nextCursor"),1);
        assertEquals("CAPTURE_READY",event(older).get("event"));
        assertNull(older.get("nextCursor"));
        assertThrows(IllegalArgumentException.class,()->WorkerDiagnostics.page(log,"",26));
        assertEquals("DIAGNOSTICS_CURSOR_EXPIRED",assertThrows(TaskService.Conflict.class,
                ()->WorkerDiagnostics.page(log,"ca32cedd-d7d6-46ca-93c3-5a12e31bed1a",1)).getMessage());
    }

    private static Map<?,?> event(Map<String,Object> page){
        return (Map<?,?>)((List<?>)page.get("events")).get(0);
    }
}
