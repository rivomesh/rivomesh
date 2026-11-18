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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class WorkerDiagnosticsTest {
    @TempDir Path directory;

    @Test void rateBudgetCapsBurstsAndRefillsAtTheOwner(){
        WorkerDiagnostics.ReadBudget budget=new WorkerDiagnostics.ReadBudget();
        long start=-10_000_000L;
        for(int request=0;request<16;request++)assertTrue(budget.take(start));
        assertFalse(budget.take(start));
        assertFalse(budget.take(start+TimeUnit.MILLISECONDS.toNanos(124)));
        long refilled=start+TimeUnit.MILLISECONDS.toNanos(125);
        assertTrue(budget.take(refilled));
        assertFalse(budget.take(refilled));
        long later=start+TimeUnit.SECONDS.toNanos(10);
        for(int request=0;request<16;request++)assertTrue(budget.take(later));
        assertFalse(budget.take(later));
        assertTrue(new WorkerDiagnostics.ReadBudget().take(start));
    }

    @Test void pagesOnlyStructuredEventsAndKeepsAnAnchorAcrossNewEvents()throws Exception{
        Path log=directory.resolve("job.log");
        WorkerDiagnostics.append(log,WorkerDiagnostics.Event.CAPTURE_READY,null);
        WorkerDiagnostics.append(log,WorkerDiagnostics.Event.CAPTURE_FAILURE,Phase.SOURCE_DECODE);
        Files.write(log.resolveSibling("job.log.events"),
                "ca32cedd-d7d6-46ca-93c3-5a12e31bed1a 2026-11-18T00:00:00Z CAPTURE_FAILURE BUSINESS_VALUE\n"
                        .getBytes(StandardCharsets.UTF_8),StandardOpenOption.APPEND);

        Map<String,Object> first=WorkerDiagnostics.page(log,"",1,null);
        assertEquals("CAPTURE_FAILURE",event(first).get("event"));
        assertEquals("SOURCE_DECODE",event(first).get("phase"));
        assertFalse(first.toString().contains("BUSINESS_VALUE"));

        WorkerDiagnostics.append(log,WorkerDiagnostics.Event.RETRY,null);
        Map<String,Object> older=WorkerDiagnostics.page(log,(String)first.get("nextCursor"),1,null);
        assertEquals("CAPTURE_READY",event(older).get("event"));
        assertNull(older.get("nextCursor"));
        assertThrows(IllegalArgumentException.class,()->WorkerDiagnostics.page(log,"",26,null));
        assertEquals("DIAGNOSTICS_CURSOR_EXPIRED",assertThrows(TaskService.Conflict.class,
                ()->WorkerDiagnostics.page(log,"ca32cedd-d7d6-46ca-93c3-5a12e31bed1a",1,null)).getMessage());
    }

    @Test void targetPagesFilterBeforePaginationAndDiscardFailureDetails()throws Exception{
        Path log=directory.resolve("job.log");
        WorkerDiagnostics.append(log,WorkerDiagnostics.Event.CAPTURE_READY,null);
        WorkerDiagnostics.observeTarget(log,"TARGET_PAUSED pg SECRET_FROM_FAILURE_CODE");
        WorkerDiagnostics.observeTarget(log,"TARGET_PAUSED pg unexpected payload with spaces");
        WorkerDiagnostics.observeTarget(log,"TARGET_RETRYING PG TARGET_CONNECTION_RETRY");
        WorkerDiagnostics.observeTarget(log,"TARGET_RETRY_CLEARED pg");

        Map<String,Object> page=WorkerDiagnostics.page(log,"",1,"pg");
        assertEquals("pg",page.get("subscriptionId"));
        assertEquals("TARGET_RETRY_CLEARED",event(page).get("event"));
        String cursor=(String)page.get("nextCursor");
        Map<String,Object> prior=WorkerDiagnostics.page(log,cursor,1,"pg");
        assertEquals("TARGET_PAUSED",event(prior).get("event"));
        assertNull(prior.get("nextCursor"));
        assertFalse(Files.readString(log.resolveSibling("job.log.target-events")).contains("SECRET"));
        assertEquals("TARGET_RETRYING",event(WorkerDiagnostics.page(log,"",1,"PG")).get("event"));
        assertEquals("CAPTURE_READY",event(WorkerDiagnostics.page(log,"",1,null)).get("event"));
        assertEquals("DIAGNOSTICS_CURSOR_EXPIRED",assertThrows(TaskService.Conflict.class,
                ()->WorkerDiagnostics.page(log,cursor,1,"PG")).getMessage());
        assertThrows(IllegalArgumentException.class,()->WorkerDiagnostics.page(log,"",1,"../pg"));
    }

    private static Map<?,?> event(Map<String,Object> page){
        return (Map<?,?>)((List<?>)page.get("events")).get(0);
    }
}
