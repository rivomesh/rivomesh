package io.github.rivomesh.platform;

import io.github.rivomesh.Model;
import io.github.rivomesh.validation.ValidationArchive;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HandoffRestoreFenceTest {
    @TempDir Path temporary;

    @Test void pendingRestoreFencesHandoffEvenWhenBothFilesExist()throws Exception{
        Path root=temporary.toRealPath();
        Path shared=Files.createDirectory(root.resolve("shared"),PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rwx------")));
        Path reports=Files.createDirectory(root.resolve("reports"));
        String reportId=UUID.randomUUID().toString(),ownerDirectory=UUID.randomUUID().toString();
        Path report=Files.createDirectory(reports.resolve(reportId));
        Files.createFile(report.resolve("validation.lock"));
        Files.writeString(report.resolve("summary.json"),"{\"id\":\""+reportId+"\",\"state\":\"COMPLETE\",\"sourceComplete\":true}");
        Files.write(report.resolve("rows.mv.db"),new byte[]{1,2,3});
        var identity=new ValidationArchive.Scope("task",ownerDirectory,reportId);
        var file=new ValidationArchive.FileSeal(3,"0".repeat(64));
        Files.write(report.resolve("archive.json"),Model.JSON.writeValueAsBytes(new ValidationArchive.Seal(1,identity,
                "sealed_copy","retained_hold","admin",Instant.now().toString(),Map.of("rows.mv.db",file,"summary.json",file))));
        Files.write(report.resolve("archive-restore.json"),Model.JSON.writeValueAsBytes(new ValidationArchive.Restoration(
                1,"sealed_copy","restore_job","admin",false)));
        var scope=new SharedHandoffEvidence.Scope("cluster","task","assessment","revision","config",ownerDirectory,"candidate");

        assertEquals("HANDOFF_REPORT_ARCHIVE_RESTORE_REQUIRED",assertThrows(TaskService.Conflict.class,
                ()->SharedHandoffEvidence.export(shared,scope,reports,List.of())).getMessage());
        Files.write(report.resolve("archive-restore.json"),Model.JSON.writeValueAsBytes(new ValidationArchive.Restoration(
                1,"sealed_copy","restore_job","admin",true)));
        assertEquals(1,SharedHandoffEvidence.export(shared,scope,reports,List.of()).reports());
    }
}
