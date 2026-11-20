package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.cdc.JournalWriterAssignment;
import io.github.rivomesh.cdc.TargetProduct;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/** A managed writer's placement fields are immutable through ordinary configuration operations. */
record ManagedWriterDeclaration(String assignment,String node,String journalUrl,String namespace,boolean splitStoreHandoff) {
    private static final ObjectMapper YAML=new ObjectMapper(new YAMLFactory());

    static ManagedWriterDeclaration read(String yaml)throws Exception{
        JsonNode document=YAML.readTree(yaml);
        if(document==null||document.path("formatVersion").asInt()!=3)return null;
        JsonNode journal=document.path("journal"),mode=journal.path("managedWriter"),assignment=journal.path("writerAssignment");
        JsonNode split=journal.path("splitStoreHandoff");
        if(!mode.isMissingNode()&&!mode.isBoolean()||!split.isMissingNode()&&!split.isBoolean())
            throw new TaskService.Conflict("MANAGED_WRITER_DECLARATION_INVALID");
        if(assignment.isMissingNode()||assignment.isNull()){
            if(mode.asBoolean())throw new TaskService.Conflict("MANAGED_WRITER_DECLARATION_INVALID");
            return null;
        }
        if(!mode.asBoolean())throw new TaskService.Conflict("WRITER_ASSIGNMENT_STANDALONE_REQUIRED");
        String node=document.path("nodeId").textValue(),url=journal.path("url").textValue();
        String namespace=journal.path("namespace").asText("rivomesh_v3");
        if(!JournalWriterAssignment.validId(assignment.textValue())||node==null||!node.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
                ||url==null||!url.startsWith("jdbc:postgresql:")||!namespace.matches("[A-Za-z_][A-Za-z0-9_]{0,62}")
                ||!"EXTERNAL".equals(journal.path("mode").textValue())
                ||!"POSTGRESQL".equals(journal.path("product").asText("POSTGRESQL")))
            throw new TaskService.Conflict("MANAGED_WRITER_DECLARATION_INVALID");
        return new ManagedWriterDeclaration(assignment.textValue(),node,url,namespace,split.asBoolean());
    }

    @FunctionalInterface
    interface JournalOperation<T> { T apply(Connection journal)throws Exception; }

    <T> T journalTransaction(String yaml,JournalOperation<T> operation)throws Exception{
        if(!splitStoreHandoff||!equals(read(yaml)))throw new TaskService.Conflict("HANDOFF_SPLIT_PROFILE_INVALID");
        var settings=PipelineConfig.readExternalHandoffJournal(yaml);
        String bounded=AutomaticPlacement.journalProbeUrl(journalUrl,TargetProduct.POSTGRESQL);
        try(Connection journal=JdbcConnections.open("org.postgresql.Driver",bounded,settings.username,settings.password)){
            journal.setAutoCommit(false);
            T result=operation.apply(journal);
            journal.commit();
            return result;
        }catch(SQLException failure){
            String state=failure.getSQLState();
            if(state!=null&&(state.startsWith("08")||Set.of("57P01","57P02","57P03").contains(state)))
                throw new TaskService.Conflict("HANDOFF_SPLIT_JOURNAL_UNAVAILABLE");
            throw failure;
        }
    }

    void requireOwner(String owner){
        if(!node.equals(owner))throw new TaskService.Conflict("MANAGED_WRITER_OWNER_REQUIRED");
    }
    static String successorYaml(String active,String expectedOriginal,String candidate,String nextAssignment)throws Exception{
        ManagedWriterDeclaration original=read(active);
        if(original==null||!JournalWriterAssignment.validId(nextAssignment)||original.assignment.equals(nextAssignment)
                ||Objects.equals(expectedOriginal,candidate))throw new TaskService.Conflict("HANDOFF_MANAGED_PLACEMENT_INVALID");
        original.requireOwner(expectedOriginal);
        ObjectNode document=(ObjectNode)YAML.readTree(active);
        document.put("nodeId",candidate);
        ((ObjectNode)document.get("journal")).put("writerAssignment",nextAssignment);
        String successor=YAML.writeValueAsString(document);
        read(successor).requireOwner(candidate);
        return successor;
    }
    static List<String> subscriptions(String yaml)throws Exception{
        JsonNode targets=YAML.readTree(yaml).path("targets");
        Set<String> subscribers=new TreeSet<>();
        if(!targets.isArray()||targets.isEmpty())throw new TaskService.Conflict("HANDOFF_RECIPIENT_CONFIGURATION_CHANGED");
        for(JsonNode target:targets){
            String id=target.path("id").textValue();
            if(id==null||!id.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")||!subscribers.add(id))
                throw new TaskService.Conflict("HANDOFF_RECIPIENT_CONFIGURATION_CHANGED");
        }
        return List.copyOf(subscribers);
    }
    static void requireUnchanged(String before,String after)throws Exception{
        if(!Objects.equals(read(before),read(after)))throw new TaskService.Conflict("MANAGED_WRITER_PLACEMENT_IMMUTABLE");
    }
}
