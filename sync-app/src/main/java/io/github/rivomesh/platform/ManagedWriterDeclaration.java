package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.cdc.JournalWriterAssignment;
import java.util.Objects;

/** A managed writer's placement fields are immutable through ordinary configuration operations. */
record ManagedWriterDeclaration(String assignment,String node,String journalUrl,String namespace) {
    private static final ObjectMapper YAML=new ObjectMapper(new YAMLFactory());

    static ManagedWriterDeclaration read(String yaml)throws Exception{
        JsonNode document=YAML.readTree(yaml);
        if(document==null||document.path("formatVersion").asInt()!=3)return null;
        JsonNode journal=document.path("journal"),mode=journal.path("managedWriter"),assignment=journal.path("writerAssignment");
        if(!mode.isMissingNode()&&!mode.isBoolean())throw new TaskService.Conflict("MANAGED_WRITER_DECLARATION_INVALID");
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
        return new ManagedWriterDeclaration(assignment.textValue(),node,url,namespace);
    }

    void requireOwner(String owner){
        if(!node.equals(owner))throw new TaskService.Conflict("MANAGED_WRITER_OWNER_REQUIRED");
    }
    static void requireUnchanged(String before,String after)throws Exception{
        if(!Objects.equals(read(before),read(after)))throw new TaskService.Conflict("MANAGED_WRITER_PLACEMENT_IMMUTABLE");
    }
}
