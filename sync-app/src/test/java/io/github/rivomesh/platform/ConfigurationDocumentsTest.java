package io.github.rivomesh.platform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.rivomesh.Model;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConfigurationDocumentsTest {
    @Test void encodedJdbcCredentialsAndOptionsNeverAppearInEditorResponses()throws Exception{
        ConfigurationDocuments documents=new ConfigurationDocuments();for(String connection:new String[]{"jdbc:postgresql://host/db?%70assword=private-value","jdbc:mysql://host/db?password2=private-value","jdbc:postgresql://host/db?sslpassword=private-value","jdbc:mysql://host/db?trustCertificateKeyStorePassword=private-value","useUnicode=true&%70assword=private-value","jdbc:postgresql://user:private-value@host/db"}){ObjectNode input=Model.JSON.createObjectNode();input.put("url",connection);JsonNode redacted=documents.redact(input,"task","revision");assertFalse(redacted.toString().contains("private-value"));assertEquals(input,documents.restore(redacted,input,"task","revision"));}
        ObjectNode input=Model.JSON.createObjectNode();input.put("url","jdbc:postgresql://host/db?sslmode=verify-full");assertEquals(input,documents.redact(input,"task","revision"));
    }
    @Test void oldReferencesAndMovingCredentialsBetweenTargetsAreRejected()throws Exception{
        ConfigurationDocuments documents=new ConfigurationDocuments();JsonNode input=Model.JSON.readTree("{\"targets\":[{\"id\":\"a\",\"password\":\"private-a\"},{\"id\":\"b\",\"password\":\"private-b\"}]}");JsonNode redacted=documents.redact(input,"task","revision");assertThrows(TaskService.Conflict.class,()->documents.restore(redacted,input,"task","new-revision"));ObjectNode changed=redacted.deepCopy();changed.withArray("targets").set(0,redacted.path("targets").get(1));assertThrows(TaskService.Conflict.class,()->documents.restore(changed,input,"task","revision"));
    }
}
