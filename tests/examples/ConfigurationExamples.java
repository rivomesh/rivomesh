package io.github.rivomesh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.platform.PlatformConfig;

import java.nio.file.Path;
import java.nio.file.Paths;

final class ConfigurationExamples {
    public static void main(String[] args)throws Exception{
        ObjectMapper yaml=new ObjectMapper(new YAMLFactory());
        for(String filename:args){
            Path path=Paths.get(filename);
            String leaf=path.getFileName().toString();
            JsonNode document=yaml.readTree(path.toFile());
            if(leaf.startsWith("platform")){
                PlatformConfig.load(path);
            }else if(document.path("formatVersion").asInt()==3){
                PipelineConfig.load(path);
            }else if(leaf.equals("oracle-source-probe.yml")){
                verifyOracleProbe(document);
            }else{
                Config.load(path);
            }
            System.out.println("CONFIG_EXAMPLE "+filename);
        }
    }

    private static void verifyOracleProbe(JsonNode document){
        if(!document.path("sourceId").isTextual()||!document.path("pdbName").isTextual()
                ||!document.path("tables").isArray()||document.path("tables").isEmpty())
            throw new IllegalArgumentException("ORACLE_PROBE_CONFIG_INVALID");
        for(String name:new String[]{"root","pdb"}){
            JsonNode connection=document.path(name);
            if(!connection.path("url").asText().startsWith("jdbc:oracle:thin:@//")
                    ||!connection.path("username").isTextual()||!connection.path("password").isTextual())
                throw new IllegalArgumentException("ORACLE_PROBE_ENDPOINT_INVALID");
        }
    }
}
