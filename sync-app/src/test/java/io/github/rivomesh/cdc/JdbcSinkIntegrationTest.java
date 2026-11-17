package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named="sync.integration",matches="true")
class JdbcSinkIntegrationTest {
    @Test void realTransactionalTargetsPreserveAtomicityAndFences()throws Exception{
        for(boolean mysql:Arrays.asList(true,false)){
            JdbcSink.Settings s=new JdbcSink.Settings();s.pipelineId="pipeline";s.id="target";s.generation="generation";s.fingerprint=Codec.sha256("config");
            String namespace="sink_"+UUID.randomUUID().toString().replace("-","").substring(0,12);s.metadataSchema=namespace+"_meta";s.databases.put("sales",namespace);
            s.product=mysql?TargetProduct.MYSQL:System.getProperty("target.type","GAUSS").equals("POSTGRESQL")?TargetProduct.POSTGRESQL:TargetProduct.GAUSS;
            s.driver=mysql?"com.mysql.cj.jdbc.Driver":System.getProperty("target.driver","io.github.rivomesh.gaussdb.jdbc.Driver");
            s.url=mysql?"jdbc:mysql://mysql:3306/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000":System.getProperty("target.url","jdbc:postgresql://gaussdb:5432/sync_test?prepareThreshold=0&connectTimeout=3&socketTimeout=10");
            s.username=mysql?"root":"sync_test";s.password="Sync@Test123";
            try{JdbcSinkTest.contract(s);}
            finally{try(Connection c=JdbcConnections.open(s.driver,s.url,s.username,s.password);Statement st=c.createStatement()){
                for(String schema:Arrays.asList(namespace,s.metadataSchema))st.execute(mysql?"DROP DATABASE IF EXISTS `"+schema+"`":"DROP SCHEMA IF EXISTS \""+schema+"\" CASCADE");
            }}
        }
    }
}
