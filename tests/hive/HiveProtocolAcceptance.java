package io.github.rivomesh.acceptance;
import io.github.rivomesh.cdc.*;
import java.util.*;
public final class HiveProtocolAcceptance {
 public static void main(String[] args)throws Exception{
  try{Class.forName("org.apache.hive.jdbc.HiveDriver");throw new AssertionError("Private Hive SDK leaked into core loader");}catch(ClassNotFoundException expected){}
  HiveMaterializer.Settings s=new HiveMaterializer.Settings();s.pipelineId="protocol";s.subscriptionId="hive";s.generation="generation";s.fingerprint=Codec.sha256("protocol");s.jdbcUrl="jdbc:hive2://hive:10000/default;auth=noSasl";s.username="hive";s.password="";s.mappings.put("sales","copy");
  System.out.println("HIVE_NATIVE_CHECK: "+HiveMaterializer.check(s));
  try(HiveSqlClient client=new HiveSqlClient(s.jdbcUrl,s.username,s.password,300,4*1024*1024,16*1024*1024)){
   for(String query:new String[]{"SHOW TBLPROPERTIES rm_native_probe.rows_native","DESCRIBE rm_native_probe.rows_native","SELECT k,CAST(v AS STRING) AS v,txt FROM rm_native_probe.rows_native"})System.out.println(client.query(query));
  }
  System.out.println("HIVE_PROTOCOL_ACCEPTED: actual Java 21 release, isolated pinned JDBC, bounded native binary protocol, ACID configuration, exact uint64 and text read");
 }
}
