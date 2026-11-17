package io.github.rivomesh;

import io.github.rivomesh.cdc.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named="sync.integration",matches="true")
class MysqlCdcSourceIntegrationTest {
    @Test void multipleDatabasesKeylessDuplicatesJsonAndGeneratedColumnsUseHistoricalImages()throws Exception{
        Config config=ClusterHarness.config();String second=config.source.database+"_b";
        ClusterHarness.sql("mysql","CREATE DATABASE "+Sql.mysql(config.source.database));ClusterHarness.sql("mysql","CREATE DATABASE "+Sql.mysql(second));
        String first=Sql.mysql(config.source.database)+".items",other=Sql.mysql(second)+".duplicates";
        try{
            try(MysqlCdcSource wrong=new MysqlCdcSource(config,Collections.singleton(config.source.database.toUpperCase(Locale.ROOT)),"generation")){assertEquals("SOURCE_DATABASE_NOT_VISIBLE_OR_CASE_MISMATCH",assertThrows(IllegalArgumentException.class,wrong::discover).getMessage());}
            ClusterHarness.sql("mysql","CREATE TABLE "+first+" (id BIGINT PRIMARY KEY,amount DECIMAL(30,6),doc JSON,twice DECIMAL(31,6) GENERATED ALWAYS AS (amount*2) STORED,caption VARCHAR(100) GENERATED ALWAYS AS (CONCAT('amount=',amount)) VIRTUAL) ENGINE=InnoDB");
            ClusterHarness.sql("mysql","INSERT INTO "+first+" (id,amount,doc) VALUES (1,10.123456,'{\"large\":18446744073709551615}')");
            ClusterHarness.sql("mysql","CREATE TABLE "+other+" (label VARCHAR(100),doc JSON) ENGINE=InnoDB");ClusterHarness.sql("mysql","INSERT INTO "+other+" VALUES ('duplicate','null'),('duplicate','null')");
            try(MysqlCdcSource source=new MysqlCdcSource(config,new TreeSet<>(Arrays.asList(config.source.database,second)),"generation")){
                List<TableSchema> tables=source.discover();assertEquals(2,tables.size());TableSchema table=tables.stream().filter(t->t.name.equals("items")).findFirst().get();TableSchema keyless=tables.stream().filter(t->t.name.equals("duplicates")).findFirst().get();assertTrue(keyless.keyless());
                try(SourceAdapter.Snapshot snapshot=source.snapshot(keyless)){assertEquals(2,snapshot.next(10).size());assertTrue(snapshot.next(10).isEmpty());}
                List<SourceTransaction> commits=new CopyOnWriteArrayList<>();
                try(SourceAdapter.Stream stream=source.stream(source.position(),commits::add)){
                    try(SourceAdapter.Snapshot snapshot=source.snapshot(table)){
                        ClusterHarness.sql("mysql","UPDATE "+first+" SET amount=20.987654,doc=JSON_SET(doc,'$.current',true) WHERE id=1");
                        List<Cell> old=snapshot.next(10).get(0);assertEquals("10.123456",old.get(1).value);assertEquals("20.246912",old.get(3).value);assertEquals("amount=10.123456",old.get(4).value);
                    }
                    ClusterHarness.sql("mysql","UPDATE "+first+" SET amount=30.111111 WHERE id=1");
                    ClusterHarness.sql("mysql","UPDATE "+other+" SET label='changed' LIMIT 1");ClusterHarness.sql("mysql","DELETE FROM "+other+" WHERE label='duplicate' LIMIT 1");
                    long until=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(20);while(commits.size()<4&&System.nanoTime()<until){stream.check();Thread.sleep(20);}stream.check();assertEquals(4,commits.size());
                    Change firstUpdate=commits.get(0).changes.get(0),secondUpdate=commits.get(1).changes.get(0);
                    assertEquals(Change.Operation.UPDATE,firstUpdate.operation);assertEquals("10.123456",firstUpdate.before.get(1).value);assertEquals("20.987654",firstUpdate.after.get(1).value);assertEquals("41.975308",firstUpdate.after.get(3).value);assertEquals("amount=20.987654",firstUpdate.after.get(4).value);
                    assertEquals("amount=20.987654",secondUpdate.before.get(4).value);assertEquals("amount=30.111111",secondUpdate.after.get(4).value);
                    assertTrue(firstUpdate.after.get(2).value.contains("18446744073709551615"));assertEquals(keyless.id,commits.get(2).changes.get(0).tableId);assertEquals(Change.Operation.DELETE,commits.get(3).changes.get(0).operation);
                    assertTrue(source.includes(commits.get(3).position,commits.get(0).id));
                }
            }
        }finally{ClusterHarness.sql("mysql","DROP DATABASE "+Sql.mysql(config.source.database));ClusterHarness.sql("mysql","DROP DATABASE "+Sql.mysql(second));}
    }
}
