package io.github.rivomesh.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Native server protocol proof; not a claim that the pipeline sink is wired. */
public final class ClickHouseProtocolAcceptance {
    static void require(boolean value,String message){if(!value)throw new AssertionError(message);}
    static TableSchema.Column column(String name,TableSchema.Type type,boolean nullable){TableSchema.Column c=new TableSchema.Column();c.name=name;c.type=type;c.nullable=nullable;return c;}
    static byte[] insert(String prefix,List<TableSchema.Column> columns,List<Cell> values)throws Exception{ClickHouseRowBinary.Buffer b=new ClickHouseRowBinary.Buffer(1024*1024);byte[] sql=prefix.getBytes(StandardCharsets.UTF_8);b.write(sql,0,sql.length);ClickHouseRowBinary.writeRow(b,columns,values);return b.toByteArray();}
    static void version(ClickHouseHttpClient client,String table,String key,BigInteger version,boolean deleted,String value)throws Exception{
        ClickHouseRowBinary.Buffer b=new ClickHouseRowBinary.Buffer(4096);byte[] sql=("INSERT INTO "+table+" FORMAT RowBinary\n").getBytes(StandardCharsets.UTF_8);b.write(sql,0,sql.length);byte[] fixed=key.getBytes(StandardCharsets.UTF_8);b.write(fixed,0,fixed.length);ClickHouseRowBinary.unsigned(b,version,16);b.write(deleted?1:0);ClickHouseRowBinary.string(b,value);client.execute(b.toByteArray());
    }
    public static void main(String[] args)throws Exception{
        ClickHouseHttpClient.Settings settings=new ClickHouseHttpClient.Settings();settings.url="http://clickhouse:8123";settings.username="rivomesh";settings.password="ClickHouse@Test123";
        String database="native_"+UUID.randomUUID().toString().replace("-","");
        try(ClickHouseHttpClient client=new ClickHouseHttpClient(settings)){
            System.out.println("ClickHouse runtime: "+client.query("SELECT version()").path("data").get(0).get(0).asText());client.execute("CREATE DATABASE "+database+" ENGINE=Atomic");
            List<TableSchema.Column> columns=Arrays.asList(column("id",TableSchema.Type.INT64,false),column("u",TableSchema.Type.UINT64,false),column("d",TableSchema.Type.DECIMAL,false),column("payload",TableSchema.Type.BYTES,true),column("document",TableSchema.Type.JSON,false),column("label",TableSchema.Type.STRING,true),column("flag",TableSchema.Type.BOOLEAN,false),column("f",TableSchema.Type.FLOAT32,false),column("g",TableSchema.Type.FLOAT64,false),column("moment",TableSchema.Type.DATETIME,false));columns.get(2).precision=65;columns.get(2).scale=30;
            List<String> definitions=new ArrayList<>();for(TableSchema.Column c:columns)definitions.add(c.name+" "+ClickHouseRowBinary.type(c));
            client.execute("CREATE TABLE "+database+".typed ("+String.join(",",definitions)+") ENGINE=MergeTree ORDER BY id");
            String decimal="12345678901234567890123456789012345.123456789012345678901234567890",document="{\"large\":18446744073709551615,\"text\":\"中文\"}",moment="1000-01-01 00:00:00.000001";
            List<Cell> values=Arrays.asList(Cell.value(Long.toString(Long.MIN_VALUE)),Cell.value("18446744073709551615"),Cell.value(decimal),Cell.value(Base64.getEncoder().encodeToString(new byte[]{0,1,(byte)128,(byte)255})),Cell.value(document),Cell.value(null),Cell.value("true"),Cell.value("-0.5"),Cell.value("1.2345678901234567"),Cell.value(moment));
            client.execute(insert("INSERT INTO "+database+".typed FORMAT RowBinary\n",columns,values));
            JsonNode row=client.query("SELECT toString(id),toString(u),toString(d),hex(payload),document,label,flag,f,g,moment FROM "+database+".typed").path("data").get(0);
            require(row.get(0).asText().equals(Long.toString(Long.MIN_VALUE)),"Signed boundary changed");
            require(row.get(1).asText().equals("18446744073709551615"),"Unsigned boundary changed");require(new java.math.BigDecimal(row.get(2).asText()).compareTo(new java.math.BigDecimal(decimal))==0,"Decimal256 value changed: "+row.get(2).asText());require(client.query("SELECT toTypeName(d) FROM "+database+".typed").path("data").get(0).get(0).asText().equals("Decimal(65, 30)"),"Decimal precision/scale changed");require(row.get(3).asText().equals("000180FF"),"Raw binary changed");require(row.get(4).asText().equals(document),"JSON text changed");require(row.get(5).isNull(),"SQL NULL changed");require(row.get(6).asInt()==1&&row.get(7).asDouble()==-0.5&&row.get(8).asDouble()==1.2345678901234567,"Numeric value changed");require(row.get(9).asText().equals(moment),"Wide temporal value changed");
            String table=database+".versions",key=String.join("",Collections.nCopies(64,"a"));BigInteger old=BigInteger.ONE,newer=BigInteger.ONE.shiftLeft(80).add(BigInteger.valueOf(7));
            client.execute("CREATE TABLE "+table+" (k FixedString(64),v UInt128,deleted UInt8,value String) ENGINE=ReplacingMergeTree(v) ORDER BY k");
            version(client,table,key,old,false,"old");version(client,table,key,newer,false,"new");version(client,table,key,old,false,"old replay");
            require(client.query("SELECT value,toString(v) FROM "+table+" FINAL").path("data").get(0).get(0).asText().equals("new"),"Older replay replaced newer row");
            String identity=client.query("SELECT toString(uuid) FROM system.tables WHERE database='"+database+"' AND name='versions'").path("data").get(0).get(0).asText();
            Path markers=Paths.get(System.getenv("RIVOMESH_CLICKHOUSE_RESTART_DIR"));Files.write(markers.resolve("restart.request"),new byte[]{1});long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(180);while(!Files.exists(markers.resolve("restart.done"))&&System.nanoTime()<deadline)Thread.sleep(100);require(Files.exists(markers.resolve("restart.done")),"Server restart did not execute");
            require(identity.equals(client.query("SELECT toString(uuid) FROM system.tables WHERE database='"+database+"' AND name='versions'").path("data").get(0).get(0).asText()),"Table identity changed across restart");require(client.query("SELECT value FROM "+table+" FINAL").path("data").get(0).get(0).asText().equals("new"),"Restart lost confirmed rows");
            version(client,table,key,newer.add(BigInteger.ONE),true,"deleted");require(client.query("SELECT count() FROM "+table+" FINAL WHERE deleted=0").path("data").get(0).get(0).asLong()==0,"Tombstone is visible as business data");
            System.out.println("CLICKHOUSE_PROTOCOL_ACCEPTED: native HTTP/RowBinary typed boundaries, raw bytes/JSON/NULL, UInt128 version replay, restart identity/history and tombstone filtering");
        }
    }
}
