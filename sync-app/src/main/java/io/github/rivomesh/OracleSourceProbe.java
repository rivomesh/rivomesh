package io.github.rivomesh;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.rivomesh.api.SourcePosition;
import io.github.rivomesh.cdc.TableSchema;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Verifies the Oracle authority and Flashback snapshot anchor for a declared set of source tables. */
final class OracleSourceProbe {
    private static final String DRIVER="oracle.jdbc.OracleDriver";
    private static final Pattern IDENTIFIER=Pattern.compile("[A-Z][A-Z0-9_]{0,127}");
    private static final int QUERY_SECONDS=20;

    static final class Settings {
        public ConnectionSettings root,pdb;
        public String sourceId,pdbName;
        public List<String> tables;
        public OracleArchiveWindow archiveWindow;
    }

    static final class ConnectionSettings {
        public String url,username,password;

        Connection connect()throws Exception{
            String endpoint=Config.env(url);
            if(endpoint==null||!endpoint.startsWith("jdbc:oracle:thin:@//"))
                throw new IllegalArgumentException("ORACLE_THIN_SERVICE_URL_REQUIRED");
            String user=Config.env(username);
            if(user==null||user.trim().isEmpty())throw new IllegalArgumentException("ORACLE_USER_REQUIRED");
            return JdbcConnections.open(DRIVER,endpoint,user,Config.env(password));
        }
    }

    private static final class Authority {
        String databaseId,resetlogsScn,pdbGuid,currentScn,version;
    }

    private OracleSourceProbe(){}

    static Map<String,Object> inspect(Path input)throws Exception{
        Settings settings=new ObjectMapper(new YAMLFactory()).readValue(input.toFile(),Settings.class);
        if(settings==null||settings.root==null||settings.pdb==null||settings.sourceId==null
                ||!settings.sourceId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                ||settings.pdbName==null||!IDENTIFIER.matcher(settings.pdbName).matches()
                ||settings.tables==null||settings.tables.isEmpty())
            throw new IllegalArgumentException("ORACLE_PROBE_CONFIG_INVALID");

        Set<String> unique=new LinkedHashSet<>();
        for(String table:settings.tables){
            if(table==null||!table.matches(IDENTIFIER.pattern()+"\\."+IDENTIFIER.pattern())||!unique.add(table))
                throw new IllegalArgumentException("ORACLE_PROBE_TABLE_INVALID");
        }
        try(Connection root=settings.root.connect();Connection pdb=settings.pdb.connect()){
            Authority anchor=authority(root,settings.pdbName);
            verifyPdb(pdb,settings.pdbName,anchor);
            OracleArchiveWindow.Coverage archive=settings.archiveWindow==null?null:
                    settings.archiveWindow.inspect(root,anchor.resetlogsScn,anchor.currentScn);
            List<Map<String,Object>> tables=new ArrayList<>();
            for(String qualified:unique)tables.add(verifyFlashback(pdb,qualified,anchor.currentScn));
            if(archive!=null&&settings.archiveWindow.decodeCommitted){
                Map<String,TableSchema> selected=new LinkedHashMap<>();
                for(String qualified:unique)selected.put(qualified,redoSchema(pdb,settings.pdbName,qualified));
                archive.summary.put("decoded",OracleArchiveRead.inspect(root,settings.pdbName,archive,selected));
            }
            Authority current=authority(root,settings.pdbName);
            if(!anchor.databaseId.equals(current.databaseId)||!anchor.resetlogsScn.equals(current.resetlogsScn)
                    ||!anchor.pdbGuid.equals(current.pdbGuid))
                throw new IllegalStateException("ORACLE_SOURCE_AUTHORITY_CHANGED");

            Map<String,String> cursor=new LinkedHashMap<>();
            cursor.put("databaseId",anchor.databaseId);
            cursor.put("resetlogsScn",anchor.resetlogsScn);
            cursor.put("pdbGuid",anchor.pdbGuid);
            cursor.put("scn",anchor.currentScn);
            Map<String,Object> result=new LinkedHashMap<>();
            result.put("sourceVersion",anchor.version);
            result.put("pdb",settings.pdbName);
            result.put("snapshotAnchor",new SourcePosition(1,"ORACLE",settings.sourceId,cursor));
            result.put("tables",tables);
            if(archive!=null)result.put("archiveWindow",archive.summary);
            return result;
        }
    }

    private static Authority authority(Connection root,String name)throws Exception{
        Authority result=new Authority();
        try(Statement query=root.createStatement()){
            query.setQueryTimeout(QUERY_SECONDS);
            try(ResultSet row=query.executeQuery("SELECT SYS_CONTEXT('USERENV','CON_NAME'),d.dbid,"
                    +"d.resetlogs_change#,d.current_scn,i.version_full,d.log_mode,d.database_role,"
                    +"d.open_mode,d.force_logging,d.supplemental_log_data_min FROM v$database d CROSS JOIN v$instance i")){
                if(!row.next()||!"CDB$ROOT".equals(row.getString(1)))
                    throw new IllegalStateException("ORACLE_CDB_ROOT_REQUIRED");
                result.databaseId=unsigned(row.getString(2));
                result.resetlogsScn=unsigned(row.getString(3));
                result.currentScn=unsigned(row.getString(4));
                result.version=row.getString(5);
                if(!result.version.matches("23\\.26\\.[0-9]+\\.[0-9]+\\.[0-9]+"))
                    throw new IllegalStateException("ORACLE_SOURCE_VERSION_UNSUPPORTED");
                if(!"ARCHIVELOG".equals(row.getString(6))||!"PRIMARY".equals(row.getString(7))
                        ||!"READ WRITE".equals(row.getString(8)))
                    throw new IllegalStateException("ORACLE_ARCHIVELOG_PRIMARY_REQUIRED");
                if(!"YES".equals(row.getString(9))||
                        !java.util.Arrays.asList("YES","IMPLICIT").contains(row.getString(10)))
                    throw new IllegalStateException("ORACLE_SUPPLEMENTAL_FORCE_LOGGING_REQUIRED");
            }
        }
        try(PreparedStatement query=root.prepareStatement("SELECT RAWTOHEX(guid),open_mode FROM v$pdbs WHERE name=?")){
            query.setQueryTimeout(QUERY_SECONDS);
            query.setString(1,name);
            try(ResultSet row=query.executeQuery()){
                if(!row.next()||!"READ WRITE".equals(row.getString(2)))
                    throw new IllegalStateException("ORACLE_WRITABLE_PDB_REQUIRED");
                result.pdbGuid=guid(row.getString(1));
            }
        }
        return result;
    }

    private static void verifyPdb(Connection pdb,String name,Authority root)throws Exception{
        try(PreparedStatement query=pdb.prepareStatement("SELECT RAWTOHEX(guid),SYS_CONTEXT('USERENV','CON_NAME') "
                +"FROM v$pdbs WHERE name=?")){
            query.setQueryTimeout(QUERY_SECONDS);
            query.setString(1,name);
            try(ResultSet row=query.executeQuery()){
                if(!row.next()||!name.equals(row.getString(2))||!root.pdbGuid.equals(guid(row.getString(1))))
                    throw new IllegalStateException("ORACLE_PDB_AUTHORITY_MISMATCH");
            }
        }
    }

    private static Map<String,Object> verifyFlashback(Connection pdb,String qualified,String scn)throws Exception{
        String[] names=qualified.split("\\.",2);
        boolean primaryKey;
        try(PreparedStatement query=pdb.prepareStatement("SELECT (SELECT COUNT(*) FROM all_constraints c "
                +"WHERE c.owner=t.owner AND c.table_name=t.table_name AND c.constraint_type='P' AND c.status='ENABLED') "
                +"FROM all_tables t WHERE t.owner=? AND t.table_name=? AND t.temporary='N' "
                +"AND t.iot_type IS NULL AND t.nested='NO' AND NOT EXISTS "
                +"(SELECT 1 FROM all_external_tables e WHERE e.owner=t.owner AND e.table_name=t.table_name)")){
            query.setQueryTimeout(QUERY_SECONDS);
            query.setString(1,names[0]);query.setString(2,names[1]);
            try(ResultSet row=query.executeQuery()){
                if(!row.next())throw new IllegalStateException("ORACLE_FLASHBACK_TABLE_UNAVAILABLE");
                primaryKey=row.getInt(1)>0;
            }
        }
        boolean containsRows;
        String table=Sql.pg(names[0])+"."+Sql.pg(names[1]);
        try(PreparedStatement query=pdb.prepareStatement("SELECT 1 FROM "+table+" AS OF SCN ? WHERE ROWNUM=1")){
            query.setQueryTimeout(QUERY_SECONDS);
            query.setBigDecimal(1,new java.math.BigDecimal(scn));
            try(ResultSet rows=query.executeQuery()){containsRows=rows.next();}
        }
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("table",qualified);
        result.put("primaryKey",primaryKey);
        result.put("containsRows",containsRows);
        return result;
    }

    private static TableSchema redoSchema(Connection pdb,String pdbName,String qualified)throws Exception{
        String[] names=qualified.split("\\.",2);
        TableSchema schema=new TableSchema();
        schema.database=names[0];
        schema.name=names[1];
        schema.id=TableSchema.identity(pdbName,names[0],names[1]);
        schema.sourceEngine="ORACLE";
        try(PreparedStatement query=pdb.prepareStatement("SELECT column_name,data_type,data_precision,data_scale,char_length,nullable,"
                +"virtual_column FROM all_tab_cols WHERE owner=? AND table_name=? AND hidden_column='NO' ORDER BY column_id")){
            query.setQueryTimeout(QUERY_SECONDS);
            query.setString(1,names[0]);query.setString(2,names[1]);
            try(ResultSet rows=query.executeQuery()){
                while(rows.next()){
                    if(!"NO".equals(rows.getString(7)))throw new IllegalStateException("ORACLE_VIRTUAL_COLUMN_REQUIRES_PROFILE");
                    TableSchema.Column column=new TableSchema.Column();
                    column.name=rows.getString(1);
                    column.sourceType=rows.getString(2);
                    column.nullable="Y".equals(rows.getString(6));
                    switch(column.sourceType){
                        case "NUMBER":
                            column.precision=rows.getInt(3);
                            column.scale=rows.getInt(4);
                            if(column.precision<1||column.precision>38||column.scale<0||column.scale>column.precision)
                                throw new IllegalStateException("ORACLE_NUMBER_REQUIRES_PROFILE");
                            column.type=column.scale==0&&column.precision<=18?TableSchema.Type.INT64:TableSchema.Type.DECIMAL;
                            break;
                        case "VARCHAR2":
                            column.type=TableSchema.Type.STRING;
                            column.length=rows.getLong(5);
                            break;
                        default:throw new IllegalStateException("ORACLE_REDO_COLUMN_TYPE_UNSUPPORTED");
                    }
                    schema.columns.add(column);
                }
            }
        }
        schema.validate();
        return schema;
    }

    private static String unsigned(String value){
        if(value==null||!value.matches("0|[1-9][0-9]{0,19}"))
            throw new IllegalStateException("ORACLE_SOURCE_POSITION_INVALID");
        return value;
    }

    private static String guid(String value){
        if(value==null||!value.matches("[A-F0-9]{32}"))
            throw new IllegalStateException("ORACLE_PDB_GUID_INVALID");
        return value;
    }
}
