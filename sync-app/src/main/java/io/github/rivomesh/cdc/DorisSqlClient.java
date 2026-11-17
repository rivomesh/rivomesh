package io.github.rivomesh.cdc;

import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.SyncFailure;
import java.sql.*;
import java.util.*;

/** Bounded metadata/DDL boundary; Doris is not treated as a transactional MySQL sink. */
public final class DorisSqlClient implements AutoCloseable {
    private final String url,username,password;
    private final int timeout;
    private Connection connection;
    public DorisSqlClient(String url,String username,String password,int timeoutMillis){
        if(url==null||!url.startsWith("jdbc:mysql://")||username==null||username.isEmpty()||timeoutMillis<100||timeoutMillis>120000)throw new IllegalArgumentException("DORIS_SQL_CONFIG_INVALID");this.url=boundedUrl(url,timeoutMillis);this.username=username;this.password=password;timeout=Math.max(1,(timeoutMillis+999)/1000);
    }
    private static String boundedUrl(String value,int millis){
        try{java.net.URI uri=java.net.URI.create(value.substring(5));if(!"mysql".equals(uri.getScheme())||uri.getHost()==null||uri.getUserInfo()!=null||uri.getFragment()!=null)throw new IllegalArgumentException();List<String> options=new ArrayList<>();if(uri.getRawQuery()!=null)for(String option:uri.getRawQuery().split("&")){String name=java.net.URLDecoder.decode(option.split("=",2)[0],"UTF-8");if(!name.equalsIgnoreCase("connectTimeout")&&!name.equalsIgnoreCase("socketTimeout")&&!name.equalsIgnoreCase("enableQueryTimeouts")&&!name.equalsIgnoreCase("useServerPrepStmts")&&!name.equalsIgnoreCase("emulateUnsupportedPstmts"))options.add(option);}options.add("connectTimeout="+Math.min(5000,millis));options.add("socketTimeout="+millis);options.add("enableQueryTimeouts=false");options.add("useServerPrepStmts=true");options.add("emulateUnsupportedPstmts=false");return (value.indexOf('?')<0?value:value.substring(0,value.indexOf('?')))+"?"+String.join("&",options);}catch(Exception invalid){throw new IllegalArgumentException("DORIS_SQL_URL_INVALID");}
    }
    private Connection connection()throws Exception{if(connection==null||connection.isClosed()){connection=JdbcConnections.open("com.mysql.cj.jdbc.Driver",url,username,password);try(Statement sql=connection.createStatement()){sql.setQueryTimeout(timeout);sql.execute("SET enable_decimal256=true");}}return connection;}
    static SyncFailure classified(SQLException error){String state=error.getSQLState(),message=error.getMessage();boolean unavailable="HY000".equals(state)&&error.getErrorCode()==1105&&message!=null&&message.length()<=8192&&message.trim().startsWith("errCode = 2, detailMessage = No backend available as scan node");boolean retry=unavailable||state!=null&&(state.startsWith("08")||state.startsWith("40"))||error.getErrorCode()==1205||error.getErrorCode()==1213;return new SyncFailure(retry?"DORIS_SQL_CONNECTION_RETRY":"DORIS_SQL_REJECTED","DORIS_SQL",retry,"DORIS_SQL",error,null,0,null);}
    public List<Map<String,String>> query(String statement,String... parameters)throws Exception{
        try(Statement sql=parameters.length==0?connection().createStatement():connection().prepareStatement(statement)){
            sql.setQueryTimeout(timeout);sql.setMaxRows(10001);
            if(parameters.length>0)for(int i=0;i<parameters.length;i++)((PreparedStatement)sql).setString(i+1,parameters[i]);
            try(ResultSet rows=parameters.length==0?sql.executeQuery(statement):((PreparedStatement)sql).executeQuery()){
                ResultSetMetaData meta=rows.getMetaData();List<Map<String,String>> result=new ArrayList<>();long bytes=0;
                while(rows.next()){if(result.size()==10000)throw new IllegalStateException("DORIS_CATALOG_LIMIT");Map<String,String> row=new TreeMap<>(String.CASE_INSENSITIVE_ORDER);for(int column=1;column<=meta.getColumnCount();column++){String value=rows.getString(column);bytes+=value==null?0:value.length()*2L;if(bytes>16*1024*1024)throw new IllegalStateException("DORIS_CATALOG_LIMIT");row.put(meta.getColumnLabel(column),value);}result.add(row);}return result;
            }
        }catch(SQLException failure){close();throw classified(failure);}
    }
    public void execute(String statement)throws Exception{try(Statement sql=connection().createStatement()){sql.setQueryTimeout(timeout);sql.execute(statement);}catch(SQLException failure){close();throw classified(failure);}}
    @Override public void close(){if(connection!=null)try{connection.close();}catch(SQLException ignored){}finally{connection=null;}}
}
