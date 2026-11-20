package io.github.rivomesh;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.Properties;

/** Select the requested driver explicitly when several PostgreSQL-protocol drivers coexist. */
public final class JdbcConnections {
    private JdbcConnections(){}

    public static final class DriverFailure extends SQLException {
        private DriverFailure(String code,Throwable cause){super(code,"0A000",cause);}
    }

    public static Connection open(String driver,String url,String username,String password)throws SQLException{
        try{
            Driver instance=Class.forName(driver).asSubclass(Driver.class).getDeclaredConstructor().newInstance();
            Properties properties=new Properties();if(username!=null)properties.setProperty("user",username);if(password!=null)properties.setProperty("password",password);
            Connection c;
            long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(8);
            while(true)try{c=instance.connect(url,properties);break;}
            catch(SQLException e){
                if(!localAutomaticServer(driver,url)||!automaticServerTransition(e)||System.nanoTime()-deadline>=0)throw e;
                try{Thread.sleep(150);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new SQLTransientConnectionException("JDBC_CONNECT_INTERRUPTED","08001",interrupted);}
            }
            if(c==null)throw new SQLException("DRIVER_URL_MISMATCH","08001");
            if(driver.equals("org.postgresql.Driver")||driver.equals("io.github.rivomesh.gaussdb.jdbc.Driver"))try{postgresSession(c);}catch(SQLException e){c.close();throw e;}return c;
        }catch(SQLException e){
            throw e;
        }catch(LinkageError error){
            throw incompatibleDriver(error);
        }catch(InvocationTargetException failure){
            if(failure.getCause() instanceof LinkageError error)throw incompatibleDriver(error);
            throw new DriverFailure("JDBC_DRIVER_UNAVAILABLE",failure);
        }catch(Exception e){
            throw new DriverFailure("JDBC_DRIVER_UNAVAILABLE",e);
        }
    }

    private static DriverFailure incompatibleDriver(LinkageError failure){
        return new DriverFailure(failure instanceof UnsupportedClassVersionError
                ?"JDBC_DRIVER_JAVA_VERSION_UNSUPPORTED":"JDBC_DRIVER_LINKAGE_INVALID",failure);
    }
    public static boolean localAutomaticServer(String driver,String url){return "org.h2.Driver".equals(driver)&&url!=null&&url.contains(";AUTO_SERVER=TRUE");}
    public static boolean automaticServerTransition(SQLException error){return error.getErrorCode()==90020||error.getErrorCode()==8000||error.getErrorCode()==90067;}
    private static void postgresSession(Connection c)throws SQLException{
        try(Statement st=c.createStatement()){
            st.execute("SET TIME ZONE 'UTC'");boolean emptyIsNull;
            try(ResultSet rs=st.executeQuery("SELECT CAST('' AS varchar) IS NULL")){rs.next();emptyIsNull=rs.getBoolean(1);}
            if(emptyIsNull){
                String options;try(ResultSet rs=st.executeQuery("SHOW behavior_compat_options")){rs.next();options=rs.getString(1);}
                try(PreparedStatement p=c.prepareStatement("SELECT set_config('behavior_compat_options',?,false)")){p.setString(1,(options==null||options.isEmpty()?"":options+",")+"accept_empty_str");try(ResultSet ignored=p.executeQuery()){/* preserve other compatibility options */}}
                try(ResultSet rs=st.executeQuery("SELECT CAST('' AS varchar) IS NULL")){rs.next();if(rs.getBoolean(1))throw new SQLException("TARGET_EMPTY_STRING_SEMANTICS_UNSUPPORTED","0A000");}
            }
        }
    }
}
