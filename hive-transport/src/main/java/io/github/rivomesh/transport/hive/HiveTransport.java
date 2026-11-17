package io.github.rivomesh.transport.hive;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/** Hive's JDBC dependency graph is private; only JDK types cross the boundary. */
public final class HiveTransport implements AutoCloseable {
    private static final String RESOURCE="/rivomesh-sdk/hive-jdbc-4.0.1-standalone.jar";
    private static final String DIGEST="5d74bb0130aae6a8a2fad9cf5ffa1608974a55f1013464334490f7e06311959e";
    private static volatile ClassLoader loader;
    private final Connection connection;
    private final int queryTimeout,maxBytes;
    private interface Work<T>{T run()throws Exception;}
    private static String digest(Path path)throws Exception{
        MessageDigest hash=MessageDigest.getInstance("SHA-256");try(InputStream input=Files.newInputStream(path)){byte[] bytes=new byte[65536];int count;while((count=input.read(bytes))!=-1)hash.update(bytes,0,count);}
        StringBuilder hex=new StringBuilder();for(byte b:hash.digest())hex.append(String.format(Locale.ROOT,"%02x",b&255));return hex.toString();
    }
    private static synchronized ClassLoader sdk()throws Exception{
        if(loader!=null)return loader;
        String user=System.getProperty("user.name");if(user==null||user.isEmpty())throw new IOException("HIVE_SDK_PRIVATE_CACHE_REQUIRED");
        MessageDigest nameHash=MessageDigest.getInstance("SHA-256");String userKey=Base64.getUrlEncoder().withoutPadding().encodeToString(nameHash.digest(user.getBytes(java.nio.charset.StandardCharsets.UTF_8))).substring(0,16);
        Path directory=Paths.get(System.getProperty("java.io.tmpdir")).resolve("rivomesh-hive-sdk-"+userKey);
        try{Files.createDirectory(directory,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}catch(FileAlreadyExistsException existing){}
        if(Files.isSymbolicLink(directory)||!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS)||!Files.getOwner(directory,LinkOption.NOFOLLOW_LINKS).equals(directory.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByName(user))||!Files.getPosixFilePermissions(directory,LinkOption.NOFOLLOW_LINKS).equals(PosixFilePermissions.fromString("rwx------")))throw new IOException("HIVE_SDK_PRIVATE_CACHE_REQUIRED");
        Path file=directory.resolve(DIGEST+".jar");
        if(!Files.exists(file,LinkOption.NOFOLLOW_LINKS)){
            Path temporary=directory.resolve(".partial-"+UUID.randomUUID().toString().replace("-",""));
            try{try(InputStream input=HiveTransport.class.getResourceAsStream(RESOURCE)){
                if(input==null)throw new IOException("HIVE_SDK_MISSING");
                try(OutputStream output=java.nio.channels.Channels.newOutputStream(Files.newByteChannel(temporary,EnumSet.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))))){byte[] bytes=new byte[65536];int count;while((count=input.read(bytes))!=-1)output.write(bytes,0,count);}
            }
            if(!DIGEST.equals(digest(temporary)))throw new IOException("HIVE_SDK_INTEGRITY_ERROR");Files.move(temporary,file,StandardCopyOption.ATOMIC_MOVE);
            }finally{Files.deleteIfExists(temporary);}
        }
        if(Files.isSymbolicLink(file)||!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)||!DIGEST.equals(digest(file)))throw new IOException("HIVE_SDK_INTEGRITY_ERROR");
        try(DirectoryStream<Path> leftovers=Files.newDirectoryStream(directory,".partial-*")){for(Path partial:leftovers)if(partial.getFileName().toString().matches("\\.partial-[a-f0-9]{32}")&&Files.isRegularFile(partial,LinkOption.NOFOLLOW_LINKS)&&System.currentTimeMillis()-Files.getLastModifiedTime(partial,LinkOption.NOFOLLOW_LINKS).toMillis()>86400000L)Files.deleteIfExists(partial);}
        loader=new URLClassLoader(new URL[]{file.toUri().toURL()},HiveTransport.class.getClassLoader().getParent());return loader;
    }
    private static <T>T context(ClassLoader sdk,Work<T> work)throws Exception{Thread thread=Thread.currentThread();ClassLoader old=thread.getContextClassLoader();thread.setContextClassLoader(sdk);try{return work.run();}catch(LinkageError failure){throw new IllegalStateException("HIVE_SDK_UNAVAILABLE",failure);}finally{thread.setContextClassLoader(old);}}
    public HiveTransport(String url,String username,String password,int timeout,int maximumResponseBytes)throws Exception{
        if(timeout<1||timeout>1800||maximumResponseBytes<1024||maximumResponseBytes>64*1024*1024)throw new IllegalArgumentException("HIVE_LIMIT_INVALID");queryTimeout=timeout;maxBytes=maximumResponseBytes;
        ClassLoader sdk=sdk();connection=context(sdk,()->{Driver driver=(Driver)Class.forName("org.apache.hive.jdbc.HiveDriver",true,sdk).getDeclaredConstructor().newInstance();Properties properties=new Properties();properties.setProperty("user",username);properties.setProperty("password",password==null?"":password);Connection c=driver.connect(url,properties);if(c==null)throw new SQLException("HIVE_URL_REJECTED","08001");return c;});
    }
    public List<Map<String,String>> query(String statement)throws Exception{return context(sdk(),()->{
        try(Statement sql=connection.createStatement()){sql.setQueryTimeout(queryTimeout);sql.setMaxRows(10001);sql.setFetchSize(1);
            try(ResultSet rows=sql.executeQuery(statement)){ResultSetMetaData metadata=rows.getMetaData();List<Map<String,String>> result=new ArrayList<>();long size=0;
                while(rows.next()){if(result.size()==10000)throw new IllegalStateException("HIVE_CATALOG_LIMIT");Map<String,String> row=new TreeMap<>(String.CASE_INSENSITIVE_ORDER);for(int i=1;i<=metadata.getColumnCount();i++){String value=rows.getString(i);size+=value==null?0:value.length()*2L;if(size>maxBytes)throw new IllegalStateException("HIVE_RESPONSE_LIMIT");String name=metadata.getColumnLabel(i);row.put(name,value);int dot=name.lastIndexOf('.');if(dot>=0)row.put(name.substring(dot+1),value);}result.add(row);}return result;
            }
        }
    });}
    public void execute(String statement)throws Exception{context(sdk(),()->{try(Statement sql=connection.createStatement()){sql.setQueryTimeout(queryTimeout);sql.execute(statement);return null;}});}
    @Override public void close()throws Exception{context(sdk(),()->{connection.close();return null;});}
}
