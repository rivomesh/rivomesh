package io.github.rivomesh.cdc;

import java.util.*;

public final class TableSchema {
    public enum Type { BOOLEAN,INT64,UINT64,DECIMAL,FLOAT32,FLOAT64,STRING,BYTES,DATE,TIME,DATETIME,TIMESTAMP,JSON,GEOMETRY }
    public static final class Column {
        public String name,sourceType,charset,collation,generationKind,generationExpression,defaultExpression;
        public Type type;
        public boolean nullable=true;
        public long length;
        public int precision,scale;
    }
    public static final class Index {
        public String name,method;
        public boolean unique,primary;
        public List<String> columns=new ArrayList<>();
        public List<Integer> prefixLengths=new ArrayList<>();
        public List<Boolean> descending=new ArrayList<>();
        public String expression;
    }
    public String id,database,schema="",name,sourceEngine;
    public int version=1;
    public List<Column> columns=new ArrayList<>();
    public List<String> primaryKey=new ArrayList<>(),identityKey=new ArrayList<>();
    public List<Index> indexes=new ArrayList<>();
    public Map<String,String> properties=new TreeMap<>();
    public void validate(){
        if(id==null||id.length()>64||database==null||name==null||name.isEmpty()||version<1||columns==null||columns.isEmpty())throw new IllegalArgumentException("SCHEMA_INVALID");
        Set<String> names=new HashSet<>();
        for(Column c:columns)if(c.name==null||c.name.isEmpty()||c.type==null||!names.add(c.name))throw new IllegalArgumentException("COLUMN_INVALID");
        if(!names.containsAll(primaryKey)||!names.containsAll(identityKey))throw new IllegalArgumentException("IDENTITY_COLUMN_MISSING");
    }
    public int column(String name){for(int i=0;i<columns.size();i++)if(columns.get(i).name.equals(name))return i;throw new IllegalArgumentException("COLUMN_NOT_FOUND");}
    public boolean keyless(){return primaryKey.isEmpty()&&identityKey.isEmpty();}
    public String qualifiedName(){return database+"."+(schema==null||schema.isEmpty()?"":schema+".")+name;}
    public static String identity(String database,String schema,String table){return Codec.sha256(database+"\u0000"+(schema==null?"":schema)+"\u0000"+table);}
}
