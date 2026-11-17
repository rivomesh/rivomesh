package io.github.rivomesh.cdc;

import java.util.*;

public final class Change {
    public enum Operation { INSERT,UPDATE,DELETE,SNAPSHOT,DDL,BARRIER }
    public String eventId,tableId,rowId,sql;
    public int schemaVersion=1;
    public Operation operation;
    public List<Cell> before,after;
    public Map<String,String> attributes=new TreeMap<>();
    public void validate(){
        if(eventId==null||eventId.isEmpty()||operation==null||schemaVersion<1)throw new IllegalArgumentException("CHANGE_INVALID");
        if(operation!=Operation.BARRIER&&(tableId==null||tableId.isEmpty()))throw new IllegalArgumentException("CHANGE_TABLE_REQUIRED");
        if(Arrays.asList(Operation.INSERT,Operation.SNAPSHOT,Operation.UPDATE).contains(operation)&&after==null)throw new IllegalArgumentException("AFTER_IMAGE_REQUIRED");
        if(Arrays.asList(Operation.DELETE,Operation.UPDATE).contains(operation)&&before==null)throw new IllegalArgumentException("BEFORE_IMAGE_REQUIRED");
        for(List<Cell> row:Arrays.asList(before,after))if(row!=null)for(Cell cell:row){if(cell==null)throw new IllegalArgumentException("CELL_REQUIRED");cell.validate();}
    }
}
