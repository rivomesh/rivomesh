package io.github.rivomesh.cdc;

import java.util.*;

/** Share the existing normalization contract; validation must not invent a second equality rule. */
public final class RowValues {
    private RowValues(){}
    public static String canonical(TableSchema table,List<Cell> row){
        if(row==null||row.size()!=table.columns.size())throw new IllegalArgumentException("ROW_WIDTH_INVALID");
        for(int i=0;i<row.size();i++){
            Cell cell=row.get(i);if(cell==null||!cell.present)throw new IllegalArgumentException("ROW_IMAGE_INCOMPLETE");if(cell.value==null)continue;
            TableSchema.Column col=table.columns.get(i);
            if(col.type==TableSchema.Type.INT64||col.type==TableSchema.Type.UINT64||col.type==TableSchema.Type.DECIMAL){
                int precision=col.type==TableSchema.Type.INT64?19:col.type==TableSchema.Type.UINT64?20:col.precision,scale=col.type==TableSchema.Type.DECIMAL?col.scale:0;
                if(precision<1||precision>1000||scale<0||scale>precision||cell.value.length()>2L*precision+64)throw new IllegalArgumentException("VALIDATION_NUMERIC_RANGE");
                java.math.BigDecimal n=new java.math.BigDecimal(cell.value).stripTrailingZeros();
                if(n.signum()!=0&&Math.max(0L,(long)n.precision()-n.scale())>precision-scale||Math.max(0,n.scale())>scale)throw new IllegalArgumentException("VALIDATION_NUMERIC_RANGE");
                if(col.type==TableSchema.Type.INT64)try{n.longValueExact();}catch(ArithmeticException range){throw new IllegalArgumentException("VALIDATION_NUMERIC_RANGE");}
                if(col.type==TableSchema.Type.UINT64&&(n.signum()<0||n.compareTo(new java.math.BigDecimal("18446744073709551615"))>0))throw new IllegalArgumentException("VALIDATION_NUMERIC_RANGE");
            }
        }
        return JdbcNormalization.canonical(table,row);
    }
    public static List<Cell> decode(String value){return new ArrayList<>(Arrays.asList(Codec.decode(value,Cell[].class)));}
    public static String identity(TableSchema table,List<Cell> row,String occurrence){
        if(table.keyless()){
            if(occurrence==null||!occurrence.matches("[0-9a-f]{64}"))throw new IllegalArgumentException("ROW_IDENTITY_REQUIRED");
            return Codec.encode(Collections.singletonList(Cell.value(occurrence)));
        }
        List<Cell> normalized=decode(canonical(table,row)),key=new ArrayList<>();
        for(String name:table.primaryKey.isEmpty()?table.identityKey:table.primaryKey){Cell cell=normalized.get(table.column(name));if(cell.value==null)throw new IllegalArgumentException("VALIDATION_NULL_IDENTITY");key.add(cell);}
        return Codec.encode(key);
    }
}
