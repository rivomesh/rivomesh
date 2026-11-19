package io.github.rivomesh;

import io.github.rivomesh.cdc.Cell;
import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.TableSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OracleVirtualNumbersTest {
    private static TableSchema.Column number(String name,int precision){
        TableSchema.Column column=new TableSchema.Column();
        column.name=name;
        column.sourceType="NUMBER";
        column.type=TableSchema.Type.INT64;
        column.precision=precision;
        return column;
    }

    private static TableSchema table(boolean keyless){
        TableSchema schema=new TableSchema();
        schema.database="RIVO_APP";
        schema.name=keyless?"RIVO_VIRTUAL_KEYLESS":"RIVO_VIRTUAL_ROWS";
        schema.id=TableSchema.identity("FREEPDB1",schema.database,schema.name);
        schema.sourceEngine="ORACLE";
        if(!keyless){
            schema.columns.add(number("ID",12));
            schema.primaryKey.add("ID");
        }
        schema.columns.add(number("AMOUNT",12));
        TableSchema.Column derived=number("DERIVED",16);
        derived.generationKind="VIRTUAL";
        derived.generationExpression="\"AMOUNT\"*2";
        schema.columns.add(derived);
        return schema;
    }

    @Test void verifiesNativeFlashbackValueIndependently(){
        OracleVirtualNumbers.Plan plan=OracleVirtualNumbers.plan(table(false));
        plan.verifySnapshot(List.of(Cell.value("1"),Cell.value("-7"),Cell.value("-14")));
        plan.verifySnapshot(List.of(Cell.value("2"),Cell.value(null),Cell.value(null)));
        assertEquals("ORACLE_VIRTUAL_SNAPSHOT_MISMATCH",assertThrows(IllegalStateException.class,()->
                plan.verifySnapshot(List.of(Cell.value("1"),Cell.value("7"),Cell.value("15")))).getMessage());
    }

    private static TableSchema sums(){
        TableSchema schema=table(false);
        schema.columns.add(2,number("ADJUSTMENT",12));
        schema.columns.get(3).generationExpression="\"AMOUNT\" + \"ADJUSTMENT\"";
        TableSchema.Column difference=number("DIFFERENCE",16);
        difference.generationKind="VIRTUAL";
        difference.generationExpression="\"AMOUNT\"-\"ADJUSTMENT\"";
        schema.columns.add(difference);
        return schema;
    }

    @Test void computesSumsAndDifferencesFromEachFullPhysicalImage()throws Exception{
        TableSchema schema=sums();
        String target="\"RIVO_APP\".\"RIVO_VIRTUAL_ROWS\"";
        String redo="update "+target+" set \"AMOUNT\"='-8',\"ADJUSTMENT\"='4'"
                +" where \"ID\"='1' and \"AMOUNT\"='7' and \"ADJUSTMENT\"='-2'";
        String undo="update "+target+" set \"AMOUNT\"='7',\"ADJUSTMENT\"='-2'"
                +" where \"ID\"='1' and \"AMOUNT\"='-8' and \"ADJUSTMENT\"='4'";
        Change change=OracleRedoDml.decode(schema,"UPDATE",redo,undo);
        assertEquals(List.of("1","7","-2","5","9"),change.before.stream().map(cell->cell.value).toList());
        assertEquals(List.of("1","-8","4","-4","-12"),change.after.stream().map(cell->cell.value).toList());
        var plan=OracleVirtualNumbers.plan(schema);
        plan.verifySnapshot(List.of(Cell.value("1"),Cell.value(null),Cell.value("2"),Cell.value(null),Cell.value(null)));
        plan.verifySnapshot(List.of(Cell.value("1"),Cell.value("2"),Cell.value(null),Cell.value(null),Cell.value(null)));
        plan.verifySnapshot(List.of(Cell.value("1"),Cell.value("999999999999"),Cell.value("999999999999"),
                Cell.value("1999999999998"),Cell.value("0")));
    }

    @Test void requiresBothOperandsAndPrecisionForBothInputDomains()throws Exception{
        TableSchema schema=sums();
        String target="\"RIVO_APP\".\"RIVO_VIRTUAL_ROWS\"";
        String redo="insert into "+target+" (\"ID\",\"AMOUNT\") values ('1','2')";
        String undo="delete from "+target+" where \"ID\"='1' and \"AMOUNT\"='2'";
        assertEquals("ORACLE_FULL_ROW_IMAGE_REQUIRED",assertThrows(IllegalStateException.class,()->
                OracleRedoDml.decode(schema,"INSERT",redo,undo)).getMessage());
        schema.columns.get(4).precision=12;
        assertEquals("ORACLE_VIRTUAL_PRECISION_UNSUPPORTED",assertThrows(IllegalStateException.class,()->
                OracleVirtualNumbers.plan(schema)).getMessage());
        schema.columns.get(4).precision=16;
        schema.columns.get(2).generationKind="STORED";
        assertEquals("ORACLE_VIRTUAL_OPERAND_UNSUPPORTED",assertThrows(IllegalStateException.class,()->
                OracleVirtualNumbers.plan(schema)).getMessage());
    }

    @Test void acceptsNullAndRejectsTruncatedPhysicalImages()throws Exception{
        TableSchema schema=table(false);
        String target="\"RIVO_APP\".\"RIVO_VIRTUAL_ROWS\"";
        String redo="insert into "+target+" (\"ID\",\"AMOUNT\") values ('2',NULL)";
        String undo="delete from "+target+" where \"ID\"='2' and \"AMOUNT\" IS NULL";
        Change event=OracleRedoDml.decode(schema,"INSERT",redo,undo);
        assertTrue(event.after.get(2).present);
        assertNull(event.after.get(2).value);
        assertEquals("ORACLE_FULL_ROW_IMAGE_REQUIRED",assertThrows(IllegalStateException.class,()->
                OracleRedoDml.decode(schema,"INSERT",redo.replace(",\"AMOUNT\"","").replace(",NULL",""),
                        undo.replace(" and \"AMOUNT\" IS NULL",""))).getMessage());
    }

    @Test void validatesPrecisionBeforeEvaluationAndRefusesInvalidOperands(){
        TableSchema schema=table(false);
        schema.columns.get(2).precision=13;
        OracleVirtualNumbers.Plan plan=OracleVirtualNumbers.plan(schema);
        plan.verifySnapshot(List.of(Cell.value("1"),Cell.value("999999999999"),Cell.value("1999999999998")));
        plan.verifySnapshot(List.of(Cell.value("1"),Cell.value("-999999999999"),Cell.value("-1999999999998")));
        assertEquals("ORACLE_VIRTUAL_VALUE_OUT_OF_RANGE",assertThrows(IllegalStateException.class,()->
                plan.verifySnapshot(List.of(Cell.value("1"),Cell.value("9223372036854775807"),Cell.value("0")))).getMessage());
        assertEquals("ORACLE_VIRTUAL_VALUE_OUT_OF_RANGE",assertThrows(IllegalStateException.class,()->
                plan.verifySnapshot(List.of(Cell.value("1"),Cell.value("1000000000000"),Cell.value("0")))).getMessage());
        schema.columns.get(2).precision=12;
        assertEquals("ORACLE_VIRTUAL_PRECISION_UNSUPPORTED",assertThrows(IllegalStateException.class,()->
                OracleVirtualNumbers.plan(schema)).getMessage());
        schema.columns.get(1).precision=18;
        schema.columns.get(2).precision=18;
        assertEquals("ORACLE_VIRTUAL_PRECISION_UNSUPPORTED",assertThrows(IllegalStateException.class,()->
                OracleVirtualNumbers.plan(schema)).getMessage());
    }

    @Test void neverTreatsUnexpectedVirtualLiteralAsAStoredColumn()throws Exception{
        TableSchema schema=table(false);
        String target="\"RIVO_APP\".\"RIVO_VIRTUAL_ROWS\"";
        String redo="insert into "+target+" (\"ID\",\"AMOUNT\",\"DERIVED\") values ('2','3','6')";
        String undo="delete from "+target+" where \"ID\"='2' and \"AMOUNT\"='3'";
        assertEquals("ORACLE_FULL_ROW_IMAGE_REQUIRED",assertThrows(IllegalStateException.class,()->
                OracleRedoDml.decode(schema,"INSERT",redo,undo)).getMessage());
    }

    @Test void refusesFunctionsQualifiedAndAmbiguousIdentifiers(){
        TableSchema schema=table(false);
        TableSchema.Column derived=schema.columns.get(2);
        for(String expression:List.of("ABS(\"AMOUNT\")","\"RIVO_VIRTUAL_ROWS\".\"AMOUNT\"*2",
                "\"Amount\"*2","\"AMOUNT\"*2+1","2*\"AMOUNT\"","\"AMOUNT\"*02")){
            derived.generationExpression=expression;
            assertEquals("ORACLE_VIRTUAL_EXPRESSION_UNSUPPORTED",assertThrows(IllegalStateException.class,()->
                    OracleVirtualNumbers.plan(schema)).getMessage(),expression);
        }
        derived.generationExpression="\"DERIVED\"*2";
        assertEquals("ORACLE_VIRTUAL_OPERAND_UNSUPPORTED",assertThrows(IllegalStateException.class,()->
                OracleVirtualNumbers.plan(schema)).getMessage());
        derived.generationExpression="\"AMOUNT\"*2";
        schema.primaryKey.add("DERIVED");
        assertEquals("ORACLE_VIRTUAL_PRIMARY_KEY_UNSUPPORTED",assertThrows(IllegalStateException.class,()->
                OracleVirtualNumbers.plan(schema)).getMessage());
    }

}
