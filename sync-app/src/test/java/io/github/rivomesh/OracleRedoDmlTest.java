package io.github.rivomesh;

import io.github.rivomesh.cdc.Change;
import io.github.rivomesh.cdc.RowValues;
import io.github.rivomesh.cdc.TableSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OracleRedoDmlTest {
    private static final String ORDERS="\"RIVO_APP\".\"RIVO_ORDERS\"";

    private static TableSchema table(){
        TableSchema schema=new TableSchema();
        schema.database="RIVO_APP";
        schema.name="RIVO_ORDERS";
        schema.id=TableSchema.identity("FREEPDB1",schema.database,schema.name);
        TableSchema.Column id=new TableSchema.Column();
        id.name="ID";id.type=TableSchema.Type.INT64;id.nullable=false;
        TableSchema.Column note=new TableSchema.Column();
        note.name="NOTE";note.type=TableSchema.Type.STRING;note.nullable=true;
        schema.columns.add(id);
        schema.columns.add(note);
        return schema;
    }

    @Test void decodesNativeBeforeAndAfterImages()throws Exception{
        String redo="update "+ORDERS+" set \"NOTE\" = 'after' where \"ID\" = '1' and \"NOTE\" = 'before' and ROWID = 'AAAR7mAAAAAAAMOAAA';";
        String undo="update "+ORDERS+" set \"NOTE\" = 'before' where \"ID\" = '1' and \"NOTE\" = 'after' and ROWID = 'AAAR7mAAAAAAAMOAAA';";
        Change event=OracleRedoDml.decode(table(),"UPDATE",redo,undo);
        assertEquals("1",event.before.get(0).value);
        assertEquals("before",event.before.get(1).value);
        assertEquals("1",event.after.get(0).value);
        assertEquals("after",event.after.get(1).value);
    }

    @Test void decodesQuotedStringsAndNullPredicates()throws Exception{
        String inserted="insert into "+ORDERS+"(\"ID\",\"NOTE\") values ('2','O''Brien');";
        String deleted="delete from "+ORDERS+" where \"ID\" = '2' and \"NOTE\" = 'O''Brien' and ROWID = 'AAAR7mAAAAAAAMOAAA';";
        assertEquals("O'Brien",OracleRedoDml.decode(table(),"INSERT",inserted,deleted).after.get(1).value);
        assertEquals("O'Brien",OracleRedoDml.decode(table(),"DELETE",deleted,inserted).before.get(1).value);
        Change empty=OracleRedoDml.decode(table(),"INSERT",
                "insert into "+ORDERS+"(\"ID\",\"NOTE\") values ('3',NULL);",
                "delete from "+ORDERS+" where \"ID\" = '3' and \"NOTE\" IS NULL;");
        assertTrue(empty.after.get(1).present);
        assertNull(empty.after.get(1).value);
    }

    @Test void rejectsImageGapsConflictingUndoAndTrailingStatements(){
        String redo="update "+ORDERS+" set \"NOTE\" = 'after' where \"NOTE\" = 'before';";
        String undo="update "+ORDERS+" set \"NOTE\" = 'before' where \"NOTE\" = 'after';";
        assertEquals("ORACLE_FULL_ROW_IMAGE_REQUIRED",
                assertThrows(IllegalStateException.class,()->OracleRedoDml.decode(table(),"UPDATE",redo,undo)).getMessage());
        String incorrect=undo.replace("'before'","'different'");
        assertEquals("ORACLE_REDO_UNDO_MISMATCH",
                assertThrows(IllegalStateException.class,()->OracleRedoDml.decode(table(),"UPDATE",redo,incorrect)).getMessage());
        assertEquals("ORACLE_REDO_TRAILING_SQL",
                assertThrows(IllegalStateException.class,()->OracleRedoDml.decode(table(),"UPDATE",redo+"DELETE FROM OTHER",undo)).getMessage());
    }

    @Test void recognizesOnlySingleStrictUniqueConstraint(){
        assertEquals("PAYLOAD",OracleTextJson.constraintColumn("payload IS JSON ( STRICT WITH UNIQUE KEYS )"));
        assertEquals("payload",OracleTextJson.constraintColumn("\"payload\" is json strict with unique keys"));
        assertNull(OracleTextJson.constraintColumn("payload IS JSON"));
        assertNull(OracleTextJson.constraintColumn("payload IS JSON (STRICT) WITH UNIQUE KEYS"));
        assertNull(OracleTextJson.constraintColumn("payload IS JSON (STRICT WITHOUT UNIQUE KEYS)"));
        assertNull(OracleTextJson.constraintColumn("payload IS JSON (STRICT WITH UNIQUE KEYS) OR other='allow'"));
        assertNull(OracleTextJson.constraintColumn("trim(payload) IS JSON (STRICT WITH UNIQUE KEYS)"));
    }

    @Test void decodesNationalSurrogatePairsAndKeepsTheirOriginalPadding()throws Exception{
        TableSchema table=table();
        table.columns.get(1).sourceType="NCHAR";
        String before="UNISTR('e\\0301\\D83D\\DE00\\005C'' ')";
        String after="UNISTR('\\53F0\\\\\\D834\\DD1E  ')";
        String redo="update "+ORDERS+" set \"NOTE\" = "+after+" where \"ID\" = '1' and \"NOTE\" = "+before+";";
        String undo="update "+ORDERS+" set \"NOTE\" = "+before+" where \"ID\" = '1' and \"NOTE\" = "+after+";";
        Change event=OracleRedoDml.decode(table,"UPDATE",redo,undo);
        assertEquals("e\u0301😀\\' ",event.before.get(1).value);
        assertEquals("台\\𝄞  ",event.after.get(1).value);
        assertEquals("ORACLE_REDO_LITERAL_TYPE_MISMATCH",assertThrows(IllegalStateException.class,
                ()->OracleRedoDml.decode(table(),"UPDATE",redo.replace("'1'",before),undo.replace("'1'",before))).getMessage());
        table.columns.get(1).sourceType="VARCHAR2";
        assertEquals("ORACLE_REDO_LITERAL_TYPE_MISMATCH",assertThrows(IllegalStateException.class,
                ()->OracleRedoDml.decode(table(),"UPDATE",redo,undo)).getMessage());
    }

    @Test void refusesMalformedNationalEscapesAndExpressions(){
        TableSchema table=table();
        table.columns.get(1).sourceType="NVARCHAR2";
        String inserted="insert into "+ORDERS+"(\"ID\",\"NOTE\") values ('2',%s);";
        String deleted="delete from "+ORDERS+" where \"ID\" = '2' and \"NOTE\" = %s;";
        for(String malformed:new String[]{"", "\\012", "\\00FG", "\\D83D", "\\DE00", "\\0000", "\\D83D\\0020"}){
            String literal="UNISTR('"+malformed+"')";
            assertEquals("ORACLE_REDO_UNISTR_UNSUPPORTED",assertThrows(IllegalStateException.class,
                    ()->OracleRedoDml.decode(table,"INSERT",inserted.formatted(literal),deleted.formatted(literal))).getMessage());
        }
        for(String expression:new String[]{"TO_NCHAR('A')", "UNISTR('A' || 'B')", "N'A'"}){
            assertThrows(IllegalStateException.class,
                    ()->OracleRedoDml.decode(table,"INSERT",inserted.formatted(expression),deleted.formatted(expression)));
        }
    }

    @Test void admitsOnlyTheObservedCharacterSetsForSelectedTypes(){
        assertDoesNotThrow(()->OracleCharacters.requireCharsets("AL32UTF8","AL16UTF16",true));
        assertDoesNotThrow(()->OracleCharacters.requireCharsets("AL32UTF8",null,false));
        assertEquals("ORACLE_CHARACTER_CHARSET_UNSUPPORTED",assertThrows(IllegalStateException.class,
                ()->OracleCharacters.requireCharsets("WE8ISO8859P1","AL16UTF16",true)).getMessage());
        assertEquals("ORACLE_CHARACTER_CHARSET_UNSUPPORTED",assertThrows(IllegalStateException.class,
                ()->OracleCharacters.requireCharsets("AL32UTF8","UTF8",true)).getMessage());
    }

    @Test void readsStrictJsonRedoWithoutCollapsingNumbersStringsOrNulls()throws Exception{
        TableSchema table=table();
        table.columns.get(1).type=TableSchema.Type.JSON;
        String document="{\"name\":\"O''Brien\",\"number\":123456789012345678901}";
        String redo="update "+ORDERS+" set \"NOTE\" = '"+document+"' where \"ID\" = '1' and \"NOTE\" IS NULL;";
        String undo="update "+ORDERS+" set \"NOTE\" = NULL where \"ID\" = '1' and \"NOTE\" = '"+document+"';";
        Change event=OracleRedoDml.decode(table,"UPDATE",redo,undo);
        assertNull(event.before.get(1).value);
        assertEquals(document.replace("''","'"),event.after.get(1).value);
        assertEquals("{}",OracleTextJson.value("{}"));
        assertEquals("null",OracleTextJson.value("null"));
        for(String invalid:new String[]{"{\"nested\":{\"a\":1,\"a\":2}}","{unquoted:1}","", "[] null"}){
            assertEquals("ORACLE_TEXT_JSON_VALUE_INVALID",
                    assertThrows(IllegalStateException.class,()->OracleTextJson.value(invalid)).getMessage());
        }
    }

    @Test void keepsOracleDateSecondsAndNullAcrossRedoAndUndo()throws Exception{
        TableSchema table=table();
        table.columns.get(1).type=TableSchema.Type.DATETIME;
        table.columns.get(1).sourceType="DATE";
        String old="TO_DATE('29-feb-2024 15:17:19','dd-mon-yyyy hh24:mi:ss')";
        String next="TO_DATE(' 2024-03-10 02:17:19','SYYYY-MM-DD HH24:MI:SS')";
        String redo="update "+ORDERS+" set \"NOTE\" = "+next+" where \"ID\" = '1' and \"NOTE\" = "+old+";";
        String undo="update "+ORDERS+" set \"NOTE\" = "+old+" where \"ID\" = '1' and \"NOTE\" = "+next+";";
        Change changed=OracleRedoDml.decode(table,"UPDATE",redo,undo);
        assertEquals("2024-02-29 15:17:19",changed.before.get(1).value);
        assertEquals("2024-03-10 02:17:19",changed.after.get(1).value);
        assertEquals("2024-03-10T02:17:19",RowValues.decode(RowValues.canonical(table,changed.after)).get(1).value);
        assertEquals("2024-03-10 00:00:00",OracleDate.snapshotValue(" 2024-03-10 00:00:00"));
        assertNull(OracleDate.snapshotValue(null));

        Change nulled=OracleRedoDml.decode(table,"UPDATE",redo.replace(next,"NULL"),
                undo.replace("\"NOTE\" = "+next,"\"NOTE\" IS NULL"));
        assertNull(nulled.after.get(1).value);
    }

    @Test void OracleDateRejectsUnknownFormatsAndMismatchedLiteralTypes(){
        assertEquals("ORACLE_DATE_VALUE_OUT_OF_RANGE",assertThrows(IllegalStateException.class,
                ()->OracleDate.snapshotValue("-2024-02-29 15:17:19")).getMessage());
        assertEquals("ORACLE_DATE_VALUE_OUT_OF_RANGE",assertThrows(IllegalStateException.class,
                ()->OracleDate.snapshotValue(" 1582-10-14 15:17:19")).getMessage());
        assertEquals("ORACLE_DATE_FORMAT_UNSUPPORTED",assertThrows(IllegalStateException.class,
                ()->OracleDate.redoValue("01-JAN-24 03:04:05","DD-MON-RR HH24:MI:SS")).getMessage());
        assertEquals("ORACLE_DATE_FORMAT_UNSUPPORTED",assertThrows(IllegalStateException.class,
                ()->OracleDate.redoValue("29-FEB-2023 03:04:05","DD-MON-YYYY HH24:MI:SS")).getMessage());
        String date="TO_DATE(' 2024-02-29 15:17:19','SYYYY-MM-DD HH24:MI:SS')";
        String redo="insert into "+ORDERS+"(\"ID\",\"NOTE\") values ('2',"+date+");";
        String undo="delete from "+ORDERS+" where \"ID\" = '2' and \"NOTE\" = "+date+";";
        assertEquals("ORACLE_REDO_LITERAL_TYPE_MISMATCH",assertThrows(IllegalStateException.class,
                ()->OracleRedoDml.decode(table(),"INSERT",redo,undo)).getMessage());
        TableSchema table=table();
        table.columns.get(1).type=TableSchema.Type.DATETIME;
        table.columns.get(1).sourceType="DATE";
        assertEquals("ORACLE_REDO_LITERAL_TYPE_MISMATCH",assertThrows(IllegalStateException.class,
                ()->OracleRedoDml.decode(table,"INSERT",redo.replace(date,"'2024-02-29 15:17:19'"),
                        undo.replace(date,"'2024-02-29 15:17:19'"))).getMessage());
    }
}
