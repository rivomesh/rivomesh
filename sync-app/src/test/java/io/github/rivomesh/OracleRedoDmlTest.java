package io.github.rivomesh;

import io.github.rivomesh.cdc.Change;
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
}
