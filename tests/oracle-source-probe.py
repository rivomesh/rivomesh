#!/usr/bin/env python3
"""Verify Oracle 26ai source identity and SCN flashback with a disposable Free database."""

import argparse
import base64
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def execute(*args, stdin=None, env=None, timeout=120, checked=True):
    return subprocess.run(args, input=stdin, env=env, text=True, capture_output=True,
                          timeout=timeout, check=checked)


def required(check, description, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if value := check():
            return value
        time.sleep(2)
    raise AssertionError(description + ' timed out')


def mine_archive(sql, start, end, select_rows, *, checked=True, committed_only=True):
    assert start.isdecimal() and end.isdecimal() and int(end)>int(start)
    options='dbms_logmnr.dict_from_online_catalog'
    if committed_only:
        options+=' + dbms_logmnr.committed_data_only'
    return sql(f"""
        DECLARE
            added NUMBER := 0;
        BEGIN
            FOR file_row IN (
                SELECT DISTINCT name FROM v$archived_log
                WHERE resetlogs_change#=(SELECT resetlogs_change# FROM v$database)
                  AND name IS NOT NULL AND status='A' AND deleted='NO'
                  AND first_change# <= {end} AND next_change# > {start}
            ) LOOP
                dbms_logmnr.add_logfile(
                    logfilename=>file_row.name,
                    options=>CASE WHEN added=0 THEN dbms_logmnr.new ELSE dbms_logmnr.addfile END);
                added := added + 1;
            END LOOP;
            dbms_logmnr.start_logmnr(
                startscn=>{start}, endscn=>{end},
                options=>{options});
        END;
        /
        {select_rows}
        EXEC dbms_logmnr.end_logmnr
    """,checked=checked)


def verify_virtual_archive(changes):
    assert len(changes)==4 and all(len(row)==8 and row[2:4]==['0','0'] for row in changes), changes
    assert len({(row[4],row[5]) for row in changes})==1, changes
    assert len(changes[0][4])==16 and changes[0][5].isdecimal(), changes
    assert all('"AMOUNT"' in sql and '"DERIVED"' not in sql for row in changes for sql in row[6:]), changes
    updates=[row for row in changes if row[:2]==['RIVO_VIRTUAL_ROWS','UPDATE']]
    inserted=[row for row in changes if row[:2]==['RIVO_VIRTUAL_ROWS','INSERT']]
    deleted=[row for row in changes if row[:2]==['RIVO_VIRTUAL_KEYLESS','DELETE']]
    assert len(updates)==2 and len(inserted)==len(deleted)==1, changes
    assert "'7'" in updates[0][6] and "'5'" in updates[0][7], changes
    assert "'5'" in updates[1][6] and "'-8'" in updates[1][7], changes
    assert '"AMOUNT" IS NULL' in inserted[0][6] and ',NULL)' in inserted[0][7], changes
    assert "'3'" in deleted[0][6] and "'3'" in deleted[0][7], changes


def virtual_evidence(sql, output, observed):
    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        CREATE TABLE rivo_app.rivo_virtual_rows (
            id NUMBER(12) PRIMARY KEY, amount NUMBER(12),
            derived NUMBER(16,0) GENERATED ALWAYS AS (amount*2) VIRTUAL);
        CREATE TABLE rivo_app.rivo_virtual_keyless (
            amount NUMBER(12), derived NUMBER(16,0) GENERATED ALWAYS AS (amount*2) VIRTUAL);
        ALTER TABLE rivo_app.rivo_virtual_rows ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
        ALTER TABLE rivo_app.rivo_virtual_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
        INSERT INTO rivo_app.rivo_virtual_rows(id,amount) VALUES(1,7);
        INSERT INTO rivo_app.rivo_virtual_keyless(amount) VALUES(3);
        INSERT INTO rivo_app.rivo_virtual_keyless(amount) VALUES(3);
        COMMIT;""")
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    start=sql('SELECT current_scn FROM v$database;').stdout.strip()
    assert start.isdecimal()
    shape=sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        SELECT table_name||'|'||data_type||'|'||data_precision||'|'||data_scale||'|'||
            virtual_column||'|'||default_length||'|'||data_default_vc||'|'||NVL(TO_CHAR(segment_column_id),'NONE')
        FROM all_tab_cols
        WHERE owner='RIVO_APP' AND table_name IN ('RIVO_VIRTUAL_ROWS','RIVO_VIRTUAL_KEYLESS')
          AND column_name='DERIVED' ORDER BY table_name;
        UPDATE rivo_app.rivo_virtual_rows SET amount=5 WHERE id=1;
        UPDATE rivo_app.rivo_virtual_rows SET amount=-8 WHERE id=1;
        INSERT INTO rivo_app.rivo_virtual_rows(id,amount) VALUES(2,NULL);
        DELETE FROM rivo_app.rivo_virtual_keyless WHERE ROWNUM=1;
        COMMIT;
        SELECT 'NEW|'||amount||'|'||derived FROM rivo_app.rivo_virtual_rows WHERE id=1;
        SELECT 'NULL|'||COUNT(*) FROM rivo_app.rivo_virtual_rows WHERE id=2 AND amount IS NULL AND derived IS NULL;
    """).stdout.splitlines()
    shape=[line.strip() for line in shape if '|' in line]
    (output/'native-virtual-dictionary.json').write_text(json.dumps(shape,indent=2)+'\n')

    end=sql('SELECT current_scn FROM v$database;').stdout.strip()
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    mined=mine_archive(sql,start,end,"""
        SELECT table_name||'|'||operation||'|'||status||'|'||csf||'|'||RAWTOHEX(xid)||'|'||commit_scn
          ||'|'||sql_undo||'|'||sql_redo
        FROM v$logmnr_contents
        WHERE seg_owner='RIVO_APP' AND table_name IN ('RIVO_VIRTUAL_ROWS','RIVO_VIRTUAL_KEYLESS')
          AND src_con_name='FREEPDB1'
        ORDER BY scn, rs_id, ssn;
    """).stdout.splitlines()
    changes=[line.strip().split('|',7) for line in mined if '|' in line]
    (output/'native-virtual-archive.json').write_text(json.dumps(changes,indent=2)+'\n')

    presence=mine_archive(sql,start,end,"""
        SELECT scn||'|'||
            dbms_logmnr.column_present(redo_value,'FREEPDB1:RIVO_APP.RIVO_VIRTUAL_ROWS.AMOUNT')||'|'||
            dbms_logmnr.column_present(undo_value,'FREEPDB1:RIVO_APP.RIVO_VIRTUAL_ROWS.AMOUNT')||'|'||
            dbms_logmnr.column_present(redo_value,'FREEPDB1:RIVO_APP.RIVO_VIRTUAL_ROWS.DERIVED')||'|'||
            dbms_logmnr.column_present(undo_value,'FREEPDB1:RIVO_APP.RIVO_VIRTUAL_ROWS.DERIVED')
        FROM v$logmnr_contents
        WHERE seg_owner='RIVO_APP' AND table_name='RIVO_VIRTUAL_ROWS' AND src_con_name='FREEPDB1'
          AND operation='UPDATE' AND status=0 AND csf=0
        ORDER BY scn, rs_id, ssn;
    """,checked=False)
    present_rows=[line.strip().split('|') for line in presence.stdout.splitlines() if '|' in line]
    (output/'native-virtual-column-presence.json').write_text(json.dumps({
        'fields':['scn','amountRedo','amountUndo','derivedRedo','derivedUndo'],
        'rows':present_rows,'exitCode':presence.returncode,
        'oracleErrors':[line for line in presence.stdout.splitlines() if line.startswith('ORA-')],
    },indent=2)+'\n')
    flashback=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
        SELECT 'OLD|'||amount||'|'||derived FROM rivo_app.rivo_virtual_rows AS OF SCN {start} WHERE id=1;
        SELECT 'KEYLESS|'||COUNT(*) FROM rivo_app.rivo_virtual_keyless AS OF SCN {start} WHERE derived=6;
    """,checked=False)
    operands=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
        SELECT 'OLD|'||amount||'|'||(amount*2) FROM rivo_app.rivo_virtual_rows AS OF SCN {start} WHERE id=1;
        SELECT 'KEYLESS|'||COUNT(*) FROM rivo_app.rivo_virtual_keyless AS OF SCN {start} WHERE amount*2=6;
    """,checked=False)
    historical=[line.strip() for line in flashback.stdout.splitlines() if '|' in line]
    recomputed=[line.strip() for line in operands.stdout.splitlines() if '|' in line]
    (output/'native-virtual-flashback.json').write_text(json.dumps({
        'scn':start,'rows':historical,'exitCode':flashback.returncode,
        'oracleErrors':[line for line in flashback.stdout.splitlines() if line.startswith('ORA-')],
        'physicalExpressionRows':recomputed,'physicalExpressionExitCode':operands.returncode,
        'physicalExpressionErrors':[line for line in operands.stdout.splitlines() if line.startswith('ORA-')],
    },indent=2)+'\n')
    assert shape[-2:]==['NEW|-8|-16','NULL|1'], shape
    assert len(shape)==4 and all('|NUMBER|16|0|YES|10|"AMOUNT"*2' in line for line in shape[:2]), shape
    observed('Oracle dictionary exposes virtual NUMBER definitions and source reads preserve negative products and NULL')
    verify_virtual_archive(changes)
    observed('Archived LogMiner supplies keyed and keyless physical operands, including intermediate values in one transaction')
    if presence.returncode==0 and len(present_rows)==2 and all(row[1:]==['1','1','0','0'] for row in present_rows):
        observed('LogMiner COLUMN_PRESENT confirms physical operands are present and virtual values are absent')
    assert operands.returncode==0 and recomputed==['OLD|7|14','KEYLESS|2'], operands.stdout
    assert flashback.returncode==0 and historical==['OLD|7|14','KEYLESS|2'], flashback.stdout
    observed('Oracle Flashback at the original SCN computes historical virtual values and exact keyless multiplicity')


def lob_evidence(sql, output, observed, *, verify_preflight=None):
    variants={
        'RIVO_BF_BLOB':('BASICFILE','BLOB'),
        'RIVO_BF_CLOB':('BASICFILE','CLOB'),
        'RIVO_SF_BLOB':('SECUREFILE','BLOB'),
        'RIVO_SF_CLOB':('SECUREFILE','CLOB'),
    }

    def write(table, kind, call):
        return f"""DECLARE locator {kind};
        BEGIN
            SELECT payload INTO locator FROM rivo_app.{table} WHERE id=5 FOR UPDATE;
            {call}
        END;
        /"""

    for table,(storage,kind) in variants.items():
        original="TO_BLOB(HEXTORAW('001122FF'))" if kind=='BLOB' else "TO_CLOB('clob-old')"
        chunk="UTL_RAW.CAST_TO_RAW(RPAD('A',5000,'A'))" if kind=='BLOB' else "RPAD('A',5000,'A')"
        sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE TABLE rivo_app.{table} (id NUMBER(12) PRIMARY KEY, payload {kind})
                LOB(payload) STORE AS {storage} (ENABLE STORAGE IN ROW);
            ALTER TABLE rivo_app.{table} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            GRANT SELECT, FLASHBACK ON rivo_app.{table} TO C##RIVO_PROBE;
            INSERT INTO rivo_app.{table} VALUES(1,{original});
            INSERT INTO rivo_app.{table} VALUES(2,EMPTY_{kind}());
            INSERT INTO rivo_app.{table} VALUES(3,NULL);
            INSERT INTO rivo_app.{table} VALUES(4,NULL);
            INSERT INTO rivo_app.{table} VALUES(5,EMPTY_{kind}());
            {write(table,kind,f'DBMS_LOB.WRITEAPPEND(locator,5000,{chunk});')}
            COMMIT;""")
    tables=','.join("'"+table+"'" for table in variants)
    storage_rows=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
        SELECT table_name||'|'||securefile||'|'||in_row FROM all_lobs
        WHERE owner='RIVO_APP' AND table_name IN ({tables}) AND column_name='PAYLOAD'
        ORDER BY table_name;""").stdout.splitlines()
    storage=[row.strip().split('|') for row in storage_rows if '|' in row]
    (output/'native-lob-storage.json').write_text(json.dumps(storage,indent=2)+'\n')
    assert storage==[[table,'YES' if mode=='SECUREFILE' else 'NO','YES']
                     for table,(mode,_) in variants.items()],storage
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    start=sql('SELECT current_scn FROM v$database;').stdout.strip()
    assert start.isdecimal()

    for table,(_,kind) in variants.items():
        middle="TO_BLOB(HEXTORAW('AABBCCDD'))" if kind=='BLOB' else "TO_CLOB('clob-middle')"
        final="TO_BLOB(HEXTORAW('CAFEBABE'))" if kind=='BLOB' else "TO_CLOB('clob-final')"
        fragment="HEXTORAW('00FF')" if kind=='BLOB' else "'XY'"
        sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
            UPDATE rivo_app.{table} SET payload={middle} WHERE id=1;
            UPDATE rivo_app.{table} SET payload={final} WHERE id=1;
            DECLARE locator {kind};
            BEGIN
                SELECT payload INTO locator FROM rivo_app.{table} WHERE id=1 FOR UPDATE;
                DBMS_LOB.WRITE(locator,2,2,{fragment});
            END;
            /
            {write(table,kind,f'DBMS_LOB.WRITE(locator,2,4999,{fragment});')}
            DELETE FROM rivo_app.{table} WHERE id=2;
            UPDATE rivo_app.{table} SET payload=EMPTY_{kind}() WHERE id=3;
            COMMIT;""")
    end=sql('SELECT current_scn FROM v$database;').stdout.strip()
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    query=f"""
        SET LONG 32767 LONGCHUNKSIZE 32767
        SELECT JSON_OBJECT('table' VALUE table_name, 'operation' VALUE operation, 'scn' VALUE TO_CHAR(scn),
          'xid' VALUE RAWTOHEX(xid), 'commitScn' VALUE TO_CHAR(commit_scn), 'status' VALUE status,
          'continues' VALUE csf, 'redo' VALUE sql_redo, 'undo' VALUE sql_undo NULL ON NULL RETURNING CLOB)
        FROM v$logmnr_contents WHERE src_con_name='FREEPDB1' AND (
          (seg_owner='RIVO_APP' AND table_name IN ({tables})) OR
          operation IN ('SEL_LOB_LOCATOR','LOB_WRITE','LOB_TRIM','LOB_ERASE'))
        ORDER BY commit_scn,rawtohex(xid),scn,rs_id,ssn;
    """
    def archive(name):
        result=mine_archive(sql,start,end,query)
        records=[json.loads(line) for line in result.stdout.splitlines() if line.lstrip().startswith('{')]
        (output/name).write_text(json.dumps(records,indent=2)+'\n')
        return records

    original=archive('native-lob-archive.json')
    replay=archive('native-lob-archive-replay.json')
    assert original and original==replay
    for table in variants:
        assert any(row['table']==table for row in original), table
    observed('Closed Oracle LOB archive returns a deterministic native record stream on replay')

    native_values={}
    for table,(_,kind) in variants.items():
        content='DBMS_LOB.SUBSTR(payload,20,1)'
        tail='DBMS_LOB.SUBSTR(payload,4,4997)'
        if kind=='BLOB':
            content=f'RAWTOHEX({content})'
            tail=f'RAWTOHEX({tail})'
        projection=(f"""id||'|'||CASE WHEN payload IS NULL THEN 'NULL' ELSE
                    TO_CHAR(DBMS_LOB.GETLENGTH(payload))||'|'||CASE WHEN id=5 THEN {tail}
                    ELSE {content} END END""")
        select=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
            SELECT 'OLD|'||{projection} FROM rivo_app.{table} AS OF SCN {start} ORDER BY id;
            SELECT 'NEW|'||{projection} FROM rivo_app.{table} ORDER BY id;""",checked=False)
        native_values[table]={
            'rows':[row.strip() for row in select.stdout.splitlines() if row.lstrip().startswith(('OLD|','NEW|'))],
            'error':[row for row in select.stdout.splitlines() if row.startswith('ORA-')],
            'exitCode':select.returncode,
        }
    (output/'native-lob-flashback.json').write_text(json.dumps(native_values,indent=2)+'\n')
    for table,(_,kind) in variants.items():
        old,new,old_tail,new_tail=(('001122FF','CA00FFBE','41414141','414100FF') if kind=='BLOB'
                                  else ('clob-old','cXYb-final','AAAA','AAXY'))
        assert native_values[table]['exitCode']==0,native_values[table]
        assert native_values[table]['rows']==[
            f'OLD|1|{len(old)//2 if kind=="BLOB" else len(old)}|{old}',
            'OLD|2|0|','OLD|3|NULL','OLD|4|NULL',f'OLD|5|5000|{old_tail}',
            f'NEW|1|{len(new)//2 if kind=="BLOB" else len(new)}|{new}',
            'NEW|3|0|','NEW|4|NULL',f'NEW|5|5000|{new_tail}',
        ],native_values[table]
    observed('Oracle Flashback distinguishes real LOB bytes, empty values and SQL NULL under both storage modes')
    if verify_preflight is not None:
        refusals={}
        for table,(mode,_) in variants.items():
            expected=('ORACLE_SECUREFILES_LOB_UNDO_UNAVAILABLE' if mode=='SECUREFILE'
                      else 'ORACLE_BASICFILES_LOB_REQUIRES_PROFILE')
            result=verify_preflight(table,start,end)
            assert result.returncode and expected in result.stderr,(table,result.stderr)
            refusals[table]=expected
        (output/'native-lob-preflight.json').write_text(json.dumps(refusals,indent=2)+'\n')
        observed('The restricted Oracle source role receives the expected BasicFiles and SecureFiles preflight errors')


def seed_savepoint_tables(sql):
    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        CREATE TABLE rivo_app.rivo_savepoint_rows (id NUMBER(12) PRIMARY KEY, note VARCHAR2(60));
        CREATE TABLE rivo_app.rivo_savepoint_keyless (note VARCHAR2(60));
        ALTER TABLE rivo_app.rivo_savepoint_rows ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
        ALTER TABLE rivo_app.rivo_savepoint_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
        GRANT SELECT, FLASHBACK ON rivo_app.rivo_savepoint_rows TO C##RIVO_PROBE;
        GRANT SELECT, FLASHBACK ON rivo_app.rivo_savepoint_keyless TO C##RIVO_PROBE;""")
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        INSERT INTO rivo_app.rivo_savepoint_rows VALUES (1,'initial');
        INSERT INTO rivo_app.rivo_savepoint_rows VALUES (2,'move-restored');
        INSERT INTO rivo_app.rivo_savepoint_rows VALUES (3,'delete-restored');
        INSERT INTO rivo_app.rivo_savepoint_keyless VALUES ('duplicate');
        INSERT INTO rivo_app.rivo_savepoint_keyless VALUES ('duplicate');
        INSERT INTO rivo_app.rivo_savepoint_keyless VALUES ('untouched');
        COMMIT;""")


def savepoint_evidence(sql, output, observed, review):
    tables=['RIVO_APP.RIVO_SAVEPOINT_ROWS','RIVO_APP.RIVO_SAVEPOINT_KEYLESS']

    def evidence(name, content):
        (output/('savepoint-'+name+'.json')).write_text(json.dumps(content,indent=2)+'\n')

    def snapshot(at, *, after_ddl=False):
        result=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
            SELECT 'ROW|'||id||'|'||note FROM rivo_app.rivo_savepoint_rows AS OF SCN {at} ORDER BY id;
            SELECT 'KEYLESS|'||note||'|'||COUNT(*) FROM rivo_app.rivo_savepoint_keyless AS OF SCN {at}
                GROUP BY note ORDER BY note;""",checked=False)
        if after_ddl and result.returncode and 'ORA-01466' in result.stdout:
            return None
        assert result.returncode==0,(at,result.stdout,result.stderr)
        return [line.strip() for line in result.stdout.splitlines() if line.strip().startswith(('ROW|','KEYLESS|'))]

    def flashback_baseline():
        scn=sql('SELECT current_scn FROM v$database;').stdout.strip()
        assert scn.isdecimal()
        data=snapshot(scn,after_ddl=True)
        return (scn,data) if data else None

    start,baseline=required(flashback_baseline,'Savepoint Flashback anchor after fixture DDL',30)
    assert baseline==['ROW|1|initial','ROW|2|move-restored','ROW|3|delete-restored',
                      'KEYLESS|duplicate|2','KEYLESS|untouched|1'],baseline
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        INSERT INTO rivo_app.rivo_savepoint_rows VALUES (4,'prefix-kept');
        SAVEPOINT outer_step;
        UPDATE rivo_app.rivo_savepoint_rows SET note='first-unsaved' WHERE id=1;
        SAVEPOINT inner_step;
        UPDATE rivo_app.rivo_savepoint_rows SET note='second-unsaved' WHERE id=1;
        DELETE FROM rivo_app.rivo_savepoint_rows WHERE id=3;
        DELETE FROM rivo_app.rivo_savepoint_keyless WHERE note='duplicate' AND ROWNUM=1;
        ROLLBACK TO inner_step;
        UPDATE rivo_app.rivo_savepoint_rows SET id=20,note='move-unsaved' WHERE id=2;
        INSERT INTO rivo_app.rivo_savepoint_rows VALUES (98,'insert-unsaved');
        INSERT INTO rivo_app.rivo_savepoint_keyless VALUES ('keyless-unsaved');
        ROLLBACK TO outer_step;
        UPDATE rivo_app.rivo_savepoint_rows SET note='retained-after' WHERE id=1;
        DELETE FROM rivo_app.rivo_savepoint_keyless WHERE note='duplicate' AND ROWNUM=1;
        INSERT INTO rivo_app.rivo_savepoint_keyless VALUES ('retained-new');
        COMMIT;""")
    middle=sql('SELECT current_scn FROM v$database;').stdout.strip()
    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        UPDATE rivo_app.rivo_savepoint_rows SET note='full-rollback-sentinel' WHERE id=2;
        INSERT INTO rivo_app.rivo_savepoint_rows VALUES (99,'full-rollback-insert');
        ROLLBACK;
        INSERT INTO rivo_app.rivo_savepoint_rows VALUES (5,'tail-kept');
        COMMIT;""")
    end=sql('SELECT current_scn FROM v$database;').stdout.strip()
    assert all(scn.isdecimal() for scn in (start,middle,end)) and int(start)<int(middle)<int(end)
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')

    table_scope="""seg_owner='RIVO_APP' AND table_name IN ('RIVO_SAVEPOINT_ROWS','RIVO_SAVEPOINT_KEYLESS')
                   AND src_con_name='FREEPDB1'"""
    def rows(condition, *, committed_only=True):
        query=f"""
            SET LONG 32767 LONGCHUNKSIZE 32767
            SELECT JSON_OBJECT('table' VALUE table_name, 'operation' VALUE operation,
                'scn' VALUE TO_CHAR(scn), 'startScn' VALUE TO_CHAR(start_scn), 'commitScn' VALUE TO_CHAR(commit_scn),
                'xid' VALUE RAWTOHEX(xid), 'rollback' VALUE rollback, 'rowId' VALUE row_id,
                'rsId' VALUE rs_id, 'ssn' VALUE ssn, 'sequence' VALUE sequence#,
                'ubaFile' VALUE ubafil, 'ubaBlock' VALUE ubablk, 'ubaRecord' VALUE ubarec, 'ubaSequence' VALUE ubasqn,
                'object' VALUE data_obj#, 'status' VALUE status, 'continues' VALUE csf,
                'redo' VALUE sql_redo, 'undo' VALUE sql_undo NULL ON NULL RETURNING CLOB)
            FROM v$logmnr_contents WHERE {condition}
            ORDER BY commit_scn,rawtohex(xid),scn,rs_id,ssn;
        """
        result=mine_archive(sql,start,end,query,committed_only=committed_only)
        return [json.loads(line) for line in result.stdout.splitlines() if line.lstrip().startswith('{')]

    committed=rows(table_scope)
    repeated=rows(table_scope)
    evidence('committed-archive',committed)
    evidence('committed-replay',repeated)
    assert committed and committed==repeated
    observed('The closed Oracle archive produces the same savepoint records on repeated reads')
    unfiltered=rows(table_scope,committed_only=False)
    evidence('unfiltered-archive',unfiltered)
    assert any('full-rollback-sentinel' in (record['redo'] or '') for record in unfiltered)
    assert all('full-rollback-sentinel' not in ((record['undo'] or '')+(record['redo'] or ''))
               and 'full-rollback-insert' not in ((record['undo'] or '')+(record['redo'] or ''))
               for record in committed)
    observed('The committed-only archive excludes the separate fully rolled back transaction')

    prefix={record['xid'] for record in committed if 'prefix-kept' in (record['redo'] or '')}
    suffix={record['xid'] for record in committed if 'tail-kept' in (record['redo'] or '')}
    assert len(prefix)==len(suffix)==1 and prefix.isdisjoint(suffix)
    mixed_xid=next(iter(prefix))
    assert len(mixed_xid)==16 and all(digit in '0123456789ABCDEF' for digit in mixed_xid)
    transactions=rows(f"RAWTOHEX(xid)='{mixed_xid}'")
    evidence('mixed-xid-archive',transactions)
    assert transactions and all(record['xid']==mixed_xid for record in transactions)
    mixed=[record for record in committed if record['xid']==mixed_xid]
    raw_mixed=[record for record in unfiltered if record['xid']==mixed_xid]
    assert len(mixed)==4 and all(record['rollback']==record['status']==record['continues']==0 for record in mixed)
    assert len(raw_mixed)==18 and sum(record['rollback']==1 for record in raw_mixed)==7,raw_mixed
    assert all(record['undo'] is None for record in raw_mixed if record['rollback']==1)
    for value in ('first-unsaved','second-unsaved','move-unsaved','insert-unsaved','keyless-unsaved'):
        assert any(value in (record['redo'] or '') for record in raw_mixed)
        assert all(value not in ((record['redo'] or '')+(record['undo'] or '')) for record in mixed)
    observed('Committed-only LogMiner removes seven canceled changes and their seven incomplete compensation records')

    snapshots={scn:snapshot(scn) for scn in (start,middle,end)}
    evidence('flashback',snapshots)
    assert snapshots[start]==baseline,snapshots
    retained=['ROW|1|retained-after','ROW|2|move-restored','ROW|3|delete-restored','ROW|4|prefix-kept']
    keyless=['KEYLESS|duplicate|1','KEYLESS|retained-new|1','KEYLESS|untouched|1']
    assert snapshots[middle]==retained+keyless and snapshots[end]==retained+['ROW|5|tail-kept']+keyless,snapshots
    observed('Flashback proves the surviving same-row update, restored primary key, delete and keyless multiplicities')

    preview={}
    for name,lower,upper in [('mixed',start,middle),('tail',middle,end)]:
        result=review(tables,lower,upper)
        preview[name]={'exitCode':result.returncode}
        if result.returncode==0:
            preview[name]['decoded']=json.loads(result.stdout)['archiveWindow']['decoded']
        else:
            preview[name]['error']=next((line.strip().split(': ')[-1]
                                          for line in result.stderr.splitlines() if 'ORACLE_' in line),'UNKNOWN')
    evidence('product-preflight',preview)
    assert preview['mixed']['exitCode']==0,preview
    assert preview['mixed']['decoded']['committedTransactions']==1,preview
    assert preview['mixed']['decoded']['operations']=={'INSERT':2,'UPDATE':1,'DELETE':1},preview
    assert preview['tail']['exitCode']==0,preview
    assert preview['tail']['decoded']['committedTransactions']==1,preview
    assert preview['tail']['decoded']['operations']=={'INSERT':1},preview
    observed('The exact product probe decodes only four surviving changes and one independently selected clean commit')


def seed_native_json(sql):
    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        CREATE TABLE rivo_app.rivo_native_jr (id NUMBER(12) PRIMARY KEY, payload JSON, note VARCHAR2(50));
        CREATE TABLE rivo_app.rivo_native_jk (tag VARCHAR2(20), payload JSON);
        CREATE TABLE rivo_app.rivo_json_control (token VARCHAR2(30) PRIMARY KEY);
        GRANT SELECT, FLASHBACK ON rivo_app.rivo_native_jr TO C##RIVO_PROBE;
        GRANT SELECT, FLASHBACK ON rivo_app.rivo_native_jk TO C##RIVO_PROBE;
        GRANT SELECT, FLASHBACK ON rivo_app.rivo_json_control TO C##RIVO_PROBE;""")
    supplemental={}
    for table in ('RIVO_NATIVE_JR','RIVO_NATIVE_JK','RIVO_JSON_CONTROL'):
        result=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
            ALTER TABLE rivo_app.{table} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;""",checked=False)
        supplemental[table]={'exitCode':result.returncode,'errors':oracle_errors(result)}
    assert supplemental['RIVO_JSON_CONTROL']['exitCode']==0,supplemental
    sql(r"""ALTER SESSION SET CONTAINER=FREEPDB1;
        INSERT INTO rivo_app.rivo_native_jr VALUES
            (1,JSON('{"z":"caf\u00e9 \ud83d\ude00","n":9007199254740993,"a":[true,null,0.125]}'),'base');
        INSERT INTO rivo_app.rivo_native_jr VALUES(2,NULL,'sql-null');
        INSERT INTO rivo_app.rivo_native_jr VALUES(3,JSON('null'),'json-null');
        INSERT INTO rivo_app.rivo_native_jr VALUES(4,JSON_SCALAR(DATE '2024-02-29'),'typed-date');
        INSERT INTO rivo_app.rivo_native_jr VALUES(5,JSON_SCALAR(HEXTORAW('00FF')),'typed-binary');
        INSERT INTO rivo_app.rivo_native_jk VALUES('duplicate',JSON('{"counter":5,"nil":null}'));
        INSERT INTO rivo_app.rivo_native_jk VALUES('duplicate',JSON('{"counter":5,"nil":null}'));
        COMMIT;""")
    return supplemental


def oracle_errors(result):
    return [line.strip() for line in (result.stdout+'\n'+result.stderr).splitlines()
            if line.lstrip().startswith(('ORA-','SP2-'))]


def native_json_evidence(options, sql, pdb_service, environment, observed, review, supplemental):
    keyed='RIVO_NATIVE_JR'
    keyless='RIVO_NATIVE_JK'
    control='RIVO_JSON_CONTROL'

    def evidence(name, content):
        (options.output/f'native-json-{name}.json').write_text(json.dumps(content,indent=2)+'\n')

    evidence('supplemental',supplemental)
    data_types=sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        SELECT table_name||'|'||column_name||'|'||data_type FROM all_tab_cols
        WHERE owner='RIVO_APP' AND table_name IN ('RIVO_NATIVE_JR','RIVO_NATIVE_JK')
          AND column_name='PAYLOAD' ORDER BY table_name;""").stdout.splitlines()
    assert [line.strip() for line in data_types if '|' in line]==[
        'RIVO_NATIVE_JK|PAYLOAD|JSON','RIVO_NATIVE_JR|PAYLOAD|JSON']

    def current_scn():
        scn=sql('SELECT current_scn FROM v$database;').stdout.strip()
        assert scn.isdecimal()
        return scn

    def flashback_ready():
        scn=current_scn()
        check=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
            SELECT 'KEYED|'||COUNT(*) FROM rivo_app.{keyed} AS OF SCN {scn};
            SELECT 'KEYLESS|'||COUNT(*) FROM rivo_app.{keyless} AS OF SCN {scn};""",checked=False)
        if check.returncode and 'ORA-01466' in check.stdout:
            return None
        assert check.returncode==0,oracle_errors(check)
        assert [line.strip() for line in check.stdout.splitlines() if '|' in line]==['KEYED|5','KEYLESS|2']
        return scn

    build=sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        EXEC SYS.DBMS_LOGMNR_D.BUILD(OPTIONS=>SYS.DBMS_LOGMNR_D.STORE_IN_REDO_LOGS)
        SELECT 'DICT|'||MAX(start_scn) FROM SYS.DBA_LOGMNR_DICTIONARY_BUILDLOG;""",checked=False)
    dictionary_scn=next((line.strip().split('|')[1] for line in build.stdout.splitlines()
                         if line.lstrip().startswith('DICT|')),None)
    dictionary={'exitCode':build.returncode,'errors':oracle_errors(build),'startScn':dictionary_scn}
    evidence('pdb-dictionary',dictionary)
    positions={'baseline':required(flashback_ready,'Native JSON Flashback anchor after DDL',30)}
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    changes={
        'insert':"""
            INSERT INTO rivo_app.rivo_native_jr VALUES(6,JSON('{"inserted":[1,true,null]}'),'inserted');
            INSERT INTO rivo_app.rivo_native_jr VALUES(7,
                JSON(TO_CLOB('{"pad":"')||RPAD('x',3500,'x')||RPAD('x',3500,'x')||'","tail":"native-end"}'),
                'large');
            INSERT INTO rivo_app.rivo_native_jk VALUES('inserted',JSON('false'));""",
        'replace':"""
            UPDATE rivo_app.rivo_native_jr SET payload=JSON('{"replaced":2.125,"old":null}') WHERE id=1;
            UPDATE rivo_app.rivo_native_jr SET payload=JSON('null') WHERE id=2;
            UPDATE rivo_app.rivo_native_jr SET payload=NULL WHERE id=3;
            UPDATE rivo_app.rivo_native_jr SET note='scalar-only' WHERE id=5;
            UPDATE rivo_app.rivo_native_jk SET payload=JSON('true') WHERE tag='inserted';""",
        'path-update':"""
            UPDATE rivo_app.rivo_native_jr
                SET payload=JSON_TRANSFORM(payload, SET '$.inserted[0]'=19) WHERE id=6;
            UPDATE rivo_app.rivo_native_jr
                SET payload=JSON_TRANSFORM(payload, SET '$.tail'='path-edited') WHERE id=7;
            UPDATE rivo_app.rivo_native_jk
                SET payload=JSON_TRANSFORM(payload, SET '$.counter'=8) WHERE tag='duplicate' AND ROWNUM=1;""",
        'delete':"""
            DELETE FROM rivo_app.rivo_native_jr WHERE id IN (6,7);
            DELETE FROM rivo_app.rivo_native_jk WHERE tag='duplicate'
                AND JSON_VALUE(payload,'$.counter' RETURNING NUMBER)=5;""",
    }
    for step,statements in changes.items():
        sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
            {statements}
            INSERT INTO rivo_app.{control} VALUES('{step}');
            COMMIT;""")
        positions[step]=current_scn()
    end=current_scn()
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    evidence('positions',positions)
    assert all(int(a)<int(b) for a,b in zip(positions.values(),list(positions.values())[1:]))

    def raw_rows(condition, *, committed_only=True):
        query=f"""
            SET LONG 32767 LONGCHUNKSIZE 32767
            SELECT JSON_OBJECT('owner' VALUE seg_owner, 'segment' VALUE seg_name, 'table' VALUE table_name,
                'object' VALUE data_obj#, 'pdb' VALUE src_con_name, 'operation' VALUE operation,
                'scn' VALUE TO_CHAR(scn), 'commitScn' VALUE TO_CHAR(commit_scn), 'xid' VALUE RAWTOHEX(xid),
                'rowId' VALUE row_id, 'rsId' VALUE rs_id, 'ssn' VALUE ssn, 'status' VALUE status,
                'continues' VALUE csf, 'rollback' VALUE rollback, 'info' VALUE info,
                'redo' VALUE sql_redo, 'undo' VALUE sql_undo NULL ON NULL RETURNING CLOB)
            FROM v$logmnr_contents WHERE {condition}
            ORDER BY commit_scn,RAWTOHEX(xid),scn,rs_id,ssn;
        """
        result=mine_archive(sql,positions['baseline'],end,query,committed_only=committed_only)
        return [json.loads(line) for line in result.stdout.splitlines() if line.lstrip().startswith('{')]

    xids={}
    markers=raw_rows(f"src_con_name='FREEPDB1' AND seg_owner='RIVO_APP' AND table_name='{control}'")
    for step in changes:
        selected=[row for row in markers if row['operation']=='INSERT' and f"'{step}'" in (row['redo'] or '')]
        assert len(selected)==1 and re.fullmatch('[0-9A-F]{16}',selected[0]['xid']),selected
        xids[step]=selected[0]['xid']
    assert len(set(xids.values()))==len(changes)
    evidence('markers',{'xids':xids,'rows':markers})
    scope='RAWTOHEX(xid) IN ('+','.join(f"'{xid}'" for xid in xids.values())+')'
    rows=raw_rows(scope)
    reproduced=raw_rows(scope)
    evidence('archive',rows)
    evidence('archive-replay',reproduced)
    assert rows and rows==reproduced
    evidence('archive-unfiltered',raw_rows(scope,committed_only=False))
    observed('Closed LogMiner reads preserve every marker XID and give the same archived record stream on replay')

    archived_json=[row for row in rows if row['table'] in (keyed,keyless)]
    assert {row['xid'] for row in archived_json}==set(xids.values())
    assert {row['table'] for row in archived_json}=={keyed,keyless}
    evidence('traditional-archive-shape',{
        'nativeRows':len(archived_json),
        'reportedUnsupported':sum(row['operation']=='UNSUPPORTED' and row['status']==2 and
                                  row['redo']==row['undo']=='Unsupported' for row in archived_json),
        'scalarInsertRows':sum(row['operation']=='INSERT' and row['status']==0 for row in markers),
    })

    def read_candidate(mode, prefix, table, column, function):
        qualified=f'{prefix}RIVO_APP.{table}.{column}'
        query=f"""
            SET LONG 32767 LONGCHUNKSIZE 32767
            SELECT JSON_OBJECT('operation' VALUE operation, 'xid' VALUE RAWTOHEX(xid),
                'rowId' VALUE row_id, 'rsId' VALUE rs_id, 'ssn' VALUE ssn,
                'redo' VALUE DBMS_LOGMNR.{function}(redo_value,'{qualified}'),
                'undo' VALUE DBMS_LOGMNR.{function}(undo_value,'{qualified}') NULL ON NULL RETURNING CLOB)
            FROM v$logmnr_contents
            WHERE {scope} AND seg_owner='RIVO_APP' AND table_name='{table}'
            ORDER BY commit_scn,scn,rs_id,ssn;
        """
        if mode=='root':
            result=mine_archive(sql,positions['baseline'],end,query,checked=False)
        else:
            result=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
                BEGIN
                    SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN=>{dictionary_scn}, ENDSCN=>{end},
                        OPTIONS=>SYS.DBMS_LOGMNR.DDL_DICT_TRACKING+SYS.DBMS_LOGMNR.DICT_FROM_REDO_LOGS);
                END;
                /
                {query}
                EXEC SYS.DBMS_LOGMNR.END_LOGMNR
                """,checked=False)
        return {
            'exitCode':result.returncode,'errors':oracle_errors(result),
            'rows':[json.loads(line) for line in result.stdout.splitlines() if line.lstrip().startswith('{')],
        }

    def mine_pair(mode,prefix,table,column):
        return {entry:read_candidate(mode,prefix,table,column,function)
                for entry,function in (('presence','COLUMN_PRESENT'),('values','MINE_VALUE'))}

    def scalar_proven(pair):
        return pair['values']['exitCode']==pair['presence']['exitCode']==0 and (
            len(pair['values']['rows'])==len(pair['presence']['rows'])==len(changes) and
            {row['redo'] for row in pair['values']['rows']}==set(changes) and
            all(row['redo']==1 for row in pair['presence']['rows']))

    controls=[]
    selected=None
    for mode in ('pdb','root'):
        if mode=='pdb' and (build.returncode or not dictionary_scn or not dictionary_scn.isdecimal()):
            continue
        for prefix in ('FREEPDB1:',''):
            pair=mine_pair(mode,prefix,control,'TOKEN')
            supported=scalar_proven(pair)
            controls.append({'mode':mode,'prefix':prefix,'scalarConfirmed':supported,'result':pair})
            if supported and selected is None:
                selected=(mode,prefix)
    evidence('mine-controls',controls)
    candidates={}
    if selected is not None:
        candidates={table:mine_pair(*selected,table,'PAYLOAD') for table in (keyed,keyless)}
        observed('A known VARCHAR2 marker independently validates COLUMN_PRESENT and MINE_VALUE')
    evidence('mine-value',{'validatedControl':selected,'tables':candidates})
    refusals={}
    for table in (keyed,keyless):
        result=review([f'RIVO_APP.{table}'],positions['baseline'],end)
        code='ORACLE_REDO_COLUMN_TYPE_UNSUPPORTED'
        assert result.returncode and code in result.stderr,(table,result.stderr)
        refusals[table]=code
    evidence('refusals',refusals)
    observed('Current product preflight refuses native JSON tables and preserves the documented capture boundary')

    def sql_snapshot():
        helper=Path(__file__).resolve().parent/'oracle'/'NativeJsonFlashback.java'
        command=(options.java,'-cp',str(options.jdbc_driver.resolve()),str(helper),pdb_service,*positions.values())
        result=execute(*command,env=environment,checked=False)
        if result.returncode:
            (options.output/'native-json-jdbc-failure.log').write_text(result.stderr)
            result.check_returncode()
        output={}
        for line in result.stdout.splitlines():
            scn,table,row_id,key,kind,typed,serialized,raw_kind,raw=line.split('\t')
            def decode(value):
                return None if value=='-' else base64.b64decode(value,validate=True)
            text=decode(serialized)
            driver_text=decode(typed)
            raw_bytes=decode(raw)
            output.setdefault(scn,[]).append({
                'table':table,'rowId':row_id,'key':key,'kind':kind,
                'serialized':None if text is None else text.decode('utf-8'),
                'serializedSha256':None if text is None else hashlib.sha256(text).hexdigest(),
                'typedText':None if driver_text is None else driver_text.decode('utf-8'),
                'rawKind':raw_kind,'rawBase64':None if raw_bytes is None else raw,
            })
        return output

    snapshots=sql_snapshot()
    assert snapshots==sql_snapshot() and snapshots.keys()==set(positions.values())
    evidence('jdbc-flashback',snapshots)

    def value(record):
        text=record['serialized']
        return json.loads(text,parse_int=Decimal,parse_float=Decimal) if text is not None else None

    def typed_value(text):
        def number(token):
            return ('number',Decimal(token))
        return json.loads(text,parse_int=number,parse_float=number)

    for snapshot in snapshots.values():
        assert all((row['serialized'] is None)==(row['kind']=='SQL_NULL') for row in snapshot)
        for row in snapshot:
            if row['kind'] not in ('SQL_NULL','DATE','BINARY'):
                assert typed_value(row['typedText'])==typed_value(row['serialized']),row
    states={step:{record['key']:record for record in snapshots[scn] if record['table']==keyed}
            for step,scn in positions.items()}
    base=states['baseline']
    assert value(base['1'])=={'z':'café 😀','n':Decimal('9007199254740993'),'a':[True,None,Decimal('0.125')]}
    assert base['2']['kind']=='SQL_NULL' and base['2']['serialized'] is None
    assert base['3']['kind']=='NULL' and base['3']['serialized']=='null'
    assert base['4']['kind']=='DATE' and base['5']['kind']=='BINARY'
    assert value(states['insert']['6'])=={'inserted':[1,True,None]}
    assert value(states['insert']['7'])=={'pad':'x'*7000,'tail':'native-end'}
    assert value(states['replace']['1'])=={'replaced':Decimal('2.125'),'old':None}
    assert states['replace']['2']['kind']=='NULL' and states['replace']['3']['kind']=='SQL_NULL'
    assert value(states['path-update']['6'])=={'inserted':[19,True,None]}
    assert value(states['path-update']['7'])=={'pad':'x'*7000,'tail':'path-edited'}
    assert set(states['delete'])==set(base)
    for step,counters in [('baseline',[5,5]),('path-update',[5,8]),('delete',[8])]:
        duplicates=[value(record)['counter'] for record in snapshots[positions[step]]
                    if record['table']==keyless and record['key']=='duplicate']
        assert sorted(duplicates)==counters,duplicates
    observed('Restricted JDBC Flashback repeats exact serialized JSON bytes and preserves nulls, extended types and keyless values')

    comparisons=[]
    previous=positions['baseline']
    for step in changes:
        after=positions[step]
        before_rows={(row['table'],row['rowId']):row for row in snapshots[previous]}
        after_rows={(row['table'],row['rowId']):row for row in snapshots[after]}
        for identity in sorted(before_rows.keys()|after_rows.keys()):
            before_row=before_rows.get(identity)
            after_row=after_rows.get(identity)
            scalar_only=step=='replace' and identity[0]==keyed and after_row is not None and after_row['key']=='5'
            if before_row==after_row and not scalar_only:
                continue
            table,row_id=identity
            operation='INSERT' if before_row is None else 'DELETE' if after_row is None else 'UPDATE'
            records=[row for row in rows if (row['table'],row['rowId'],row['xid'],row['operation'])
                     ==(table,row_id,xids[step],operation)]

            def image_matches(side, expected):
                if expected is None or table not in candidates:
                    return None
                if len(records)!=1:
                    return False
                item=records[0]
                matched={}
                for entry in ('presence','values'):
                    for row in candidates[table][entry]['rows']:
                        if (row['xid'],row['rsId'],row['ssn'])==(item['xid'],item['rsId'],item['ssn']):
                            matched[entry]=row[side]
                if matched.get('presence')!=1 or 'values' not in matched:
                    return False
                logged=matched['values']
                if expected['kind']=='SQL_NULL':
                    return logged is None
                if not isinstance(logged,str) or expected['kind'] in ('DATE','BINARY'):
                    return False
                try:
                    return typed_value(logged)==typed_value(expected['serialized'])
                except json.JSONDecodeError:
                    return False

            comparisons.append({
                'step':step,'table':table,'rowId':row_id,'key':(after_row or before_row)['key'],
                'xid':xids[step],'operation':operation,'scalarOnlyUpdate':scalar_only,
                'beforeKind':None if before_row is None else before_row['kind'],
                'afterKind':None if after_row is None else after_row['kind'],
                'beforeTextSha256':None if before_row is None else before_row['serializedSha256'],
                'afterTextSha256':None if after_row is None else after_row['serializedSha256'],
                'sqlRecords':[{'rsId':row['rsId'],'ssn':row['ssn'],'status':row['status'],
                               'continues':row['continues'],'rollback':row['rollback']} for row in records],
                'mineValueMatchesBefore':image_matches('undo',before_row),
                'mineValueMatchesAfter':image_matches('redo',after_row),
            })
        previous=after
    evidence('image-comparison',comparisons)
    assert len(comparisons)==14,comparisons


def raw_evidence(sql, output, observed):
    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        CREATE TABLE rivo_app.rivo_raw_probe (id NUMBER(12) PRIMARY KEY, octets RAW(32));
        ALTER TABLE rivo_app.rivo_raw_probe ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;""")
    start = sql('SELECT current_scn FROM v$database;').stdout.strip()
    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        INSERT INTO rivo_app.rivo_raw_probe VALUES(1,HEXTORAW('000AFF80'));
        INSERT INTO rivo_app.rivo_raw_probe VALUES(2,NULL);
        COMMIT;""")
    snapshot = sql('SELECT current_scn FROM v$database;').stdout.strip()
    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        UPDATE rivo_app.rivo_raw_probe SET octets=HEXTORAW('00FF10') WHERE id=1;
        DELETE FROM rivo_app.rivo_raw_probe WHERE id=2;
        COMMIT;""")
    end = sql('SELECT current_scn FROM v$database;').stdout.strip()
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    flashback = sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
        SELECT id||'|'||COALESCE(RAWTOHEX(octets),'NULL') FROM rivo_app.rivo_raw_probe AS OF SCN {snapshot} ORDER BY id;""").stdout.strip()
    assert flashback.splitlines() == ['1|000AFF80','2|NULL']
    archived = mine_archive(sql,start,end,"""
        SELECT operation||'|'||status||'|'||csf||'|'||sql_redo||'|'||sql_undo
        FROM v$logmnr_contents
        WHERE seg_owner='RIVO_APP' AND table_name='RIVO_RAW_PROBE' AND src_con_name='FREEPDB1'
          AND operation IN ('INSERT','UPDATE','DELETE','UNSUPPORTED')
        ORDER BY scn,rs_id,ssn;
    """).stdout
    rows=[line.strip().split('|',4) for line in archived.splitlines() if '|' in line]
    assert len(rows)==4 and all(row[1:3]==['0','0'] and len(row)==5 for row in rows), rows
    assert sorted(row[0] for row in rows)==['DELETE','INSERT','INSERT','UPDATE']
    assert any('00FF10' in row[3].upper() and '000AFF80' in row[4].upper() for row in rows if row[0]=='UPDATE')
    (output/'native-raw-redo.json').write_text(json.dumps({'flashback': flashback.splitlines(), 'archives': rows},indent=2)+'\n')
    observed('Native Oracle RAW redo and undo retain binary values, NULL, and the original Flashback image')


def pipeline_profile(options, oracle, sql, root_service, pdb_service, environment, observed):
    target = oracle + '-pg'
    redis = oracle + '-redis'
    clickhouse = oracle + '-ch'
    mysql = oracle + '-my'
    kingbase = oracle + '-kb'
    dameng = oracle + '-dm'
    doris = oracle + '-doris'
    hive = oracle + '-hive'
    env = dict(environment, POSTGRES_PASSWORD=secrets.token_urlsafe(32), RM_ORACLE_TEST_TOKEN=secrets.token_urlsafe(32))
    if options.clickhouse:
        env['CLICKHOUSE_PASSWORD']=secrets.token_hex(24)
    if options.mysql_target:
        env['MYSQL_ROOT_PASSWORD']=secrets.token_hex(24)
    if options.kingbase_target:
        env['KINGBASE_FIXTURE_PASSWORD']='Kb!'+secrets.token_hex(12)
    if options.dm_target:
        env['RM_PG_DM_PASSWORD']='Dm8!'+secrets.token_hex(10)
    if options.date_values or options.mysql_target or options.kingbase_target or options.dm_target or options.doris_target or options.hive_target:
        env['TZ'] = 'America/New_York'
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    platform = None
    long_transaction = None
    output = options.output
    program = (options.java, '-cp', str(options.artifact.resolve()) + os.pathsep
               + str(options.jdbc_driver.resolve()), 'io.github.rivomesh.Main')

    def target_sql(statement, checked=True):
        return execute('docker','exec',target,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','sync_test',
                       '-At','-c',statement,checked=checked).stdout.strip()

    def dm_sql(statement, checked=True):
        return execute(options.java,'-cp',str(options.artifact.resolve())+os.pathsep+str(dm_probe_classes),
                       'DmSourceTargetProbe',statement,env=env,checked=checked).stdout.strip()

    def doris_sql(statement, checked=True):
        return execute('docker','exec',doris,'mysql','--protocol=TCP','-h127.0.0.1','-P9030','-uroot',
                       '--skip-column-names','--batch','--raw','-e',statement,checked=checked).stdout.strip('\n')

    def hive_compose(*args, checked=True, timeout=120):
        return execute('docker','compose','-p',hive,'-f',str(hive_document),*args,
                       checked=checked,timeout=timeout)

    def hive_sql(statement, checked=True):
        return hive_compose('exec','-T','hive','beeline','--silent=true','--showHeader=false',
                            '--outputformat=tsv2','--nullemptystring=false','-u',
                            'jdbc:hive2://127.0.0.1:10000/default;auth=noSasl','-e',statement,
                            checked=checked).stdout.strip('\n')

    def analytics_values(query, namespace, stage):
        def raw(value):
            return base64.b64encode(bytes.fromhex(value)).decode()

        wide=Decimal('12345678901234567890123456.123456789012')
        keys=[(1,raw('000aff80' if stage==0 else '00ff10')),(2,'NULL')]
        keyless=[raw('0100ff')]*2+['NULL'] if stage==0 else [raw('0100ff' if stage==1 else 'ffff00')]
        types=[('A',wide,'2024-03-10 02:17:19')] if stage==0 else [('Z',wide,'NULL')]
        binary=[(raw('000aff80' if stage==0 else '81ff00'),1),(raw('000aff8020'),2)]
        if stage<2:
            types.append(('A ',Decimal(2),'NULL'))
        else:
            keys.append((3,raw('aa0080')))
            binary.append((raw('00ff10'),3))

        def rows(table, columns):
            result=query(f'SELECT {columns} FROM {namespace}.{table}',checked=False)
            return [line.split('\t') for line in result.splitlines()]

        return (sorted((int(row),value) for row,value in rows('RIVO_SYNC_RAW_ROWS',"ID,COALESCE(OCTETS,'NULL')"))==keys and
                sorted(row[0] for row in rows('RIVO_SYNC_RAW_KEYLESS',"COALESCE(OCTETS,'NULL')"))==sorted(keyless) and
                sorted((key,Decimal(value),date.replace('T',' ')) for key,value,date in
                       rows('RIVO_SYNC_MYSQL_TYPES',"CODE,AMOUNT,COALESCE(OCURRED,'NULL')"))==sorted(types) and
                sorted((key,int(value)) for key,value in rows('RIVO_SYNC_RAW_KEYS','OCTETS,ID'))==sorted(binary))

    def dm_types():
        rows=dm_sql("""
            SELECT "CODE","AMOUNT",TO_CHAR("OCURRED",'YYYY-MM-DD HH24:MI:SS')
            FROM "dm_copy"."RIVO_SYNC_MYSQL_TYPES" ORDER BY "CODE" """,checked=False).splitlines()
        normalized=[]
        for row in rows:
            code,amount,ocurred=row.split('|')
            normalized.append(code.encode().hex().upper()+'|'+amount+'|'+ocurred)
        return normalized

    def dm_binary_keys():
        rows=dm_sql("""
            SELECT "OCTETS","ID" FROM "dm_copy"."RIVO_SYNC_RAW_KEYS" """,checked=False).splitlines()
        return sorted((row.upper() for row in rows),key=lambda row:row.split('|',1)[0])

    def kb_sql(statement, checked=True):
        return execute('docker','exec',kingbase,'/home/kingbase/install/kingbase/bin/ksql',
                       '-U','system','-d','test','-p','54321','-At','-F','|','-c',statement,
                       checked=checked).stdout.strip()

    def kb_types():
        return kb_sql("""
            SELECT upper(encode(convert_to("CODE",'UTF8'),'hex')),"AMOUNT"::text,
                   COALESCE(TO_CHAR("OCURRED",'YYYY-MM-DD HH24:MI:SS'),'<NULL>')
            FROM kb_copy."RIVO_SYNC_MYSQL_TYPES" ORDER BY encode(convert_to("CODE",'UTF8'),'hex')""",
                      checked=False).splitlines()

    def kb_binary_keys():
        return kb_sql("""
            SELECT upper(encode("OCTETS",'hex')),"ID" FROM kb_copy."RIVO_SYNC_RAW_KEYS"
            ORDER BY encode("OCTETS",'hex')""",checked=False).splitlines()

    def mysql_sql(statement, checked=True):
        return execute('docker','exec',mysql,'sh','-c',
                       'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql --protocol=TCP --host=127.0.0.1 -uroot -NB -e "$1"',
                       'sh',statement,checked=checked).stdout.strip()

    def mysql_types():
        return mysql_sql("""
            SELECT CONCAT(HEX(CODE),'|',AMOUNT,'|',IFNULL(DATE_FORMAT(OCURRED,'%Y-%m-%d %H:%i:%s'),'<NULL>'))
            FROM mysql_copy.RIVO_SYNC_MYSQL_TYPES ORDER BY HEX(CODE)""",checked=False).splitlines()

    def mysql_binary_keys():
        return mysql_sql('SELECT CONCAT(HEX(OCTETS),\'|\',ID) FROM mysql_copy.RIVO_SYNC_RAW_KEYS ORDER BY HEX(OCTETS)',
                         checked=False).splitlines()

    def ch_sql(statement, checked=True):
        return execute('docker','exec',clickhouse,'sh','-c',
                       'clickhouse-client --user sync_test --password "$CLICKHOUSE_PASSWORD" --query "$1"',
                       'sh',statement,checked=checked).stdout.strip()

    def ch_raw_rows(table, keyed=False):
        value = "if(isNull(OCTETS),'<NULL>',lower(hex(assumeNotNull(OCTETS))))"
        selected = f"concat(toString(ID),'|',{value})" if keyed else value
        order = 'ID' if keyed else value
        return ch_sql(f'SELECT {selected} FROM ch_app.`{table}` ORDER BY {order}',checked=False).splitlines()

    def stop_platform(force=False):
        nonlocal platform
        if platform is None:
            return
        if platform.poll() is None:
            os.killpg(platform.pid,signal.SIGKILL if force else signal.SIGTERM)
            try:
                platform.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(platform.pid,signal.SIGKILL)
                platform.wait(timeout=10)
        platform = None

    def start_platform(config, filename):
        nonlocal platform
        with (output/filename).open('w') as logs:
            platform = subprocess.Popen((*program,'serve',str(config)),env=env,stdout=logs,
                                        stderr=subprocess.STDOUT,start_new_session=True)

    def api(path, payload=None):
        headers = {'Authorization':'Bearer '+env['RM_ORACLE_TEST_TOKEN']}
        if payload is not None:
            headers.update({'Content-Type':'application/json','X-Request-ID':'oracle_'+secrets.token_hex(12)})
        request = urllib.request.Request(base+path,headers=headers,
                                         data=json.dumps(payload).encode() if payload is not None else None)
        try:
            with opener.open(request,timeout=10) as response:
                return json.load(response)
        except (OSError,urllib.error.URLError):
            return {}

    def progress():
        return api('/api/tasks/oracle_native').get('status',{})

    def hive_cut(snapshot_count):
        status=progress()
        sub=next((item for item in status.get('subscriptions',[]) if item['id']=='hive'),None)
        if (not status.get('capture_scn') or status.get('phase')!='STREAMING'
                or status.get('completed_tables')!=snapshot_count or sub is None or sub['state']!='RUNNING'
                or sub['safeCoveredSequence']!=status['captured_sequence']):
            return None
        suffix=hashlib.sha256(b'oracle_native:hive').hexdigest()[:48]
        observed=hive_sql("SELECT id,hash FROM hive_meta.rm3_hive_"+suffix+
                          " WHERE kind='RECEIPT' ORDER BY id").splitlines()
        receipts=set(observed)
        assert len(receipts)==len(observed),'Hive receipt was duplicated'
        return (status['sourcePosition'],receipts) if receipts else None

    def redis_events():
        raw = execute('docker','exec',redis,'redis-cli','--json','-2','-n','2','XRANGE','RM_ORACLE_PARITY','-','+').stdout
        records = []
        for position, fields in json.loads(raw):
            properties = dict(zip(fields[::2], fields[1::2]))
            event = json.loads(properties['body'])
            assert properties['rivomesh_event_id'] == event['id']
            records.append((position,event))
        return records

    def redis_cut(kind,count):
        events=redis_events()
        return events if sum(event['kind']==kind and (kind!='COMMIT' or event['totalChanges']>0)
                             for _,event in events)==count else None

    def row_note():
        return target_sql('SELECT "NOTE" FROM copy_app."RIVO_ORDERS" WHERE "ID"=1',checked=False)

    def date_rows(table, key=False):
        value = """COALESCE(TO_CHAR("EVENT_AT",'YYYY-MM-DD HH24:MI:SS'),'<NULL>')"""
        column = '"ID"||\'|\'||'+value if key else value
        order = '"ID"' if key else '"EVENT_AT" NULLS LAST'
        return target_sql(f'SELECT {column} FROM copy_app."{table}" ORDER BY {order}',checked=False).splitlines()

    def virtual_rows(keyed=False):
        table='RIVO_SYNC_VIRTUAL_ROWS' if keyed else 'RIVO_SYNC_VIRTUAL_KEYLESS'
        values="||'|'||".join(f"""COALESCE("{column}"::text,'<NULL>')"""
                              for column in ('AMOUNT','ADJUSTMENT','DERIVED','SUM_VALUE','DIFFERENCE'))
        selected=f'''"ID"||'|'||{values}''' if keyed else values
        order='"ID"' if keyed else '"AMOUNT" NULLS LAST'
        return target_sql(f'SELECT {selected} FROM copy_app."{table}" ORDER BY {order}',checked=False).splitlines()

    def raw_rows(table, keyed=False):
        value = """COALESCE(encode("OCTETS",'hex'),'<NULL>')"""
        selected = f""" "ID"||'|'||{value} """ if keyed else value
        order = '"ID"' if keyed else value
        return target_sql(f'SELECT {selected} FROM copy_app."{table}" ORDER BY {order}',
                          checked=False).splitlines()

    def rotate():
        sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')

    source_tables = ['RIVO_APP.RIVO_ORDERS', 'RIVO_APP.RIVO_KEYLESS']
    if options.character_pipeline:
        import oracle_character_evidence
        source_tables += [f'RIVO_APP.{table}' for table in oracle_character_evidence.TABLES]
    if options.text_json:
        sql(r"""ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE TABLE rivo_app.rivo_json_rows (
                id NUMBER(12) PRIMARY KEY,
                payload VARCHAR2(1000 BYTE) CHECK (payload IS JSON (STRICT WITH UNIQUE KEYS)),
                plain VARCHAR2(100 BYTE),
                CHECK (plain IS JSON (STRICT WITH UNIQUE KEYS)) DISABLE NOVALIDATE);
            CREATE TABLE rivo_app.rivo_json_keyless (
                "payload" VARCHAR2(100 BYTE) CHECK ("payload" IS JSON (STRICT WITH UNIQUE KEYS)));
            CREATE TABLE rivo_app.rivo_native_json (
                id NUMBER(12) PRIMARY KEY, payload JSON);
            CREATE TABLE rivo_app.rivo_json_key (
                payload VARCHAR2(100 BYTE) PRIMARY KEY
                    CHECK (payload IS JSON (STRICT WITH UNIQUE KEYS)));
            INSERT INTO rivo_app.rivo_json_rows VALUES
                (1,'{"name":"O''Brien","number":123456789012345678901,"where":"\u53f0\u5317"}','{value:12}');
            INSERT INTO rivo_app.rivo_json_rows VALUES (2,NULL,NULL);
            INSERT INTO rivo_app.rivo_json_rows VALUES (3,'null',NULL);
            INSERT INTO rivo_app.rivo_json_keyless VALUES ('{"value":7}');
            INSERT INTO rivo_app.rivo_json_keyless VALUES ('{"value":7}');
            COMMIT;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_json_rows TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_json_keyless TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_native_json TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_json_key TO C##RIVO_PROBE;
            ALTER TABLE rivo_app.rivo_json_rows ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            ALTER TABLE rivo_app.rivo_json_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;""")
        for invalid in ('{unquoted:1}', '{"same":1,"same":2}'):
            duplicate=sql("ALTER SESSION SET CONTAINER=FREEPDB1;\n"
                          f"INSERT INTO rivo_app.rivo_json_rows VALUES (99,'{invalid}',NULL);",checked=False)
            assert duplicate.returncode and 'ORA-02290' in duplicate.stdout
        observed('The active Oracle text constraint rejects lax syntax and duplicate object keys')
        rotate()
        source_tables += ['RIVO_APP.RIVO_JSON_ROWS', 'RIVO_APP.RIVO_JSON_KEYLESS']
    if options.date_values:
        sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE TABLE rivo_app.rivo_date_rows (id NUMBER(12) PRIMARY KEY, event_at DATE);
            CREATE TABLE rivo_app.rivo_date_keyless (event_at DATE);
            INSERT INTO rivo_app.rivo_date_rows VALUES
                (1,TO_DATE('2024-02-29 15:17:19','YYYY-MM-DD HH24:MI:SS'));
            INSERT INTO rivo_app.rivo_date_rows VALUES (2,NULL);
            INSERT INTO rivo_app.rivo_date_keyless VALUES
                (TO_DATE('2024-03-10 02:17:19','YYYY-MM-DD HH24:MI:SS'));
            INSERT INTO rivo_app.rivo_date_keyless VALUES
                (TO_DATE('2024-03-10 03:17:19','YYYY-MM-DD HH24:MI:SS'));
            COMMIT;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_date_rows TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_date_keyless TO C##RIVO_PROBE;
            ALTER TABLE rivo_app.rivo_date_rows ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            ALTER TABLE rivo_app.rivo_date_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;""")
        rotate()
        source_tables += ['RIVO_APP.RIVO_DATE_ROWS','RIVO_APP.RIVO_DATE_KEYLESS']

    if options.virtual_columns:
        sql('''ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE TABLE rivo_app.rivo_sync_virtual_rows (
                id NUMBER(12) PRIMARY KEY, amount NUMBER(12), adjustment NUMBER(12),
                derived NUMBER(16,0) GENERATED ALWAYS AS (amount*2) VIRTUAL,
                sum_value NUMBER(16,0) GENERATED ALWAYS AS (amount+adjustment) VIRTUAL,
                difference NUMBER(16,0) GENERATED ALWAYS AS (amount-adjustment) VIRTUAL);
            CREATE TABLE rivo_app.rivo_sync_virtual_keyless (
                amount NUMBER(12), adjustment NUMBER(12),
                derived NUMBER(16,0) GENERATED ALWAYS AS (amount*2) VIRTUAL,
                sum_value NUMBER(16,0) GENERATED ALWAYS AS (amount+adjustment) VIRTUAL,
                difference NUMBER(16,0) GENERATED ALWAYS AS (amount-adjustment) VIRTUAL);
            CREATE TABLE rivo_app.rivo_sync_virtual_unsupported (
                amount NUMBER(12), derived NUMBER(16,0) GENERATED ALWAYS AS (ABS(amount)) VIRTUAL);
            ALTER TABLE rivo_app.rivo_sync_virtual_rows ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            ALTER TABLE rivo_app.rivo_sync_virtual_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_sync_virtual_rows TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_sync_virtual_keyless TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_sync_virtual_unsupported TO C##RIVO_PROBE;
            INSERT INTO rivo_app.rivo_sync_virtual_rows(id,amount,adjustment) VALUES(1,7,-2);
            INSERT INTO rivo_app.rivo_sync_virtual_keyless(amount,adjustment) VALUES(3,2);
            INSERT INTO rivo_app.rivo_sync_virtual_keyless(amount,adjustment) VALUES(3,2);
            INSERT INTO rivo_app.rivo_sync_virtual_keyless(amount,adjustment) VALUES(4,1);
            COMMIT;''')
        rotate()
        source_tables += ['RIVO_APP.RIVO_SYNC_VIRTUAL_ROWS','RIVO_APP.RIVO_SYNC_VIRTUAL_KEYLESS']

    if options.raw_values:
        sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE TABLE rivo_app.rivo_sync_raw_rows (id NUMBER(12) PRIMARY KEY, octets RAW(32));
            CREATE TABLE rivo_app.rivo_sync_raw_keyless (octets RAW(16));
            INSERT INTO rivo_app.rivo_sync_raw_rows VALUES (1,HEXTORAW('000aff80'));
            INSERT INTO rivo_app.rivo_sync_raw_rows VALUES (2,NULL);
            INSERT INTO rivo_app.rivo_sync_raw_keyless VALUES (HEXTORAW('0100ff'));
            INSERT INTO rivo_app.rivo_sync_raw_keyless VALUES (HEXTORAW('0100ff'));
            INSERT INTO rivo_app.rivo_sync_raw_keyless VALUES (NULL);
            COMMIT;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_sync_raw_rows TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_sync_raw_keyless TO C##RIVO_PROBE;
            ALTER TABLE rivo_app.rivo_sync_raw_rows ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            ALTER TABLE rivo_app.rivo_sync_raw_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;""")
        rotate()
        source_tables += ['RIVO_APP.RIVO_SYNC_RAW_ROWS','RIVO_APP.RIVO_SYNC_RAW_KEYLESS']

    if options.mysql_target or options.kingbase_target or options.dm_target or options.doris_target or options.hive_target:
        sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE TABLE rivo_app.rivo_sync_mysql_types
                (code VARCHAR2(20 CHAR) PRIMARY KEY, amount NUMBER(38,12) NOT NULL, ocurred DATE);
            CREATE TABLE rivo_app.rivo_sync_raw_keys (octets RAW(16) PRIMARY KEY, id NUMBER(12));
            INSERT INTO rivo_app.rivo_sync_mysql_types VALUES
                ('A',12345678901234567890123456.123456789012,
                 TO_DATE('2024-03-10 02:17:19','YYYY-MM-DD HH24:MI:SS'));
            INSERT INTO rivo_app.rivo_sync_mysql_types VALUES ('A ',2.000000000000,NULL);
            INSERT INTO rivo_app.rivo_sync_raw_keys VALUES (HEXTORAW('000aff80'),1);
            INSERT INTO rivo_app.rivo_sync_raw_keys VALUES (HEXTORAW('000aff8020'),2);
            ALTER TABLE rivo_app.rivo_sync_mysql_types ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            ALTER TABLE rivo_app.rivo_sync_raw_keys ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_sync_mysql_types TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_sync_raw_keys TO C##RIVO_PROBE;
            COMMIT;""")
        rotate()
        source_tables += ['RIVO_APP.RIVO_SYNC_MYSQL_TYPES','RIVO_APP.RIVO_SYNC_RAW_KEYS']

    started=False
    started_redis=False
    started_clickhouse=False
    started_mysql=False
    started_kingbase=False
    started_dameng=False
    started_doris=False
    hive_document=None
    try:
        if options.hive_target:
            profile = Path(__file__).resolve().parent/'hive/hive-site.xml'
            data = ['metadata:/opt/hive/rivomesh-meta','warehouse:/opt/hive/data/warehouse',
                    str(profile)+':/opt/hive/conf/hive-site.xml:ro']
            image='apache/hive:4.0.1'
            hive_document=output/'hive-compose.json'
            hive_document.write_text(json.dumps(dict(
                services={
                    'hive_init':dict(image=image,user='root',volumes=data,entrypoint=['bash','-euc'],
                                     command=['chown -R hive:hive /opt/hive/rivomesh-meta /opt/hive/data/warehouse; '
                                              "su hive -s /bin/bash -c 'schematool -dbType derby -initSchema'"]),
                    'hive':dict(image=image,mem_limit='4g',cpus=3,volumes=data,ports=['127.0.0.1::10000'],
                                depends_on=dict(hive_init=dict(condition='service_completed_successfully')),
                                tmpfs=['/run/rivomesh-hive:rw,nosuid,nodev,noexec,size=64k,mode=1777'],
                                environment=dict(SERVICE_NAME='hiveserver2',IS_RESUME='true',HADOOP_HEAPSIZE='1536',
                                                 HADOOP_CLIENT_OPTS='-Xmx1536m',
                                                 HIVESERVER2_PID_DIR='/run/rivomesh-hive')),
                },
                volumes=dict(metadata={},warehouse={}),
            )))
            hive_compose('up','-d','hive',timeout=240)
            required(lambda:hive_sql('SELECT 1',checked=False)=='1','HiveServer2 available',240)
            hive_port=hive_compose('port','hive','10000').stdout.strip().rsplit(':',1)[1]
            (output/'hive-version.txt').write_text(hive_sql('SELECT version()')+'\n')
        if options.doris_target:
            execute('docker','run','--rm','-d','--name',doris,
                    '--label','io.github.rivomesh.test=oracle-source-target',
                    '-p','127.0.0.1::9030','-p','127.0.0.1::8040','--memory','7g','--cpus','3',
                    '-e','FE_HEAP=768m','-e','BE_HEAP=256m','-e','BE_CONFIG_EXTRA=mem_limit = 4g',
                    'apache/doris:all-in-one-4.1.3')
            started_doris=True
            required(lambda:doris_sql('SELECT 1',checked=False)=='1' and
                     '\ttrue\t' in doris_sql('SHOW BACKENDS',checked=False),'Doris target ready',180)
            doris_port,load_port=(execute('docker','port',doris,port+'/tcp').stdout.strip().rsplit(':',1)[1]
                                  for port in ('9030','8040'))
            (output/'doris-version.txt').write_text(doris_sql('SELECT VERSION()')+'\n')
        if options.dm_target:
            fixtures=Path(__file__).resolve().parent/'dm'
            dm_probe_classes=output/'dm-probe'
            dm_probe_classes.mkdir(exist_ok=True)
            execute(str(Path(options.java).with_name('javac')),'-cp',str(options.artifact.resolve()),
                    '-d',str(dm_probe_classes),str(fixtures/'DmSourceTargetProbe.java'))
            dm_env=dict(env,SYSDBA_PWD=env['RM_PG_DM_PASSWORD'],SYSAUDITOR_PWD=env['RM_PG_DM_PASSWORD'],
                        SYSSSO_PWD=env['RM_PG_DM_PASSWORD'])
            execute('docker','run','--rm','-d','--name',dameng,
                    '--label','io.github.rivomesh.test=oracle-source-target','-p','127.0.0.1::5236',
                    '--memory','2g','--cpus','2','-e','SYSDBA_PWD','-e','SYSAUDITOR_PWD','-e','SYSSSO_PWD',
                    '-e','UNICODE_FLAG=1','-e','CASE_SENSITIVE=1','-e','BLANK_PAD_MODE=1','-e','PAGE_SIZE=32',
                    '-e','BUFFER=128','-e','TIME_ZONE=+00:00',
                    '-v',str(fixtures/'start.sh')+':/fixture/start.sh:ro',
                    '-v',str(fixtures/'health.sh')+':/fixture/health.sh:ro',
                    '--entrypoint','/bin/bash','dm8:dm8_20250924_rev288894_HWarm_kylin10_64','/fixture/start.sh',
                    env=dm_env)
            started_dameng=True
            required(lambda:execute('docker','exec',dameng,'/bin/bash','/fixture/health.sh',checked=False).returncode==0,
                     'Dameng target ready',150)
            dm_port=execute('docker','port',dameng,'5236/tcp').stdout.strip().rsplit(':',1)[1]
            env['RIVOMESH_DM_PROBE_URL']=f'jdbc:dm://127.0.0.1:{dm_port}'
            (output/'dm-version.txt').write_text(dm_sql('--version')+'\n')
        if options.kingbase_target:
            fixture=Path(__file__).resolve().parent/'kingbase/start.sh'
            execute('docker','run','--rm','-d','--name',kingbase,
                    '--label','io.github.rivomesh.test=oracle-source-target','-p','127.0.0.1::54321',
                    '--memory','1500m','--cpus','2','--stop-signal','SIGINT',
                    '-e','DB_PASSWORD','-v',str(fixture)+':/fixture/start.sh:ro',
                    '--entrypoint','/bin/bash','kingbase_v009r001c001b0025_single_arm:v1','/fixture/start.sh',
                    env=dict(env,DB_PASSWORD=env['KINGBASE_FIXTURE_PASSWORD']))
            started_kingbase=True
            required(lambda:kb_sql('SELECT 1',checked=False)=='1','Kingbase target ready',120)
            kb_port=execute('docker','port',kingbase,'54321/tcp').stdout.strip().rsplit(':',1)[1]
            (output/'kingbase-version.txt').write_text(kb_sql('SELECT version()')+'\n')
        if options.mysql_target:
            execute('docker','run','--rm','-d','--name',mysql,
                    '--label','io.github.rivomesh.test=oracle-source-target','-p','127.0.0.1::3306',
                    '--tmpfs','/var/lib/mysql:rw,size=768m,mode=1777',
                    '-e','MYSQL_ROOT_PASSWORD','-e','MYSQL_ROOT_HOST=%','mysql:8.4',env=env)
            started_mysql=True
            required(lambda:mysql_sql('SELECT 1',checked=False)=='1','MySQL target ready',120)
            mysql_port=execute('docker','port',mysql,'3306/tcp').stdout.strip().rsplit(':',1)[1]
            (output/'mysql-version.txt').write_text(mysql_sql('SELECT VERSION()')+'\n')
        if options.clickhouse:
            started_clickhouse=True
            execute('docker','run','--rm','-d','--name',clickhouse,
                    '--label','io.github.rivomesh.test=oracle-source-target','-p','127.0.0.1::8123',
                    '--tmpfs','/var/lib/clickhouse:rw,size=768m,mode=1777',
                    '-e','CLICKHOUSE_USER=sync_test','-e','CLICKHOUSE_PASSWORD',
                    '-e','CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1','clickhouse/clickhouse-server:25.8.33.6',env=env)
            required(lambda:ch_sql('SELECT 1',checked=False)=='1','ClickHouse target ready',60)
            ch_port=execute('docker','port',clickhouse,'8123/tcp').stdout.strip().rsplit(':',1)[1]
            observed_version=ch_sql('SELECT version()')
            (output/'clickhouse-version.txt').write_text(observed_version+'\n')
        if options.redis:
            execute('docker','run','--rm','-d','--name',redis,
                    '--label','io.github.rivomesh.test=oracle-source-target','-p','127.0.0.1::6379',
                    'redis:7.4.2-alpine','redis-server','--appendonly','yes','--appendfsync','always')
            started_redis=True
            required(lambda:execute('docker','exec',redis,'redis-cli','PING',checked=False).stdout.strip()=='PONG',
                     'Redis target ready',30)
            redis_port=execute('docker','port',redis,'6379/tcp').stdout.strip().rsplit(':',1)[1]
        execute('docker','run','--rm','-d','--name',target,
                '--label','io.github.rivomesh.test=oracle-source-target','-p','127.0.0.1::5432',
                '-e','POSTGRES_PASSWORD','-e','POSTGRES_DB=sync_test','postgres:17.12',env=env)
        started=True
        required(lambda:target_sql('SELECT 1',checked=False)=='1','PostgreSQL ready',60)
        target_port=execute('docker','port',target,'5432/tcp').stdout.strip().rsplit(':',1)[1]
        sql("""GRANT SELECT ON sys.v_$transaction TO C##RIVO_PROBE;
            GRANT SELECT ON sys.v_$pdb_incarnation TO C##RIVO_PROBE;""")
        with tempfile.TemporaryDirectory(prefix='rivomesh-oracle-pipeline-') as tmp:
            folder=Path(tmp).resolve()
            tasks=folder/'tasks'
            tasks.mkdir()

            def endpoint(url):
                return {'url':url,'username':'C##RIVO_PROBE','password':'${RIVOMESH_ORACLE_PROBE_PASSWORD}'}

            pipeline={
                'formatVersion':3,'pipelineId':'oracle_native','nodeId':'oracle-agent','databases':['RIVO_APP'],
                'source':{'type':'ORACLE','clusterId':'oracle-native','database':'FREEPDB1',
                          'expectedVersion':'23.26','topology':'SINGLE',
                          'oracle':{'root':endpoint(root_service),'pdb':endpoint(pdb_service),
                                    'tables':source_tables,
                                    'archivePollMillis':500}},
                'targets':[{'id':'pg','product':'POSTGRESQL',
                            'url':f'jdbc:postgresql://127.0.0.1:{target_port}/sync_test?connectTimeout=3',
                            'username':'postgres','password':'${POSTGRES_PASSWORD}','mappings':{'RIVO_APP':'copy_app'},
                            'metadataSchema':'copy_meta'}],
                'chunkSize':2,'heartbeatMillis':500,'leaseMillis':5000,'pollIntervalMillis':150,
            }
            if options.redis:
                pipeline['targets'].append(dict(id='redis',product='REDIS_STREAMS',
                                                url=f'redis://127.0.0.1:{redis_port}/2',
                                                mappings={'RIVO_APP':'oracle_events'},
                                                messaging=dict(stream='RM_ORACLE_PARITY')))
            if options.dm_target:
                pipeline['targets'].append(dict(id='dm',product='DM',url=env['RIVOMESH_DM_PROBE_URL'],
                                                username='SYSDBA',password='${RM_PG_DM_PASSWORD}',
                                                metadataSchema='dm_meta',mappings={'RIVO_APP':'dm_copy'},
                                                dm=dict(acknowledgeTextConversions=True)))
            if options.hive_target:
                pipeline['chunkSize']=1000
                pipeline['targets'].append(dict(id='hive',product='HIVE',
                                                url=f'jdbc:hive2://127.0.0.1:{hive_port}/default;auth=noSasl',
                                                username='hive',password='',metadataSchema='hive_meta',
                                                mappings={'RIVO_APP':'hive_app'},
                                                hive=dict(acknowledgeIndexOmission=True,queryTimeoutSeconds=300)))
            if options.doris_target:
                pipeline['targets'].append(dict(id='doris',product='DORIS',
                                                url=f'jdbc:mysql://127.0.0.1:{doris_port}/?useSSL=false&connectTimeout=3000',
                                                username='root',password='',metadataSchema='doris_meta',
                                                mappings={'RIVO_APP':'doris_app'},
                                                doris=dict(loadUrl=f'http://127.0.0.1:{load_port}',replicas=1,buckets=1)))
            if options.kingbase_target:
                pipeline['targets'].append(dict(id='kb',product='KINGBASE',
                                                url=f'jdbc:kingbase8://127.0.0.1:{kb_port}/test'
                                                    '?connectTimeout=4&socketTimeout=20',
                                                username='system',password='${KINGBASE_FIXTURE_PASSWORD}',
                                                metadataSchema='kb_meta',mappings={'RIVO_APP':'kb_copy'}))
            if options.mysql_target:
                pipeline['targets'].append(dict(id='my',product='MYSQL',
                                                url=f'jdbc:mysql://127.0.0.1:{mysql_port}/?sslMode=DISABLED'
                                                    '&allowPublicKeyRetrieval=true&connectTimeout=3000',
                                                username='root',password='${MYSQL_ROOT_PASSWORD}',
                                                metadataSchema='mysql_meta',mappings={'RIVO_APP':'mysql_copy'}))
            if options.clickhouse:
                pipeline['targets'].append(dict(id='ch',product='CLICKHOUSE',
                                                url=f'http://127.0.0.1:{ch_port}',username='sync_test',
                                                password='${CLICKHOUSE_PASSWORD}',metadataSchema='ch_meta',
                                                mappings={'RIVO_APP':'ch_app'}))
            if options.savepoint_pipeline or options.character_pipeline:
                pipeline['targets'].append(dict(
                    pipeline['targets'][0],id='pg_peer',metadataSchema='copy_peer_meta',
                    mappings={'RIVO_APP':'copy_peer_app'},
                ))
            if options.virtual_columns:
                pipeline['source']['oracle']['allowVirtualNumericColumns']=True
            task_file=tasks/'oracle_native.yml'
            task_file.write_text(json.dumps(pipeline))
            checked=execute(*program,'check',str(task_file),env=env)
            assert json.loads(checked.stdout)['sourceTables']==len(source_tables)
            if options.text_json:
                invalid_profile=folder/'unsupported.yml'
                source_oracle=pipeline['source']['oracle']
                for table,error in [
                    ('RIVO_NATIVE_JSON','ORACLE_REDO_COLUMN_TYPE_UNSUPPORTED'),
                    ('RIVO_JSON_KEY','ORACLE_TEXT_JSON_PRIMARY_KEY_UNSUPPORTED'),
                ]:
                    source_oracle['tables']=['RIVO_APP.'+table]
                    invalid_profile.write_text(json.dumps(pipeline))
                    denied=execute(*program,'check',str(invalid_profile),env=env,checked=False)
                    assert denied.returncode and error in denied.stderr
                source_oracle['tables']=source_tables
                observed('Preflight refuses native binary JSON and textual JSON source keys')

            if options.virtual_columns:
                invalid=folder/'virtual-unsupported.yml'
                source_oracle=pipeline['source']['oracle']
                for tables,allow,code in [
                    (source_tables,False,'ORACLE_VIRTUAL_COLUMN_REQUIRES_PROFILE'),
                    (['RIVO_APP.RIVO_SYNC_VIRTUAL_UNSUPPORTED'],True,'ORACLE_VIRTUAL_EXPRESSION_UNSUPPORTED'),
                ]:
                    source_oracle.update(tables=tables,allowVirtualNumericColumns=allow)
                    invalid.write_text(json.dumps(pipeline))
                    denied=execute(*program,'check',str(invalid),env=env,checked=False)
                    assert denied.returncode and code in denied.stderr
                source_oracle.update(tables=source_tables,allowVirtualNumericColumns=True)
                observed('Preflight requires explicit virtual column admission and refuses function expressions')

            # Keep DML started before the snapshot open through the first archive rotation.
            long_transaction=subprocess.Popen(('docker','exec','-i',oracle,'sqlplus','-L','-s','/','as','sysdba'),
                                              stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                                              stderr=subprocess.STDOUT,text=True)
            long_transaction.stdin.write("WHENEVER SQLERROR EXIT FAILURE ROLLBACK\n"
                                         "ALTER SESSION SET CONTAINER=FREEPDB1;\n"
                                         "EXEC DBMS_APPLICATION_INFO.SET_MODULE('RIVOMESH_FIXTURE_LONG',NULL);\n"
                                         "UPDATE rivo_app.rivo_orders SET note='long transaction' WHERE id=1;\n")
            long_transaction.stdin.flush()
            active_query=("SELECT t.start_scn FROM v$transaction t JOIN v$session s "
                          "ON s.taddr=RAWTOHEX(t.addr) AND s.con_id=t.con_id "
                          "WHERE s.module='RIVOMESH_FIXTURE_LONG' AND "
                          "s.con_id=(SELECT con_id FROM v$pdbs WHERE name='FREEPDB1');")
            before=required(lambda:sql(active_query).stdout.strip() or None,'the marked Oracle transaction is active',45)
            assert before.isdecimal()
            required(lambda:int(sql('SELECT current_scn FROM v$database;').stdout.strip())>int(before),
                     'the source SCN advances while the marked transaction is open',45)

            with socket.create_server(('127.0.0.1',0)) as reservation:
                bind_port=reservation.getsockname()[1]
            base='http://127.0.0.1:'+str(bind_port)
            config=folder/'platform.json'
            config.write_text(json.dumps({'bind':'127.0.0.1','port':bind_port,'tasksDirectory':str(tasks),
                                          'dataDirectory':str(folder/'state'),'autoStart':True,
                                          'authToken':'${RM_ORACLE_TEST_TOKEN}','pollMillis':250,
                                          'sessions':{'secureCookie':False},'workerHeapMiB':256}))
            try:
                start_platform(config,'worker-platform.log')
                required(lambda:row_note()=='after' and progress().get('capture_scn'),'native Oracle snapshot',90)
                if options.text_json:
                    before_json='SELECT "PAYLOAD"::text FROM copy_app."RIVO_JSON_ROWS" WHERE "ID"=1'
                    snapshot_json=required(lambda:target_sql(before_json,checked=False),'Oracle JSON snapshot',90)
                    assert json.loads(snapshot_json)=={
                        'name': "O'Brien", 'number': 123456789012345678901, 'where': '\u53f0\u5317',
                    }
                    assert target_sql('SELECT pg_typeof("PAYLOAD")::text||\'|\'||pg_typeof("PLAIN")::text||\'|\'||"PLAIN" '
                                      'FROM copy_app."RIVO_JSON_ROWS" WHERE "ID"=1')=='jsonb|character varying|{value:12}'
                    required(lambda:target_sql('SELECT COUNT(*) FROM copy_app."RIVO_JSON_ROWS" '
                                               'WHERE ("ID"=2 AND "PAYLOAD" IS NULL) OR '
                                               '("ID"=3 AND "PAYLOAD"=\'null\'::jsonb)')=='2',
                             'SQL NULL versus JSON null',90)
                    required(lambda:target_sql('SELECT COUNT(*) FROM copy_app."RIVO_JSON_KEYLESS"',checked=False)=='2',
                             'keyless JSON snapshot',90)
                    observed('Flashback materializes strict JSONB, preserves NULL versus JSON null and retains plain VARCHAR2')
                if options.date_values:
                    required(lambda:date_rows('RIVO_DATE_ROWS',key=True)==['1|2024-02-29 15:17:19','2|<NULL>']
                             and date_rows('RIVO_DATE_KEYLESS')==['2024-03-10 02:17:19','2024-03-10 03:17:19'],
                             'Oracle DATE Flashback snapshot',90)
                    assert target_sql('SELECT pg_typeof("EVENT_AT")::text FROM copy_app."RIVO_DATE_ROWS" '
                                      'WHERE "ID"=1')=='timestamp without time zone'
                    observed('Flashback preserves Oracle DATE leap day, NULL, seconds and two distinct local clocks across a JVM DST gap')
                if options.virtual_columns:
                    required(lambda:virtual_rows(True)==['1|7|-2|14|5|9']
                             and virtual_rows()==['3|2|6|5|1','3|2|6|5|1','4|1|8|5|3'],
                             'Oracle virtual NUMBER snapshot',90)
                    observed('Flashback independently verifies keyed derived values and preserves keyless duplicate multiplicity')
                if options.raw_values:
                    required(lambda:raw_rows('RIVO_SYNC_RAW_ROWS',True)==['1|000aff80','2|<NULL>']
                             and sorted(raw_rows('RIVO_SYNC_RAW_KEYLESS'))==['0100ff','0100ff','<NULL>'],
                             'Oracle RAW Flashback snapshot',90)
                    assert target_sql('SELECT pg_typeof("OCTETS")::text FROM copy_app."RIVO_SYNC_RAW_ROWS" '
                                      'WHERE "ID"=1')=='bytea'
                    if options.clickhouse:
                        required(lambda:ch_raw_rows('RIVO_SYNC_RAW_ROWS',True)==['1|000aff80','2|<NULL>']
                                 and sorted(ch_raw_rows('RIVO_SYNC_RAW_KEYLESS'))==['0100ff','0100ff','<NULL>'],
                                 'ClickHouse Oracle RAW materialized snapshot',120)
                        observed('ClickHouse materializes the original Oracle RAW snapshot with exact NULL and duplicate counts')
                    observed('Flashback preserves Oracle RAW bytes, SQL NULL and keyless duplicate multiplicity')
                if options.mysql_target:
                    expected_before=['41|12345678901234567890123456.123456789012|2024-03-10 02:17:19',
                                     '4120|2.000000000000|<NULL>']
                    required(lambda:mysql_types()==expected_before
                             and mysql_binary_keys()==['000AFF80|1','000AFF8020|2'],
                             'MySQL copies Oracle DATE, exact DECIMAL and both kinds of keys',90)
                    shape=mysql_sql("""SELECT CONCAT(COLUMN_NAME,'|',COLUMN_TYPE,'|',IFNULL(COLLATION_NAME,''))
                        FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql_copy'
                        AND TABLE_NAME IN ('RIVO_SYNC_MYSQL_TYPES','RIVO_SYNC_RAW_KEYS')
                        AND COLUMN_NAME IN ('CODE','OCTETS','OCURRED') ORDER BY COLUMN_NAME""")
                    assert sorted(shape.splitlines())==['CODE|varchar(20)|utf8mb4_0900_bin',
                                                         'OCTETS|varbinary(16)|','OCURRED|datetime(6)|'],shape
                    mysql_snapshot_query=("SELECT snapshot_id FROM mysql_meta.rm3_sink_table "
                                          "WHERE snapshot_state='COMPLETE' ORDER BY table_id")
                    original_mysql_snapshots=required(
                        lambda: observed_ids if len((observed_ids:=mysql_sql(mysql_snapshot_query)).splitlines())
                        ==len(source_tables) else None,'all MySQL target snapshots complete',90)
                    observed('MySQL preserves civil DATE, exact NUMBER, trailing-space distinct and RAW binary primary keys')
                if options.kingbase_target:
                    expected_before=['41|12345678901234567890123456.123456789012|2024-03-10 02:17:19',
                                     '4120|2.000000000000|<NULL>']
                    required(lambda:kb_types()==expected_before
                             and kb_binary_keys()==['000AFF80|1','000AFF8020|2'],
                             'Kingbase copies Oracle DATE, exact DECIMAL and both kinds of keys',120)
                    kb_snapshot_query=("SELECT snapshot_id FROM kb_meta.rm3_sink_table "
                                       "WHERE snapshot_state='COMPLETE' ORDER BY table_id")
                    original_kb_snapshots=required(
                        lambda: observed_ids if len((observed_ids:=kb_sql(kb_snapshot_query)).splitlines())
                        ==len(source_tables) else None,'all Kingbase target snapshots complete',120)
                    observed('Kingbase preserves civil DATE, exact Oracle NUMBER, text keys and RAW primary keys')
                if options.dm_target:
                    expected_before=['41|12345678901234567890123456.123456789012|2024-03-10 02:17:19',
                                     '4120|2.000000000000|<NULL>']
                    required(lambda:dm_types()==expected_before
                             and dm_binary_keys()==['000AFF80|1','000AFF8020|2'],
                             'Dameng copies Oracle DATE, exact DECIMAL and both kinds of keys',120)
                    dm_snapshot_query=("SELECT snapshot_id FROM \"dm_meta\".\"rm3_sink_table\" "
                                       "WHERE snapshot_state='COMPLETE' ORDER BY table_id")
                    original_dm_snapshots=required(
                        lambda: observed_ids if len((observed_ids:=dm_sql(dm_snapshot_query)).splitlines())
                        ==len(source_tables) else None,'all Dameng target snapshots complete',120)
                    observed('Dameng preserves civil DATE, exact Oracle NUMBER, text keys and RAW primary keys')
                if options.doris_target:
                    required(lambda:analytics_values(doris_sql,'doris_app',0),'Oracle exact Doris snapshot with duplicates',180)
                    observed('Doris retains exact Oracle NUMBER, civil DATE, RAW, SQL NULL and keyless duplicates')
                if options.hive_target:
                    required(lambda:analytics_values(hive_sql,'hive_app',0),'Oracle rows in the Hive ACID view',240)
                    observed('Hive retains exact Oracle NUMBER, civil DATE, RAW, SQL NULL and keyless duplicates')
                snapshots=target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
                assert len(snapshots.splitlines())==len(source_tables)
                assert target_sql('SELECT COUNT(*) FROM copy_app."RIVO_KEYLESS"')=='1'
                checkpoint=progress()['sourcePosition']['payload']
                assert int(checkpoint['archiveStartScn'])<=int(before)<int(checkpoint['scn']), (before,checkpoint)
                assert len(checkpoint['pdbIncarnation'].split(':'))==2
                observed('Oracle Flashback snapshots keyed and keyless tables while retaining the pre-snapshot transaction floor')
                if options.redis:
                    baseline=required(lambda:redis_cut('SNAPSHOT_END',len(source_tables)), 'Oracle Redis snapshot completion',60)
                    snapshot_events={event['snapshotId'] for _,event in baseline if event['kind']=='SNAPSHOT_END'}
                    assert len(snapshot_events)==len(source_tables)
                    assert {event['schema']['sourceEngine'] for _,event in baseline if event['kind']=='SCHEMA'}=={'ORACLE'}
                    observed('Redis receives the Oracle SCN-anchored Flashback snapshots and normalized table schemas')
                    if options.raw_values:
                        raw_columns={event['schema']['name']:next(column for column in event['schema']['columns']
                                     if column['name']=='OCTETS') for _,event in baseline
                                     if event['kind']=='SCHEMA' and event['schema']['name'].startswith('RIVO_SYNC_RAW_')}
                        assert {name:(column['type'],column['length']) for name,column in raw_columns.items()} == {
                            'RIVO_SYNC_RAW_ROWS':('BYTES',32), 'RIVO_SYNC_RAW_KEYLESS':('BYTES',16),
                        }, raw_columns

                rotate()
                required(lambda:int(progress().get('capture_scn','0'))>int(checkpoint['scn']),
                         'archive-only progress while the Oracle transaction remains open',45)
                assert row_note()=='after'
                online_raw=("""
                    UPDATE rivo_app.rivo_sync_raw_rows SET octets=HEXTORAW('00ff10') WHERE id=1;
                    DELETE FROM rivo_app.rivo_sync_raw_keyless WHERE octets=HEXTORAW('0100ff') AND ROWNUM=1;
                    DELETE FROM rivo_app.rivo_sync_raw_keyless WHERE octets IS NULL;
                """ if options.raw_values else '')
                online_mysql=("""
                    UPDATE rivo_app.rivo_sync_mysql_types SET code='Z',ocurred=NULL WHERE code='A';
                    UPDATE rivo_app.rivo_sync_raw_keys SET octets=HEXTORAW('81ff00') WHERE id=1;
                """ if options.mysql_target or options.kingbase_target or options.dm_target or options.doris_target or options.hive_target else '')
                out,_=long_transaction.communicate(online_raw+online_mysql+"COMMIT;\nEXIT\n",timeout=30)
                assert long_transaction.returncode==0,out
                long_transaction=None
                if options.text_json:
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        UPDATE rivo_app.rivo_json_rows SET payload='[true,{"version":2}]' WHERE id=1;
                        DELETE FROM rivo_app.rivo_json_keyless WHERE ROWNUM=1;
                        COMMIT;""")
                if options.date_values:
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        UPDATE rivo_app.rivo_date_rows SET event_at=NULL WHERE id=1;
                        UPDATE rivo_app.rivo_date_rows SET
                            event_at=TO_DATE('2024-03-10 02:17:19','YYYY-MM-DD HH24:MI:SS') WHERE id=2;
                        DELETE FROM rivo_app.rivo_date_keyless
                            WHERE event_at=TO_DATE('2024-03-10 02:17:19','YYYY-MM-DD HH24:MI:SS');
                        COMMIT;""")
                if options.virtual_columns:
                    virtual_start=sql('SELECT current_scn FROM v$database;').stdout.strip()
                    sql('''ALTER SESSION SET CONTAINER=FREEPDB1;
                        UPDATE rivo_app.rivo_sync_virtual_rows SET amount=5 WHERE id=1;
                        UPDATE rivo_app.rivo_sync_virtual_rows SET amount=-8,adjustment=4 WHERE id=1;
                        INSERT INTO rivo_app.rivo_sync_virtual_rows(id,amount,adjustment) VALUES (2,NULL,6);
                        DELETE FROM rivo_app.rivo_sync_virtual_keyless WHERE amount=3 AND ROWNUM=1;
                        COMMIT;''')
                    virtual_end=sql('SELECT current_scn FROM v$database;').stdout.strip()
                rotate()
                required(lambda:row_note()=='long transaction','long Oracle transaction delivered after commit',60)
                observed('Oracle LogMiner delivers an earlier-started transaction only after its archive contains the commit')
                if options.redis:
                    online = required(lambda:redis_cut('COMMIT',1),'Oracle archived commit in native Redis',60)
                    assert online[:len(baseline)] == baseline
                    commits = [event for _,event in online if event['kind']=='COMMIT' and event['totalChanges']>0]
                    assert len(commits)==1 and commits[0]['position']['sourceType']=='ORACLE'
                    assert int(commits[0]['position']['payload']['scn'])>=int(checkpoint['scn'])
                    assert commits[0]['position']['payload']['xid']
                    observed('The native Redis event carries the full original Oracle XID and archived commit SCN')
                if options.raw_values:
                    required(lambda:raw_rows('RIVO_SYNC_RAW_ROWS',True)==['1|00ff10','2|<NULL>']
                             and raw_rows('RIVO_SYNC_RAW_KEYLESS')==['0100ff'],
                             'Oracle archived RAW update and delete',60)
                    if options.clickhouse:
                        required(lambda:ch_raw_rows('RIVO_SYNC_RAW_ROWS',True)==['1|00ff10','2|<NULL>']
                                 and ch_raw_rows('RIVO_SYNC_RAW_KEYLESS')==['0100ff'],
                                 'ClickHouse Oracle RAW keyless archived delete',90)
                        observed('ClickHouse applies the Oracle archived RAW update and removes exactly one duplicate')
                    observed('Archived Oracle RAW redo and undo update exact bytes and remove one matching keyless row')
                if options.mysql_target:
                    expected_online=['4120|2.000000000000|<NULL>','5A|12345678901234567890123456.123456789012|<NULL>']
                    required(lambda:mysql_types()==expected_online
                             and mysql_binary_keys()==['000AFF8020|2','81FF00|1'],
                             'MySQL applies both Oracle primary-key changes without collapsing neighboring keys',90)
                    observed('Oracle archived CDC changes text and binary keys in MySQL while keeping distinct values')
                if options.kingbase_target:
                    expected_online=['4120|2.000000000000|<NULL>','5A|12345678901234567890123456.123456789012|<NULL>']
                    required(lambda:kb_types()==expected_online
                             and kb_binary_keys()==['000AFF8020|2','81FF00|1'],
                             'Kingbase applies archived primary-key changes',90)
                    observed('Kingbase applies both original Oracle text and RAW key changes')
                if options.dm_target:
                    expected_online=['4120|2.000000000000|<NULL>','5A|12345678901234567890123456.123456789012|<NULL>']
                    required(lambda:dm_types()==expected_online
                             and dm_binary_keys()==['000AFF8020|2','81FF00|1'],
                             'Dameng applies archived primary-key changes',120)
                    observed('Dameng applies both original Oracle text and RAW key changes')
                if options.doris_target:
                    required(lambda:analytics_values(doris_sql,'doris_app',1),'Doris Oracle archive key changes',180)
                    observed('Doris applies both Oracle key changes and removes exactly one duplicate occurrence')
                if options.hive_target:
                    required(lambda:analytics_values(hive_sql,'hive_app',1),'Oracle rows in the Hive ACID view',240)
                    hive_prior,hive_receipts=required(lambda:hive_cut(len(source_tables)),
                                                      'Hive acknowledges the original snapshot and online SCN',240)
                    observed('Hive applies both Oracle key changes and removes exactly one duplicate occurrence')
                if options.text_json:
                    required(lambda:json.loads(target_sql(before_json))==[True,{'version':2}]
                             and target_sql('SELECT COUNT(*) FROM copy_app."RIVO_JSON_KEYLESS"')=='1',
                             'Oracle archived JSON update and keyless removal',60)
                    observed('Archived JSON updates and a keyless JSON delete preserve typed values and one occurrence')
                if options.date_values:
                    required(lambda:date_rows('RIVO_DATE_ROWS',key=True)==['1|<NULL>','2|2024-03-10 02:17:19']
                             and date_rows('RIVO_DATE_KEYLESS')==['2024-03-10 03:17:19'],
                             'Oracle DATE archived redo and undo',60)
                    observed('Archived redo and undo preserve Oracle DATE and NULL, and identify the exact keyless row across the DST gap')

                if options.virtual_columns:
                    required(lambda:virtual_rows(True)==['1|-8|4|-16|-4|-12','2|<NULL>|6|<NULL>|<NULL>|<NULL>']
                             and virtual_rows()==['3|2|6|5|1','4|1|8|5|3'],
                             'Oracle virtual NUMBER archived redo',60)
                    observed('Archived updates and SQL NULL preserve products, sums and differences and delete one exact keyless row')
                    mined=mine_archive(sql,virtual_start,virtual_end,"""
                        SELECT operation||'|'||status||'|'||csf||'|'||sql_undo||'|'||sql_redo
                        FROM v$logmnr_contents
                        WHERE seg_owner='RIVO_APP' AND src_con_name='FREEPDB1'
                          AND table_name IN ('RIVO_SYNC_VIRTUAL_ROWS','RIVO_SYNC_VIRTUAL_KEYLESS')
                          AND operation IN ('UPDATE','INSERT','DELETE')
                        ORDER BY scn,rs_id,ssn;
                    """).stdout.splitlines()
                    records=[line.strip().split('|',4) for line in mined if '|' in line]
                    (output/'native-virtual-arithmetic-redo.json').write_text(json.dumps(records,indent=2)+'\n')
                    assert sorted(row[0] for row in records)==['DELETE','INSERT','UPDATE','UPDATE'], records
                    assert all(row[1:3]==['0','0'] and all(any('"'+name+'"' in image for image in row[3:])
                               for name in ('AMOUNT','ADJUSTMENT')) for row in records), records
                    assert all(not any('"'+name+'"' in image for name in ('DERIVED','SUM_VALUE','DIFFERENCE'))
                               for row in records for image in row[3:]), records
                    observed('Native redo and undo contain both physical operands and omit all three generated values')

                stop_platform(force=True)
                offline_raw=("""
                    INSERT INTO rivo_app.rivo_sync_raw_rows VALUES(3,HEXTORAW('aa0080'));
                    UPDATE rivo_app.rivo_sync_raw_keyless SET octets=HEXTORAW('ffff00')
                        WHERE octets=HEXTORAW('0100ff');
                """ if options.raw_values else '')
                offline_mysql=("""
                    DELETE FROM rivo_app.rivo_sync_mysql_types WHERE code='A ';
                    INSERT INTO rivo_app.rivo_sync_raw_keys VALUES(HEXTORAW('00ff10'),3);
                """ if options.mysql_target or options.kingbase_target or options.dm_target or options.doris_target or options.hive_target else '')
                sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                    INSERT INTO rivo_app.rivo_orders VALUES(4,'offline');
                    DELETE FROM rivo_app.rivo_keyless WHERE ROWNUM=1;
                """+offline_raw+offline_mysql+"COMMIT;")
                if options.text_json:
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        INSERT INTO rivo_app.rivo_json_rows VALUES (4,'{"after":"restart"}',NULL);
                        UPDATE rivo_app.rivo_json_keyless SET "payload"='{"value":8}';
                        COMMIT;""")
                if options.date_values:
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        INSERT INTO rivo_app.rivo_date_rows VALUES(3,DATE '2025-01-01');
                        UPDATE rivo_app.rivo_date_keyless
                            SET event_at=TO_DATE('2024-03-10 02:11:00','YYYY-MM-DD HH24:MI:SS');
                        COMMIT;""")
                if options.virtual_columns:
                    sql('''ALTER SESSION SET CONTAINER=FREEPDB1;
                        INSERT INTO rivo_app.rivo_sync_virtual_rows(id,amount,adjustment) VALUES (3,11,NULL);
                        UPDATE rivo_app.rivo_sync_virtual_keyless SET amount=-9,adjustment=-3 WHERE amount=3;
                        COMMIT;''')
                rotate()
                required_sql='SELECT "NOTE" FROM copy_app."RIVO_ORDERS" WHERE "ID"=4'
                start_platform(config,'worker-restart.log')
                required(lambda:target_sql(required_sql)=='offline'
                         and target_sql('SELECT COUNT(*) FROM copy_app."RIVO_KEYLESS"')=='0',
                         'Oracle archive catch-up after SIGKILL',75)
                assert snapshots==target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
                observed('Oracle offline archived work replays after Controller and Worker SIGKILL using the original snapshots')
                if options.redis:
                    recovered=required(lambda:redis_cut('COMMIT',2),
                                       'Redis catches up with the original Oracle SCN after restart',60)
                    assert recovered[:len(online)]==online
                    assert {event['snapshotId'] for _,event in recovered if event['kind']=='SNAPSHOT_END'}==snapshot_events
                    observed('Oracle restart appends the offline transaction without replacing prior Redis IDs or snapshot generations')
                if options.raw_values:
                    required(lambda:raw_rows('RIVO_SYNC_RAW_ROWS',True)==['1|00ff10','2|<NULL>','3|aa0080']
                             and raw_rows('RIVO_SYNC_RAW_KEYLESS')==['ffff00'],
                             'Oracle archived RAW restart',60)
                    if options.redis:
                        raw_events=[event for _,event in recovered[len(online):]
                                    if event['kind']=='ROW']
                        images=json.dumps(raw_events)
                        for value in ('qgCA','AQD/','//8A'):
                            assert value in images,(value,images)
                    if options.clickhouse:
                        required(lambda:ch_raw_rows('RIVO_SYNC_RAW_ROWS',True)==['1|00ff10','2|<NULL>','3|aa0080']
                                 and ch_raw_rows('RIVO_SYNC_RAW_KEYLESS')==['ffff00'],
                                 'ClickHouse resumes Oracle archived RAW after restart',90)
                        observed('ClickHouse resumes the Oracle archived SCN with the original keyless identity')
                    observed('Oracle RAW bytes resume from the original SCN and retain binary row images')
                if options.mysql_target:
                    required(lambda:mysql_types()==['5A|12345678901234567890123456.123456789012|<NULL>']
                             and mysql_binary_keys()==['000AFF8020|2','00FF10|3','81FF00|1'],
                             'MySQL consumes the original archived SCN after forced restart',90)
                    assert original_mysql_snapshots==mysql_sql(mysql_snapshot_query)
                    observed('The MySQL target resumes at the original Oracle SCN and keeps each original snapshot')
                if options.kingbase_target:
                    required(lambda:kb_types()==['5A|12345678901234567890123456.123456789012|<NULL>']
                             and kb_binary_keys()==['000AFF8020|2','00FF10|3','81FF00|1'],
                             'Kingbase consumes archived Oracle work after forced restart',90)
                    assert original_kb_snapshots==kb_sql(kb_snapshot_query)
                    observed('Kingbase resumes Oracle at the original SCN and keeps each original snapshot')
                if options.dm_target:
                    required(lambda:dm_types()==['5A|12345678901234567890123456.123456789012|<NULL>']
                             and dm_binary_keys()==['000AFF8020|2','00FF10|3','81FF00|1'],
                             'Dameng consumes archived Oracle work after forced restart',120)
                    assert original_dm_snapshots==dm_sql(dm_snapshot_query)
                    observed('Dameng resumes Oracle at the original SCN and keeps each original snapshot')
                if options.doris_target:
                    required(lambda:analytics_values(doris_sql,'doris_app',2),'Oracle archived Doris restart',180)
                    observed('Doris resumes offline Oracle work on the original SCN')
                if options.hive_target:
                    required(lambda:analytics_values(hive_sql,'hive_app',2),'Oracle rows in the Hive ACID view',240)
                    hive_next,new_receipts=required(lambda:hive_cut(len(source_tables)),
                                                    'Hive acknowledges the recovered Oracle archive',240)
                    assert int(hive_next['payload']['scn'])>int(hive_prior['payload']['scn'])
                    assert hive_receipts<new_receipts,'Hive did not retain and advance its prior native receipts'
                    observed('Hive resumes offline Oracle work on the original SCN')
                if options.text_json:
                    required(lambda:target_sql('SELECT "PAYLOAD"->>\'after\' FROM copy_app."RIVO_JSON_ROWS" '
                                               'WHERE "ID"=4')=='restart'
                             and target_sql('SELECT "payload"->>\'value\' FROM copy_app."RIVO_JSON_KEYLESS"')=='8',
                             'Oracle archived JSON restart',60)
                    assert snapshots==target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
                    observed('JSON CDC resumes after SIGKILL with the original Flashback snapshot IDs')
                if options.date_values:
                    required(lambda:date_rows('RIVO_DATE_ROWS',key=True)==
                             ['1|<NULL>','2|2024-03-10 02:17:19','3|2025-01-01 00:00:00']
                             and date_rows('RIVO_DATE_KEYLESS')==['2024-03-10 02:11:00'],
                             'Oracle DATE archived restart',60)
                    assert snapshots==target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
                    observed('Oracle DATE offline insert and keyless update resume after SIGKILL with original snapshot IDs')

                if options.virtual_columns:
                    required(lambda:virtual_rows(True)==['1|-8|4|-16|-4|-12','2|<NULL>|6|<NULL>|<NULL>|<NULL>',
                                                          '3|11|<NULL>|22|<NULL>|<NULL>']
                             and virtual_rows()==['-9|-3|-18|-12|-6','4|1|8|5|3'],'Oracle virtual NUMBER restart',60)
                    assert snapshots==target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
                    observed('PostgreSQL resumes virtual NUMBER after the Oracle archived gap with its original snapshot IDs')

                if options.character_pipeline:
                    oracle_character_evidence.verify_pipeline(
                        sql,target_sql,progress,rotate,lambda:stop_platform(force=True),
                        lambda:start_platform(config,'worker-character-restart.log'),required,output,observed)
                    return

                if options.savepoint_pipeline:
                    required(lambda:target_sql('SELECT "NOTE" FROM copy_peer_app."RIVO_ORDERS" WHERE "ID"=4',
                                               checked=False)=='offline',
                             'Secondary Oracle subscription reaches the original good commit',60)

                    def receipt_and_rows(schema, metadata):
                        state=target_sql(f"""
                            SELECT json_build_object(
                                'orders',(SELECT json_agg(json_build_array("ID","NOTE") ORDER BY "ID")
                                          FROM {schema}."RIVO_ORDERS"),
                                'keyless',(SELECT json_agg("VALUE" ORDER BY "VALUE")
                                           FROM {schema}."RIVO_KEYLESS"),
                                'snapshots',(SELECT json_agg(json_build_array(table_id,snapshot_id,next_part,row_count)
                                                             ORDER BY table_id) FROM {metadata}.rm3_sink_table),
                                'checkpoint',(SELECT json_build_array(stream_seq,transaction_id,transaction_hash)
                                              FROM {metadata}.rm3_sink_job));
                        """)
                        return json.loads(state)

                    def native_cut():
                        return {key:receipt_and_rows(schema,metadata) for key,schema,metadata in (
                            ('pg','copy_app','copy_meta'),('pg_peer','copy_peer_app','copy_peer_meta'))}

                    def source_rows(scn):
                        assert scn.isdecimal()
                        result=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
                            SELECT 'ORDER|'||id||'|'||note FROM rivo_app.rivo_orders AS OF SCN {scn} ORDER BY id;
                            SELECT 'KEYLESS|'||value FROM rivo_app.rivo_keyless AS OF SCN {scn} ORDER BY value;
                            """,checked=False)
                        assert result.returncode==0,(scn,result.stdout,result.stderr)
                        lines=[line.strip().split('|',2) for line in result.stdout.splitlines() if '|' in line]
                        return {
                            'orders':[[int(row[1]),row[2]] for row in lines if row[0]=='ORDER'],
                            'keyless':[int(row[1]) for row in lines if row[0]=='KEYLESS'],
                        }

                    def drained_cut():
                        status=progress()
                        if not status.get('capture_scn') or 'captured_sequence' not in status:
                            return None
                        views=native_cut()
                        if all(item['checkpoint'][0]==int(status['captured_sequence']) for item in views.values()):
                            return status,views
                        return None

                    before_savepoint,previous=required(drained_cut,'Both target receipts reach the captured position',45)
                    assert previous['pg']['orders']==previous['pg_peer']['orders']
                    assert previous['pg']['keyless']==previous['pg_peer']['keyless']

                    def retained_cut(prior, orders, keyless, description):
                        def retained():
                            point=drained_cut()
                            if point is None:
                                return None
                            status,views=point
                            for target,view in views.items():
                                if view['orders']!=orders or view['keyless']!=keyless:
                                    return None
                                assert view['snapshots']==prior[target]['snapshots']
                                assert view['checkpoint'][0]>prior[target]['checkpoint'][0]
                                assert view['checkpoint'][1] and view['checkpoint'][2]
                            assert all(status.get(field) is None for field in ('error_code','error_stage')), status
                            return status,views
                        return required(retained,description,75)

                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        INSERT INTO rivo_app.rivo_orders VALUES (71,'savepoint-prefix-kept');
                        INSERT INTO rivo_app.rivo_keyless VALUES (887);
                        INSERT INTO rivo_app.rivo_keyless VALUES (887);
                        SAVEPOINT fixture_cut;
                        UPDATE rivo_app.rivo_orders SET note='savepoint-rolled-back' WHERE id=1;
                        DELETE FROM rivo_app.rivo_keyless WHERE value=887 AND ROWNUM=1;
                        INSERT INTO rivo_app.rivo_keyless VALUES (889);
                        ROLLBACK TO fixture_cut;
                        UPDATE rivo_app.rivo_orders SET note='savepoint-suffix-kept' WHERE id=1;
                        COMMIT;""")
                    rotate()
                    old_orders=previous['pg']['orders']
                    old_keyless=previous['pg']['keyless'] or []
                    assert all(row[0]<71 for row in old_orders) and all(value<887 for value in old_keyless)
                    orders=[[key,'savepoint-suffix-kept' if key==1 else note] for key,note in old_orders]
                    orders.append([71,'savepoint-prefix-kept'])
                    online_status,online=retained_cut(previous,orders,old_keyless+[887,887],
                                                       'Both targets reach the first committed savepoint result')
                    online_source=source_rows(online_status['capture_scn'])
                    assert online_source=={'orders':orders,'keyless':old_keyless+[887,887]},online_source
                    observed('Both PostgreSQL subscriptions apply the surviving savepoint changes and retain duplicate keyless rows')

                    stop_platform(force=True)
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        UPDATE rivo_app.rivo_orders SET note='savepoint-offline-kept' WHERE id=71;
                        SAVEPOINT offline_cut;
                        UPDATE rivo_app.rivo_orders SET id=73 WHERE id=71;
                        DELETE FROM rivo_app.rivo_orders WHERE id=1;
                        INSERT INTO rivo_app.rivo_keyless VALUES (889);
                        ROLLBACK TO offline_cut;
                        DELETE FROM rivo_app.rivo_keyless WHERE value=887 AND ROWNUM=1;
                        INSERT INTO rivo_app.rivo_orders VALUES (72,'offline-tail-kept');
                        COMMIT;""")
                    rotate()
                    assert native_cut()==online
                    start_platform(config,'worker-savepoint-restart.log')
                    offline_orders=[[key,'savepoint-offline-kept' if key==71 else note] for key,note in orders]
                    offline_orders.append([72,'offline-tail-kept'])
                    offline_status,offline=retained_cut(online,offline_orders,old_keyless+[887],
                                                         'Both original subscriptions resume with only offline survivors')
                    offline_source=source_rows(offline_status['capture_scn'])
                    assert offline_source=={'orders':offline_orders,'keyless':old_keyless+[887]},offline_source
                    assert int(before_savepoint['capture_scn'])<int(online_status['capture_scn'])<int(offline_status['capture_scn'])
                    (output/'savepoint-target-receipts.json').write_text(json.dumps({
                        'before':{'sourcePosition':before_savepoint['sourcePosition'],'targets':previous},
                        'online':{'sourcePosition':online_status['sourcePosition'],'targets':online,'flashback':online_source},
                        'restart':{'sourcePosition':offline_status['sourcePosition'],'targets':offline,'flashback':offline_source},
                    },indent=2)+'\n')
                    observed('Both original PostgreSQL receipts advance after SIGKILL with no canceled row changes or resnapshot')
                    return

                if options.ddl_review:
                    def ddl_cut():
                        return (
                            target_sql('SELECT stream_seq,transaction_id,transaction_hash FROM copy_meta.rm3_sink_job'),
                            target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id'),
                            target_sql('SELECT "ID","NOTE" FROM copy_app."RIVO_ORDERS" ORDER BY "ID"'),
                            target_sql("SELECT column_name FROM information_schema.columns WHERE table_schema='copy_app' "
                                       "AND table_name='RIVO_ORDERS' ORDER BY ordinal_position"),
                        )

                    def drained_ddl_cut():
                        status=progress()
                        if status.get('capture_scn') and status.get('captured_sequence')==target_sql(
                                'SELECT stream_seq FROM copy_meta.rm3_sink_job'):
                            return status,ddl_cut()
                        return None

                    review_start,original_cut=required(drained_ddl_cut,'Oracle target reaches the original DDL cut',45)
                    durable=review_start['capture_scn']
                else:
                    durable=progress()['capture_scn']
                sql('ALTER SESSION SET CONTAINER=FREEPDB1;\nALTER TABLE rivo_app.rivo_orders ADD EXTRA NUMBER(12);')
                rotate()
                result=required(lambda:(p if (p:=progress()).get('error_code')=='ORACLE_REDO_DDL_REQUIRES_PLAN' else None),
                                'Oracle DDL accepted as a typed refusal',60)
                assert result['capture_scn']==durable and result['error_stage']=='DDL'
                observed('Unreviewed Oracle DDL stops at the preserved SCN and exposes the DDL phase')
                if options.ddl_review:
                    route='/api/tasks/oracle_native/capture-recovery'
                    scope='CURRENT_ORACLE_PDB_CATALOG_VS_JOURNAL'

                    def report(state,columns=None,code=None):
                        current=api(route)
                        schema=current.get('schemaDrift',{})
                        if schema.get('scope')!=scope or schema.get('state')!=state:
                            return None
                        if code is not None and schema.get('code')!=code:
                            return None
                        if columns is not None:
                            changes=schema.get('tables',[])
                            if len(changes)!=1 or changes[0]['columns']['added']!=columns:
                                return None
                        return current

                    first=required(lambda:report('CHANGES_VISIBLE',['EXTRA']),'Oracle current PDB schema diff',45)
                    assert first['phase']=='DDL' and first['sourceAdmission']=='SOURCE_CHECKPOINT_AVAILABLE'
                    assert first['scope']=='UNPROVEN_SOURCE_RANGE' and first['affectedSubscriptions']==['pg']
                    assert first['actions']==['PRESERVE_SOURCE_LOGS','REVIEW_NEW_BASELINE']
                    original_table,=first['schemaDrift']['tables']
                    assert (original_table['database'],original_table['table'],original_table['change'])==(
                        'RIVO_APP','RIVO_ORDERS','TABLE_CHANGED')
                    assert original_table['recordedVersion']==1 and ddl_cut()==original_cut
                    observed('Oracle capture review reports the saved schema against the current PDB without changing either cut')

                    if options.ddl_fingerprint:
                        evidence=first.get('blockedArchiveEvent',{})
                        event=evidence.get('event',{})
                        anchor=review_start['sourcePosition']['payload']
                        upper=event.get('archiveEndScnExclusive') or sql(
                            "SELECT MAX(next_change#) FROM v$archived_log WHERE archived='YES' AND "
                            "resetlogs_change#=(SELECT resetlogs_change# FROM v$database);").stdout.strip()
                        assert upper.isdecimal() and durable.isdecimal()
                        raw=mine_archive(sql,anchor['archiveStartScn'],upper,f"""
                            SET LONG 32767 LONGCHUNKSIZE 32767
                            SELECT JSON_OBJECT('eventScn' VALUE TO_CHAR(scn),'recordSetId' VALUE TRIM(rs_id),
                                'statementSequence' VALUE TO_CHAR(ssn),'info' VALUE info,'status' VALUE status,
                                'continues' VALUE csf,'rollback' VALUE rollback,'pdbUid' VALUE TO_CHAR(src_con_uid),
                                'pdbGuid' VALUE RAWTOHEX(src_con_guid),
                                'redoHex' VALUE RAWTOHEX(UTL_RAW.CAST_TO_RAW(sql_redo)) NULL ON NULL RETURNING CLOB)
                            FROM v$logmnr_contents WHERE operation='DDL' AND src_con_name='FREEPDB1' AND scn>{durable}
                            ORDER BY commit_scn,RAWTOHEX(xid),scn,rs_id,ssn;
                        """).stdout
                        rows=[json.loads(line) for line in raw.splitlines() if line.lstrip().startswith('{')]
                        (output/'oracle-ddl-blocked-archive.json').write_text(
                            json.dumps({'report':evidence,'rows':rows},indent=2)+'\n')
                        assert evidence.get('state')=='OBSERVED',evidence
                        matches=[row for row in rows if all(row.get(key)==event.get(key)
                                 for key in ('eventScn','recordSetId','statementSequence'))]
                        item,=matches
                        assert item['status']==item['continues']==item['rollback']==0
                        kinds={'USER DDL':'USER_DDL','USER DDL (PlSql=0 RecDep=0)':'USER_DDL',
                               'INTERNAL DDL':'INTERNAL_DDL'}
                        assert kinds[item['info']]==event['kind']
                        assert hashlib.sha256(bytes.fromhex(item['redoHex'])).hexdigest()==event['decodedSqlSha256']
                        assert event['checkpointSha256']==first['checkpointHash']
                        observed('The blocked failure fingerprint matches the exact archived DDL row')

                    sql('ALTER SESSION SET CONTAINER=FREEPDB1;\n'
                        'ALTER TABLE rivo_app.rivo_orders ADD EXTRA_NOTE VARCHAR2(30 BYTE);')
                    if options.ddl_fingerprint:
                        sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                            INSERT INTO rivo_app.rivo_orders (id,note,extra_note) VALUES
                                (84,'later-load','later-unapplied');
                            COMMIT;""")
                        rotate()
                    later=required(lambda:report('CHANGES_VISIBLE',['EXTRA','EXTRA_NOTE']),
                                   'Oracle schema review sees a later DDL',45)
                    later_table,=later['schemaDrift']['tables']
                    assert later_table['recordedSha256']==original_table['recordedSha256']
                    assert later_table['liveSha256']!=original_table['liveSha256']
                    observed('A later PDB DDL updates the advisory live digest while preserving the recorded schema hash')

                    sql('ALTER SESSION SET CONTAINER=FREEPDB1;\n'
                        'ALTER TABLE rivo_app.rivo_orders ADD EXTRA_TIME TIMESTAMP(6);')
                    unavailable=required(lambda:report('UNAVAILABLE',code='ORACLE_REDO_COLUMN_TYPE_UNSUPPORTED'),
                                         'Out-of-profile Oracle schema view',45)
                    for lookup in (later,unavailable):
                        assert lookup['checkpointHash']==first['checkpointHash']
                        assert lookup['errorGeneration']==first['errorGeneration']
                        if options.ddl_fingerprint:
                            assert lookup['blockedArchiveEvent']==first['blockedArchiveEvent']
                    assert progress()['sourcePosition']==review_start['sourcePosition'] and ddl_cut()==original_cut
                    (output/'oracle-ddl-capture-review.json').write_text(
                        json.dumps({'first':first,'later':later,'unavailable':unavailable},indent=2)+'\n')
                    observed('An unsupported current Oracle shape reports its cause without moving the durable source and target cuts')
                    if options.ddl_fingerprint:
                        observed('Later archived Oracle DDL and DML leave the recorded fault fingerprint unchanged')
                if options.date_values:
                    start=sql('SELECT current_scn FROM v$database;').stdout.strip()
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        INSERT INTO rivo_app.rivo_date_rows VALUES
                            (4,TO_DATE('1900-01-02 03:04:05 BC','YYYY-MM-DD HH24:MI:SS AD','NLS_DATE_LANGUAGE=AMERICAN'));
                        COMMIT;""")
                    end=sql('SELECT current_scn FROM v$database;').stdout.strip()
                    assert start.isdecimal() and end.isdecimal() and int(end)>int(start)
                    rotate()
                    probe=folder/'date-boundary.json'
                    probe.write_text(json.dumps({
                        'sourceId':'oracle-date-boundary','pdbName':'FREEPDB1','root':endpoint(root_service),
                        'pdb':endpoint(pdb_service),'tables':['RIVO_APP.RIVO_DATE_ROWS'],
                        'archiveWindow':{'startScn':start,'endScnExclusive':end,'decodeCommitted':True},
                    }))
                    denied=execute(*program,'probe-oracle',str(probe),env=env,checked=False)
                    assert denied.returncode and 'ORACLE_DATE_VALUE_OUT_OF_RANGE' in denied.stderr, denied.stderr
                    observed('A BCE Oracle DATE is refused in a closed native archive without losing its sign')
            finally:
                stop_platform()
                for path in (folder/'state'/'logs').glob('*.log'):
                    log=path.read_text(errors='replace')
                    for name in ('RIVOMESH_ORACLE_PROBE_PASSWORD','POSTGRES_PASSWORD',
                                 'RM_ORACLE_TEST_TOKEN','CLICKHOUSE_PASSWORD','MYSQL_ROOT_PASSWORD','KINGBASE_FIXTURE_PASSWORD','RM_PG_DM_PASSWORD'):
                        if env.get(name):
                            log=log.replace(env[name],'[REDACTED]')
                    (output/(path.stem+'-redacted.log')).write_text(log)
    finally:
        if long_transaction is not None and long_transaction.poll() is None:
            long_transaction.communicate('ROLLBACK;\nEXIT\n',timeout=30)
        if started_redis:
            execute('docker','stop','--time','10',redis)
        if started_clickhouse:
            execute('docker','stop','--time','10',clickhouse,checked=False)
        if started_mysql:
            execute('docker','stop','--time','10',mysql)
        if started_kingbase:
            execute('docker','stop','--time','15',kingbase)
        if started_dameng:
            execute('docker','stop','--time','20',dameng)
        if started_doris:
            execute('docker','stop','--time','30',doris)
        if hive_document is not None:
            hive_compose('down','-v','--remove-orphans',timeout=180)
        if started:
            execute('docker','stop','--time','30',target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', required=True, type=Path)
    parser.add_argument('--jdbc-driver', required=True, type=Path)
    parser.add_argument('--java', default='java')
    parser.add_argument('--oradata-root', type=Path,
                        help='Create a disposable Oracle data directory on this Docker-shared filesystem')
    parser.add_argument('--startup-timeout', type=int, default=240,
                        help='Seconds to wait for the disposable Oracle image to become healthy')
    parser.add_argument('--image', default='container-registry.oracle.com/database/free@sha256:'
                        'f5a1791bf5628fd1a97e07584ff5ac5537da5021c23e70f5083f48c8fcd6476e')
    parser.add_argument('--logminer', action='store_true', help='Also verify archive redo and committed-only row images')
    parser.add_argument('--hive-target', action='store_true', help='Verify the Oracle source through native Hive')
    parser.add_argument('--doris-target', action='store_true', help='Verify the Oracle source through native Doris')
    parser.add_argument('--dm-target', action='store_true', help='Verify the Oracle source through native Dameng')
    parser.add_argument('--kingbase-target', action='store_true', help='Verify the Oracle source through native Kingbase PG mode')
    parser.add_argument('--mysql-target', action='store_true', help='Verify the Oracle source through native MySQL 8.4')
    parser.add_argument('--clickhouse', action='store_true', help='Materialize the Oracle RAW profile into ClickHouse')
    parser.add_argument('--raw-values', action='store_true',
                        help='Verify Flashback and archived RAW byte values through restart')
    parser.add_argument('--redis', action='store_true' '' .rstrip(), help='Add Redis Streams to the Oracle CDC restart profile')
    parser.add_argument('--raw-evidence', action='store_true', help='Inspect Flashback and archived redo for Oracle RAW')
    parser.add_argument('--pipeline', action='store_true', help='Also verify archive CDC to PostgreSQL after Worker restart')
    parser.add_argument('--text-json', action='store_true', help='Also verify constrained VARCHAR2 to PostgreSQL JSONB')
    parser.add_argument('--date-values', action='store_true', help='Also verify Oracle DATE to PostgreSQL TIMESTAMP')
    parser.add_argument('--virtual-columns', action='store_true',
                        help='Check opt-in Oracle integer virtual columns through full and archived incremental replication')
    parser.add_argument('--virtual-evidence', action='store_true',
                        help='Inspect Oracle virtual NUMBER metadata, Flashback values and actual archived DML')
    parser.add_argument('--lob-evidence', action='store_true',
                        help='Inspect native BasicFiles and SecureFiles BLOB/CLOB Flashback and archived DML')
    parser.add_argument('--lob-preflight', action='store_true',
                        help='Verify the specific LOB refusal codes from the Java probe')
    parser.add_argument('--native-json-evidence', action='store_true',
                        help='Compare Oracle native JSON Flashback values and archived before/after records')
    parser.add_argument('--character-evidence', action='store_true',
                        help='Compare Oracle Unicode, fixed padding and national character archive images')
    parser.add_argument('--character-pipeline', action='store_true',
                        help='Verify Oracle character snapshots and recovery through two PostgreSQL subscriptions')
    parser.add_argument('--ddl-review', action='store_true',
                        help='Verify the current PDB schema comparison at a preserved archived DDL fault')
    parser.add_argument('--ddl-fingerprint', action='store_true',
                        help='Match the original fault fingerprint to its archive record before later DDL and DML')
    parser.add_argument('--savepoint-evidence', action='store_true',
                        help='Compare archived partial rollback, its Flashback outcome and the exact Java probe')
    parser.add_argument('--savepoint-pipeline', action='store_true',
                        help='Verify archived savepoints and SIGKILL restart through two PostgreSQL subscriptions')
    parser.add_argument('--output', required=True, type=Path)
    options = parser.parse_args()
    if options.startup_timeout<1:
        parser.error('--startup-timeout must be positive')
    if options.hive_target and not options.raw_values:
        parser.error('--hive-target requires --raw-values')
    if options.doris_target and not options.raw_values:
        parser.error('--doris-target requires --raw-values')
    if options.dm_target and not options.raw_values:
        parser.error('--dm-target requires --raw-values')
    if options.kingbase_target and not options.raw_values:
        parser.error('--kingbase-target requires --raw-values')
    if options.mysql_target and not options.raw_values:
        parser.error('--mysql-target requires --raw-values')
    if options.clickhouse and not options.raw_values:
        parser.error('--clickhouse requires --raw-values')
    if options.raw_values and (not options.pipeline or options.text_json or options.date_values
                               or options.virtual_columns or options.savepoint_pipeline or options.character_pipeline):
        parser.error('--raw-values uses the baseline Oracle --pipeline profile')
    if options.redis and (not options.pipeline or options.text_json or options.date_values or options.virtual_columns or options.savepoint_pipeline or options.character_pipeline):
        parser.error('--redis uses the baseline Oracle --pipeline profile')
    if options.raw_evidence and not options.logminer:
        parser.error('--raw-evidence requires --logminer')
    if options.pipeline and not options.logminer:
        parser.error('--pipeline requires --logminer')
    if options.text_json and not options.pipeline:
        parser.error('--text-json requires --pipeline')
    if options.date_values and not options.pipeline:
        parser.error('--date-values requires --pipeline')
    if options.virtual_columns and not options.pipeline:
        parser.error('--virtual-columns requires --pipeline')
    if options.virtual_evidence and not options.logminer:
        parser.error('--virtual-evidence requires --logminer')
    if options.lob_evidence and not options.logminer:
        parser.error('--lob-evidence requires --logminer')
    if options.lob_preflight and not options.lob_evidence:
        parser.error('--lob-preflight requires --lob-evidence')
    if options.native_json_evidence and not options.logminer:
        parser.error('--native-json-evidence requires --logminer')
    if options.character_evidence and not options.logminer:
        parser.error('--character-evidence requires --logminer')
    if options.character_pipeline and not options.pipeline:
        parser.error('--character-pipeline requires --pipeline')
    if options.character_pipeline and (options.character_evidence or options.savepoint_pipeline):
        parser.error('--character-pipeline uses its own archive transactions and target restart')
    if options.ddl_review and (not options.pipeline or options.character_pipeline or options.savepoint_pipeline):
        parser.error('--ddl-review requires --pipeline with the normal Oracle DDL fault scenario')
    if options.ddl_fingerprint and not options.ddl_review:
        parser.error('--ddl-fingerprint requires --ddl-review')
    if options.savepoint_evidence and not options.logminer:
        parser.error('--savepoint-evidence requires --logminer')
    if options.savepoint_pipeline and not options.pipeline:
        parser.error('--savepoint-pipeline requires --pipeline')
    options.output.mkdir(parents=True, exist_ok=True)
    name = 'rivomesh-oracle-source-' + secrets.token_hex(5)
    password = 'Rm9x' + secrets.token_hex(20)
    environment = dict(os.environ, ORACLE_PWD='Rm9s' + secrets.token_hex(20),
                       RIVOMESH_ORACLE_PROBE_PASSWORD=password)
    report = {'artifactSha256': hashlib.sha256(options.artifact.read_bytes()).hexdigest(),
              'driverSha256': hashlib.sha256(options.jdbc_driver.read_bytes()).hexdigest(),
              'image': options.image, 'runtimeVersion': execute(options.java,'-version').stderr.splitlines()[0],
              'textJson': options.text_json, 'dateValues': options.date_values,
              'virtualEvidence': options.virtual_evidence, 'virtualColumns':options.virtual_columns,
              'lobEvidence':options.lob_evidence, 'lobPreflight':options.lob_preflight,
              'nativeJsonEvidence':options.native_json_evidence,
              'characterEvidence':options.character_evidence,
              'characterPipeline':options.character_pipeline,
              'ddlReview':options.ddl_review,
              'ddlFingerprint':options.ddl_fingerprint,
              'savepointEvidence':options.savepoint_evidence, 'savepointPipeline':options.savepoint_pipeline,
              'startupTimeoutSeconds':options.startup_timeout,
              'redis': options.redis, 'rawEvidence': options.raw_evidence, 'rawValues': options.raw_values,
              'clickhouse': options.clickhouse, 'mysqlTarget':options.mysql_target,
              'kingbaseTarget':options.kingbase_target, 'dmTarget':options.dm_target,
              'dorisTarget':options.doris_target, 'hiveTarget':options.hive_target, 'project': name,
              'hostMountedOracleData': options.oradata_root is not None,
              'harnessSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'checks': [], 'status': 'FAIL'}

    def sql(statement, checked=True):
        script = ('SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 32767 VERIFY OFF\n'
                  'WHENEVER SQLERROR EXIT FAILURE ROLLBACK\n' + statement + '\nEXIT\n')
        return execute('docker', 'exec', '-i', name, 'sqlplus', '-L', '-s', '/', 'as', 'sysdba',
                       stdin=script, checked=checked)

    def observed(message):
        report['checks'].append(message)
        print('PASS ' + message, flush=True)

    started = False
    oracle_data = None
    try:
        mount = []
        if options.oradata_root is not None:
            oracle_data = tempfile.TemporaryDirectory(prefix='oracle-data-', dir=options.oradata_root.resolve())
            # The parent directory stays private while the container's Oracle UID writes the disposable directory.
            os.chmod(oracle_data.name, 0o777)
            mount = ['-v', oracle_data.name + ':/opt/oracle/oradata',
                     '--tmpfs','/opt/oracle/admin:rw,size=64m,mode=0777',
                     '--tmpfs','/opt/oracle/diag:rw,size=128m,mode=0777',
                     '--tmpfs','/opt/oracle/audit:rw,size=64m,mode=0777',
                     '--tmpfs','/tmp:rw,size=256m,mode=1777']
        execute('docker', 'run', '-d', '--name', name, *mount,
                '--label', 'io.github.rivomesh.test=oracle-source', '--cpus=2', '--memory=3g',
                '--shm-size=1g', '-p', '127.0.0.1::1521', '-e', 'ORACLE_PWD',
                '-e', 'ENABLE_ARCHIVELOG=true', '-e', 'ENABLE_FORCE_LOGGING=true',
                options.image, env=environment, timeout=240)
        started = True
        def oracle_ready():
            state, health = execute('docker','inspect','-f',
                                    '{{.State.Status}} {{.State.Health.Status}}',name).stdout.split()
            if state in ('exited','dead'):
                raise RuntimeError('The isolated Oracle fixture stopped before becoming healthy')
            return health == 'healthy'

        required(oracle_ready, 'Oracle database health', options.startup_timeout)
        port = execute('docker', 'port', name, '1521/tcp').stdout.strip().rsplit(':', 1)[1]
        report['sourceVersion'] = sql('SELECT version_full FROM v$instance;').stdout.strip()
        if sql("SELECT log_mode FROM v$database;").stdout.strip() != 'ARCHIVELOG':
            sql('SHUTDOWN IMMEDIATE\nSTARTUP MOUNT\nALTER DATABASE ARCHIVELOG;\n'
                'ALTER DATABASE OPEN;\nALTER PLUGGABLE DATABASE ALL OPEN;')
        if sql('SELECT force_logging FROM v$database;').stdout.strip() != 'YES':
            sql('ALTER DATABASE FORCE LOGGING;')
        supplemental = sql('SELECT supplemental_log_data_min FROM v$database;').stdout.strip()
        if supplemental not in ('YES', 'IMPLICIT'):
            sql('ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;')
        sql(f"""CREATE USER C##RIVO_PROBE IDENTIFIED BY "{password}" CONTAINER=ALL;
            GRANT CREATE SESSION TO C##RIVO_PROBE CONTAINER=ALL;
            GRANT SELECT ON sys.v_$database TO C##RIVO_PROBE CONTAINER=ALL;
            GRANT SELECT ON sys.v_$instance TO C##RIVO_PROBE CONTAINER=ALL;
            GRANT SELECT ON sys.v_$pdbs TO C##RIVO_PROBE CONTAINER=ALL;
            GRANT SELECT ON sys.v_$thread TO C##RIVO_PROBE;
            GRANT SELECT ON sys.v_$archived_log TO C##RIVO_PROBE;
            ALTER USER C##RIVO_PROBE SET CONTAINER_DATA=(CDB$ROOT,FREEPDB1) CONTAINER=CURRENT;
            ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE USER RIVO_APP NO AUTHENTICATION DEFAULT TABLESPACE USERS QUOTA 20M ON USERS;
            CREATE TABLE rivo_app.rivo_orders(id NUMBER(12) PRIMARY KEY, note VARCHAR2(50));
            CREATE TABLE rivo_app.rivo_keyless(value NUMBER(12));
            INSERT INTO rivo_app.rivo_orders VALUES(1,'before');
            INSERT INTO rivo_app.rivo_keyless VALUES(7);
            INSERT INTO rivo_app.rivo_keyless VALUES(7);
            COMMIT;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_orders TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_keyless TO C##RIVO_PROBE;
            """)
        if options.logminer:
            sql("""GRANT LOGMINING TO C##RIVO_PROBE;
                GRANT EXECUTE ON sys.dbms_logmnr TO C##RIVO_PROBE;
                GRANT SELECT ON sys.v_$logmnr_contents TO C##RIVO_PROBE;""")
            sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                ALTER TABLE rivo_app.rivo_orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
                ALTER TABLE rivo_app.rivo_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;""")
            sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
        if options.savepoint_evidence:
            seed_savepoint_tables(sql)
        if options.native_json_evidence:
            native_json_supplemental=seed_native_json(sql)
        if options.character_evidence or options.character_pipeline:
            import oracle_character_evidence
            oracle_character_evidence.seed(sql)
        root_service = f'jdbc:oracle:thin:@//127.0.0.1:{port}/FREE'
        pdb_service = f'jdbc:oracle:thin:@//127.0.0.1:{port}/FREEPDB1'
        document = {
            'sourceId': 'oracle-fixture', 'pdbName': 'FREEPDB1',
            'root': {'url': root_service, 'username': 'C##RIVO_PROBE',
                     'password': '${RIVOMESH_ORACLE_PROBE_PASSWORD}'},
            'pdb': {'url': pdb_service, 'username': 'C##RIVO_PROBE',
                    'password': '${RIVOMESH_ORACLE_PROBE_PASSWORD}'},
            'tables': ['RIVO_APP.RIVO_ORDERS', 'RIVO_APP.RIVO_KEYLESS']
        }
        with tempfile.TemporaryDirectory(prefix='rivomesh-oracle-') as directory:
            profile = Path(directory) / 'oracle.json'
            profile.write_text(json.dumps(document))
            program = (options.java, '-cp', str(options.artifact.resolve()) + os.pathsep
                       + str(options.jdbc_driver.resolve()), 'io.github.rivomesh.Main', 'probe-oracle',
                       str(profile))
            def fresh_anchor():
                attempt=execute(*program,env=environment,checked=False)
                if attempt.returncode and 'ORA-01466' in attempt.stderr:
                    return None  # The disposable fixture has just changed the table definitions.
                attempt.check_returncode()
                return json.loads(attempt.stdout)

            result=required(fresh_anchor,'Flashback anchor after fixture DDL',20)
            anchor = result['snapshotAnchor']
            assert anchor['sourceType'] == 'ORACLE' and anchor['sourceId'] == 'oracle-fixture'
            assert anchor['payload']['scn'].isdecimal() and anchor['payload']['resetlogsScn'].isdecimal()
            assert len(anchor['payload']['pdbGuid']) == 32
            assert report['sourceVersion'] == result['sourceVersion']
            observed('Oracle root and PDB identity produce a typed DBID, incarnation, PDB GUID and SCN anchor')
            assert result['tables'] == [
                {'table': 'RIVO_APP.RIVO_ORDERS', 'primaryKey': True, 'containsRows': True},
                {'table': 'RIVO_APP.RIVO_KEYLESS', 'primaryKey': False, 'containsRows': True}]
            observed('The restricted read role can flash back keyed and keyless source tables at the same SCN')

            scn = anchor['payload']['scn']
            values = sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
                UPDATE rivo_app.rivo_orders SET note='after' WHERE id=1;
                DELETE FROM rivo_app.rivo_keyless WHERE ROWNUM=1;
                COMMIT;
                SELECT note FROM rivo_app.rivo_orders AS OF SCN {scn} WHERE id=1;
                SELECT COUNT(*) FROM rivo_app.rivo_keyless AS OF SCN {scn};
                SELECT note FROM rivo_app.rivo_orders WHERE id=1;
                SELECT COUNT(*) FROM rivo_app.rivo_keyless;
                """).stdout.split()
            assert values == ['before', '2', 'after', '1'], values
            observed('Historical SCN keeps the original row and duplicate multiplicity after source changes')

            document['pdb']['url'] = root_service
            profile.write_text(json.dumps(document))
            denied = execute(*program, env=environment, checked=False)
            assert denied.returncode != 0 and 'ORACLE_PDB_AUTHORITY_MISMATCH' in denied.stderr
            observed('A root connection substituted for the PDB is refused before flashback')
            if options.logminer:
                sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                    INSERT INTO rivo_app.rivo_orders VALUES(2,'rolled back');
                    ROLLBACK;""")
                end = sql('SELECT current_scn FROM v$database;').stdout.strip()
                assert end.isdecimal() and int(end) > int(scn)
                sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
                mined = mine_archive(sql,scn,end,"""
                    SELECT operation||'|'||commit_scn||'|'||RAWTOHEX(xid)||'|'||sql_undo||'|'||sql_redo
                    FROM v$logmnr_contents
                    WHERE seg_owner='RIVO_APP' AND table_name='RIVO_ORDERS' AND src_con_name='FREEPDB1'
                    AND operation IN ('INSERT','UPDATE','DELETE') AND status=0;
                    SELECT operation||'|'||commit_scn||'|'||RAWTOHEX(xid)||'|'||sql_undo||'|'||sql_redo
                    FROM v$logmnr_contents
                    WHERE seg_owner='RIVO_APP' AND table_name='RIVO_KEYLESS' AND src_con_name='FREEPDB1'
                    AND operation IN ('INSERT','UPDATE','DELETE') AND status=0;
                """).stdout.splitlines()
                changes = [line.strip().split('|') for line in mined if '|' in line]
                updates = [row for row in changes if row[0]=='UPDATE']
                deletes = [row for row in changes if row[0]=='DELETE']
                assert len(changes)==2 and len(updates)==len(deletes)==1, changes
                assert updates[0][1:3]==deletes[0][1:3], changes
                assert "'before'" in updates[0][3] and "'after'" in updates[0][4], changes
                assert '"VALUE"' in deletes[0][3].upper() and "'7'" in deletes[0][3], changes
                assert int(updates[0][1]) > int(scn), changes
                observed('LogMiner reads archived before/after and keyless undo with one XID and excludes the rolled-back insert')
                (options.output / 'native-synthetic-redo.json').write_text(json.dumps(changes, indent=2) + '\n')
                document['pdb']['url'] = pdb_service
                document['archiveWindow'] = {'startScn': scn, 'endScnExclusive': end}
                profile.write_text(json.dumps(document))
                coverage = json.loads(execute(*program, env=environment).stdout)['archiveWindow']
                assert coverage['kind']=='CATALOG_CONTIGUOUS'
                assert coverage['startScn']==scn and coverage['endScnExclusive']==end and coverage['segments']>0
                observed('One Oracle redo thread has continuous catalog coverage for the retained SCN window')
                document['archiveWindow']['decodeCommitted'] = True
                profile.write_text(json.dumps(document))
                typed = json.loads(execute(*program, env=environment).stdout)['archiveWindow']['decoded']
                assert typed['committedTransactions']==1 and typed['operations']=={'UPDATE':1,'DELETE':1}
                assert len(typed['contentSha256'])==64
                last=typed['lastCommit']
                assert last['sourceId']==anchor['sourceId'] and last['sourceType']=='ORACLE'
                for field in ('databaseId','resetlogsScn','pdbGuid','pdbUid'):
                    assert last['payload'][field]==anchor['payload'][field]
                assert last['payload']['scn']==updates[0][1] and last['payload']['xid']==updates[0][2]
                replayed = json.loads(execute(*program, env=environment).stdout)['archiveWindow']['decoded']
                assert replayed==typed
                observed('The Java probe decodes one committed Oracle transaction and repeats the same typed content hash')
                sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                    INSERT INTO rivo_app.rivo_orders VALUES(3,'outside archives');
                    COMMIT;""")
                live_end = sql('SELECT current_scn FROM v$database;').stdout.strip()
                assert live_end.isdecimal() and int(live_end)>int(end)
                document['archiveWindow']['endScnExclusive'] = live_end
                profile.write_text(json.dumps(document))
                missing = execute(*program, env=environment, checked=False)
                assert missing.returncode and 'ORACLE_ARCHIVE_LOG_GAP' in missing.stderr
                observed('An unarchived part of the requested SCN window is refused')
                sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
                extended = json.loads(execute(*program, env=environment).stdout)['archiveWindow']['decoded']
                assert extended['committedTransactions']==2
                assert extended['operations']=={'UPDATE':1,'DELETE':1,'INSERT':1}
                assert int(extended['lastCommit']['payload']['scn']) > int(last['payload']['scn'])
                assert extended['lastCommit']['payload']['xid']!=last['payload']['xid']
                assert extended['contentSha256']!=typed['contentSha256']
                observed('After archive rotation, a second complete commit has a distinct typed SCN and XID')
            def archive_preview(selected,start,end):
                document['tables']=selected
                document['archiveWindow']={'startScn':start,'endScnExclusive':end,'decodeCommitted':True}
                profile.write_text(json.dumps(document))
                return execute(*program,env=environment,checked=False)

            if options.lob_evidence:
                lob_preflight=lambda table,start,end:archive_preview([f'RIVO_APP.{table}'],start,end)
                lob_evidence(sql,options.output,observed,verify_preflight=lob_preflight if options.lob_preflight else None)
            if options.savepoint_evidence:
                savepoint_evidence(sql,options.output,observed,archive_preview)
            if options.native_json_evidence:
                native_json_evidence(options,sql,pdb_service,environment,observed,
                                     archive_preview,native_json_supplemental)
            if options.character_evidence:
                oracle_character_evidence.observe(sql,mine_archive,required,options.output,observed,archive_preview)
        if options.raw_evidence:
            raw_evidence(sql,options.output,observed)
        if options.pipeline:
            pipeline_profile(options,name,sql,root_service,pdb_service,environment,observed)
        if options.dm_target:
            report['dmVersion']=(options.output/'dm-version.txt').read_text().strip()
        if options.kingbase_target:
            report['kingbaseVersion']=(options.output/'kingbase-version.txt').read_text().strip()
        if options.mysql_target:
            report['mysqlVersion']=(options.output/'mysql-version.txt').read_text().strip()
        if options.clickhouse:
            report['clickhouseVersion']=(options.output/'clickhouse-version.txt').read_text().strip()
        if options.virtual_evidence:
            virtual_evidence(sql,options.output,observed)
        report['status'] = 'PASS'
    except Exception as failure:
        detail = str(failure)
        if isinstance(failure, subprocess.CalledProcessError):
            detail += '\n' + (failure.stdout or '') + '\n' + (failure.stderr or '')
        for secret in (password, environment['ORACLE_PWD']):
            detail = detail.replace(secret, '[REDACTED]')
        traces = execute('docker','logs','--tail','70',name,checked=False)
        log = detail + '\n' + traces.stdout + traces.stderr
        for secret in (password, environment['ORACLE_PWD']):
            log = log.replace(secret,'[REDACTED]')
        (options.output / 'failure.log').write_text(log + '\n')
        raise
    finally:
        if started:
            execute('docker', 'stop', '--time', '45', name, checked=False)
            cleanup = execute('docker', 'rm', '-f', '-v', name, checked=False)
            report['cleanupExitCode'] = cleanup.returncode
            report['remainingContainers'] = len(execute('docker','ps','-aq','--filter','name='+name).stdout.splitlines())
            if cleanup.returncode or report['remainingContainers']:
                report['status'] = 'FAIL'
        if oracle_data is not None:
            oracle_data.cleanup()
            report['remainingOracleDataDirectories'] = int(Path(oracle_data.name).exists())
        (options.output / 'manifest.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
