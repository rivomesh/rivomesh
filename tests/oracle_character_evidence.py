"""Bounded Unicode and LogMiner observations for a disposable Oracle PDB."""

import json
import re


TYPES = {
    'VB': 'VARCHAR2(32 BYTE)',
    'VC': 'VARCHAR2(16 CHAR)',
    'FC': 'CHAR(8 CHAR)',
    'VN': 'NVARCHAR2(16)',
    'FN': 'NCHAR(8)',
}
TABLES = {f'RIVO_CHAR_{kind}_{key}': (kind, key == 'PK')
          for kind in TYPES for key in ('PK', 'KL')}
MARKERS = 'RIVO_CHAR_MARKER'
ORIGINAL = "é😀e\u0301' "
UPDATED = "e\u0301𝄞\\台'"
INSERTED = "中😀é' "


def _oracle_literal(value):
    if value is None:
        return 'NULL'
    if value == '':
        return "''"
    units = value.encode('utf-16-be').hex().upper()
    return "UNISTR('" + ''.join('\\' + units[i:i+4] for i in range(0,len(units),4)) + "')"


def _rows(text):
    return [json.loads(line) for line in text.splitlines() if line.lstrip().startswith('{')]


def _errors(result):
    return [line.strip() for line in (result.stdout+'\n'+result.stderr).splitlines()
            if line.lstrip().startswith(('ORA-','SP2-'))]


def _store(output, name, value):
    (output/f'oracle-char-{name}.json').write_text(json.dumps(value,indent=2)+'\n')


def seed(sql):
    ddl=['ALTER SESSION SET CONTAINER=FREEPDB1;']
    for table,(kind,keyed) in TABLES.items():
        identity='ID NUMBER(12) PRIMARY KEY' if keyed else 'TAG VARCHAR2(16 BYTE)'
        ddl.extend([
            f'CREATE TABLE RIVO_APP.{table} ({identity}, PAYLOAD {TYPES[kind]});',
            f'ALTER TABLE RIVO_APP.{table} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;',
            f'GRANT SELECT, FLASHBACK ON RIVO_APP.{table} TO C##RIVO_PROBE;',
        ])
    ddl.extend([
        f'CREATE TABLE RIVO_APP.{MARKERS} (TOKEN VARCHAR2(16 BYTE) PRIMARY KEY);',
        f'ALTER TABLE RIVO_APP.{MARKERS} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;',
    ])
    sql('\n'.join(ddl))
    dml=['ALTER SESSION SET CONTAINER=FREEPDB1;']
    for table,(_,keyed) in TABLES.items():
        values=[('1',ORIGINAL),('2',''),('3',' ')] if keyed else [
            ("'duplicate'",ORIGINAL),("'duplicate'",ORIGINAL),("'empty'",''),("'spaces'",' ')]
        dml.extend(f'INSERT INTO RIVO_APP.{table} VALUES ({key},{_oracle_literal(value)});'
                   for key,value in values)
    sql('\n'.join(dml+['COMMIT;']))


def observe(sql, mine_archive, required, output, observed, review):
    encodings=sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
        SELECT parameter||'|'||value FROM nls_database_parameters
        WHERE parameter IN ('NLS_CHARACTERSET','NLS_NCHAR_CHARACTERSET') ORDER BY parameter;""").stdout
    assert [line.strip() for line in encodings.splitlines() if '|' in line]==[
        'NLS_CHARACTERSET|AL32UTF8','NLS_NCHAR_CHARACTERSET|AL16UTF16'],encodings
    scope_names=','.join(f"'{table}'" for table in TABLES)
    columns=sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
        SELECT JSON_OBJECT('table' VALUE table_name,'type' VALUE data_type,'length' VALUE char_length,
            'sizeBytes' VALUE data_length,'semantics' VALUE char_used)
        FROM all_tab_cols WHERE owner='RIVO_APP' AND table_name IN ({scope_names})
          AND column_name='PAYLOAD' ORDER BY table_name;""").stdout
    dictionary=_rows(columns)
    assert len(dictionary)==len(TABLES)
    _store(output,'dictionary',{'databaseCharset':'AL32UTF8','nationalCharset':'AL16UTF16','columns':dictionary})

    def scn():
        position=sql('SELECT current_scn FROM v$database;').stdout.strip()
        assert position.isdecimal()
        return position

    def flashback(at, *, after_ddl=False):
        queries=['ALTER SESSION SET CONTAINER=FREEPDB1;']
        for table,(_,keyed) in TABLES.items():
            identity='TO_CHAR(ID)' if keyed else 'TAG'
            queries.append(f"""
                SELECT JSON_OBJECT('table' VALUE '{table}','key' VALUE {identity},'rowId' VALUE ROWIDTOCHAR(ROWID),
                    'textBytes' VALUE RAWTOHEX(UTL_I18N.STRING_TO_RAW(PAYLOAD,'AL32UTF8')),
                    'utf16' VALUE RAWTOHEX(UTL_I18N.STRING_TO_RAW(PAYLOAD,'AL16UTF16')),
                    'codePoints' VALUE LENGTH4(PAYLOAD),'codeUnits' VALUE LENGTH2(PAYLOAD),
                    'sourceBytes' VALUE LENGTHB(PAYLOAD),'physicalDump' VALUE DUMP(PAYLOAD,1016) NULL ON NULL)
                FROM RIVO_APP.{table} AS OF SCN {at} ORDER BY {identity},ROWID;""")
        result=sql('\n'.join(queries),checked=False)
        if after_ddl and result.returncode and 'ORA-01466' in result.stdout:
            return None
        assert result.returncode==0,(at,_errors(result))
        return _rows(result.stdout)

    def anchor():
        position=scn()
        rows=flashback(position,after_ddl=True)
        return (position,rows) if rows else None

    start,baseline=required(anchor,'Oracle character Flashback anchor after fixture DDL',30)
    assert len(baseline)==35,baseline
    _store(output,'flashback-baseline',baseline)
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    positions={'baseline':start}
    def statement(phase,table,keyed):
        if phase=='insert':
            key='4' if keyed else "'new'"
            return f'INSERT INTO {table} VALUES ({key},{_oracle_literal(INSERTED)});'
        if phase=='update':
            predicate="ID=1" if keyed else "TAG='duplicate' AND ROWNUM=1"
            return f'UPDATE {table} SET PAYLOAD={_oracle_literal(UPDATED)} WHERE {predicate};'
        if phase=='nullify':
            value,predicate=('NULL','ID=4') if keyed else ("''","TAG='new'")
            return f'UPDATE {table} SET PAYLOAD={value} WHERE {predicate};'
        predicate='ID=1' if keyed else f"TAG='duplicate' AND PAYLOAD LIKE {_oracle_literal('é%')} AND ROWNUM=1"
        return f'DELETE FROM {table} WHERE {predicate};'

    actions=('insert','update','nullify','delete')
    for phase in actions:
        statements=['ALTER SESSION SET CONTAINER=FREEPDB1;']
        statements.extend(statement(phase,f'RIVO_APP.{table}',keyed) for table,(_,keyed) in TABLES.items())
        statements.extend([f"INSERT INTO RIVO_APP.{MARKERS} VALUES ('{phase}');",'COMMIT;'])
        sql('\n'.join(statements))
        positions[phase]=scn()
    def closed_upper_bound():
        position=scn()
        return position if int(position)>int(positions['delete']) else None

    end=required(closed_upper_bound,'Oracle SCN after the last character commit',15)
    sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
    _store(output,'positions',positions)
    assert all(int(a)<int(b) for a,b in zip(positions.values(),list(positions.values())[1:]))

    def raw_rows(condition, label):
        query=f"""
            SET LONG 32767 LONGCHUNKSIZE 32767
            SELECT JSON_OBJECT('table' VALUE table_name,'operation' VALUE operation,
                'scn' VALUE TO_CHAR(scn),'commitScn' VALUE TO_CHAR(commit_scn),'xid' VALUE RAWTOHEX(xid),
                'rowId' VALUE row_id,'rsId' VALUE rs_id,'ssn' VALUE ssn,'status' VALUE status,
                'continues' VALUE csf,'rollback' VALUE rollback,'info' VALUE info,
                'redoHex' VALUE RAWTOHEX(UTL_RAW.CAST_TO_RAW(sql_redo)),
                'undoHex' VALUE RAWTOHEX(UTL_RAW.CAST_TO_RAW(sql_undo)) NULL ON NULL RETURNING CLOB)
            FROM v$logmnr_contents WHERE {condition}
            ORDER BY commit_scn,RAWTOHEX(xid),scn,rs_id,ssn;
        """
        raw=mine_archive(sql,start,end,query).stdout
        (output/f'oracle-char-{label}-oracle-output.txt').write_text(raw)
        rows=_rows(raw)
        for row in rows:
            for side in ('redo','undo'):
                value=row[side+'Hex']
                row[side]=None
                if value is not None:
                    try:
                        row[side]=bytes.fromhex(value).decode('utf-8')
                    except UnicodeDecodeError:
                        row[side+'InvalidUtf8']=True
        return rows

    markers=raw_rows(f"src_con_name='FREEPDB1' AND seg_owner='RIVO_APP' AND table_name='{MARKERS}'",'markers')
    xids={}
    for phase in actions:
        selected=[row for row in markers if row['operation']=='INSERT' and f"'{phase}'" in (row['redo'] or '')]
        assert len(selected)==1 and re.fullmatch('[0-9A-F]{16}',selected[0]['xid']),selected
        xids[phase]=selected[0]['xid']
    assert len(set(xids.values()))==len(actions)
    _store(output,'markers',{'xids':xids,'rows':markers})
    xid_scope='RAWTOHEX(xid) IN ('+','.join(f"'{xid}'" for xid in xids.values())+')'
    archive=raw_rows(xid_scope,'archive')
    replay=raw_rows(xid_scope,'archive-replay')
    _store(output,'archive',archive)
    _store(output,'archive-replay',replay)
    assert archive and archive==replay
    observed('Closed LogMiner returns identical full SQL text and physical records across the same Unicode transaction window')

    def extract(tables,column):
        def field(function,side):
            cases=[]
            for table in tables:
                call=f"SYS.DBMS_LOGMNR.{function}({side}_value,'RIVO_APP.{table}.{column}')"
                if function=='MINE_VALUE':
                    call=f"RAWTOHEX(UTL_I18N.STRING_TO_RAW({call},'AL32UTF8'))"
                cases.append(f"WHEN '{table}' THEN {call}")
            return 'CASE table_name '+' '.join(cases)+' END'
        names=','.join(f"'{table}'" for table in tables)
        query=f"""
            SELECT JSON_OBJECT('table' VALUE table_name,'xid' VALUE RAWTOHEX(xid),'operation' VALUE operation,
                'rowId' VALUE row_id,'rsId' VALUE rs_id,'ssn' VALUE ssn,
                'redoPresent' VALUE {field('COLUMN_PRESENT','redo')},
                'undoPresent' VALUE {field('COLUMN_PRESENT','undo')},
                'redoBytes' VALUE {field('MINE_VALUE','redo')},
                'undoBytes' VALUE {field('MINE_VALUE','undo')} NULL ON NULL)
            FROM v$logmnr_contents
            WHERE {xid_scope} AND seg_owner='RIVO_APP' AND table_name IN ({names})
            ORDER BY commit_scn,scn,rs_id,ssn;
        """
        result=mine_archive(sql,start,end,query,checked=False)
        return {'exitCode':result.returncode,'errors':_errors(result),'rows':_rows(result.stdout)}

    extracted={'marker':extract([MARKERS],'TOKEN')}
    for kind in TYPES:
        extracted[kind]=extract([table for table,(data_type,_) in TABLES.items() if data_type==kind],'PAYLOAD')
    _store(output,'column-extraction',extracted)
    control=extracted['marker']
    assert control['exitCode']==0 and len(control['rows'])==len(actions),control
    assert all(row['redoPresent']==1 for row in control['rows'])
    assert {bytes.fromhex(row['redoBytes']).decode('utf-8') for row in control['rows']}==set(actions)
    observed('Ordinary VARCHAR2 transaction markers validate the LogMiner value and presence functions')

    snapshots={phase:flashback(position) for phase,position in positions.items()}
    _store(output,'flashback',snapshots)
    assert snapshots['baseline']==baseline
    for snapshot in snapshots.values():
        for row in snapshot:
            if row['textBytes'] is None:
                assert row['utf16'] is None and row['codePoints'] is None,row
                continue
            text=bytes.fromhex(row['textBytes']).decode('utf-8')
            kind=TABLES[row['table']][0]
            native_charset='AL16UTF16' if kind in ('VN','FN') else 'AL32UTF8'
            assert row['utf16']==text.encode('utf-16-be').hex().upper(),row
            assert row['codeUnits']==len(text.encode('utf-16-be'))//2 and row['codePoints']==len(text),row
            assert f'CharacterSet={native_charset}' in row['physicalDump'],row

    def stored(kind,text):
        if text is None or text=='':
            return None
        units=len(text.encode('utf-16-be'))//2 if kind=='FN' else len(text)
        if kind in ('FC','FN'):
            text=text+' '*(8-units)
        return text.encode('utf-8').hex().upper()

    for table,(kind,keyed) in TABLES.items():
        for phase,rows in snapshots.items():
            actual=sorted((row['key'],row['textBytes'] or '') for row in rows if row['table']==table)
            original_value=ORIGINAL if phase in ('baseline','insert') else UPDATED
            extra=INSERTED if phase=='insert' else None
            if keyed:
                expected=[('2',None),('3',' ')]
                if phase!='delete':
                    expected.append(('1',original_value))
                if phase!='baseline':
                    expected.append(('4',extra if phase!='update' else INSERTED))
            else:
                expected=[('duplicate',original_value),('empty',None),('spaces',' ')]
                if phase!='delete':
                    expected.append(('duplicate',ORIGINAL))
                if phase!='baseline':
                    expected.append(('new',extra if phase!='update' else INSERTED))
            expected=sorted((key,stored(kind,value) or '') for key,value in expected)
            assert actual==expected,(table,phase,actual,expected)
    observed('Oracle Flashback preserves Unicode scalars, character bytes, padding, NULL and keyless multiplicities')

    comparisons=[]
    previous=snapshots['baseline']
    for phase in actions:
        after=snapshots[phase]
        before_rows={(row['table'],row['rowId']):row for row in previous}
        after_rows={(row['table'],row['rowId']):row for row in after}
        for identity in sorted(before_rows.keys()|after_rows.keys()):
            old,new=before_rows.get(identity),after_rows.get(identity)
            if old==new:
                continue
            table,row_id=identity
            kind=TABLES[table][0]
            operation='INSERT' if old is None else 'DELETE' if new is None else 'UPDATE'
            records=[row for row in archive if (row['table'],row['rowId'],row['xid'])==(table,row_id,xids[phase])]
            values=[row for row in extracted[kind]['rows']
                    if (row['table'],row['rowId'],row['xid'],row['operation'])==(table,row_id,xids[phase],operation)]
            def compare(side,expected):
                if expected is None:
                    return None
                return len(values)==1 and values[0][side+'Present']==1 and values[0][side+'Bytes']==expected['textBytes']
            comparisons.append({'phase':phase,'table':table,'operation':operation,'rowId':row_id,
                                'beforeHex':old and old['textBytes'],'afterHex':new and new['textBytes'],
                                'beforeExtracted':compare('undo',old),'afterExtracted':compare('redo',new),
                                'records':records})
        previous=after
    _store(output,'image-comparison',comparisons)
    assert len(comparisons)==40,comparisons

    probes={}
    for kind in TYPES:
        selected=[f'RIVO_APP.{table}' for table,(data_type,_) in TABLES.items() if data_type==kind]
        result=review(selected,start,end)
        if result.returncode:
            code=next((line.strip().split(': ')[-1] for line in result.stderr.splitlines() if 'ORACLE_' in line),'UNKNOWN')
            probes[kind]={'exitCode':result.returncode,'error':code}
        else:
            probes[kind]={'exitCode':0,'decoded':json.loads(result.stdout)['archiveWindow']['decoded']}
    _store(output,'product-preflight',probes)
    for kind in ('VB','VC'):
        decoded=probes[kind]['decoded']
        assert decoded['committedTransactions']==4 and decoded['operations']=={'INSERT':2,'UPDATE':4,'DELETE':2},decoded
    for kind in ('FC','VN','FN'):
        assert probes[kind]['error']=='ORACLE_REDO_COLUMN_TYPE_UNSUPPORTED',probes[kind]
    observed('Product archive preview accepts both VARCHAR2 length forms and refuses the three unadmitted types')
