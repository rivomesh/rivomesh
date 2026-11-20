package io.github.rivomesh;

import io.github.rivomesh.cdc.TableSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;

/** Reads PostgreSQL index metadata without constructing or executing target DDL. */
final class PostgresIndexCatalog {
    static final String CAPTURED = "pg.secondaryIndexes";

    private PostgresIndexCatalog() {}

    static String tableOid(Connection connection, String namespace, String table) throws Exception {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT rel.oid::text FROM pg_class rel
                JOIN pg_namespace ns ON ns.oid=rel.relnamespace
                WHERE ns.nspname=? AND rel.relname=? AND rel.relkind='r'
                """)) {
            query.setString(1, namespace);
            query.setString(2, table);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) throw new IllegalStateException("SOURCE_SCHEMA_CHANGED_REQUIRES_PLAN");
                return rows.getString(1);
            }
        }
    }

    static void read(Connection connection, String oid, TableSchema table) throws Exception {
        var indexes = new LinkedHashMap<String, TableSchema.Index>();
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT idx.relname, am.amname, i.indisunique, a.attname,
                       (i.indoption[(k.position-1)::int] & 1)<>0 AS descending,
                       i.indisvalid AND i.indisready AND i.indislive
                           AND i.indexprs IS NULL AND i.indpred IS NULL
                           AND i.indnkeyatts=i.indnatts AND NOT i.indnullsnotdistinct
                           AND opc.opcdefault AND i.indcollation[(k.position-1)::int]=a.attcollation
                           AND i.indoption[(k.position-1)::int]=0 AS ordinary,
                       pg_get_indexdef(i.indexrelid)
                FROM pg_index i
                JOIN pg_class idx ON idx.oid=i.indexrelid
                JOIN pg_am am ON am.oid=idx.relam
                CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum,position)
                LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
                LEFT JOIN pg_opclass opc ON opc.oid=i.indclass[(k.position-1)::int]
                WHERE i.indrelid=?::oid AND NOT i.indisprimary AND k.position<=i.indnkeyatts
                ORDER BY idx.relname,k.position
                """)) {
            query.setString(1, oid);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    String name = rows.getString(1);
                    TableSchema.Index index = indexes.get(name);
                    if (index == null) {
                        index = new TableSchema.Index();
                        index.name = name;
                        index.method = rows.getString(2);
                        index.unique = rows.getBoolean(3);
                        indexes.put(name, index);
                    }
                    String column = rows.getString(4);
                    if (column != null) {
                        index.columns.add(column);
                        index.prefixLengths.add(0);
                        index.descending.add(rows.getBoolean(5));
                    }
                    if (!rows.getBoolean(6)) index.expression = rows.getString(7);
                }
            }
        }
        table.indexes.addAll(indexes.values());
        table.properties.put(CAPTURED, "true");
    }
}
