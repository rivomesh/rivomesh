package io.github.rivomesh.cdc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Physical evidence for the small, explicitly supported PostgreSQL restoration profile. */
final class PostgresUniqueRestoreEvidence {
    private PostgresUniqueRestoreEvidence() {}

    static String observe(Connection c, String schema, TableSchema table, IndexPolicy.Decision plan,
                          String constraint) throws Exception {
        List<String> identity = new ArrayList<>();
        try (Statement sql = c.createStatement(); ResultSet r = sql.executeQuery("""
                SELECT current_setting('server_version_num')::int,current_setting('server_encoding'),
                       pg_is_in_recovery(),(SELECT system_identifier::text FROM pg_control_system()),
                       (SELECT oid::text FROM pg_database WHERE datname=current_database())
                """)) {
            if (!r.next() || r.getInt(1) / 10000 != 17 || !"UTF8".equals(r.getString(2)) || r.getBoolean(3)) {
                throw new IllegalStateException("UNIQUE_RESTORE_TARGET_PROFILE_UNSUPPORTED");
            }
            identity.add(r.getString(4));
            identity.add(r.getString(5));
        }
        String indexSql = """
                SELECT r.oid::text,pg_relation_filenode(r.oid)::text,k.oid::text,pg_relation_filenode(k.oid)::text,
                       i.indnkeyatts,i.indnatts,
                       am.amname='btree' AND NOT i.indisunique AND i.indisvalid AND i.indisready AND i.indislive
                       AND i.indpred IS NULL AND i.indexprs IS NULL,
                       EXISTS(SELECT 1 FROM pg_class proposed WHERE proposed.relnamespace=n.oid AND proposed.relname=?)
                       OR EXISTS(SELECT 1 FROM pg_constraint proposed WHERE proposed.conrelid=r.oid AND proposed.conname=?)
                  FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
                  JOIN pg_index i ON i.indrelid=r.oid
                  JOIN pg_class k ON k.oid=i.indexrelid
                  JOIN pg_am am ON am.oid=k.relam
                 WHERE n.nspname=? AND r.relname=? AND k.relnamespace=n.oid AND k.relname=?
                """;
        String indexOid;
        try (PreparedStatement sql = c.prepareStatement(indexSql)) {
            sql.setString(1, constraint);
            sql.setString(2, constraint);
            sql.setString(3, schema);
            sql.setString(4, table.name);
            sql.setString(5, plan.targetName);
            try (ResultSet r = sql.executeQuery()) {
                if (!r.next() || r.getInt(5) != plan.columns.size() || r.getInt(5) != r.getInt(6) || !r.getBoolean(7)) {
                    throw new IllegalStateException("UNIQUE_RESTORE_ORDINARY_INDEX_CHANGED");
                }
                if (r.getBoolean(8)) throw new IllegalStateException("UNIQUE_RESTORE_NAME_IN_USE");
                indexOid = r.getString(3);
                for (int i = 1; i <= 4; i++) identity.add(r.getString(i));
            }
        }
        // PostgreSQL's indkey, indclass, indcollation and indoption vectors use zero-based subscripts.
        String columnsSql = """
                SELECT a.attname,t.typname,tn.nspname,a.attgenerated='',a.attidentity='',
                       coll.collname,cn.nspname,coll.collisdeterministic,
                       op.opcdefault,onsp.nspname,i.indoption[k]=0,i.indcollation[k]=a.attcollation,a.attcollation::text
                  FROM pg_index i
                  CROSS JOIN LATERAL generate_series(0,i.indnkeyatts-1) AS key_position(k)
                  JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[k] AND NOT a.attisdropped
                  JOIN pg_type t ON t.oid=a.atttypid
                  JOIN pg_namespace tn ON tn.oid=t.typnamespace
                  LEFT JOIN pg_collation coll ON coll.oid=a.attcollation
                  LEFT JOIN pg_namespace cn ON cn.oid=coll.collnamespace
                  JOIN pg_opclass op ON op.oid=i.indclass[k]
                  JOIN pg_namespace onsp ON onsp.oid=op.opcnamespace
                 WHERE i.indexrelid=?::oid ORDER BY k
                """;
        try (PreparedStatement sql = c.prepareStatement(columnsSql)) {
            sql.setString(1, indexOid);
            try (ResultSet r = sql.executeQuery()) {
                for (String name : plan.columns) {
                    if (!r.next() || !name.equals(r.getString(1)) || !r.getBoolean(9)
                            || !"pg_catalog".equals(r.getString(10)) || !r.getBoolean(11) || !r.getBoolean(12)) {
                        throw new IllegalStateException("UNIQUE_RESTORE_ORDINARY_INDEX_CHANGED");
                    }
                    TableSchema.Column source = table.columns.get(table.column(name));
                    if (source.generationKind != null || !r.getBoolean(4) || !r.getBoolean(5)
                            || !"pg_catalog".equals(r.getString(3))) {
                        throw new IllegalStateException("UNIQUE_RESTORE_KEY_TYPE_UNSUPPORTED");
                    }
                    if (source.type == TableSchema.Type.INT64) {
                        if (!"int8".equals(r.getString(2)) || !"0".equals(r.getString(13))) {
                            throw new IllegalStateException("UNIQUE_RESTORE_KEY_TYPE_UNSUPPORTED");
                        }
                    } else if (source.type == TableSchema.Type.STRING && source.sourceType != null
                            && source.sourceType.startsWith("varchar(")) {
                        if (!"utf8mb4".equals(source.charset) || !"utf8mb4_0900_bin".equals(source.collation)
                                || !List.of("varchar", "text").contains(r.getString(2))
                                || !"C".equals(r.getString(6)) || !"pg_catalog".equals(r.getString(7)) || !r.getBoolean(8)) {
                            throw new IllegalStateException("UNIQUE_RESTORE_COLLATION_UNSUPPORTED");
                        }
                    } else {
                        throw new IllegalStateException("UNIQUE_RESTORE_KEY_TYPE_UNSUPPORTED");
                    }
                    identity.add(name);
                    identity.add(r.getString(2));
                    identity.add(r.getString(13));
                }
                if (r.next()) throw new IllegalStateException("UNIQUE_RESTORE_ORDINARY_INDEX_CHANGED");
            }
        }
        return Codec.sha256(Codec.encode(identity));
    }
}
