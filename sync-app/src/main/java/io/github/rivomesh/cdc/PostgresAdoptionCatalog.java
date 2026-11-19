package io.github.rivomesh.cdc;

import io.github.rivomesh.TargetOwnership;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/** Native DDL evidence for the single-table PostgreSQL adoption profile. */
final class PostgresAdoptionCatalog {
    private PostgresAdoptionCatalog() {}

    static String inspect(Connection connection, String schema, TableSchema table, String targetMetadata,
                          String journalNamespace, boolean adopted, IndexPolicy.Decision candidate, boolean restored) throws Exception {
        List<String> evidence = new ArrayList<>();
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT current_setting('server_version_num')::int,current_setting('server_encoding'),
                       pg_is_in_recovery(),current_setting('fsync'),
                       (SELECT system_identifier::text FROM pg_control_system()),
                       (SELECT oid::text FROM pg_database WHERE datname=current_database()),
                       substring(pg_walfile_name(pg_current_wal_insert_lsn()),1,8),
                       EXISTS(SELECT 1 FROM pg_namespace WHERE nspname IN (?,?)),
                       EXISTS(SELECT 1 FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
                               WHERE n.nspname=? AND r.relname=?),
                       EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtenabled<>'D'),
                       has_database_privilege(current_database(),'CREATE')
                """)) {
            query.setString(1, targetMetadata);
            query.setString(2, journalNamespace);
            query.setString(3, schema);
            query.setString(4, TargetOwnership.TABLE);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || row.getInt(1) / 10000 != 17 || !"UTF8".equals(row.getString(2))
                        || row.getBoolean(3) || !"on".equals(row.getString(4))) {
                    throw new IllegalStateException("ADOPTION_PLAN_TARGET_PROFILE_UNSUPPORTED");
                }
                if (!adopted && (row.getBoolean(8) || row.getBoolean(9))) throw new IllegalStateException("ADOPTION_PLAN_METADATA_PRESENT");
                if (!adopted && row.getBoolean(10)) throw new IllegalStateException("ADOPTION_PLAN_TARGET_EVENT_TRIGGER_PRESENT");
                if (!row.getBoolean(11)) throw new IllegalStateException("ADOPTION_PLAN_TARGET_OWNERSHIP_REQUIRED");
                evidence.addAll(List.of(row.getString(5), row.getString(6), row.getString(7)));
            }
        }
        String tableOid;
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT r.oid::text,pg_relation_filenode(r.oid)::text,r.relowner::text,n.oid::text,n.nspowner::text,
                       p.oid::text,i.indexrelid::text,pg_relation_filenode(i.indexrelid)::text,
                       pg_has_role(r.relowner,'USAGE') AND has_schema_privilege(n.oid,'CREATE'),
                       NOT EXISTS(SELECT 1 FROM pg_subscription_rel WHERE srrelid=r.oid),
                       (SELECT count(*) FROM pg_index other WHERE other.indrelid=r.oid)=?
                  FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace
                  JOIN pg_am heap ON heap.oid=r.relam
                  JOIN pg_constraint p ON p.conrelid=r.oid AND p.contype='p'
                  JOIN pg_index i ON i.indexrelid=p.conindid AND i.indrelid=r.oid
                  JOIN pg_class k ON k.oid=i.indexrelid AND k.relnamespace=n.oid
                  JOIN pg_am method ON method.oid=k.relam
                  JOIN pg_opclass op ON op.oid=i.indclass[0]
                  JOIN pg_attribute a ON a.attrelid=r.oid AND a.attnum=i.indkey[0]
                 WHERE n.nspname=? AND r.relname=? AND a.attname=?
                   AND heap.amname='heap' AND r.relkind='r' AND r.relpersistence='p'
                   AND p.convalidated AND NOT p.condeferrable AND NOT p.condeferred
                   AND method.amname='btree' AND op.opcname='int8_ops' AND op.opcdefault
                   AND op.opcnamespace='pg_catalog'::regnamespace AND i.indcollation[0]=0 AND i.indoption[0]=0
                   AND i.indisprimary AND i.indisunique AND i.indimmediate AND i.indisvalid AND i.indisready AND i.indislive
                   AND NOT i.indnullsnotdistinct AND i.indnkeyatts=1 AND i.indnatts=1
                   AND i.indpred IS NULL AND i.indexprs IS NULL
                """)) {
            query.setInt(1, candidate == null ? 1 : restored ? 3 : 2);
            query.setString(2, schema);
            query.setString(3, table.name);
            query.setString(4, table.primaryKey.get(0));
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !row.getBoolean(11)) throw new IllegalStateException("ADOPTION_PLAN_TARGET_INDEX_MISMATCH");
                if (!row.getBoolean(9)) throw new IllegalStateException("ADOPTION_PLAN_TARGET_OWNERSHIP_REQUIRED");
                if (!row.getBoolean(10)) throw new IllegalStateException("ADOPTION_PLAN_TARGET_REPLICATION_CONFIGURED");
                tableOid = row.getString(1);
                for (int column = 1; column <= 8; column++) evidence.add(row.getString(column));
            }
        }
        checkColumns(connection, tableOid, table);
        if (candidate != null) {
            evidence.add(PostgresAdoptedUnique.ordinaryHash(connection, schema, table, candidate, restored));
        }
        return Codec.sha256(Codec.encode(evidence));
    }

    private static void checkColumns(Connection connection, String oid, TableSchema table) throws Exception {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attcollation=0,
                       c.collname='C' AND c.collnamespace='pg_catalog'::regnamespace AND c.collencoding=-1,
                       a.atthasdef OR a.attgenerated<>'' OR a.attidentity<>''
                  FROM pg_attribute a LEFT JOIN pg_collation c ON c.oid=a.attcollation
                 WHERE a.attrelid=?::oid AND a.attnum>0 AND NOT a.attisdropped
                 ORDER BY a.attnum
                """)) {
            query.setString(1, oid);
            try (ResultSet row = query.executeQuery()) {
                for (TableSchema.Column column : table.columns) {
                    String type = switch (column.type) {
                        case INT64 -> "bigint";
                        case JSON -> "jsonb";
                        case STRING -> "character varying(" + column.length + ")";
                        default -> throw new IllegalStateException("ADOPTION_PLAN_SOURCE_TYPE_UNSUPPORTED");
                    };
                    if (!row.next() || !column.name.equals(row.getString(1)) || !type.equals(row.getString(2))
                            || row.getBoolean(3) == column.nullable || row.getBoolean(6)
                            || !row.getBoolean(column.type == TableSchema.Type.STRING ? 5 : 4)) {
                        throw new IllegalStateException("ADOPTION_PLAN_TARGET_COLUMN_MISMATCH");
                    }
                }
                if (row.next()) throw new IllegalStateException("ADOPTION_PLAN_TARGET_COLUMN_MISMATCH");
            }
        }
    }
}
