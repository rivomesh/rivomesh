package io.github.rivomesh.cdc;

import io.github.rivomesh.Sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** A statement-level witness recorded by PostgreSQL in the same transaction as each business write. */
final class PostgresTargetWriteWitness {
    static final String TRIGGER = "_rivomesh_writer_witness";
    private static final String FUNCTION = "rm3_record_target_write";

    private PostgresTargetWriteWitness() {}

    static void install(Connection connection, String metadata, String control, String schema, String table,
                        String pipeline, String target) throws SQLException {
        String relation = Sql.pg(schema) + "." + Sql.pg(table);
        String function = Sql.pg(metadata) + "." + Sql.pg(FUNCTION) + "()";
        String body = """
                BEGIN
                    IF TG_RELID <> TG_ARGV[2]::oid THEN
                        RAISE EXCEPTION 'TARGET_WRITER_FENCE_WRONG_TABLE';
                    END IF;
                    UPDATE %s SET write_seen=true
                     WHERE pipeline_id=TG_ARGV[0] AND subscription_id=TG_ARGV[1] AND phase='TARGET_WRITER';
                    IF NOT FOUND THEN
                        RAISE EXCEPTION 'TARGET_WRITER_FENCE_INACTIVE';
                    END IF;
                    RETURN NULL;
                END;
                """.formatted(control);
        String relationOid;
        try (PreparedStatement query = connection.prepareStatement("SELECT to_regclass(?)::oid::text")) {
            query.setString(1, relation);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || (relationOid = row.getString(1)) == null) throw failure("TARGET_WRITER_WITNESS_TABLE_MISSING");
            }
        }
        try (Statement sql = connection.createStatement()) {
            sql.execute("SET LOCAL standard_conforming_strings TO on");
            sql.execute("CREATE FUNCTION " + function + " RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER"
                    + " SET search_path TO pg_catalog AS " + literal(body));
            sql.execute("REVOKE ALL ON FUNCTION " + function + " FROM PUBLIC");
            sql.execute("CREATE TRIGGER " + Sql.pg(TRIGGER) + " AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON "
                    + relation + " FOR EACH STATEMENT EXECUTE FUNCTION " + Sql.pg(metadata) + "." + Sql.pg(FUNCTION)
                    + "(" + literal(pipeline) + "," + literal(target) + "," + literal(relationOid) + ")");
            sql.execute("ALTER TABLE ONLY " + relation + " ENABLE ALWAYS TRIGGER " + Sql.pg(TRIGGER));
        }
    }

    static String inspect(Connection connection, String metadata, String schema, String table, String appRole) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT tr.oid::text,f.oid::text,tr.xmin::text,f.xmin::text,pg_get_triggerdef(tr.oid),pg_get_functiondef(f.oid),
                       tr.tgenabled='A' AND tr.tgtype=60 AND tr.tgnargs=3 AND tr.tgqual IS NULL
                           AND f.prosecdef AND f.provolatile='v' AND f.pronargs=0 AND f.proconfig=ARRAY['search_path=pg_catalog']
                           AND f.prorettype='pg_catalog.trigger'::regtype AND f.proowner=r.relowner
                           AND NOT has_function_privilege(a.oid,f.oid,'EXECUTE')
                  FROM pg_trigger tr JOIN pg_class r ON r.oid=tr.tgrelid JOIN pg_namespace n ON n.oid=r.relnamespace
                  JOIN pg_proc f ON f.oid=tr.tgfoid JOIN pg_namespace fn ON fn.oid=f.pronamespace
                  CROSS JOIN pg_roles a
                 WHERE n.nspname=? AND r.relname=? AND tr.tgname=? AND NOT tr.tgisinternal
                   AND fn.nspname=? AND f.proname=? AND a.rolname=?
                   AND NOT EXISTS(SELECT 1 FROM pg_trigger extra WHERE extra.tgrelid=r.oid AND NOT extra.tgisinternal AND extra.oid<>tr.oid)
                """)) {
            query.setString(1, schema);
            query.setString(2, table);
            query.setString(3, TRIGGER);
            query.setString(4, metadata);
            query.setString(5, FUNCTION);
            query.setString(6, appRole);
            try (ResultSet row = query.executeQuery()) {
                if (!row.next() || !row.getBoolean(7)) throw failure("TARGET_WRITER_WITNESS_CHANGED");
                return Codec.sha256(Codec.encode(List.of(row.getString(1), row.getString(2), row.getString(3),
                        row.getString(4), row.getString(5), row.getString(6))));
            }
        }
    }

    static void remove(Connection connection, String metadata, String schema, String table) throws SQLException {
        try (Statement sql = connection.createStatement()) {
            sql.execute("DROP TRIGGER " + Sql.pg(TRIGGER) + " ON " + Sql.pg(schema) + "." + Sql.pg(table));
            sql.execute("DROP FUNCTION " + Sql.pg(metadata) + "." + Sql.pg(FUNCTION) + "()");
        }
    }

    private static String literal(String value) { return "'" + value.replace("'", "''") + "'"; }
    private static IllegalStateException failure(String code) { return new IllegalStateException(code); }
}
