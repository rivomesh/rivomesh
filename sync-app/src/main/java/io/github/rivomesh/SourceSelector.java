package io.github.rivomesh;

import io.github.rivomesh.Model.Position;
import java.sql.*;
import java.util.*;

/** Candidate endpoints belong to one operator-managed replication topology; this class never promotes servers. */
public final class SourceSelector {
    static final class Probe {
        Config.Endpoint endpoint;
        String uuid, executed, purged, groupId;
        long serverId;
        boolean writable, groupMember, groupPrimary, groupOnline, singlePrimary = true, replicationHealthy = true;
        String reason;
        boolean eligible(String topology) {
            if (!writable) return false;
            if ("GROUP_REPLICATION".equals(topology) && !groupMember) return false;
            if ("ASYNC".equals(topology) && groupMember) return false;
            return groupMember ? groupPrimary && groupOnline && singlePrimary : replicationHealthy;
        }
    }
    private final Config config;
    public SourceSelector(Config config) { this.config = config; }

    public MysqlSource select(Position captured, String required) throws Exception {
        List<Probe> probes = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Config.Endpoint endpoint : config.endpoints()) {
            try {
                Probe probe = probe(config, endpoint);
                if (seen.add(probe.uuid)) probes.add(probe);
            } catch (SQLException e) {
                // Permission/configuration errors must not be mistaken for an unreachable standby.
                if (!Sql.retryable(e)) throw e;
            }
        }
        Map<Long, String> serverIds = new HashMap<>();
        for (Probe probe : probes) {
            String previous = serverIds.put(probe.serverId, probe.uuid);
            if ((previous != null && !previous.equals(probe.uuid)) || probe.serverId == config.source.serverId)
                throw new IllegalArgumentException("MySQL 节点及复制客户端的 server_id 必须互不相同");
        }
        Probe chosen = choose(probes, config.source.topology, captured == null ? null : captured.gtidSet, required);
        return new MysqlSource(config, chosen.endpoint, chosen);
    }

    static Probe choose(List<Probe> probes, String topology, String captured, String required) throws SyncFailure {
        List<Probe> primary = new ArrayList<>();
        for (Probe probe : probes) if (probe.eligible(topology)) primary.add(probe);
        if (primary.size() > 1) throw new SyncFailure("SOURCE_MULTIPLE_WRITERS", "SOURCE", true, "检测到多个可写主库，等待数据库高可用系统完成隔离");
        if (primary.isEmpty()) {
            for (Probe probe : probes) if (probe.reason != null)
                throw new SyncFailure("SOURCE_REPLICATION_ERROR", "SOURCE", true, "源复制状态异常: " + probe.endpoint + "，" + probe.reason);
            throw new SyncFailure("SOURCE_NO_PRIMARY", "SOURCE", true, "没有健康的可写主库；检查主从 IO/SQL 线程及 Group Replication 状态");
        }
        Probe chosen = primary.get(0);
        if (captured != null && !Gtids.contains(chosen.executed, required))
            throw new SyncFailure("SOURCE_LAGGING", "SOURCE", true, "候选主库缺少已采集或全量已读取所依赖的 GTID，等待追平，不回退进度");
        if (captured != null && !Gtids.contains(captured, chosen.purged))
            throw new SyncFailure("SOURCE_LOG_GAP", "SOURCE", false, "候选主库已清理尚未采集的 GTID 日志，需要恢复日志或选择日志完整的主库");
        return chosen;
    }

    static Probe probe(Config config, Config.Endpoint endpoint) throws SQLException {
        Probe probe = new Probe(); probe.endpoint = endpoint;
        try (Connection c = new MysqlSource(config, endpoint, null).connect(); Statement s = c.createStatement()) {
            int major,minor,patch;
            try(ResultSet rs=s.executeQuery("SELECT VERSION()")){
                rs.next();java.util.regex.Matcher version=java.util.regex.Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)").matcher(rs.getString(1));
                if(!version.find())throw new IllegalArgumentException("MYSQL_VERSION_UNRECOGNIZED");
                major=Integer.parseInt(version.group(1));minor=Integer.parseInt(version.group(2));patch=Integer.parseInt(version.group(3));
                if(major<5||(major==5&&minor<7))throw new IllegalArgumentException("MYSQL_57_OR_NEWER_REQUIRED");
                String versionPrefix=major+"."+minor;
                if(config.source.expectedVersion!=null&&!config.source.expectedVersion.equals(versionPrefix)
                        &&!config.source.expectedVersion.equals(versionPrefix+"."+patch))
                    throw new IllegalArgumentException("MYSQL_VERSION_PROFILE_MISMATCH");
            }
            boolean mysql8=major>=8;
            boolean replicaNames=major>8||(major==8&&(minor>0||patch>=22));
            String rowOptions=mysql8?"@@global.binlog_row_value_options":"''";
            String compression=major>8||(major==8&&(minor>0||patch>=20))?"@@global.binlog_transaction_compression":"0";
            String replicaUpdates=major>8||(major==8&&(minor>0||patch>=26))?"@@global.log_replica_updates":"@@global.log_slave_updates";
            try (ResultSet rs = s.executeQuery("SELECT @@global.log_bin,@@global.binlog_format,@@global.binlog_row_image,@@global.server_uuid,"
                    + rowOptions+","+compression+",@@global.gtid_mode,@@global.enforce_gtid_consistency,"
                    + "@@global.gtid_executed,@@global.gtid_purged,@@global.read_only,@@global.super_read_only,@@global.server_id,"+replicaUpdates)) {
                rs.next();
                if (!rs.getBoolean(1) || !"ROW".equalsIgnoreCase(rs.getString(2)) || !"FULL".equalsIgnoreCase(rs.getString(3))
                        || !rs.getString(5).isEmpty() || rs.getBoolean(6) || !"ON".equalsIgnoreCase(rs.getString(7))
                        || !"ON".equalsIgnoreCase(rs.getString(8)) || !rs.getBoolean(14))
                    throw new IllegalArgumentException("源节点必须启用 GTID、enforce_gtid_consistency、log_replica_updates/log_slave_updates、ROW/FULL binlog，并关闭适用版本的事务压缩和部分 JSON 行镜像: " + endpoint);
                probe.uuid = rs.getString(4); probe.executed = Gtids.parse(rs.getString(9)).toString(); probe.purged = Gtids.parse(rs.getString(10)).toString();
                probe.writable = !rs.getBoolean(11) && !rs.getBoolean(12); probe.serverId = rs.getLong(13);
            }
            String groupColumns=mysql8?"MEMBER_ROLE,MEMBER_STATE":"MEMBER_STATE";
            try (ResultSet rs = s.executeQuery("SELECT "+groupColumns+" FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@global.server_uuid")) {
                if (rs.next()) {
                    probe.groupMember = true; probe.groupPrimary = mysql8&&"PRIMARY".equals(rs.getString(1));
                    String memberState=rs.getString(mysql8?2:1);probe.groupOnline = "ONLINE".equals(memberState);
                    if (!probe.groupOnline) probe.reason = "Group Replication member_state=" + memberState;
                }
            } catch (SQLException e) { if (!"42S02".equals(e.getSQLState())) throw e; }
            if (probe.groupMember) {
                try (ResultSet rs = s.executeQuery("SELECT @@global.group_replication_group_name,@@global.group_replication_single_primary_mode")) {
                    rs.next(); probe.groupId = rs.getString(1); probe.singlePrimary = rs.getBoolean(2);
                }
                if(!mysql8)try(ResultSet rs=s.executeQuery("SHOW GLOBAL STATUS LIKE 'group_replication_primary_member'")){
                    probe.groupPrimary=rs.next()&&probe.uuid.equals(rs.getString(2));
                }
            } else {
                String io=replicaNames?"Replica_IO_Running":"Slave_IO_Running",sql=replicaNames?"Replica_SQL_Running":"Slave_SQL_Running";
                try (ResultSet rs = s.executeQuery(replicaNames?"SHOW REPLICA STATUS":"SHOW SLAVE STATUS")) {
                    while (rs.next()) {
                        if (rs.getInt("Last_SQL_Errno") != 0 || rs.getInt("Last_IO_Errno") != 0
                                || "Yes".equalsIgnoreCase(rs.getString(io)) || "Yes".equalsIgnoreCase(rs.getString(sql)))
                            { probe.replicationHealthy = false; probe.reason = "replica_io_errno=" + rs.getInt("Last_IO_Errno") + ",replica_sql_errno=" + rs.getInt("Last_SQL_Errno") + ",io_running=" + rs.getString(io) + ",sql_running=" + rs.getString(sql); }
                    }
                }
            }
        }
        return probe;
    }
}
