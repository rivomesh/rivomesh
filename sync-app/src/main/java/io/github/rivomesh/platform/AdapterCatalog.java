package io.github.rivomesh.platform;

import java.util.*;

/** Availability is explicit; planned connectors are never selectable as working adapters. */
public final class AdapterCatalog {
    private AdapterCatalog() {}
    public static List<Map<String,String>> entries() {
        List<Map<String,String>> rows=new ArrayList<>();
        add(rows,"MYSQL","SOURCE","AVAILABLE","5.7.44, 8.0.46, 8.2.0 and 8.4.13 V3 standalone, GTID promotion and single-primary Group Replication");
        add(rows,"GAUSS_POSTGRESQL","SINK","PARTIALLY_VERIFIED","openGauss 7.0.0-RC3 verified; distributed/commercial GaussDB unverified");
        add(rows,"POSTGRESQL","SINK","AVAILABLE","PostgreSQL 17.12 snapshot / CDC / restart / JSON verified");
        add(rows,"MYSQL","SINK","AVAILABLE","V3 Web/CLI: separate MySQL 8.4 instance; PG/Gauss/MySQL fan-out and both journal recovery modes verified");
        add(rows,"RABBITMQ","SINK","PARTIALLY_VERIFIED","4.1.4 three-member quorum queue: leader loss, minority refusal, replay, and consumer acknowledgement verified; TLS and capacity profiles pending");
        add(rows,"NATS_JETSTREAM","SINK","PARTIALLY_VERIFIED","2.11.8 File stream: worker/broker restart and three replicas with client TLS, leader loss, loss of majority and retry verified; client certificates, node-to-node TLS and scale remain open");
        add(rows,"KAFKA","SINK","PARTIALLY_VERIFIED","3.9.2 dedicated single-partition topic: KRaft data leader election, insufficient ISR refusal and original consumer offset recovery verified; full controller-loss/security profiles pending");
        add(rows,"REDIS_STREAMS","SINK","PARTIALLY_VERIFIED","7.4.2 direct endpoint: AOF replica promotion, WAITAOF, pending-entry and consumer acknowledgement recovery verified; Sentinel/Cluster/TLS pending");
        add(rows,"PULSAR","SINK","PARTIALLY_VERIFIED","4.0.14 persistent non-partitioned topic, isolated Java 8 SDK; transport, snapshot/CDC, worker kill and three broker stop/start rounds verified; single-broker TLS/JWT, certificate refusal and restart verified; multi-broker and BookKeeper failure pending");
        add(rows,"ROCKETMQ","SINK","PARTIALLY_VERIFIED","Native TCP client 5.5.1 / broker 5.5.0: local SYNC_FLUSH, one queue, snapshot/CDC and restart verified. Optional Controller profile checks three data replicas, clean election, two live synchronized copies and native receipts. ACL/TLS and full CDC Worker failover require separate acceptance.");
        add(rows,"ACTIVEMQ_ARTEMIS","SINK","PARTIALLY_VERIFIED","2.57.0 broker / isolated Java 8 Core client 2.19.1 with Netty 4.1.130: transport, persistent non-destructive queue, consumer ACK, broker/client restart and native receipt refusal of an older same-ID nonempty backup verified; mixed pipeline/security/cluster acceptance pending");
        add(rows,"CLICKHOUSE","SINK","PARTIALLY_VERIFIED","25.8.33.6 native HTTP/RowBinary, versioned FINAL views, keyless/PK replay and old nonempty backup refusal verified; MySQL snapshot/CDC, independent outage, worker restart and missing-view isolation verified; single direct server profile");
        add(rows,"ACTIVEMQ_CLASSIC","SINK","PARTIALLY_VERIFIED","5.19.12 single KahaDB broker / Java8 client5.16.8: persistent local transactions, selected-consumer ACK, wrong-selector/purge guards, restart and actual older nonempty backup refusal verified; MySQL8.4+PostgreSQL17 snapshot/CDC, broker outage, worker kill, SQL failure/retry and purge isolation verified; TLS/cluster/capacity acceptance pending");
        add(rows,"DORIS","SINK","PARTIALLY_VERIFIED","4.1.3 ARM64 / Java8 native Stream Load, Unique MoW views and durable receipts; native restart/old nonempty backup refusal and actual MySQL8.4+PostgreSQL17 snapshot/CDC/outage/catchup/worker kill/SQL repair/view-loss isolation verified; clusters/TLS/capacity/other JDKs pending");
        add(rows,"POSTGRESQL","SOURCE","EXPERIMENTAL","17.12 and 18.7 pgoutput; 17.12 synchronized-slot promotion and 18.7 stored generated columns verified");
        add(rows,"ORACLE","SOURCE","EXPERIMENTAL","26ai Free single PDB: Flashback snapshot and archived LogMiner to PostgreSQL, durable SCN restart and DDL refusal verified");
        add(rows,"OCEANBASE","SINK","EXPERIMENTAL","CE 4.3.5.6 ARM64 / Java 8 MySQL-mode tenant: native snapshots, CDC, target outage, Worker restart and isolated SQL repair passed; Oracle mode and clustered profiles pending");
        add(rows,"HIVE","SINK","EXPERIMENTAL","Hive4.0.1 / Java8 ACID ORC history/views; query-results cache must be disabled for writer/readers; experimental first profile, see versioned acceptance records; not a transactional JDBC sink");
        add(rows,"KINGBASE","SINK","EXPERIMENTAL","V009R001C001B0025 PG/UTF8 / Java8 driver9.0.1: native transactions and actual MySQL8.4+PostgreSQL17 snapshot/CDC, signed microsecond TIME/keyless, target outage, worker kill, SQL repair and missing-table isolation verified; first profile only, other modes/RAC/distributed/security/capacity pending");
        add(rows,"DM","SINK","EXPERIMENTAL","DM8.1.4.80 native0/UTF8/blank1 with Java8 driver8.1.3.140: native transactions, reviewed JSON/wide-decimal text, empty-layout guards and actual MySQL8.4+PostgreSQL17 snapshot/CDC/TIME/text-key/outage/worker-kill/SQL-repair/missing-table isolation verified; other modes/clusters/security/capacity pending");
        add(rows,"GBASE_8C","SINK","EXPERIMENTAL","V6_S1.3.0B19 PG/UTF8 / vendor JDBC3.0.0B02: first native transactions and actual MySQL8.4+PostgreSQL17 snapshot/CDC/TIME/keyless/text-key/outage/worker-kill/SQL-repair/missing-table isolation verified; other modes/distributed/security/capacity pending; 8a/8s are separate families");
        for(String id:Arrays.asList("GBASE_8A","GBASE_8S"))
            add(rows,id,"SINK","PLANNED","");
        return rows;
    }
    private static void add(List<Map<String,String>> rows,String id,String role,String status,String note) {
        Map<String,String> row=new LinkedHashMap<>();row.put("id",id);row.put("role",role);row.put("status",status);row.put("note",note);
        if("SINK".equals(role)){
            boolean reviewedSql="POSTGRESQL".equals(id)||"MYSQL".equals(id);
            row.put("transactionSkip",reviewedSql?"REVIEWED_LOCAL_TRANSACTION":"UNAVAILABLE");
            row.put("transactionCompensation",reviewedSql?"REVIEWED_BEFORE_IMAGES":"UNAVAILABLE");
            row.put("rebuildMode","PLANNED".equals(status)?"UNAVAILABLE":"MANUAL_NEW_BASELINE");
            row.put("failureReceiptReview",switch(id){
                case "KAFKA"->"NATIVE_KAFKA_PRECEDING_COMMIT";
                case "CLICKHOUSE","DORIS","HIVE"->"NATIVE_MATERIALIZED_PRECEDING_COMMIT";
                case "ACTIVEMQ_ARTEMIS","ACTIVEMQ_CLASSIC","ROCKETMQ"->"NATIVE_BROKER_PRECEDING_COMMIT";
                default->"UNAVAILABLE";
            });
            row.put("gapNotification","KAFKA".equals(id)?"MANAGED_PHYSICAL_KAFKA":"UNAVAILABLE");
        }
        rows.add(row);
    }
}
