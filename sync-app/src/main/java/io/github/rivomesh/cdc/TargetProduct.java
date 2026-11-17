package io.github.rivomesh.cdc;

/** Destination family is independent of its JDBC, confirmed-event, or batch transport. */
public enum TargetProduct {
    MYSQL,OCEANBASE,POSTGRESQL,GAUSS,KINGBASE,DM,GBASE_8C,H2,RABBITMQ,NATS_JETSTREAM,KAFKA,REDIS_STREAMS,PULSAR,ROCKETMQ,ACTIVEMQ_ARTEMIS,ACTIVEMQ_CLASSIC,CLICKHOUSE,DORIS,HIVE;
    public boolean mysqlDialect(){return this==MYSQL||this==OCEANBASE;}
    public boolean materialized(){return this==CLICKHOUSE||this==DORIS||this==HIVE;}
    public boolean messaging(){return this==RABBITMQ||this==NATS_JETSTREAM||this==KAFKA||this==REDIS_STREAMS||this==PULSAR||this==ROCKETMQ||this==ACTIVEMQ_ARTEMIS||this==ACTIVEMQ_CLASSIC;}
    public boolean singleMessageLog(){return this==KAFKA||this==REDIS_STREAMS||this==PULSAR||this==ROCKETMQ||this==ACTIVEMQ_ARTEMIS||this==ACTIVEMQ_CLASSIC;}
}
