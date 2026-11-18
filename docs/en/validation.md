# Verification scope

[Current status](implementation-status.md) · [First-release baseline](../verification/README.md) · [SDD acceptance](../sdd/001-control-recovery-compatibility/acceptance.en.md)

This entry points to the current baseline rather than repeating build/attempt histories. Separate versions/products/JDK scopes do not establish a complete compatibility matrix.

## MySQL 5.7.44 V3 profile

Java 8, Connector/J 8.4.0 and MySQL 5.7.44 linux/amd64 passed standalone fan-out, GTID asynchronous promotion and single-primary Group Replication. The checks include keyless duplicates, JSON, supported generated expressions, crash restart, DDL refusal, replica SQL failure and binlog-loss refusal with the checkpoint retained. Restore the missing logs or select an eligible source that still has them before resuming. Run `tests/matrix.sh --v3 5.7.44` to reproduce. See the [acceptance record](../verification/README.md#so-02-mysql-57-v3).
