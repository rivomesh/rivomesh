# 验证范围

[当前实施状态](zh-CN/implementation-status.md) · [首版验证基线](verification/README.md) · [SDD验收](sdd/001-control-recovery-compatibility/acceptance.md)

本页统一指向当前基线，不再重复记录逐次构建、失败尝试和旧发布候选。不同版本/产品/JDK的记录不能合并为全矩阵通过。

## MySQL 5.7.44 当前 V3 档案

早期 Java 8 记录使用 Connector/J 8.4.0 和 MySQL 5.7.44 linux/amd64，已通过单机多库多目标、GTID 主从切换和单主组复制。当前复验使用 Eclipse Temurin 21 LTS。覆盖无主键重复行、JSON、支持的生成列表达式、进程强杀续传、DDL 暂停、主从 SQL 错误与日志缺失保护。日志缺失时保留原断点，恢复缺失日志或切换到持有完整日志的合格节点后再恢复。执行 `tests/matrix.sh --v3 5.7.44` 复验。见[验收摘要](verification/README.md#so-02-mysql-57-v3)。
