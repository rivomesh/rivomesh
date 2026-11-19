# RocketMQ 5.5.0 Controller promotion fixture

[中文](#rocketmq-550-controller-切换夹具)

This fixture isolates broker-native recovery from Rivomesh's current adapter contract. It uses three real RocketMQ 5.5.0 brokers with independent data volumes and one NameServer with an embedded Controller. The Controller is a single instance for this broker-loss test. It stays online throughout the test. The broker data path uses Controller auto-switch replication, synchronous disk flush, acknowledgements from the entire in-sync set, at least two in-sync replicas, and clean elections. The Controller assigns broker roles and replica IDs. [RocketMQ Controller mode](https://rocketmq.apache.org/docs/deploymentOperations/03autofailover/)

The fixture checks the live policies and Controller/NameServer route, waits for the same one-queue topic configuration on all replicas, writes a publication owner and an event with the bundled SDK, and reads their raw commit-log positions from all three replicas. It kills the initial broker and waits for a new master epoch, a different persistent broker ID and two live in-sync members. It verifies the old topic, logical queue offset, physical CommitLog offset and event digest on both survivors, then appends another event. It also checks that the released Rivomesh adapter refuses the unsupported Controller profile before and after promotion. The separate topic-configuration check is required because a CommitLog acknowledgement can precede replication of the topic metadata.

The SDK fixture derives the same content digest as `ROCKETMQ_OFFSET_V1`. The publisher without opt-in requires `ASYNC_MASTER` with Controller and DLedger commit-log modes disabled. This fixture supplies broker-native evidence for a future adapter profile. It does not establish Rivomesh Worker HA, Controller HA, cross-version offsets, storage loss recovery, ACL, or TLS.

RocketMQ's versioned AutoSwitch replication copies data at the primary's physical offsets and can truncate an inconsistent suffix after comparing epochs. Test equality of exact offsets and bytes on the chosen version and topology. DLedger CommitLog uses a different data format. [RocketMQ 5.5.0 replication source](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/store/src/main/java/org/apache/rocketmq/store/ha/autoswitch/AutoSwitchHAClient.java), [Controller migration notes](https://rocketmq.apache.org/docs/deploymentOperations/03autofailover/).

With cached `apache/rocketmq:5.5.0` and `maven:3.9-eclipse-temurin-21` images, run:

```bash
python3 tests/rocketmq-controller-promotion.py \
  --artifact /path/to/rivomesh.jar --java-home /path/to/temurin-21 \
  --output /path/to/new-rocketmq-controller-report
```

The native ARM64 run preserved queue offset 1, physical offset 451 and the content digest when the master epoch increased from 1 to 2. The next message landed at queue offset 2 and physical offset 906. An earlier broker-native run reproduced the metadata gap: the promoted broker had the confirmed CommitLog bytes and refused topic lookup because its topic configuration had not arrived at the time of the crash.

Add `--product` to run the opt-in candidate through the same scenario. This checks the production publisher creating the generation-bound topic, authenticating its own receipts, reopening the stale SDK route on promotion and resuming from the native progress token. The final phase kills the remaining follower and requires the live-replica minimum to reject the send without changing the confirmed token or queue tail.

The fixture opens no host ports. The private output holds SDK and broker logs, versioned inputs, observed replica cuts and the cleanup result.

## RocketMQ 5.5.0 Controller 切换夹具

夹具使用三个真实 RocketMQ 5.5.0 Broker，各自使用独立数据卷。一个 NameServer 内嵌单实例 Controller，用例只停止原主 Broker，Controller 始终在线。启用 Controller 自动切换、同步刷盘、SyncStateSet 全员确认、至少两个同步副本和干净选主。角色与副本编号由 Controller 分配。[RocketMQ 主备自动切换](https://rocketmq.apache.org/zh/docs/deploymentOperations/03autofailover/)

夹具读取运行配置并对齐 Controller 与 NameServer 路由，确认所有同步副本已有相同的单队列元数据，再通过包内 SDK 写入归属记录和事件，从三个副本按原生物理位置读取。强杀原主后，检查新主选举任期、永久副本编号和两个仍存活的同步成员，核对旧主题、逻辑队列偏移、CommitLog 物理偏移和内容摘要，再确认新事件。当前发行包的适配器在切换前后都应明确拒绝此未启用的复制档案。CommitLog 确认可能早于主题元数据复制，须单独核对主题配置。

SDK 夹具使用与 `ROCKETMQ_OFFSET_V1` 一致的内容摘要算法，记录独立的 Broker 原生证据。默认发布者要求 `ASYNC_MASTER` 并关闭 Controller 模式与 DLedger CommitLog。完整 Worker 自动切换、Controller 高可用、独立存储失效、跨版本偏移、ACL 与 TLS 需分别验收。

5.5.0 的 AutoSwitch 复制按主节点物理偏移写入，并在核对任期时可能截断不一致尾部。物理偏移应在确切版本和拓扑中实测。DLedger CommitLog 数据格式不同。[RocketMQ 5.5.0 复制源码](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/store/src/main/java/org/apache/rocketmq/store/ha/autoswitch/AutoSwitchHAClient.java) · [切换和迁移说明](https://rocketmq.apache.org/zh/docs/deploymentOperations/03autofailover/)

ARM64 原生实测中，master epoch 从 1 增至 2，旧消息的队列偏移 1、物理偏移 451 和内容摘要保持一致。切换后新消息的队列偏移为 2，物理偏移为 906。较早的实测复现了元数据缺口。备用节点虽持有已确认的 CommitLog 字节，原主故障时主题配置尚未同步，因此提升后查询主题失败。

对实现了可选 Controller 档案的候选包加 `--product`，可验证产品发布者创建代次绑定主题、核对回执、主备切换时更换 SDK 连接并从原生位置续传。最后停止仍在线的备用节点，确认低于同步副本数时拒绝发送，回执与 Broker 队列末尾不变。

运行上面的命令，使用缓存的 RocketMQ 与 Temurin 21 Maven 镜像。夹具无宿主机端口，私有输出目录保存日志、输入摘要、两个选举阶段的原生位置和清理结果。
