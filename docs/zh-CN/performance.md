# 性能测量与硬件参考

English: [Performance](../en/performance.md).

“多少硬件能达到多少性能”必须来自明确负载的可复现测量。CPU、内存、存储延迟、网络、行宽、事务大小、表数量、目标数量、索引、日志模式和其他负载都会影响结果，不能脱离这些条件承诺吞吐或恢复时间。

## 复现首个基准

先构建发布包，测量过程中不要重新构建该 JAR。需要 Python 3、Docker Compose 和夹具使用的三个镜像。

```bash
mvn -s build/settings.xml -DskipTests package
python3 tests/benchmark.py --rows 10000 --payload-bytes 256 \
  --storage-description '填写物理磁盘、Docker 虚拟机、CPU/内存及同时运行的其他负载'
```

macOS 可以在 Python 命令前加 `caffeinate -i` 防止空闲休眠；合盖、手动休眠及其他挂起仍会使计时失效。指定资源条件的例子：

```bash
RIVOMESH_BENCH_CPUS=2 RIVOMESH_BENCH_DB_MEMORY=2g \
RIVOMESH_BENCH_WORKER_MEMORY=1g python3 tests/benchmark.py \
  --rows 100000 --chunk-size 1000 --payload-bytes 1024 --timeout 1800 \
  --storage-description '填写实际存储和宿主机资源竞争情况'
```

CPU 和数据库内存限制分别作用于**每个**数据库容器，CPU 限制同时作用于 Worker。默认每个容器 1 CPU、1 GiB 内存；Java 堆 256 MiB，数据库缓冲区 256 MiB。这是上限，不代表宿主机为其预留或独占资源。报告记录实际镜像 ID、Docker 虚拟机 CPU/内存、容器限制、数据库持久化参数、JVM 版本、负载及发布包 SHA-256。本地日志为宿主机绑定目录，数据库文件为夹具数据卷，应分别记录对应物理存储。

每轮使用随机 Compose 项目，复制发布包到 `target/benchmarks/<run-id>/app.jar`。只创建自己的源库/目标库，不映射宿主端口，结束时只清理本项目的容器和数据卷。复制的发布包、私有夹具配置、有界日志及 `report.json` 留供复核。失败结果为 `status: FAILED`，不能纳入性能参考表；启动或运行错误查看 `containers.log`。

## 指标含义

| 字段 | 含义 |
|---|---|
| `snapshotVisibilitySeconds` | 从启动 Worker 到最初全部目标行可见，包含 JVM 启动、发现和自动建表 |
| `snapshotRowsPerSecond` | 初始行数除以上述可见耗时 |
| `sourceMutationSeconds` | 源端提交更新、删除和插入的耗时 |
| `cdcCatchUpSeconds` | 从首次源端变更到目标最终状态可见，包含源端写入 |
| `cdcChangesPerSecond` | 变更行数（含删除和新增）除以上述耗时 |
| `afterWritesCatchUpSeconds` | 源端写完后，到观察到目标最终状态的耗时 |
| `snapshotContentSha256`、`cdcContentSha256` | 源/目标按 ID 排序的行字节流必须一致，覆盖 ID、版本和正文原始字节 |

每个源事务最多变更 500 行。校验采用流式读取，不把全部数据集载入 Python 内存。两阶段计时结束后分别执行字节校验，仅行数一致不能产生通过结果。最终目标需要同时包含更新、预期删除和新增。轮询与 Docker CLI 开销包含在观察耗时内。

首个配置限定为单 MySQL 8.4 源、单 PostgreSQL 17.12 目标、单主键表、固定宽度 ASCII 正文和 LOCAL 日志。加载源数据会预热缓存，不是冷存储试验。它不测目标极限吞吐、p95/p99 事件延迟、持久化 SNAPSHOT_END 确认、故障切换 RTO 或稳态饱和性能，也不能证明 JSON/无主键/大量表、多目标、外部日志、商业数据库或集群的性能。

## 10 万行实测参考

两种日志模式各执行一次，使用相同候选包、MySQL 8.4.13、PostgreSQL 17.12 与 Temurin 8u512。宿主机为 Apple M1 Pro ARM64、10 个逻辑 CPU、16 GiB 内存，Docker Desktop 使用 SSD。每个服务限 1 CPU 和 1 GiB。Worker JVM 堆与数据库缓冲区各为 256 MiB。每行有 1,024 字节负载，快照每片 1,000 行，源端每事务最多 500 条变更。

| 日志 | 全量可见耗时 | 全量行/秒 | 12 万条变更可见耗时 | 变更/秒 | 源端写完后追平 |
|---|---:|---:|---:|---:|---:|
| LOCAL，宿主机挂载 | 24.83 秒 | 4,027 | 53.55 秒 | 2,241 | 23.13 秒 |
| EXTERNAL，目标 PostgreSQL 独立 schema | 21.61 秒 | 4,628 | 46.25 秒 | 2,595 | 14.04 秒 |

两组在全量后和增量后都通过源目标内容哈希核对。排查时，相同堆大小的 Worker 曾因单批载入过多事务耗尽内存。当前日志读取将每批事务前缀限制为 8 MiB，超过上限的首笔事务单独处理。PostgreSQL 压测的 UPDATE 将主键参数绑定为 BIGINT 后，执行计划从全表扫描切换为主键索引扫描。超大单事务和海量表需要另测。[实测报告和候选包](../verification/baseline.json)

## Temurin 21 多轮硬件参考

宿主机为 Apple M1 Pro ARM64，10 个逻辑 CPU、16 GiB 内存。Docker Desktop 29.9.2 报告 10 个 vCPU、15.6 GiB 内存。各配置使用同一发布包，MySQL 8.4.13、PostgreSQL 17.12 和 Temurin 21.0.13+9。每种配置独立运行三轮，三个容器各限 1 GiB 及表中 CPU 数，Worker 堆 256 MiB。数据库使用持久化夹具卷。LOCAL 日志使用宿主机挂载，EXTERNAL 日志位于目标 PostgreSQL 的独立 schema。

每轮全量 20,000 行，每行有效载荷 256 字节，随后核对 24,000 条新增、更新和删除。表中括号为三轮最小值至最大值。

| 日志 | 每容器 CPU | 全量行/秒中位数 | 增量变更/秒中位数 | 源端写完后追平中位数 |
|---|---:|---:|---:|---:|
| LOCAL | 1 | 1,943（1,758–2,222） | 2,983（2,133–3,699） | 0.27 秒（0.26–0.42） |
| LOCAL | 2 | 2,986（2,442–3,345） | 2,962（2,954–3,000） | 0.29 秒（0.29–0.36） |
| EXTERNAL | 1 | 2,558（2,469–2,763） | 2,925（2,784–3,030） | 1.00 秒（0.88–1.29） |
| EXTERNAL | 2 | 3,143（1,554–3,382） | 3,038（2,169–3,472） | 0.36 秒（0.28–0.37） |

12 轮全量与增量均核对了排序后的源、目标数据字节。追平判定同时核对首尾主键，因为仅完成更新的中间状态也可能满足最终行数和版本合计。全量计时包含 Worker 启动，增量计时包含源端写入。测量值适用于该负载、资源配额和宿主机。[各轮实测、镜像与候选包](../verification/baseline.json)

## 建立硬件标准

保留原始报告，每种负载与资源组合至少执行三轮，报告中位数及范围。比较时保持发布包、版本、持久化条件、数据集和存储一致；记录宿主机空闲/有负载状态、磁盘延迟/IOPS、网络 RTT/带宽、资源利用率及持续积压增长。不能根据 CPU 配额或小型本地试验线性外推。

最终参考矩阵仍需独立物理机、远端目标、行宽/事务/表数量变化、全部目标与两种日志模式、长期运行、消费者背压、容量耗尽及带积压恢复验收。单次小规模通过只能证明工具在该数据集可用，不代表整个硬件标准完成。

## 工具验证范围

工具曾通过小型全量/增量及五JDK×两种日志的验证，摘要收敛到[验证基线](../verification/baseline.json)。旧候选的重复原始报告已移除；这些历史结果不代表当前候选、全连接器或硬件性能标准已验收。

## 运行时与日志配置

基准测试使用 Temurin 21。`--journal-mode LOCAL|EXTERNAL` 默认 LOCAL；EXTERNAL 使用目标 PostgreSQL 同一实例上的独立日志 schema，日志与目标争用该容器资源，不代表远程独立日志性能。示例：

```bash
python3 tests/benchmark.py --java 21 --journal-mode EXTERNAL --rows 1000 \
  --storage-description '填写实际存储及共享宿主机负载'
```

可以选择运行时不等于兼容性已经验收，需要检查通过的原始报告和实际 JVM 版本。这个 SQL 配置只能证明该运行时的 MySQL 源/PostgreSQL 目标全量/增量路径；消息、安全、故障切换及其他驱动仍需单独验收。

可用同一个 Temurin 21 发布包，顺序比较两种日志模式和不同 CPU 配额：

```bash
python3 tests/runtime-matrix.py --rows 20000 --cpu-limits 1 2 --repeat 3 \
  --artifact sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar \
  --storage-description '记录物理存储、虚拟机限制及共享宿主机资源竞争'
```

矩阵在 `target/runtime-matrix/<id>/` 保留进度、逐轮日志及原始报告引用。每轮启动独立的服务，核对源与目标字节、实际日志后端、清理结果、实际 JVM 版本、发布包 SHA-256 及三个容器的 CPU 配额。单轮失败后继续其余测量，只有所有轮次通过的配置才报告中位数和范围。发布包改变会终止矩阵。测量期间避免运行其他竞争资源的任务及重新构建发布包。

### Controller、Agent 与 Worker 运行时验收

使用同一个发布包在 Temurin 21 上启动实际进程，验证 MySQL 到 PostgreSQL / MySQL 的全量和增量。结果记录各角色的真实 JVM 版本、架构及包摘要。

```bash
python3 tests/control-runtime.py --suite processes \
    --artifact sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar \
    --output target/control-runtime-processes
```
