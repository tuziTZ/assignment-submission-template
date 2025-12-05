```bash
.
├── ─code               										# 项目代码
│   ├───spark-scala													
│   │   ├───fenbushi_shuffle_test.sh				# 作业提交脚本
│   │   └───spark-shuffle-test.jar					
│   │
│   └───SparkDemo														# 工作负载生成代码
│       ├───out
│       │   └───artifacts
│       │       └───spark_shuffle_test_jar	# 编译结果
│       │               spark-shuffle-test.jar
│       └───src
│           └───main
│               └───scala
│                       ShufflePerf.scala		# 数据集构建与工作负载生成
│
└── README.md               								# 项目核心文档
```

## 研究目的
比较Spark中的两种Shuffle算法：基于Hash和基于Sort。

## 研究内容
对比分析Spark中基于Hash和基于Sort的两种Shuffle算法的执行流程，探讨它们各自的优缺点及适用场景。

**Spark Shuffle 机制与原理分析**

在 Spark 的执行模型中，Shuffle 阶段负责衔接上游的 Map 任务与下游的 Reduce 类任务（如 reduceByKey、groupByKey、join 等）。由于 Shuffle 涉及跨节点的数据交换、磁盘 I/O 和网络 I/O，其性能通常决定作业的整体执行效率。Spark 目前主要有两种 Shuffle 实现机制：**基于 Hash 的 Shuffle** 与 **基于 Sort 的 Shuffle**，两者的实现路径及性能特征存在明显差异。

1. **Hash-Based Shuffle**

Spark 早期默认采用基于 Hash 的 Shuffle。其核心机制是：

每个 Map Task 会根据 key 的哈希结果，将数据划分到下游各个 Reduce Task 对应的文件中。若下游有 R 个任务，则每个 Map Task 会产生 R 个文件，同一 Stage 若有 M 个 Map Task，则共生成 M×R 个中间文件。当 Map Task 多、Reduce 并行度大时，中间文件数量呈指数级增长，带来大量的随机磁盘 I/O 和元数据管理开销。

Spark 后续引入了 **Consolidation 机制（spark.shuffle.consolidateFiles）**，允许多个 Map Task 复用同一组文件，从而将文件数量从 M×R 减少到约 _Executor Cores × R_。但由于稳定性原因，该机制并未成为默认选项。

**优点：**

+ 无需排序，适用于无需排序的场景（如简单聚合）。
+ 避免排序内存开销，单条 Map 输出延迟较低。

**缺点：**

+ 中间文件数量极大，对文件系统压力高。
+ 小文件随机读写导致磁盘 I/O 性能较差。
+ 扩展性较差，限制系统在大规模集群下的稳定性。

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572275211-08ac5c59-ee05-4d76-a9a7-fae3bfa63511.png)

2. **Sort-Based Shuffle（Spark 默认实现）**

为解决 Hash Shuffle 文件爆炸问题，Spark 1.1 引入 Sort-Based Shuffle，在 Spark 1.2 中成为默认实现。核心变化是：

**每个 Map Task 只生成一个数据文件 + 一个索引文件**（共 2 个文件），大幅减少了中间文件数量。数据先写入内存数据结构（Map 或 Array），到达阈值后进行排序，再批量写磁盘。所有临时文件在最终阶段被合并生成单一输出文件。

Sort-Based Shuffle 内部包含三种运行机制：

1. **普通运行机制（默认）**：先排序再写磁盘，适用于大多数场景。
2. **Bypass 机制（hash 风格回退）**：当 Reduce 分区数 ≤ spark.shuffle.sort.bypassMergeThreshold（默认 200）且无聚合操作时，不进行排序，性能更接近 Hash Shuffle。
3. **Tungsten-Sort Shuffle**：对序列化后的二进制数据进行指针排序，实现“排序指针而非数据本体”，减少 GC 与内存复制，进一步提升性能。但其使用条件较严格，如要求 Kryo 序列化器且无聚合操作。

**优点：**

+ 中间文件数量仅为 2M，显著提升大规模集群可扩展性。
+ 顺序写磁盘优于 Hash 的随机 I/O，读性能更稳定。
+ 兼容 Tungsten 优化，可获得更高执行效率。

**缺点：**

+ 需要排序，带来额外 CPU 计算与内存开销。
+ 在分区数较少、无排序需求的简单任务中，可能不如 Hash Shuffle 快。

![](attachment:6e42db73-a823-47d1-80e0-a326df85503a:3a2d37c338fd287135746a6aca23718c.png)![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572294172-53103961-6bec-493d-bedc-daa5bb71d026.png)

3. **为什么 Spark 最终放弃 Hash Shuffle？**

根本原因在于：

**Hash Shuffle 会导致无法控制的中间文件数量，严重限制大规模集群的稳定性与性能上限。**

当数据规模和集群规模扩大时，文件系统压力、磁盘 I/O 瓶颈和 Shuffle 读时的小文件随机读写都会成为主要性能瓶颈。Sort-Based Shuffle 将文件数量固定为 2M 级别，使得 Spark 能够支持上千甚至上万节点的生产级集群。

## 实验
选取合适的计算负载，在不同规模的数据集下分别采用基于Hash和基于Sort的Shuffle算法。通过记录Shuffle数据量、内存使用量及作业执行时间等指标，对比分析两种Shuffle算法的性能特征与适用场景。

### 3.1 实验环境
本实验基于一套由云主机构建的 Spark 分布式计算环境完成，实验平台的硬件与软件配置如下。

#### 3.1.1 硬件环境
本实验使用 **4 台云主机构建 Spark 集群**，其中 **1 台兼任 Master 和 Worker，另外 3 台作为 Worker 节点**。所有节点均来自同一云服务商，底层配置保持一致，以保证实验的可重复性与一致性。

| **节点角色** | **节点数量** | **CPU** | **内存** | **磁盘** | **网络带宽** | **备注** |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Master（兼 Worker）** | 1 | 2 vCPU | 2 GB | 通用型 SSD 云硬盘 | 10 Mbps | 负责任务调度及部分计算 |
| **Worker** | 3 | 2 vCPU | 2 GB | 通用型 SSD 云硬盘 | 10 Mbps | 负责实际任务执行 |


**硬件特性总结：**

+ 总节点数：**4 台**
+ 总可用 CPU 核数：**2 × 4 = 8 核**
+ 总内存：**2 GB × 4 = 8 GB**
+ 存储类型：**通用型 SSD 云硬盘（系统盘）**
+ 网络带宽：**10 Mbps（共享带宽）**

该硬件环境偏轻量级，计算资源受限，因此适合进行小规模数据集与 Shuffle 密集型作业的性能分析，适用于验证 Spark Shuffle 性能、分区调优策略的影响。

#### 3.1.2 软件环境
所有集群节点安装相同的软件版本，以确保运行一致性。

| **软件组件** | **版本信息** | **说明** |
| :---: | :---: | :---: |
| **操作系统** | Ubuntu Linux 24.04 LTS 64位 | 集群均为同一镜像 |
| **JDK** | OpenJDK 1.8（Java 8） | Spark 1.6.3 官方推荐版本 |
| **Spark** | Apache Spark 1.6.3 | 经典 RDD 架构，无 DataFrame Tungsten 优化 |
| **Scala** | 2.10（与 Spark 1.6.3 兼容） | 主要用于编写 Spark 程序 |


+ Spark 1.6.3 采用旧版 Shuffle（HashShuffle / SortShuffle），适合观察 Shuffle 行为差异。
+ JDK 8 是 Spark 1.x 的长期稳定版本，避免 JDK 11+ 的兼容性问题。
+ 云主机间通过私有网络通信，所有 Worker 与 Master 的 SSH 免密登录已配置完成。

#### 3.1.3 集群部署
+ **Master 运行 Spark Master 进程 + Worker 进程**
+ 其余 3 台节点均运行 Spark Worker
+ 程序在 **Master 节点上传、构建与提交（spark-submit）**
+ Spark 集群采用 **Standalone 模式**

### 3.2 实验负载
本实验围绕 Spark Shuffle 过程的性能特征展开，通过构造可控的数据分布与算子组合，系统性评估不同 Shuffle Manager、数据规模、Key 分布、分区数以及负载类型对整体执行时间的影响。实验的所有数据均由程序在集群内部在线生成，不依赖任何外部数据集，以确保可重复性与一致性。

#### 3.2.1 数据集构造
本实验的数据由 `ShufflePerf` 程序动态生成，生成方式如下：

+ 数据形式：RDD[(Int, Int)]
+ 数据量：由 `DATA_SIZE` 参数控制，例如 **100k、200k**
+ Key 值个数：由 `NUM_KEYS` 参数控制，如 **100、1000**
+ Value 固定为 1，用于构建典型 Shuffle 场景
+ RDD 构建方式：通过 `parallelize` + `mapPartitionsWithIndex` 在线随机生成  → 不需要分发外部数据，保证每次实验环境一致。

#### 3.2.2 Key 分布
为模拟不同的数据倾斜程度，实验设计了三种 Key 分布：

| **分布类型** | **含义** | **特征** | **代码实现方式** |
| :---: | :---: | :---: | :---: |
| **uniform** | 均匀分布 | 每个 Key 出现概率近似相同 | `k = i % numKeys` |
| **zipf** | 重尾分布 | 少数 Key 高频出现，模拟严重倾斜 | 指数 θ=1.2 的反函数采样 |
| **skew** | 自定义倾斜分布 | 某个 Key（例如 0）以 10% 概率被选中 | `if (rnd < 0.1) k=0 else ...` |


这使得实验可以分别观测：

+ Shuffle 在均匀负载下的开销
+ Zipf 重尾导致的 **极端数据倾斜**
+ Skew 中 **热点 Key 占比提高但仍有多 Key 分布** 的情况

从而全面分析 Shuffle 的数据分布敏感性。

#### 3.2.3 负载类型
本实验设计了四类典型的 Shuffle 负载，覆盖 Spark 中最常见的 Shuffle 操作场景：

**(1) Aggregation（agg）**

执行 `reduceByKey` 聚合操作，最终对所有 Key 的 value 求和。

+ 评估 **经典 Shuffle 聚合** 的性能
+ 对比不同 Key 分布（倾斜 vs 均匀）对 reduceByKey 的影响

**(2) Join（join）**

生成两个 RDD 并执行 `RDD.join()` 操作。

+ 评估连接操作的大量 Shuffle 开销
+ 分析重尾 Key 对 Join 阶段溢写与数据倾斜的影响

特点：

+ join 是 Shuffle 最重的操作之一
+ 容易发生 reducer hotspot → Shuffle spill 增加

**(3) Repartition（repart）**

执行 `repartition(parts * 2)`

+ 测量纯粹的 Shuffle 重分区带来的网络与磁盘开销
+ 不涉及计算，仅验证数据重分发成本

**(4) Sort（sort）**

执行 `sortByKey`

+ 评估全局排序（global sort）引发的多级 Shuffle
+ 比较 HashShuffle 与 SortShuffle 在排序任务中的行为差异

#### 3.2.4 Shuffle Manager
实验对比两种 Shuffle Manager：

| **Shuffle Manager** | **Spark 默认** | **特性** |
| :---: | :---: | :---: |
| **hash** | Spark 1.6 默认使用 HashShuffle | 中间文件数量多、任务数多时压力大 |
| **sort** | 开启 `spark.shuffle.manager=sort` | Spark 1.6 的改进 Shuffle，减少中间文件数 |


脚本会对每个参数组合分别运行：

```bash
spark.shuffle.manager = hash
spark.shuffle.manager = sort
```

从而比较不同 Shuffle 实现对性能的影响。

#### 3.2.5 参数组合
脚本运行以下参数组合：

+ 数据规模 DATA_SIZE：50000, 100000, 150000, 200000, 500000
+ Key 数 NUM_KEYS：100, 1000, 10000
+ 分区数 PARTS：4, 8, 16
+ Key 分布 DIST：uniform, zipf, skew
+ 负载 WORKLOAD：agg, join, repart, sort
+ Shuffle Manager：hash, sort

每次运行输出 CSV 记录，包括：

+ 运行时长
+ shuffle 结果数
+ JVM 内存信息
+ 输入 record 数
+ 参数组合信息

用于后续分析 Shuffle 行为随参数变化的规律

### 3.3 实验步骤
列出执行实验的关键步骤，并对关键步骤进行截图，如 MapReduce / Spark / Flink 部署成功后的进程信息、作业执行成功的信息等，**截图能够通过显示用户账号等个性化信息佐证实验的真实性**。

#### 3.3.1 代码开发与项目构建
##### 3.3.1.1 创建 Maven 项目
+ 在 IntelliJ IDEA 中创建 Scala + Maven 项目
+ 配置 `pom.xml`，添加 Spark 依赖（Spark 1.6.3 + Scala 2.10）

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572328636-f0b72e46-c159-4e9a-a4ca-1b53843d8fb0.png)

##### 3.3.1.2 编写 Spark 程序（Scala）
+ 在 `src/main/scala` 下编写 Spark Shuffle 性能测试程序
+ 包含数据生成、map、reduce、repartition、shuffle 等典型操作

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572337818-0ab214f5-6a2d-4912-a794-dbe30143d76c.png)

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572345556-09bacf64-4738-4429-bbe6-fabb8376ca15.png)

##### 3.3.1.3 使用 Maven 打包成 JAR
+ 运行 `mvn package`
+ 生成可用于 `spark-submit` 的 `jar-with-dependencies.jar`

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572640087-d4c102f3-2725-4121-a06c-7403b38c3454.png)

##### 3.3.1.4 单机测试代码正确性
+ 使用hash、sort两种设置
+ 测试运行时间

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572657840-8e029f4f-4d4e-4903-8549-16a53a4e161d.png)

#### 3.3.2 Spark 集群部署与配置
##### 3.3.2.1 云主机准备与基础配置
+ 4 台云主机 Ubuntu 系统初始化
+ 配置 hostname、hosts

![](attachment:98f84c78-7220-4ddc-91ce-201536384c53:image.png)![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572667995-56759fca-41e3-4275-8583-0e738f4eb39e.png)

##### 3.3.2.2 配置无密码 SSH（免密登录）
+ 在 Master 上 `ssh-keygen`
+ 将公钥复制到 3 个 Worker
+ 验证从 Master 到所有 Worker 的免密登录成功

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572684077-42048c95-f5a8-4d61-a2ad-0bfab8623a62.png)

##### 3.3.2.3 安装与配置 Java 8
+ 安装 OpenJDK 8
+ 所有节点配置 JAVA_HOME

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572693424-01e440ed-39a9-4994-9f13-69111511c88c.png)

##### 3.3.2.4 安装并配置 Spark 1.6.3
+ 解压 Spark
+ 修改 `spark-env.sh`

![](attachment:57b38ae9-cfad-48ca-a5ff-20ff9647ffaf:image.png)![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572708374-ff00c87c-3583-4a16-a0f5-4d8c973d05ff.png)

+ 配置 Master IP 和 Worker 节点列表

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572717351-d7e48842-c2b9-4929-aa4c-5a91aba45e0c.png)

#### 3.3.3 启动与验证
##### 3.3.3.1 使用 start-all.sh 启动集群
在 Master 运行：

```bash
./sbin/start-all.sh
```

启动 Spark Master + Workers

##### 3.3.3.2 查看进程是否正常运行
在 Master 和 Worker 上执行：

```bash
jps
```

检查是否出现：

+ Master
+ Worker

![](attachment:fe797576-f3c5-45f9-9af2-e7eb8dde1b49:ffab5336-7b81-498f-88a9-60b74000afd2.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573177585-e0434a49-93e4-48a1-a2a5-99cba181e529.png)

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572739976-d91ec94d-b08f-4867-ab31-ea63f46bf98d.png)![](attachment:a4d83f44-caf5-4ca6-887d-bcaa8a4d1239:b7896148e43f098a437146102fdfe7dd.png)![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764572745043-c6dacc3f-c94d-4355-977c-b069189e47c8.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573123800-c3c4bfa1-b1ce-4dad-a12f-d80c2177ceb0.png)

+ 启动history server

![](attachment:5af6107b-8d4f-48cc-9994-163edd121189:c9e33afe6daa219d9c2b552d746cbe73.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573238559-2e551e06-8ead-4caf-a494-d47c1f4adba0.png)

#### 3.3.4 作业提交与执行
##### 3.3.4.1 上传 JAR 包到 Master 节点
+ 使用 scp上传jar包到ecnu01

![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573270048-c399babd-fea0-4e9b-a39b-be32337853fe.png)

##### 3.3.4.2 执行 Spark 程序（spark-submit）
+ 执行spark-submit（通过.sh脚本中的代码）

![](attachment:5bc28754-e16d-4a1f-9132-97f4e74adcc1:image.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573299522-b55943ad-8b3f-43d4-8fdf-58b76e3a779f.png)

+ 作业运行截图

![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573332490-7a6c4481-29f6-40ca-8c81-7c643b2bf401.png)

+ Shuffle 阶段日志

job0完成截图

![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573365711-94fda804-1d8a-45f5-9404-16fc955c19ee.png)

job1完成截图

![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573403988-516b9980-75dd-41ee-a1c2-cbce14125d14.png)

+ 作业运行时的进程

![](attachment:8bf5d684-c42a-49d0-ade2-c279350f2056:df4ed2db-7ed2-44d0-9a21-4d7fc6139c7d.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573430566-4cca78de-b2c3-4ed6-8b7d-9f99546e0a34.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573476503-d3120a56-85ef-4367-83db-e355db1165fb.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573513558-d639be67-2c21-4297-8d0d-1fd59e2976ff.png)

##### 3.3.4.3 在 Web UI 上观察作业执行
+ Spark 1.6.3 集群包含 4 个 Worker 节点，共计 8 个 CPU 核与 4 GB 内存。所有 Worker 均处于 ALIVE 状态
+ 在运行 Shuffle 性能测试任务时，Spark 已将全部 8 个核心与各节点 1GB 内存分配给当前应用，资源利用率达到 100%
+ 从Completed Applications可见，任务均能正常启动与结束，说明代码打包、任务提交及 Spark 集群运行全流程均已正确配置

![](attachment:1c941e0c-1263-4a6e-a6c6-70ab9855769a:19c4dfdcb79300930c6928441fd157cf.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764573582297-e25df9a2-58c8-4f52-95c8-e1db8043bf4f.png)

### <font style="color:rgb(51, 51, 51);">3.4 实验结果</font>
<font style="color:rgba(0, 0, 0, 0.85);">本次实验以数据规模为核心变量，在完全一致的控制条件下对 Hash 算法与 Sort 算法的性能展开对比测试，实验统一设置的控制变量包括：负载类型（Workload）均涵盖聚合、连接、重分区、排序操作；数据分布（Dist）均包含均匀分布、齐夫分布、偏态分布；键数量（NumKeys）均设置 100 和 1000 两个梯度；分区数（Parts）均采用 8 和 16 两种配置，确保两种算法性能对比的公平性与科学性。</font>

| **Hash算法性能统计** | | | | |
| --- | --- | --- | --- | --- |
| **<font style="color:rgb(51, 51, 51);">Data_size</font>** | <font style="color:rgb(51, 51, 51);">50000</font> | <font style="color:rgb(51, 51, 51);">100000</font> | <font style="color:rgb(51, 51, 51);">150000</font> | <font style="color:rgb(51, 51, 51);">200000</font> |
| **<font style="color:rgb(51, 51, 51);">Workload</font>** | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> |
| **<font style="color:rgb(51, 51, 51);">Dist</font>** | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> |
| **<font style="color:rgb(51, 51, 51);">NumKeys</font>** | <font style="color:rgb(51, 51, 51);">100 1000</font> | <font style="color:rgb(51, 51, 51);">100 1000 </font> | <font style="color:rgb(51, 51, 51);">100 1000</font> | <font style="color:rgb(51, 51, 51);">100 1000</font> |
| **<font style="color:rgb(51, 51, 51);">Parts</font>** | <font style="color:rgb(51, 51, 51);">8 16</font> | <font style="color:rgb(51, 51, 51);">8 16</font> | <font style="color:rgb(51, 51, 51);">8 16</font> | <font style="color:rgb(51, 51, 51);">8 16</font> |
| ** Duration** | 11.58s | 42.54s | 105.01s | 147.40s |
| **MinDua** | 0.68s | 0.67s | 0.73s | 0.68s |
| **MaxDua** | 155.53s | 683.54s | 1561.29s | 2189.18s |
| ** Memory** | 217.33MB | 219.34MB | 218.54MB | 216.00MB |
| **MinMem** | 198.52MB | 200.20MB | 144.41MB | 127.31MB |
| **MaxMem** | 261.14MB | 273.10MB | 281.33MB | 287.83MB |
| **FreeMem** | 764.17MB | 762.16 | 762.96MB | 765.50MB |


| **Sort算法性能统计** | | | | |
| --- | --- | --- | --- | --- |
| **<font style="color:rgb(51, 51, 51);">Data_size</font>** | <font style="color:rgb(51, 51, 51);">50000</font> | <font style="color:rgb(51, 51, 51);">100000</font> | <font style="color:rgb(51, 51, 51);">150000</font> | <font style="color:rgb(51, 51, 51);">200000</font> |
| **<font style="color:rgb(51, 51, 51);">Workload</font>** | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> |
| **<font style="color:rgb(51, 51, 51);">Dist</font>** | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> |
| **<font style="color:rgb(51, 51, 51);">NumKeys</font>** | <font style="color:rgb(51, 51, 51);">100 1000</font> | <font style="color:rgb(51, 51, 51);">100 1000 </font> | <font style="color:rgb(51, 51, 51);">100 1000</font> | <font style="color:rgb(51, 51, 51);">100 1000</font> |
| **<font style="color:rgb(51, 51, 51);">Parts</font>** | <font style="color:rgb(51, 51, 51);">8 16</font> | <font style="color:rgb(51, 51, 51);">8 16</font> | <font style="color:rgb(51, 51, 51);">8 16</font> | <font style="color:rgb(51, 51, 51);">8 16</font> |
| ** Duration** | 9.67s | 51.45s | 101.88s | 205.02s |
| **MinDua** | 0.75s | 0.67s | 0.76s | 0.74s |
| **MaxDua** | 135.44s | 638.96s | 1387.77s | 2805.10s |
| ** Memory** | 217.15MB | 221.18MB | 218.74MB | 209.94MB |
| **MinMem** | 198.75MB | 199.82MB | 143.43MB | 125.58MB |
| **MaxMem** | 256.17MB | 278.00MB | 283.55MB | 245.44MB |
| **FreeMem** | 764.35MB | 760.32MB | 762.76MB | 771.56MB |


**核心性能总结：**

+ <font style="color:rgb(0, 0, 0);">执行时间上，小数据量时 Sort 略占优势，大数据量时 Hash 的效率优势凸显，这一差异源于 Sort 算法需额外执行排序与合并步骤，大数据量下该额外开销会显著增加，而 Hash 算法无需排序，更适配大规模数据处理场景。两者的最小执行时间均维持在 0.63-0.76 的低水平区间，而最大执行时间均随数据量增长大幅攀升。</font>
+ <font style="color:rgb(0, 0, 0);">内存使用上，在 50000、100000、150000 三个数据规模下，Hash 与 Sort 的平均内存占用基本持平，仅在 200000 数据量时，Sort 的平均内存略低于 Hash ，这是由于 Sort 在大数据量下通过磁盘外排机制优化了内存占用，而 Hash 的多缓冲区策略仍维持稳定内存消耗。</font><font style="color:rgba(0, 0, 0, 0.85);">两者的最小内存均随数据规模增大呈现下降趋势，降幅相近；最大内存方面，前三个数据规模下 Sort 略低于Hash ,但两者差距不大，而 200000 数据量时，Sort 的最大内存显著低于 Hash，进一步体现 Sort 在大数据量下的内存优化效果。</font>
+ <font style="color:rgb(0, 0, 0);">内存使用无论 Hash 算法还是 Sort 算法，都不是随数据规模线性增大：</font>
    - <font style="color:rgb(0, 0, 0);">Spark 通过固定比例的内存分配机制限制 Shuffle 阶段的内存使用，无论数据规模多大，Shuffle 内存的上限均由该比例或固定配置值决定，当数据量超出缓冲区阈值时，Spark 不会继续分配更多内存，而是触发spill 机制，因此内存使用被严格限制在阈值范围内。</font>
    - <font style="color:rgb(0, 0, 0);">本次实验中 Executor 总内存未随数据规模调整，Shuffle 内存作为 Executor 内存的固定比例部分，自然也维持恒定上限；分区数固定，Hash Shuffle 的缓冲区数量和 Sort Shuffle 的排序分区数未变化，进一步限制了内存缓冲区的总占用量。</font>
    - <font style="color:rgb(0, 0, 0);">Spark Shuffle 的数据处理采用批次化策略，进一步降低了内存占用对数据总量的依赖，无论数据规模多大，内存只需支撑单批次数据的处理，而非全量数据存储。</font>

<font style="color:rgba(0, 0, 0, 0.85);">为使实验结论更具有普适性，本次在中等数据规模10 万下增加了</font>**<font style="color:rgba(0, 0, 0, 0.85);">补充实验</font>**<font style="color:rgba(0, 0, 0, 0.85);">，保持 Workload（agg、join、repart、sort）与数据分布（uniform、zipf、skew）与前期一致的基础上，新增 NumKeys=10000 和 Parts=4 的参数配置，对前期实验进行针对性拓展与深化。真实大数据业务中，键数量可能达到万级（如高基数维度的聚合任务），分区数也可能因集群资源限制采用 4 等较小配置。在分布式计算中，键数量的激增会直接影响 Shuffle 过程中数据分区的均衡性与 Hash 冲突概率，少分区则会改变 Reducer 的任务粒度与数据合并开销。</font>

| **100k 补充实验性能统计** | | |
| :---: | --- | --- |
| **<font style="color:rgb(51, 51, 51);">Algorithm</font>** | <font style="color:rgb(51, 51, 51);">Hash</font> | <font style="color:rgb(51, 51, 51);">Sort</font> |
| **<font style="color:rgb(51, 51, 51);">Workload</font>** | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> | <font style="color:rgb(51, 51, 51);">agg join repart sort</font> |
| **<font style="color:rgb(51, 51, 51);">Dist</font>** | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> | <font style="color:rgb(51, 51, 51);">uniform zipf skew</font> |
| **<font style="color:rgb(51, 51, 51);">NumKeys</font>** | <font style="color:rgb(51, 51, 51);">100 1000 10000</font> | <font style="color:rgb(51, 51, 51);">100 1000 10000</font> |
| **<font style="color:rgb(51, 51, 51);">Parts</font>** | <font style="color:rgb(51, 51, 51);">4 8 16</font> | <font style="color:rgb(51, 51, 51);">4 8 16</font> |
| ** Duration** | 43.53s | 46.42s |
| **MinDua** | 0.63s | 0.65s |
| **MaxDua** | 683.54s | 638.96s |
| ** Memory** | 215.15MB | 215.27MB |
| **MinMem** | 192.10MB | 192.68MB |
| **MaxMem** | 273.22MB | 278.00MB |
| **FreeMem** | 766.35MB | 766.23MB |


+ <font style="color:rgb(51, 51, 51);">平均执行时间方面 Hash 仍保持 3 秒左右的优势，这与前期中等数据量下 Hash 略优于 Sort 的结论一致；最大执行时间方面，Hash 的最大耗时略高，这是因为高键数量导致 Hash 算法因 Hash 冲突显著增加耗时。</font>
+ <font style="color:rgb(51, 51, 51);">平均内存使用量二者几乎无差异，也延续了前期结论，说明即使新增高键数量和少分区参数，性能特征依然稳定。</font>
+ <font style="color:rgb(51, 51, 51);">最大内存使用量 Sort 算法反而高于 Hash 算法：</font>
    - <font style="color:rgb(51, 51, 51);">高键数量场景下，需在内存缓冲区中同时存储大量 Key-Value 对以完成排序，缓冲区不仅要容纳数据，还要为排序算法预留临时空间（如比较、交换的内存开销）。相比之下，Hash Shuffle 缓冲区只需存储数据本身。</font><font style="color:rgba(0, 0, 0, 0.85);">高键数量场景下，排序流程耗时更长，缓冲区占用内存的时间窗口更大，更易出现内存峰值叠加（如排序未完成时新数据持续写入），进一步推高最大内存。</font>
    - <font style="color:rgb(51, 51, 51);">Sort Shuffle 在 spill 文件合并阶段的内存需求与分区数负相关，当 Parts=4 时，Reducer 数量减少，每个 Reducer 需要合并的 Mapper 端 spill 文件数量会增加。Sort 在合并 spill 文件时，需在内存中开辟缓冲区缓存多个文件的待合并数据（采用归并排序的多路合并策略），少分区导致单 Reducer 合并的数据量增大，合并阶段的内存峰值随之升高。而 Hash Shuffle 为每个 Reducer 维护独立缓冲区，即使 Parts 减少，也仅减少缓冲区数量，不会产生额外的合并内存开销。</font>

<font style="color:rgb(51, 51, 51);">以下柱状图表示在两种shuffle manager实验条件下，固定数据量的内存/时间的倾向分布。</font>

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764572460862-40fe9204-f051-4542-b77f-8c8f555469b1.png)

+ **执行时间（Duration）：两者差异较小，但 Sort 略微稳健。**<font style="color:rgb(51, 51, 51);"> 时间分布图中，两者绝大多数任务都非常快（集中在左侧），但在长尾（耗时较长）的区间中，两者的表现基本持平，说明在中小规模的数据量下，排序带来的 CPU 开销并没有显著拖慢整体速度。</font>

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764572451425-b579afdd-a81c-4b2c-80dc-fa0a7b850d12.png)

+ **内存效率（Memory Usage）：Sort Shuffle 优于 Hash Shuffle。**<font style="color:rgb(51, 51, 51);"> 从内存分布图可以看出，Sort Shuffle（青色柱）的频数峰值集中在较低的内存区间（约 195-215 MB），而 Hash Shuffle（红色柱）的分布整体向右偏移，在 220 MB 以上的高内存区间出现频率更高。这表明 Sort Shuffle 在内存控制上更为出色。这是因为 </font>**Hash Shuffle **<font style="color:rgb(51, 51, 51);">的 Map Task 需要为下游的每一个 Reduce Task 维护一个独立的写入缓冲区。当分区数较多时，并发存在的 Buffer 数量巨大（M×R），导致内存消耗急剧膨胀，且产生大量随机 I/O 小文件。而 </font>**Sort Shuffle **<font style="color:rgb(51, 51, 51);">的 Map Task 将输出数据按照分区 ID 排序，并写入同一个文件（配合索引文件）。这种机制复用了缓冲区，极大减少了内存占用。</font>

## 结果分析
#### <font style="color:rgb(51, 51, 51);">4.1 单变量分析</font>
##### 4.1.1 数据规模（Data_size）
以下折线图展示了在不同数据规模（50,000 至 200,000）下，Hash 与 Sort 两种 Shuffle Manager 的平均执行时间与内存占用的变化趋势。

+ **执行时间（Average Duration）：两者呈线性增长且高度重合。**<font style="color:rgb(51, 51, 51);"> 随着数据量从 50k 增加到 200k，两者的运行时间基本呈线性上升。图中红线（Hash）与青线（Sort）水平相近，且置信区间（阴影部分）大面积重叠，说明在当前数据规模下，Sort Shuffle 虽然引入了排序步骤，但并没有比 Hash Shuffle 慢，两者的端到端处理速度基本持平。</font>

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764572481673-5c34d13e-0ce5-4b2c-8f27-b40f3b0b60b8.png)

+ **内存效率（Average Memory Usage）：大数量级下 Sort 优势初显。**<font style="color:rgb(51, 51, 51);"> 在中小数据量（50k-150k）区间内，两者内存占用互有高低，波动较大，这可能是受 JVM GC 机制影响。但在数据量达到最大的 200k 时，Sort Shuffle（青色节点）的内存占用明显回落并低于 Hash Shuffle（红色节点）。这表明随着数据规模的扩张，Hash 模式产生了大量的小缓冲区，不利于JVM垃圾回收；而Sort 模式产生了少量大缓冲区，更容易进行管理和回收，在内存利用方面有优势。</font>

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764572475119-484af7a4-948d-47d9-aa1e-c0dfe8d0dc22.png)

结论：<font style="color:rgb(51, 51, 51);"> Hash Shuffle 省去了排序步骤，理论上计算更快，但随着数据量增长，其需要维护的海量并发写入缓冲区（Buffer）会导致内存压力持续累积。Sort Shuffle 以 CPU 计算成本换取内存I/O的优势，虽然增加了排序计算成本，但通过顺序读写和复用缓冲区，抵消了计算耗时（因此时间持平），并在大数据量下有效遏制了内存膨胀（因此 200k 时内存更低），更适合处理大规模负载。</font>

##### <font style="color:rgb(51, 51, 51);">4.1.2 键数量（num_keys）</font>
以下柱状图展示了在固定数据规模（100k）下，随着 Key 数量（100 至 10,000）的变化，Hash 与 Sort 两种 Shuffle Manager 在四种不同负载（`agg`, `join`, `repart`, `sort`）下的平均执行时间与内存占用对比。

+ **执行时间（Duration）：两者性能表现基本持平。**  
在 `agg`、`repart` 和 `sort` 负载下，Hash（橙色）与 Sort（蓝色）的执行时间非常接近，且置信区间（黑色垂线）有显著重叠，表明两者在大多数场景下的处理速度无统计学显著差异。唯一的波动出现在 `join` 负载中，Sort 在 Key 数量较少（100）时似乎略慢，但随着 Key 数量增加到 10,000，两者的差异缩小并趋于一致，且整体方差极大，说明该场景下性能受随机因素影响较多，两者并无决定性差距。

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764572564364-d0ac9d29-c159-44db-ae99-503869bb0061.png)

+ **内存占用（Memory Usage）：两者内存消耗高度一致。**  
观察第二张内存占用图，无论是在哪种负载类型（workload）或 Key 数量下，Hash 与 Sort 的内存占用柱状图高度几乎完全持平（稳定在 200MB - 250MB 之间）。这说明在当前 100k 数据量级和 Key 数量范围内，Sort Shuffle 的排序开销并未导致太多额外内存占用，而 Hash Shuffle 维护缓冲区的开销也未见明显劣势，两者的内存管理效率处于同一水平线。

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764572548484-898c0e1c-44d5-45e8-a422-7137ac3c5644.png)

**结论：**在 100k 数据量级下，Key 数量变化对于 Hash Shuffle 与 Sort Shuffle 的存储和运算性能没有明显影响。Hash Shuffle 省去了排序步骤，本应更快，但在 Key 数量较多时，其需要维护大量并发 Buffer 的开销抵消了这一优势；而 Sort Shuffle 虽然增加了排序环节，但通过高效的顺序读写机制优化了 I/O，成功弥补了计算耗时。因此，在中小规模数据集和常规 Key 数量范围内，两者的性能表现没有显著区别。

##### <font style="color:rgb(51, 51, 51);">4.1.3 分区数（parts）</font>
以下柱状图展示了在固定数据规模（100k）下，随着分区数（Partitions: 4, 8, 16）的增加，Hash 与 Sort 两种 Shuffle Manager 的平均执行时间与内存占用对比。

+ **执行时间（Duration）：低分区下性能高度趋同，Sort 在排序负载略占优。**  
在执行时间方面，两者互有胜负但差距极小。随着分区数的增加，系统开销略微增大，导致耗时普遍微涨。在 `agg` 和 `repart` 负载中，两者几乎平手；在 `join` 负载中，由于方差较大，两者无统计学差异；唯独在 `sort` 负载且分区数为 16 时，Sort Shuffle（蓝色）表现出微弱优势，略快于 Hash Shuffle。这表明在特定的排序敏感场景下，Sort Shuffle 的机制可能更契合后续计算需求。

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764572507623-464dc57a-041b-4cec-9a78-d531c6b87349.png)

+ **内存效率（Memory Usage）：两者对分区数扩增的敏感度一致。**  
观察内存图表，随着分区数从 4 增加到 16，无论是 Hash（橙色）还是 Sort（蓝色），其内存占用均呈现轻微上升趋势（约从 200MB 增至 220MB）。值得注意的是，在所有四种负载（`agg`/`join`/`repart`/`sort`）下，Hash 和 Sort 的柱状高度几乎完全对齐。这说明在低分区数（<16）场景下，Hash Shuffle 需要维护的并发 Buffer 数量较少，并未造成显著的内存压力，因此其内存消耗与 Sort Shuffle 处于同一水平。

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764572492945-b71fef22-3c29-4721-b7ac-d4f1579cb4f0.png)

结论：**在分区较少的情况下，Hash 与 Sort Shuffle 的性能瓶颈尚未分化。** Hash Shuffle 的主要问题是“并发打开的文件/Buffer数 = 分区数”，但当分区数仅为 4 到 16 时，这种 IO 和内存的碎片化压力完全在系统可承受范围内，因此未能体现出劣势。同理，Sort Shuffle 虽然有排序开销，但在小规模下被高效的顺序读写所摊薄。简言之，在低并发分区场景下，两者的架构差异被掩盖，性能表现基本等价。

##### <font style="color:rgb(51, 51, 51);">4.1.4 数据分布（distribution）</font>
以下柱状图展示了在三种不同数据分布（Skew 偏斜、Uniform 均匀、Zipf 长尾）下，Hash 与 Sort 两种 Shuffle Manager 的执行时间与内存占用对比。

+ **执行时间（Execution Time）：Zipf 倾斜场景下 Hash 优势显著。**<font style="color:rgb(51, 51, 51);">  
</font><font style="color:rgb(51, 51, 51);">在 </font>`<font style="color:rgb(51, 51, 51);">skew</font>`<font style="color:rgb(51, 51, 51);"> 和 </font>`<font style="color:rgb(51, 51, 51);">uniform</font>`<font style="color:rgb(51, 51, 51);"> 这种相对常规的分布下，Sort Shuffle（青色）表现略优（快 1.7% - 3.3%），这得益于其顺序 I/O 带来的读写效率优势。然而，在极端的 </font>`<font style="color:rgb(51, 51, 51);">zipf</font>`<font style="color:rgb(51, 51, 51);"> 分布下，两者的处理时间均激增两个数量级（从 4s 级跳变至 200s 级），此时 Hash Shuffle（红色）反超并确立了显著优势（比 Sort 快 14.2%）。 </font>`<font style="color:rgb(51, 51, 51);">zipf</font>`<font style="color:rgb(51, 51, 51);"> 分布意味着存在极少数数据量巨大的“热点 Key”。Sort Shuffle 必须对这些堆积如山的数据进行 </font>![image](https://cdn.nlark.com/yuque/__latex/ff9462c23532b269082b52ddca8699b2.svg)<font style="color:rgb(51, 51, 51);"> 的全量排序，CPU 计算瓶颈暴露无遗；而 Hash Shuffle 仅需 </font>![image](https://cdn.nlark.com/yuque/__latex/8c51f5913186f8ac629f1d5838940f33.svg)<font style="color:rgb(51, 51, 51);"> 的哈希映射操作，省去了昂贵的排序计算，因此在处理这种极端计算密集型负载时速度更快。</font>

![](https://cdn.nlark.com/yuque/0/2025/png/63194899/1764576839286-8db608f8-5fb7-435d-95df-28260ec6d9dc.png)

+ **内存效率（Memory Usage）：Sort 在极端压力下更能“抗压”。**<font style="color:rgb(51, 51, 51);">  
</font><font style="color:rgb(51, 51, 51);">在常规分布下，两者的内存占用差异微乎其微（Improvement 接近 0%）。但在高压的 </font>`<font style="color:rgb(51, 51, 51);">zipf</font>`<font style="color:rgb(51, 51, 51);"> 分布下，Sort Shuffle（青色）的内存占用明显低于 Hash Shuffle（红色），优势扩大至 1.6% 以上，虽然百分比数值看似小，但在 Log 标度下代表了稳定的内存控制能力。  
</font><font style="color:rgb(51, 51, 51);">这是因为，Hash Shuffle 为每个 Reduce 分区维护独立的写入 Buffer，面对 Zipf 造成的极度数据倾斜，热点分区的 Buffer 会迅速膨胀，导致内存压力大增；而 Sort Shuffle 采用固定内存阈值的排序溢写机制，一旦内存达到上限即写入磁盘，严格“锁死”了内存占用的天花板，从而在极端数据倾斜下表现出更好的内存稳定性。</font>

![](https://cdn.nlark.com/yuque/0/2025/png/63194899/1764576703422-320afd0b-f45e-40ff-bab1-5061c90ccf9c.png)

结论：<font style="color:rgb(51, 51, 51);">数据分布形态是决定二者性能优劣的关键变量。在常规均匀数据下，Sort Shuffle 凭借优秀的 I/O 模式略占上风（更快且内存相当）。但在极端数据倾斜（Zipf）场景中，两者体现了经典的时空权衡：Hash Shuffle 牺牲了更多的内存空间来换取更快的计算速度（不做排序）；Sort Shuffle 则牺牲了部分计算时间（强制排序），换取了更低、更可控的内存占用。如果任务容易出现严重倾斜且内存紧张，Sort 是更安全的选择；若内存充裕且追求极致速度，Hash 则更佳。</font>

##### 4.1.5 工作负载（workloads）
![](attachment:2d8839bd-b273-4e5e-bb1b-c41c8080e8f6:e18f2a45507336ff450834d703d77251.png)以下柱状图展示了在四种不同负载类型（`agg`, `join`, `repart`, `sort`）下，Hash 与 Sort 两种 Shuffle Manager 的执行时间与内存占用对比。

+ **执行时间（Duration）：Hash 在重负载 Join 中速度优势显著。**  
在耗时最长的 `join` 负载中，Hash Shuffle（红色）表现出明显的性能优势，比 Sort Shuffle（蓝色）快了约 **13.0%**。同时，在 `agg` 和 `repart` 负载下，Hash Shuffle 也保持了微弱的领先（快 1.8% - 5.2%）。这是因为 Hash Shuffle 的核心优势在于“不排序”。对于 `join` 这种涉及大量数据混洗的重型操作，省去强制排序的计算步骤可以显著降低 CPU 开销和端到端延迟。唯独在 `sort` 负载下，Sort Shuffle 因其天然的排序特性，避免了后续额外的排序步骤，因此反超 Hash Shuffle（快 2.9%）。

![](https://cdn.nlark.com/yuque/0/2025/png/63194899/1764577697128-2d7f8979-d521-4ea1-9cdd-96dd18db866f.png)

+ **内存效率（Memory Usage）：Sort 在高压场景下更节省内存。**  
观察内存消耗最大的 `join` 负载，Sort Shuffle（蓝色）的内存占用明显低于 Hash Shuffle，提升为 2.4%。而在其他轻量级负载（agg, repart）中，两者的内存差距很小（接近 0%）。这是因为`join` 操作通常伴随着巨大的 Shuffle 数据量。Hash Shuffle 需要为每个 Reducer 维护独立的内存缓冲区，当数据量大时，内存碎片化严重且容易触顶。相比之下，Sort Shuffle 采用基于内存阈值的排序和溢写（Spill）机制，能够更严格地限制内存使用上限，通过有序的磁盘 I/O 缓解了峰值内存压力，因此在重负载下更“省内存”。

结论：负载类型直接决定了二者的适用性。对于计算密集且对延迟敏感的 `join` 任务，Hash Shuffle 是更优选择，因为它通过跳过排序步骤获得了显著的速度提升（13%）。然而，如果任务面临内存瓶颈（如大规模 Join 导致的 OOM 风险），Sort Shuffle 则更为稳健，它牺牲了部分计算速度，换取了更低且可控的内存占用。简言之：追求速度选 Hash，追求内存安全或需排序选 Sort。

![](https://cdn.nlark.com/yuque/0/2025/png/63194899/1764577571187-0b9daf53-ec45-4fb1-913f-438d7fada9d3.png)

#### 4.2 数据规模扩展下的多维参数敏感性分析
##### 4.2.1 不同的数据分布dist、工作负载workload对时间/内存的影响（固定key和分区）
** 时间性能分析（Duration）**  

**均匀分布（**`**uniform**`**）下的表现：** 在均匀分布下，无论操作是 `agg`、`repart`、`sort` 还是 `join`，Hash Shuffle（橙色线）和 Sort Shuffle（蓝色线）的耗时曲线都非常接近且增长平缓 。这表明在理想的数据条件下，Sort Shuffle 的排序开销相对较低，两种机制都能高效运作。

**数据倾斜（**`**skew**`** 和 **`**zipf**`**）下的性能灾难：** 数据倾斜是 Shuffle 性能的总要影响因素。特别是在 `join | zipf`（高度倾斜）子图中，耗时随数据量增加而急剧攀升，远超其他所有场景。这证明了**负载均衡问题**比 Shuffle 机制本身的差异对性能影响更大。

+ **机制对比：** 在 `join | zipf` 的最高数据点，Sort Shuffle 的耗时明显高于 Hash Shuffle。这暗示着 Sort Shuffle 在处理高度倾斜的超大分区时，其内存管理和溢写归并的复杂性带来了比 Hash Shuffle 更高的执行开销。

![](attachment:10bc3e99-6275-4de9-af12-16c9fe255407:eaa421e3e75263c708d9b8276e959bc1.png)![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764575586014-17a7fca6-d705-481e-987e-823d805d3128.png)

**内存消耗分析（Used Memory）**

**普遍趋同性：**随着数据量的增加，在下图的所有子图中，两种机制的内存消耗曲线高度重合，且波动范围极小。

**倾斜下的内存行为：** 在 `join | zipf` 场景下，Sort Shuffle（蓝色线）的内存使用在数据量最大（200k Records）时出现显著下降或高波动性（误差棒大）。这反映了 Sort Shuffle 在检测到内存压力时，会更积极地进行溢写（Spill）操作，将数据从内存推到磁盘，从而在某一观测点上降低了堆内存使用量。这体现了 Sort Shuffle **应对内存压力的鲁棒性**。

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764575594074-f96e3535-62d2-476b-969e-9b31626a1f0b.png)

**结论：**数据倾斜是性能瓶颈的首要因素，其影响远超机制差异，其恶化效果随数据量的增长而放大  。Sort Shuffle 在处理超大倾斜分区时的开销可能更大。在高负载下，Sort Shuffle 更积极地启动溢写（Spill）机制，其内存管理更具鲁棒性，能够有效将内存压力转移到磁盘。  

##### 4.2.2 不同的key、工作负载workload对时间/内存的影响（固定数据分布和分区）
**时间性能分析（Duration）**

**Key 数量对 Shuffle 速度的影响不显著：** 对比 `numKeys: 100` 和 `numKeys: 1000` 的结果，两种机制的相对表现并未发生根本性改变，所有操作的耗时都随数据量（Records）的增长而呈上升趋势 。Hash Shuffle 在 `repart` 和 `sort` 操作中始终保持略微的领先。 在固定数据总量下，键值数量越多，每个 Key 对应的记录数就越少。这意味着 Shuffle 后的 Reduce 任务需要处理的**单个 Key 组的数据量变小**，从而减轻了下游 Reduce 任务的 CPU 处理负担，**加速了聚合和连接**。  

**Join 操作：** 随着 `numKeys` 从 100 增加到 1000，`join` 操作的耗时曲线依然保持非线性增长，且 Sort Shuffle 略慢于 Hash Shuffle。这表明在 Shuffle Write 阶段，键值数量的变化主要影响下游 Reduce 任务的 Hash Map 大小，而对 Shuffle 机制的**底层开销结构**（排序 vs. 不排序）影响不大。

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764577524598-f356bfda-376d-4071-8c00-d57c6099cd7f.png)

**内存消耗分析（Used Memory）**

****内存消耗随数据量的增长而呈现出轻微的上升或波动。  

**Key 数量与内存波动：** 在下图 Impact of Count on Memory 中， Hash Shuffle（橙色线）的内存使用在大多数场景下（尤其是 `agg`, `repart`, `sort`）比 Sort Shuffle 略低或波动性更大。

Hash Shuffle 在 Shuffle Write 阶段不需要在内存中维护复杂的排序结构，它的内存使用主要与**缓冲区大小**和 **Map 任务处理的 Key 数量**有关。由于 Hash Shuffle 仅依赖于 Key 的 Hash 值进行分发，键值数量的变化本身对写缓冲区的影响相对较小，因此它的内存曲线在不同 `numKeys` 级别上表现得相对稳定。 

在 `numKeys: 100` 和 `numKeys: 1000` 的 `join` 操作中，Sort Shuffle（蓝色线）的内存使用**略高于** Hash Shuffle。Sort Shuffle 需要在内存中构建一个**排序数据结构**（例如基于数组的 SortableIterator），用于在溢写前对数据进行排序。当 Key 的数量或数据块的复杂性增加时，维护和操作这个排序结构会带来**更高的堆内存开销**。 虽然数据的总记录数固定，但 `numKeys` 增加意味着需要管理**更多独立的分组元数据**。Sort Shuffle 必须将这些分组元数据和记录本身进行排序，因此在 `join` 等涉及大量数据处理的场景中，它的内存占用略高于 Hash Shuffle。  

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764577527621-12cc6dcd-c3c4-43bc-bcd2-b80675915ddc.png)

**结论： **键值数量的增加（在固定数据总量下）能够加速 Reduce 侧操作（如 `agg`, `join`），因为它减少了每个 Key 组的记录数。Hash Shuffle 在 Shuffle Write 阶段因免排序而保持速度优势。**  **Hash 始终因免排序而占优。在本实验的低分区和中小规模下，**键值数量不是区分 Hash 和 Sort 内存效率的关键因素**。内存使用主要由 JVM 基线和数据总量决定。 在中低键值数下，**Sort Shuffle 的内存消耗略高于 Hash Shuffle**，这主要归因于 Sort Shuffle 机制需要在内存中维护和操作复杂的**排序数据结构**所产生的额外开销。当 Key 数量极高（数据稀疏）时，这种差异被稀释，两种机制的内存使用趋于一致。  

##### 4.2.3 不同的分区、工作负载workload对时间/内存的影响（固定key和数据分布）
**时间性能分析（Duration）**

在不同的分区数下，耗时曲线均随数据量增长而向上倾斜。  

**分区数与时间：** 对比 `parts: 8` 和 `parts: 16`，尽管分区数量翻倍，但执行时间并**没有线性减少**，反而由于 Shuffle 开销增加，`repart` 和 `sort` 的绝对耗时略有增加。

+ **机制对比：** 在 `parts: 16` 的场景下，Hash Shuffle（橙色）依然在 `repart` 和 `sort` 操作中表现出微弱的速度优势。这进一步巩固了结论：只要分区数处于低位，Hash Shuffle 省略排序的策略在时间上更占便宜。

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764577531018-c22efc0c-188d-400d-8149-60a55fa3197a.png)

**内存消耗分析（Used Memory）**

分区数与内存：在 Impact of Partition Count on Memory 中，随着数据量的增长，内存曲线整体略有抬升，我们可以观察到 parts: 16 的内存基线略高于 parts: 8。

+ **Hash Shuffle 的内存原理体现（间接）：** Hash Shuffle 必须为每个分区维护一个缓冲区。虽然 16个分区依然很小，但**分区数增加必然带来内存消耗的基线抬升**。在 Sort Shuffle 中，所有分区数据合并到一个文件并用索引区分，对内存的影响相对较小。
+ **稳定性对比：** 再次在 `parts: 16 | join` 中观察到 Sort Shuffle（蓝色线）在数据量最大时的内存波动和下降，这是其**高效溢写机制**的直观体现。

![](https://cdn.nlark.com/yuque/0/2025/png/49415363/1764577533659-f62228bf-3d32-4804-84bb-1edb67ef256f.png)

**结论：** 在**低分区数范围**内，Hash Shuffle 的速度优势仍然保持。Sort Shuffle 牺牲时间换取稳定性的代价可见。**分区数量的增加是潜在的内存压力源**。虽然本实验未触发 Hash Shuffle 的 OOM 风险，但 Sort Shuffle 的内存管理机制在保证**高分区可扩展性**方面更有优势。  

#### 4.3 多变量交互效应分析
以下热力图展示了在最极端的 **Join 负载 + Zipf 倾斜数据** 组合下，Hash 与 Sort 两种 Shuffle Manager 在不同 Key 数量（100-10000）与分区数（4-16）下的性能表现。

+ **执行时间（Duration）：Hash 在特定参数组合下出现“极速”与“极慢”的两极分化。**  
观察左侧 Hash Shuffle 的热力图，在 Key=100 且 Partition=8 时，出现了全场最快速度 **240.0s**（深绿色块）。然而，仅增加 Key 数量至 1000，同一分区下的耗时瞬间飙升至 **683.5s**（深红色块），性能衰退近 3 倍。相比之下，右侧 Sort Shuffle 的性能分布虽然整体偏慢（普遍在 400s-600s 之间），但色块过渡相对平缓，没有出现 Hash 那种剧烈的波动。这种现象揭示了 Hash Shuffle 对哈希冲突的高度敏感性。在特定 Key/Partition 组合下（如 100 Keys / 8 Partitions），可能恰好实现了完美的负载均衡，因此利用其不排序的特性达到了极致速度。一旦参数改变导致哈希冲突加剧（如 1000 Keys / 8 Partitions），Zipf 分布下的热点数据堆积在特定 Buffer 中，引发了严重的写入阻塞和 GC 停顿。而 Sort Shuffle 由于强制进行全量排序和溢写，虽然较慢，但其性能更具可预测性，不易受哈希偶然性的影响。

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764574481240-cbc004b1-01da-424c-81d0-62ef586f3ea1.png)

+ **内存效率（Memory Usage）：高并发分区导致两者内存压力同步上升。**  
观察下方的内存热力图，无论是 Hash 还是 Sort，当分区数增加到 16 时（最右列），颜色均变为深橙色或红色（>270MB），表明内存占用显著增加。在 Key 数量较多（10000）时，Sort Shuffle 在 8 分区下的内存控制（248.9MB，绿色）略优于 Hash Shuffle（255.6MB，浅绿）。这是因为随着分区数的增加，Shuffle 阶段需要同时打开的文件句柄或内存缓冲区数量成倍增长。对于 Hash Shuffle，这意味着更多的并发 Buffer 占用了堆内存；对于 Sort Shuffle，这意味着归并排序阶段需要更大的缓冲区来维持性能。因此，在 Zipf 倾斜数据下，过高的分区数（如 16）都会成为两者的内存瓶颈，但 Sort Shuffle 凭借溢写机制，在处理海量 Key 时的内存膨胀得到了一定程度的遏制。

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764574494322-cb6ceae0-8af9-4e4f-9227-e73fc46ae287.png)

结论：在面对极端的倾斜数据（Zipf Join）时，**Hash Shuffle 是一把“双刃剑”**：参数调优得当时能获得极致性能（240s），但稍有不慎便会遭遇严重的哈希冲突导致性能崩盘（683s）。**Sort Shuffle 则表现较为稳健**，虽然牺牲了部分峰值速度，但在不同参数组合下的方差较小，且在大规模 Key 的内存控制上略胜一筹。对于数据分布不可预测的生产环境，Sort Shuffle 是更安全的选择。



以下柱状箱线图展示了在固定数据规模（100,000）下，不同数据分布（Skew, Uniform, Zipf）与分区数量（4, 8, 16）组合场景中，Hash 与 Sort 两种 Shuffle Manager 的内存占用与执行耗时对比。

+ **执行时间（Duration）：长尾分布下两者性能相当。**  
在 Skew 和 Uniform 分布下，两者的执行时间均较短，且差异不大。然而，在 Zipf 分布下，两者的执行时间均激增至 100 秒量级。即便如此，深紫色（Hash）与青绿色（Sort）的箱体高度依然高度接近。这表明在面对极端的数据倾斜（Data Skew）时，Shuffle 机制本身的选择不再是瓶颈，数据分布本身成为了主导计算耗时的核心因素。

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764574875201-871e7f7a-ee72-463e-b5ec-104b12d6de09.png)

+ **内存效率（Used Memory）：分区数增加显著推高 Hash 内存压力。**  
在所有数据分布类型下（Skew/Uniform/Zipf），随着分区数（Parts）从 4 增加到 16，Hash Shuffle（深紫色）的内存占用中位数呈现明显的上升趋势，且波动范围（箱体高度）显著扩大。相比之下，Sort Shuffle（青绿色）的内存占用相对平稳，对分区数的敏感度较低。特别是在 Zipf 分布和 Uniform 分布的高分区场景下，Sort 的内存控制表现优于或持平于 Hash。

![](https://cdn.nlark.com/yuque/0/2025/png/42377900/1764574890181-6002901d-ed09-4385-b404-ed596976156d.png)

结论：Hash Shuffle 的内存消耗与分区数（Mappers * Reducers）呈正相关，因为其需要为每个分区维护独立的缓冲区，随着分区数增加，内存碎片化和缓冲区总量迅速膨胀，容易引发 OOM 或频繁 GC（体现在第一张图中 Hash 的高波动性）。而 Sort Shuffle 引入了排序和合并机制，虽然理论计算量更大，但其内存使用相对固定，更适应高并发分区场景。数据表明，在分区数较多时，Sort Shuffle 展现出了更优的内存稳定性；而在严重的数据倾斜（Zipf）场景下，两者的计算时延表现基本一致，均受限于数据倾斜本身。

#### 4.4 统计分析
| **Duration执行时间** | | | |
| :---: | --- | --- | --- |
| **<font style="color:rgb(15, 17, 21);">统计维度</font>** | **<font style="color:rgb(15, 17, 21);">Hash</font>** | **<font style="color:rgb(15, 17, 21);">Sort</font>** | **<font style="color:rgb(15, 17, 21);">差异分析</font>** |
| **<font style="color:rgb(15, 17, 21);">平均值</font>** | <font style="color:rgb(15, 17, 21);">68.94秒</font> | <font style="color:rgb(15, 17, 21);">80.19秒</font> | <font style="color:rgb(15, 17, 21);">-11.26秒 (-14.04%)</font> |
| **<font style="color:rgb(15, 17, 21);">标准差</font>** | <font style="color:rgb(15, 17, 21);">284.50</font> | <font style="color:rgb(15, 17, 21);">348.97</font> | <font style="color:rgb(15, 17, 21);">离散度均很大</font> |
| **<font style="color:rgb(15, 17, 21);">中位数</font>** | <font style="color:rgb(15, 17, 21);">1.22秒</font> | <font style="color:rgb(15, 17, 21);">1.23秒</font> | <font style="color:rgb(15, 17, 21);">-0.0046秒</font> |
| **<font style="color:rgb(15, 17, 21);">分布形态</font>** | <font style="color:rgb(15, 17, 21);">高度非正态 (W=0.258,p=0.000)</font> | <font style="color:rgb(15, 17, 21);">高度非正态 (W=0.242, p=0.000)</font> | <font style="color:rgb(15, 17, 21);">均严重右偏</font> |


<font style="color:rgb(0, 0, 0);">Sort Shuffle 的均值显著高于 Hash Shuffle，核心源于两者的流程开销差异：</font>

+ **<font style="color:rgb(0, 0, 0) !important;">Hash Shuffle 的 Mapper 端逻辑</font>**<font style="color:rgb(0, 0, 0) !important;">：</font><font style="color:rgb(0, 0, 0);">为每个 Reducer 维护独立内存缓冲区，数据按</font>`<font style="color:rgb(0, 0, 0);">Hash(key) % R</font>`<font style="color:rgb(0, 0, 0);">（</font>_<font style="color:rgb(0, 0, 0);">R</font>_<font style="color:rgb(0, 0, 0);">为 Reducer 数）分区后写入对应缓冲区；缓冲区满溢时直接 spill 至磁盘，最终生成</font>_<font style="color:rgb(0, 0, 0);">M</font>_<font style="color:rgb(0, 0, 0);">×</font>_<font style="color:rgb(0, 0, 0);">R</font>_<font style="color:rgb(0, 0, 0);">个小文件（</font>_<font style="color:rgb(0, 0, 0);">M</font>_<font style="color:rgb(0, 0, 0);">为 Mapper 数）。该流程无额外排序、合并操作，时间复杂度为</font>_<font style="color:rgb(0, 0, 0);">O</font>_<font style="color:rgb(0, 0, 0);">(</font>_<font style="color:rgb(0, 0, 0);">n</font>_<font style="color:rgb(0, 0, 0);">)（</font>_<font style="color:rgb(0, 0, 0);">n</font>_<font style="color:rgb(0, 0, 0);">为 Mapper 输出数据量）。</font>
+ **<font style="color:rgb(0, 0, 0) !important;">Sort Shuffle 的 Mapper 端逻辑</font>**<font style="color:rgb(0, 0, 0);">：数据先写入内存，满溢时触发 spill 并对 spill 数据按 Key 排序；所有 spill 完成后，合并所有 spill 文件为 1 个全局排序的输出文件（同时生成索引文件，记录各 Reducer 数据的偏移量）。该流程增加了 “Key 排序（时间复杂度</font>_<font style="color:rgb(0, 0, 0);">O</font>_<font style="color:rgb(0, 0, 0);">(</font>_<font style="color:rgb(0, 0, 0);">n</font>_<font style="color:rgb(0, 0, 0);">log</font>_<font style="color:rgb(0, 0, 0);">n</font>_<font style="color:rgb(0, 0, 0);">)）” 与 “spill 文件合并（时间复杂度</font>_<font style="color:rgb(0, 0, 0);">O</font>_<font style="color:rgb(0, 0, 0);">(</font>_<font style="color:rgb(0, 0, 0);">k</font>_<font style="color:rgb(0, 0, 0);">×</font>_<font style="color:rgb(0, 0, 0);">m</font>_<font style="color:rgb(0, 0, 0);">log</font>_<font style="color:rgb(0, 0, 0);">m</font>_<font style="color:rgb(0, 0, 0);">)，</font>_<font style="color:rgb(0, 0, 0);">k</font>_<font style="color:rgb(0, 0, 0);">为 spill 文件数，</font>_<font style="color:rgb(0, 0, 0);">m</font>_<font style="color:rgb(0, 0, 0);">为单 spill 文件数据量）” 步骤，带来稳定的额外开销，因此其均值高于 Hash Shuffle。</font>

Hash Shuffle 的中位数与 Sort Shuffle 差异很小。中位数作为 50% 分位数的位置统计量，反映了样本中中等负载任务的耗时水平，说明在多数中小规模数据任务中，两种 Shuffle 的执行开销处于同一量级。

Hash Shuffle 与 Sort Shuffle 的均值均显著高于其中位数，通过 Shapiro-Wilk 正态性检验可知，执行时间均呈强右偏分布：即样本中存在少量耗时远高于均值的极端观测值（长尾任务）。这是因为：

+ **中小规模负载下**，两种 Shuffle 的核心开销（Hash 的分区 IO、Sort 的轻量排序）均处于低水平，因此多数任务耗时集中于中位数附近；
+ **大规模负载下**，Hash Shuffle 因无需排序，仅需承担小文件 IO 开销，其耗时增长幅度低于 Sort Shuffle（Sort 需承担大规模数据的排序 + 合并开销），构成了分布的 “右尾”，同时拉低了 Hash Shuffle 的均值（相较于 Sort Shuffle）。

Hash Shuffle 的标准差与 Sort Shuffle 的均远大于其均值，执行时间的离散程度极高（变异系数分别为 4.13、4.35），<font style="color:rgb(0, 0, 0);">源于其开销对负载特征（数据规模、分布）的敏感性：</font>

+ **<font style="color:rgb(0, 0, 0) !important;">Hash Shuffle 的离散性来源</font>**<font style="color:rgb(0, 0, 0);">：其开销波动主要由</font>**<font style="color:rgb(0, 0, 0) !important;">小文件 IO 异质性</font>**<font style="color:rgb(0, 0, 0);">驱动。当 Reducer 数量</font>_<font style="color:rgb(0, 0, 0);">R</font>_<font style="color:rgb(0, 0, 0);">增大时，小文件数量</font>_<font style="color:rgb(0, 0, 0);">M</font>_<font style="color:rgb(0, 0, 0);">×</font>_<font style="color:rgb(0, 0, 0);">R</font>_<font style="color:rgb(0, 0, 0);">线性增长，磁盘寻道时间、IO 上下文切换开销的波动会随文件数增加而放大；同时，数据的 Hash 分区均衡性（若 Key 的 Hash 值分布不均，部分缓冲区会频繁 spill）进一步加剧了耗时波动。</font>
+ **<font style="color:rgb(0, 0, 0) !important;">Sort Shuffle 的离散性来源</font>**<font style="color:rgb(0, 0, 0);">：其开销波动主要由</font>**<font style="color:rgb(0, 0, 0) !important;">排序与外排开销的非线性增长</font>**<font style="color:rgb(0, 0, 0);">驱动。当数据量未超出内存阈值时，排序可在内存中完成（开销较低）；当数据量超出阈值时，需触发外排，其开销与数据规模、数据无序度呈正相关，外排开销波动显著，最终表现为高离散度。</font>

| **Duration执行时间统计检验** | |
| :---: | --- |
| **<font style="color:rgb(15, 17, 21);">方差齐性(Levene)</font>** | <font style="color:rgb(15, 17, 21);">F=0.157, p=0.692</font> |
| **<font style="color:rgb(15, 17, 21);">t检验</font>** | <font style="color:rgb(15, 17, 21);">-0.397</font> |
| **<font style="color:rgb(15, 17, 21);">p值</font>** | <font style="color:rgb(15, 17, 21);">0.692</font> |
| **<font style="color:rgb(15, 17, 21);">Cohen's d</font>** | <font style="color:rgb(15, 17, 21);">-0.035</font> |
| **<font style="color:rgb(15, 17, 21);">非参数检验(U)</font>** | <font style="color:rgb(15, 17, 21);">U=31292, p=0.779</font> |
| **<font style="color:rgb(15, 17, 21);">ANOVA F值</font>** | <font style="color:rgb(15, 17, 21);">0.158</font> |
| **<font style="color:rgb(15, 17, 21);">95%置信区间</font>** | <font style="color:rgb(15, 17, 21);">[-66.85, 44.33]</font> |


<font style="color:rgba(0, 0, 0, 0.85);">Levene 检验的 p 值大于 0.05，说明 Hash Shuffle 与 Sort Shuffle 执行时间的方差满足齐性假设，表明实验数据的离散程度在两组间具有一致性，是参数检验（如 t 检验）的前提条件。</font>

<font style="color:rgba(0, 0, 0, 0.85);">t 检验结果中 p 值大于 0.05，结合 95% 置信区间包含 0 的特征，说明两组 Shuffle 的执行时间均值在统计上无显著差异，符合上面的统计分析中”相差不大“的结果，验证了多数中等负载场景下，两种 Shuffle 的执行效率处于同一水平的实验假设，体现了实验结果的内部一致性。</font>

<font style="color:rgba(0, 0, 0, 0.85);">置信区间的范围覆盖正负值，Cohen's d 绝对值 < 0.2，非参数检验 p 值大于 0.05，与参数 t 检验的结论完全一致。ANOVA 进一步验证了组间变异的不显著性，与 t 检验、非参数检验的结果形成交叉验证，说明实验结果并非偶然误差导致。</font>

| **Memory Usage内存使用量** | | | |
| :---: | --- | --- | --- |
| **<font style="color:rgb(15, 17, 21);">统计维度</font>** | **<font style="color:rgb(15, 17, 21);">Hash</font>** | **<font style="color:rgb(15, 17, 21);">Sort</font>** | **<font style="color:rgb(15, 17, 21);">差异分析</font>** |
| **<font style="color:rgb(15, 17, 21);">平均值</font>** | <font style="color:rgb(15, 17, 21);">216.4MB</font> | <font style="color:rgb(15, 17, 21);">215.3MB</font> | <font style="color:rgb(15, 17, 21);">+1.1 MB (+0.51%)</font> |
| **<font style="color:rgb(15, 17, 21);">标准差</font>** | <font style="color:rgb(15, 17, 21);">20.0</font> | <font style="color:rgb(15, 17, 21);">19.8</font> | <font style="color:rgb(15, 17, 21);">离散度低</font> |
| **<font style="color:rgb(15, 17, 21);">中位数</font>** | <font style="color:rgb(15, 17, 21);">212.9MB</font> | <font style="color:rgb(15, 17, 21);">213.1MB</font> | <font style="color:rgb(15, 17, 21);">-0.2 MB (-0.09%)</font> |
| **<font style="color:rgb(15, 17, 21);">分布形态</font>** | <font style="color:rgb(15, 17, 21);">高度非正态 (W=0.870,p=0.000)</font> | <font style="color:rgb(15, 17, 21);">高度非正态 (W=0.879, p=0.000)</font> | <font style="color:rgb(15, 17, 21);">均严重右偏</font> |


<font style="color:rgba(0, 0, 0, 0.85);">Spark Hash Shuffle 与 Sort Shuffle 的内存使用均值和中位数处于同一量级。两者标准差均处于较低水平，说明两种 Shuffle 的内存使用量波动幅度小，整体表现稳定。这是因为：</font>

+ <font style="color:rgb(0, 0, 0);">两者均依赖 Spark 配置的</font>`<font style="color:rgb(0, 0, 0);">spark.shuffle.memoryFraction</font>`<font style="color:rgb(0, 0, 0);">（默认占 Executor 内存的 20%）分配内存缓冲区，用于暂存 Mapper 输出数据</font>
+ <font style="color:rgb(0, 0, 0);">Hash Shuffle 为每个 Reducer 维护独立内存缓冲区，Sort Shuffle 使用单块全局内存缓冲区（数据按 Key 排序后分区） ，在多数中小负载场景下，数据量未超出缓冲区阈值，两种策略的内存占用均被阈值限制在相近水平，因此中位数、平均值差异极小。</font>

<font style="color:rgba(0, 0, 0, 0.85);">Hash Shuffle 的平均内存略高于 Sort，原因是：</font>

+ <font style="color:rgba(0, 0, 0, 0.85);">Hash Shuffle 为每个 Reducer 维护独立缓冲区，总内存占用为缓冲区数量（Reducer 数）× 单缓冲区大小</font>
+ <font style="color:rgba(0, 0, 0, 0.85);">Sort Shuffle 的全局缓冲区仅需覆盖所有数据的排序与分区需求，无需为每个 Reducer 单独分配缓冲区。</font>
+ <font style="color:rgba(0, 0, 0, 0.85);">当 Reducer 数量较多时，Hash 的多缓冲区累加内存会略高于 Sort，但由于单缓冲区初始大小较小，最终差异仅为 0.51%。</font>

<font style="color:rgba(0, 0, 0, 0.85);">两种 Shuffle 的标准差均处于较低水平，内存使用量波动幅度小，整体表现稳定，这是因为当数据量超出内存阈值时，会将溢出数据写入磁盘而非持续占用内存，因此内存使用量被严格约束在阈值附近。</font>

<font style="color:rgba(0, 0, 0, 0.85);">通过 Shapiro-Wilk 正态性检验可知，内存使用量均呈严重右偏分布：</font>

+ <font style="color:rgba(0, 0, 0, 0.85);">少数大规模数据负载场景下，数据量接近内存阈值时，Hash 的多缓冲区会因部分分区数据集中而临时占用更多内存，Sort 的全局缓冲区也会因排序数据量突增而短暂升高内存占用 。 此类少数极端情况构成了分布的 “右尾”，但由于 spill 机制的存在，内存不会持续高占用，因此仅表现为分布偏态而非离散度升高。</font>

| **Memory Usage内存使用量** | |
| :---: | --- |
| **<font style="color:rgb(15, 17, 21);">方差齐性(Levene)</font>** | <font style="color:rgb(15, 17, 21);">F=0.027, p=0.868</font> |
| **<font style="color:rgb(15, 17, 21);">t检验</font>** | <font style="color:rgb(15, 17, 21);">0.620</font> |
| **<font style="color:rgb(15, 17, 21);">p值</font>** | <font style="color:rgb(15, 17, 21);">0.536</font> |
| **<font style="color:rgb(15, 17, 21);">Cohen's d</font>** | <font style="color:rgb(15, 17, 21);">0.055</font> |
| **<font style="color:rgb(15, 17, 21);">非参数检验(U)</font>** | <font style="color:rgb(15, 17, 21);">U=31972, p=0.893</font> |
| **<font style="color:rgb(15, 17, 21);">ANOVA F值</font>** | <font style="color:rgb(15, 17, 21);">0.385</font> |
| **<font style="color:rgb(15, 17, 21);">95%置信区间</font>** | <font style="color:rgb(15, 17, 21);">[-2.38, 4.58]</font> |


<font style="color:rgba(0, 0, 0, 0.85);">Levene 检验中 p 值远大于 0.05，说明 Hash 与 Sort Shuffle 的内存使用量方差满足齐性假设，为后续参数检验（如 t 检验）的适用性提供了关键前提，体现了实验统计分析的严谨性与规范性。</font>

<font style="color:rgba(0, 0, 0, 0.85);">t 检验 p 值大于 0.05，且 95% 置信区间包含 0，Cohen's d 值仅为 0.055，说明两组 Shuffle 的内存使用量均值在统计层面无显著差异，这与上述“平均值、中位数处于同一量级”的结论一致。</font>

<font style="color:rgba(0, 0, 0, 0.85);">本实验额外采用非参数检验、方差分析，与 t 检验的结论形成交叉支撑，说明实验结果并非偶然误差导致。</font>

#### 4.5 推荐算法
考虑<font style="color:rgba(0, 0, 0, 0.85);">本次实验的</font><font style="color:rgb(0, 0, 0) !important;">所有测试场景</font><font style="color:rgba(0, 0, 0, 0.85);">（包含不同数据规模、负载类型、数据分布、键数量、分区数等参数组合），</font>分别比较执行时间和内存使用，然后看每个配置下哪个算法在更多指标上胜出，<font style="color:rgba(0, 0, 0, 0.85);">两种算法的适用场景覆盖度如下图所示。二者占比差异较小，在本次实验的多参数组合场景中，两种 Shuffle 算法的 “最优适配场景” 分布相对均衡，Hash Shuffle 的最佳推荐占比略高于 Sort Shuffle，这是因为根据前面的分析，两种算法在内存上（Sort 占优）的差异相比执行时间非常微小，而 Hash Shuffle 因为没有排序操作，在时间性能上表现更好，因而更被推荐。</font>

![](attachment:234d38ae-c235-4328-95d3-b9138cdfc314:e2f3d82fc3f2c82636f6881816856e6a.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764654026136-7fd095fc-164a-4e38-b9c3-e7aa76856ba8.png)

<font style="color:rgba(0, 0, 0, 0.85);">聚焦</font>**<font style="color:rgb(0, 0, 0) !important;">100k 数据规模</font>**<font style="color:rgb(0, 0, 0) !important;">下，我们比较了两种算法在 4 类负载（agg、join、repart、sort）下的6种性能表现。不同负载下，两种算法的性能维度存在差异化特征。对于</font>**<font style="color:rgb(0, 0, 0) !important;">非排序类负载</font>**<font style="color:rgb(0, 0, 0) !important;">（agg/join/repart），我们推荐优先选择 </font>**<font style="color:rgb(0, 0, 0) !important;">Hash 算法</font>**<font style="color:rgb(0, 0, 0) !important;">，</font><font style="color:rgba(0, 0, 0, 0.85);">Hash Shuffle 无需执行排序合并步骤，直接通过 Hash 分区完成数据分发，也无排序的额外内存需求，缓冲区采用 “溢出即释放” 逻辑，在轻量负载下内存快速释放。对于</font>**<font style="color:rgba(0, 0, 0, 0.85);">排序负载</font>**<font style="color:rgba(0, 0, 0, 0.85);">（sort）优先选择 </font>**<font style="color:rgba(0, 0, 0, 0.85);">Sort 算法</font>**<font style="color:rgba(0, 0, 0, 0.85);">，Sort Shuffle 原生集成排序流程，在排序负载中无需额外调用排序算子，内存缓冲区同时服务于数据缓存和排序，内存利用率被最大化。</font>

![](attachment:80300044-0876-43ae-920a-604a543042df:ba7dbd543297ace6d42476805ebf075a.png)![](https://cdn.nlark.com/yuque/0/2025/png/61536182/1764657756290-58007dbf-71af-4526-b87a-062a173fc0c2.png)

<font style="color:rgba(0, 0, 0, 0.85);"></font>

### <font style="color:rgb(0, 0, 0);">MTS 指标的设立逻辑</font>
<font style="color:rgb(0, 0, 0);">MTS（综合性能指标）是</font>**<font style="color:rgb(0, 0, 0) !important;">结合 “执行时间” 和 “内存占用” 的归一化加权分数</font>**<font style="color:rgb(0, 0, 0);">，设立步骤如下：</font>

1. **<font style="color:rgb(0, 0, 0) !important;">数据归一化</font>**<font style="color:rgb(0, 0, 0);">：</font>
+ <font style="color:rgb(0, 0, 0);">对</font>`<font style="color:rgb(0, 0, 0);">执行时间（duration_s）</font>`<font style="color:rgb(0, 0, 0);">和</font>`<font style="color:rgb(0, 0, 0);">内存占用（used_memory_mb）</font>`<font style="color:rgb(0, 0, 0);">分别做归一化（将原始值映射到</font>`<font style="color:rgb(0, 0, 0);">[0,1]</font>`<font style="color:rgb(0, 0, 0);">区间）</font>
2. **<font style="color:rgb(0, 0, 0) !important;">加权计算 MTS</font>**<font style="color:rgb(0, 0, 0);">：</font>
+ <font style="color:rgb(0, 0, 0);">给时间和内存分配权重（代码中</font>`<font style="color:rgb(0, 0, 0);">时间权重=0.6</font>`<font style="color:rgb(0, 0, 0);">、</font>`<font style="color:rgb(0, 0, 0);">内存权重=0.4</font>`<font style="color:rgb(0, 0, 0);">），最终 MTS 为：norm_mts_score=0.6×norm_duration+0.4×norm_memory</font>

      其中<font style="color:rgb(0, 0, 0);">norm_duration与norm_memory分别为归一化后的时间与内存</font>

+ <font style="color:rgb(0, 0, 0);">MTS 分数</font>**<font style="color:rgb(0, 0, 0) !important;">越低</font>**<font style="color:rgb(0, 0, 0);">，代表该配置的 “时间 + 内存” 综合性能越好。</font>



![](https://cdn.nlark.com/yuque/0/2025/png/63194899/1764667144627-2234c1b6-6a2d-4b8c-82f3-8e50237180a2.png)

<font style="color:rgb(15, 17, 21);">图表展示了不同 Shuffle 算法在不同数据分布下的表现。</font>**<font style="color:rgb(15, 17, 21);">整体趋势</font>**<font style="color:rgb(15, 17, 21);">是：</font>

+ **<font style="color:rgb(15, 17, 21);">算法层面</font>**<font style="color:rgb(15, 17, 21);">：</font>`<font style="color:rgb(15, 17, 21);background-color:rgb(235, 238, 242);">排序聚合 (sort-agg)</font>`<font style="color:rgb(15, 17, 21);"> 在面对</font>**<font style="color:rgb(15, 17, 21);">倾斜 (skew)</font>**<font style="color:rgb(15, 17, 21);"> 和</font>**<font style="color:rgb(15, 17, 21);">均匀 (uniform)</font>**<font style="color:rgb(15, 17, 21);"> 数据时，表现均优于 </font>`<font style="color:rgb(15, 17, 21);background-color:rgb(235, 238, 242);">哈希聚合 (hash-agg)</font>`<font style="color:rgb(15, 17, 21);"></font>
+ **<font style="color:rgb(15, 17, 21);">数据分布层面</font>**<font style="color:rgb(15, 17, 21);">：无论哪种算法，在</font>**<font style="color:rgb(15, 17, 21);">均匀分布</font>**<font style="color:rgb(15, 17, 21);">数据上的性能都最好，而</font>**<font style="color:rgb(15, 17, 21);">Zipf分布</font>**<font style="color:rgb(15, 17, 21);">（一种典型的高度倾斜分布）会给所有操作带来最严重的性能挑战。</font>

**<font style="color:rgb(15, 17, 21);">优化建议</font>**<font style="color:rgb(15, 17, 21);">应结合业务数据的实际特点：</font>**<font style="color:rgb(15, 17, 21);">若数据分布相对均匀，</font>**`**<font style="color:rgb(15, 17, 21);background-color:rgb(235, 238, 242);">sort-agg</font>**`**<font style="color:rgb(15, 17, 21);"> 是最佳选择；若已知数据存在倾斜，更应优先采用 </font>**`**<font style="color:rgb(15, 17, 21);background-color:rgb(235, 238, 242);">sort-agg</font>**`**<font style="color:rgb(15, 17, 21);"> 以获得更稳健的性能；而如果数据是类似Zipf的极端倾斜分布，首选是 </font>**`**<font style="color:rgb(15, 17, 21);background-color:rgb(235, 238, 242);">hash-agg</font>**`**<font style="color:rgb(15, 17, 21);">，</font>**<font style="color:rgb(15, 17, 21);">最好避免用</font>**<font style="color:rgb(15, 17, 21);"> </font>**`**<font style="color:rgb(15, 17, 21);background-color:rgb(235, 238, 242);">repart</font>**`**<font style="color:rgb(15, 17, 21);"> </font>**<font style="color:rgb(15, 17, 21);">这种workload。</font>

## 失败的50万数据探索
我们在实验过程中发现**数据量为 500000 时仅能跑出 8 分区，而16 分区失败。**经过查阅相关文献和进行补充实验，原因总结如下：

+ Spark 中每个 Executor 的内存会分配给多个并行任务，每个任务处理一个分区的数据。当分区数增加时，单个分区的数据量会减小，任务处理单分区时所需的内存会降低，反而能减少内存溢出风险。而我们的测试过程中包含聚合、连接等 Shuffle 操作，Shuffle 过程对内存更敏感。Executor 需要为每个下游分区创建缓冲区，分区数越多，缓冲区总占用越大。Shuffle Read 需要拉取所有上游分区的数据并在内存中聚合，分区数过多会导致内存中同时处理的分片增多，若 Executor 内存不足则会溢出。若**分区数远超 Executor 核心数，会产生大量小任务，Driver 需维护更多任务元数据，间接占用 Driver 内存。**
+ **数据分布的影响。**现在成功的数据分布是均匀分布，而在数据倾斜分布下，极少数键对应了绝大多数数据（例如 1% 的键承载了 99% 的数据），这种**极端不均衡在 16 个分区下会被放大，最终触发内存溢出或任务超时**：分区数据量的 “两极分化” 加剧在 8 个分区时，倾斜的数据会被 “分摊” 到 8 个分区中，即使某个分区数据量较大（比如占总数据的 30%），但总分区数少，单个分区的绝对数据量仍可能处于 Executor 内存可承受范围。而 16 个分区时，由于数据倾斜集中在少数键，Spark 的 Hash 分区器会将这些 “热点键” 集中分配到1-2 个分区中，导致这两个分区的数据量可能高达总数据的 80%-90%。此时，单分区数据量远超 Executor 内存的处理能力（尤其是聚合操作需要缓存大量中间结果），直接触发 OOM（内存溢出）。
    - **倾斜分布**时，**对于 Hash Shuffle**，每个 Executor 需为 16 个下游分区创建写缓冲区，虽然大部分缓冲区空闲，但 “热点分区” 的缓冲区需要频繁刷写磁盘（数据量太大），导致**磁盘 I/O 骤增**，任务运行时间超过超时阈值；在聚合时需要在内存中构建哈希表存储聚合结果，倾斜数据会导致**哈希表急剧膨胀**，远超分配的内存比例，直接溢出。**对于 Sort Shuffle**，需要先对数据排序再分发给 16 个分区，“热点分区” 的排序过程会占用大量内存（排序算法需要缓存待排序数据），若内存不足则会溢写磁盘，但**倾斜数据的排序溢写效率极低**，最终导致任务卡住，**排序后的倾斜数据也需在内存中缓存**完整的聚合结果，大数量级下内存无法容纳，同样触发 OOM。
    - **长尾分布**时，数据不会像 skew 那样集中在 1-2 个键，而是存在多个 “次热点键”（例如 10 个键各占 5% 的数据）。在 16 个分区下，这些次热点键可能被分配到 5-6 个分区中，每个分区的数据量虽不及 skew 的热点分区，但单个分区数据量仍可能达到总数据的 10%-15%。当这 5-6 个 “次热点分区” 同时被同一个 Executor 处理时，多个分区的内存需求会叠加，**若 Executor 内存配置不足，则会因总内存超限而失败，Shuffle 阶段的资源竞争加剧 Zipf 分布的 “长尾特性”** ，此时，Executor 的资源会向次热点分区倾斜，导致小分区任务等待资源，整体任务运行时间拉长，甚至触发 Driver 的 “任务无响应” 判断。

## 分工
尽可能详细地写出每个人的具体工作和贡献度，并按贡献度大小进行排序。

