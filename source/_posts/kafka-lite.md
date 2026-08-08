---
title: 从零手写一个迷你 Kafka：kafka-lite 架构全解析
date: 2026-08-08 10:00:00
categories: [技术]
tags: [消息队列, Kafka, Java]
---

> 前阵子想真正搞懂 Kafka 的核心架构，与其看书背书，不如自己动手写一个。于是有了 **kafka-lite**：一个纯 JDK、运行时零第三方依赖的迷你消息队列，用来**对齐 Kafka 最核心的那些概念**——分区日志、消费组 offset、自定义 TCP 协议。
>
> 项目地址：[github.com/onep1us/kafka-lite](https://github.com/onep1us/kafka-lite)

---

## 为什么写一个"迷你 Kafka"

读 Kafka 源码很容易陷入细节：控制器、ISR、HW/LEO、副本复制、压缩……概念太多，经常读着读着就忘了主线。写一个最小版本的好处是：**把每个概念都亲手实现一遍，主线就长在自己脑子里了**。

kafka-lite 的目标很克制：

- 实现 Kafka 的**核心概念**（topic + 分区、磁盘追加日志、消费组 offset、客户端-服务端协议）
- **刻意不做**复杂机制（多 Broker、副本复制、再平衡、压缩、事务），保持项目一眼能读完
- 运行时**零第三方依赖**，只用 JDK 自带的设施（`java.util.logging`、NIO、`RandomAccessFile`），测试才用 JUnit 5

## 核心特性

- **Topic + 分区**：每个 topic 可指定分区数，每个分区一个独立的磁盘追加日志
- **磁盘持久化**：消息追加到 `dataDir/topics/<topic>/<partition>.log`，Broker 重启后消息不丢
- **消费组 + offset 管理**：`(groupId, topic, partition) → offset` 持久化，重启后可续读
- **Producer / Consumer 客户端**：Java API，同步确认（`send()` 成功返回即已落盘）
- **自定义 TCP 二进制协议**：模拟真实 Kafka 客户端-Broker 的通信方式

## 工程结构

三个 Maven 模块，依赖方向 `client → broker → common`：

```
kafka-lite/
├── kafka-lite-common/         # 消息模型 + 协议编解码（client 与 broker 共享的唯一契约）
│   └── com.example.kafkalite.common
│       ├── Message / TopicInfo / ErrorCode
│       └── protocol/          # MessageType / Bodies / ProtocolCodec
├── kafka-lite-broker/         # Broker：存储 + TCP 服务
│   └── com.example.kafkalite.broker
│       ├── BrokerServer / BrokerConfig      # 服务与配置，main 入口
│       ├── net/ConnectionHandler            # 每连接请求循环
│       └── store/                          # PartitionLog / TopicStore / OffsetStore
└── kafka-lite-client/         # Producer / Consumer / 演示
    └── com.example.kafkalite.client
        ├── KafkaLiteProducer / KafkaLiteConsumer
        └── demo/DemoMain      # 端到端演示
```

有意思的是 `kafka-lite-client` 依赖 broker 只是为了让 `DemoMain` 能内嵌启动 Broker；业务代码其实**只依赖 common**。这恰好复刻了真实世界的边界：客户端与 Broker 通过协议沟通，不该互相咬死实现。

![kafka-lite 整体架构：client / broker / common 三模块与一次交互的数据流](/images/kafka-lite-architecture.svg)

> 图里实线是请求数据流，虚线箭头表示 client / broker 对 common 的依赖。Broker 内部从上到下就是一条"接入 → 分发 → 存储"的链路。

## 一次 `send()` 的完整旅程

这是理解整个项目的主线，所有代码都围着它转：

![一次 send() 的完整时序：Producer → ClientConnection → ConnectionHandler → TopicStore → PartitionLog](/images/kafka-lite-send-sequence.svg)

简版路径：`Producer.send` → 编码成帧 → TCP → `BrokerServer.accept` → `ConnectionHandler`（按 `MessageType` 分发）→ `TopicStore.resolveForAppend`（partition == -1 自动路由到 0）→ `PartitionLog.append`（返回 offset）→ 响应帧原样回传。

关键点：`correlationId` 由客户端每请求自增，broker 原样回传，客户端同步等待"编号匹配"的响应帧。因为连接是同步串行的，所以**没有并发乱序问题**——这是刻意的简化。

## 存储设计：追加日志 + 内存索引 + offset

### 分区日志（`PartitionLog`）

每个 topic-分区对应一个文件，磁盘记录格式：

```
[offset:8][keyLen:4][key bytes?][valueLen:4][value]
```

![kafka-lite 存储设计：dataDir 目录布局、0.log 记录格式、内存索引的对应关系](/images/kafka-lite-storage.svg)

`keyLen == -1` 表示无 key。写入永远是"追加"：`pos = raf.length(); raf.seek(pos);`——只在文件末尾写，绝不修改已有字节。三个好处：offset 单调递增、顺序写比随机写快一个数量级、不可变 → 可随时重放历史。

内存里维护一对平行数组 `indexOffsets` / `indexPositions`，就是"offset → 文件位置"的索引（书的目录）。`read(offset)` 时二分查找定位，把 O(n) 变成 O(log n)。**注意：索引里存的是真实 offset 值，不是数组下标**——现在恰好是 0,1,2,3 只是密排巧合，做了 retention 或稀疏索引后就会不同。

> 💡 启动时会不会把全部消息读进内存？**不会**。`loadIndex` 只读每条记录头部 16 字节（offset/keyLen/valueLen）重建索引，消息数据永远在磁盘。所以内存占用跟**条数**有关、跟**消息大小**无关。

### offset 存储（`OffsetStore`）

消费进度落在 `dataDir/offsets/<group>/<topic>/<partition>`，就是一个十进制数字。写入用"临时文件 + 原子重命名"，避免写到一半进程挂了把文件写坏。

分工很清晰：**log 存"数据"，offset 文件只存"读到哪"**。消息永远从 log 读，offset 只是读的起点参数。

## 自定义 TCP 协议

协议是 client 与 broker 之间**唯一的契约**，改协议必须两端同步改。帧格式：

```
[magic "KL":2][version:1][type:1][correlationId:4][bodyLen:4][body]
```

![kafka-lite 协议帧格式：12 字节定长头 + body，大端序](/images/kafka-lite-frame.svg)

大端序，12 字节定长头。`MessageType` 用一段编码空间区分请求和响应：

- **请求（1–6）**：`CREATE_TOPIC` / `LIST_TOPICS` / `PRODUCE` / `FETCH` / `COMMIT_OFFSET` / `FETCH_OFFSET`
- **响应（11–16）**：对应每种请求一个响应类型

`MessageType.responseType()` 把请求映射到响应；`ProtocolCodec.encodeBody/decodeBody` 按类型编解码消息体。新增一种命令要动 `Bodies`（schema）、`ProtocolCodec`、`MessageType`、`ConnectionHandler` 四处——这在项目里既是扩展点，也是自测题。

传输层面有两个小优化：客户端和服务端都开了 **`TCP_NODELAY`**（关掉 Nagle 算法，消除小请求-响应往返里最多 ~40ms 的额外延迟）；`FETCH` 响应支持一次批量返回多条消息。

## 消费组语义与投递保证

offset 按 `(groupId, topic, partition)` 持久化。刻意**不做再平衡**——同组不同实例互不感知，各自从自己提交的进度消费。consumer 构造时读已提交 offset（无则 0），`poll` 推进本地 `position`，`commit` 落盘。

投递语义是标准的 **at-least-once**：

![kafka-lite 消费流程：poll 推进 position、commit 落盘，以及 at-least-once 的重复窗口](/images/kafka-lite-consume-flow.svg)

- `poll()` 从本地 position 拉取，成功后 `position = last.offset + 1`（只在内存推进）
- `commit()` 把 position 原子落盘
- 因为 position 推进和 commit 落盘不是原子的，进程在"处理完、commit 前"崩溃，恢复后就会**重复消费**那批消息

这是 at-least-once 的必然产物，防不住，但业务侧做**幂等**就能让它"看起来像恰好一次"。真实的 Kafka 用幂等生产者 + 事务来实现 exactly-once，kafka-lite 刻意不做——这是留给读者思考的延伸点。

## 并发模型与一个真实踩过的坑

整个项目是"同步串行"的简单模型：

- `PartitionLog` 内部用 `synchronized(lock)` 串行化 append/read
- `ConnectionHandler` 每连接一线程，同连接内请求串行处理
- 客户端连接同步串行，无并发乱序问题

这里有个全项目最值得读的坑。`loadIndex` 读磁盘记录时，**读完 `keyLen` 必须先跳过 key 字节、再读 `valueLen`**：

```java
// 磁盘布局：keyLen 和 valueLen 之间有 key 数据
raf.seek(raf.getFilePointer() + keyLen);   // ← 这一跳不能省
int valueLen = raf.readInt();
```

直接连读会把 key 开头的 4 字节误读成 `valueLen`。这个 bug 真实发生过（带 key 的消息重启加载后数据错乱）。读懂它，你就真正理解了磁盘格式。

## 怎么跑起来

端到端 demo（生产 → 消费 → 提交 offset → 重启 Broker → 验证续读）只要两条命令：

```bash
./mvnw -q compile
java -cp "kafka-lite-common/target/classes;kafka-lite-broker/target/classes;kafka-lite-client/target/classes" \
  com.example.kafkalite.client.demo.DemoMain
```

demo 验证了完整闭环：生产 5 条 → 消费提交 offset → **重启 Broker** → 同组新 Consumer 首次 poll 看到 0 条（offset 已持久化）→ 再追加 2 条 → 只收到这 2 条。日志和消费进度都跨重启保留。

独立启动 Broker：

```bash
java -cp "kafka-lite-common/target/classes;kafka-lite-broker/target/classes" \
  com.example.kafkalite.broker.BrokerServer --port 9092 --dataDir ./data
```

`port = 0` 表示由操作系统分配端口（测试与 demo 都这么用），通过 `BrokerServer.port()` 取实际端口。注意**Broker 重启后端口会变**，客户端配置要跟着更新。

全项目 3 个模块共 **25 个测试**，一条命令跑完：

```bash
./mvnw -q verify
```

有个设计细节值得一提：broker 的集成测试 `BrokerIntegrationTest` 用 `RawClient` 直接按协议走真实 socket，**刻意不依赖 client 模块**，避免产生 broker → client 的反向依赖。

## 范围边界：刻意不做什么

| 本项目不做 | 真实 Kafka 对应概念 |
|---|---|
| 多 Broker | 集群、Controller、ISR |
| 副本复制 | Replication、Leader/Follower |
| 分区再平衡 | Consumer Group Rebalance |
| key 路由 | Partitioning、Hash Partitioner |
| 压缩 | Compression（lz4/snappy/zstd） |
| 幂等/事务 | Idempotent Producer、Transactions |

这张表其实就是真实 Kafka 的功能目录。学完本项目再回头啃 Kafka，这些概念都有锚点了。

## 下一步：怎么把 kafka-lite 变成"真高可用"

项目已有**持久化**（重启不丢数据，durability），但缺的是**可用性**（进程挂了还能继续服务，availability）。从易到难有四级阶梯：

- **L0 快速恢复（零代码）**：systemd / `docker run --restart=always` 守护，挂了自动拉起
- **L1 共享存储 + 冷备**：多实例共享同一 `dataDir` + 切换脚本。⚠️ 红线：同一时刻只能有一个实例在写，否则日志立刻损坏
- **L2 双写 + 心跳选主（推荐）**：`PartitionLog.append` 落盘后把数据同步复制到备机，备机心跳超时自升为主，用 **epoch 序号**防"旧主复活"造成双主。这就是真实 Kafka"副本复制 + Leader 选举 + acks 语义"的最小可运行版本
- **L3 完整 ISR + 选举（可选）**：等于重写半个 Kafka，建议作为独立项目

## 写在最后

kafka-lite 的价值不在于"能用"，而在于**一上午就能读完、一周就能改造成自己想要的样子**。仓库里还附带了一套学习路径（`docs/learning.md`）和问答笔记（`docs/qa-notes.md`），包括"带 key 路由、多分区、CRC 校验、稀疏索引、分段日志 + retention、生产批量"等十几个从易到难的动手练习，每个练习都指向真实 Kafka 的一个机制。

如果你想理解 Kafka，与其对着文档背概念，不如来改一改这个迷你版。

**相关资源**

- 项目仓库：[github.com/onep1us/kafka-lite](https://github.com/onep1us/kafka-lite)
- 学习路径 `docs/learning.md`：从跑通 demo 到能动手改造的完整路线
- 问答笔记 `docs/qa-notes.md`：存储/可靠性/HA/协议/架构对比的问答精华
