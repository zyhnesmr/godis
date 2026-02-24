# Godis - Go语言实现的Redis内存数据库

## 项目概述

Godis 是一个使用 Go 语言实现的 Redis 兼容内存数据库，支持完整的 Redis 通讯协议 (RESP) 和核心功能。

## 一、系统架构设计

### 1.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Godis Server                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        Networking Layer                             │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │    │
│  │  │   TCP Server │  │  Connection  │  │  RESP Parser │              │    │
│  │  │   (Listener) │──│   Manager    │──│  /Serializer │              │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                        │
│                                      ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                       Command Processing Layer                       │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │    │
│  │  │   Command    │  │    Command   │  │    Command   │              │    │
│  │  │  Dispatcher  │──│   Executor   │──│    Queue     │              │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                        │
│                                      ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                          Data Structure Layer                        │    │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │    │
│  │  │ String  │  │   Hash  │  │   List  │  │   Set   │  │ ZSet    │   │    │
│  │  │   DB    │  │   DB    │  │   DB    │  │   DB    │  │   DB    │   │    │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘   │    │
│  │                                                                       │    │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐                   │    │
│  │  │  Stream │  │   Bit   │  │  Hyper  │  │  Geo    │                   │    │
│  │  │   DB    │  │   DB    │  │LogLog   │  │   DB    │                   │    │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                        │
│                                      ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        Storage Engine Layer                          │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │    │
│  │  │   Memory     │  │  Eviction    │  │  Expire      │              │    │
│  │  │  Manager     │  │  Policy      │  │  Manager     │              │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                       Persistence Layer                              │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │    │
│  │  │    RDB       │  │    AOF       │  │  AOF Rewrite  │              │    │
│  │  │   (Snapshot) │  │   (Log)      │  │    (Merge)    │              │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                       Transaction Layer                              │    │
│  │  ┌──────────────┐  ┌──────────────┐                                  │    │
│  │  │    MULTI     │  │     WATCH    │                                  │    │
│  │  │    EXEC      │  │    UNWATCH   │                                  │    │
│  │  └──────────────┘  └──────────────┘                                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 核心模块说明

| 模块 | 职责 |
|------|------|
| **Networking** | TCP 连接管理、RESP 协议编解码 |
| **Command** | 命令分发、执行、路由 |
| **Data Structure** | 5+1 种数据结构实现 |
| **Storage** | 内存管理、过期策略、淘汰算法 |
| **Persistence** | RDB 快照、AOF 日志 |
| **Transaction** | 事务支持、乐观锁 |

---

## 二、目录结构设计

```
godis/
├── cmd/
│   └── godis/
│       └── main.go                 # 程序入口
│
├── internal/
│   ├── config/
│   │   └── config.go               # 配置管理
│   │
│   ├── net/
│   │   ├── server.go               # TCP 服务器
│   │   ├── conn.go                 # 连接封装
│   │   └── handler.go              # 连接处理器
│   │
│   ├── protocol/
│   │   ├── resp/
│   │   │   ├── parser.go           # RESP 解析器
│   │   │   ├── serializer.go       # RESP 序列化
│   │   │   └── types.go            # RESP 数据类型
│   │   └── protocol.go             # 协议接口
│   │
│   ├── command/
│   │   ├── command.go              # 命令接口定义
│   │   ├── dispatcher.go           # 命令分发器
│   │   ├── executor.go             # 命令执行器
│   │   │
│   │   ├── commands/
│   │   │   ├── key.go              # 键管理命令 (DEL, EXISTS, EXPIRE...)
│   │   │   ├── string.go           # 字符串命令 (SET, GET, MSET...)
│   │   │   ├── hash.go             # 哈希命令 (HSET, HGET...)
│   │   │   ├── list.go             # 列表命令 (LPUSH, LPOP...)
│   │   │   ├── set.go              # 集合命令 (SADD, SMEMBERS...)
│   │   │   ├── zset.go             # 有序集合命令 (ZADD, ZRANGE...)
│   │   │   ├── stream.go           # 流命令 (XADD, XREAD...)
│   │   │   ├── pubsub.go           # 发布订阅命令 (PUBLISH, SUBSCRIBE...)
│   │   │   ├── transaction.go      # 事务命令 (MULTI, EXEC...)
│   │   │   ├── server.go           # 服务器命令 (PING, INFO...)
│   │   │   └── admin.go            # 管理命令 (CONFIG, FLUSHDB...)
│   │   │
│   │   └── reply.go                # 回复构建器
│   │
│   ├── database/
│   │   ├── db.go                   # 数据库核心
│   │   ├── dict.go                 # 字典实现
│   │   ├── object.go               # 数据对象封装
│   │   └── selector.go             # 数据库选择器
│   │
│   ├── datastruct/
│   │   ├── string/
│   │   │   ├── string.go           # 字符串实现
│   │   │   └── encoding.go         # 编码方式 (int, embstr, raw)
│   │   │
│   │   ├── hash/
│   │   │   ├── hash.go             # 哈希表实现
│   │   │   └── ziplist.go          # 压缩列表优化
│   │   │
│   │   ├── list/
│   │   │   ├── list.go             # 列表实现
│   │   │   ├── quicklist.go        # 快速列表
│   │   │   └── linkedlist.go       # 双向链表
│   │   │
│   │   ├── set/
│   │   │   ├── set.go              # 集合实现
│   │   │   └── intset.go           # 整数集合优化
│   │   │
│   │   ├── zset/
│   │   │   ├── zset.go             # 有序集合实现
│   │   │   ├── skiplist.go         # 跳表实现
│   │   │   └── dict+zset.go        # 哈希+跳表组合
│   │   │
│   │   ├── stream/
│   │   │   ├── stream.go           # 流数据结构
│   │   │   ├── radix_tree.go       # 基数树索引
│   │   │   └── consumer_group.go   # 消费者组
│   │   │
│   │   ├── bit/
│   │   │   └── bitmap.go           # 位图操作
│   │   │
│   │   ├── hyperloglog/
│   │   │   └── hll.go              # HyperLogLog 实现
│   │   │
│   │   └── geo/
│   │       └── geo.go              # 地理位置实现
│   │
│   ├── expire/
│   │   ├── expire.go               # 过期管理器
│   │   └── wheel.go                # 时间轮实现
│   │
│   ├── eviction/
│   │   ├── policy.go               # 淘汰策略接口
│   │   ├── lru.go                  # LRU 实现
│   │   ├── lfu.go                  # LFU 实现
│   │   └── ttl.go                  # TTL 淘汰
│   │
│   ├── persistence/
│   │   ├── rdb/
│   │   │   ├── rdb.go              # RDB 格式
│   │   │   ├── encoder.go          # RDB 编码器
│   │   │   └── decoder.go          # RDB 解码器
│   │   │
│   │   ├── aof/
│   │   │   ├── aof.go              # AOF 实现
│   │   │   ├── rewrite.go          # AOF 重写
│   │   │   └── fsync.go            # 持久化策略
│   │   │
│   │   └── loader.go               # 数据加载器
│   │
│   ├── pubsub/
│   │   ├── pubsub.go               # 发布订阅
│   │   ├── channel.go              # 频道管理
│   │   └── pattern.go              # 模式订阅
│   │
│   ├── transaction/
│   │   ├── transaction.go          # 事务管理
│   │   ├── multi.go               # MULTI/EXEC
│   │   └── watch.go                # WATCH 机制
│   │
│   ├── replication/
│   │   └── replication.go          # 主从复制 (可选)
│   │
│   └── cluster/
│       └── cluster.go              # 集群支持 (预留)
│
├── pkg/
│   ├── utils/
│   │   ├── bytes.go                # 字节工具
│   │   ├── math.go                 # 数学工具
│   │   └── time.go                 # 时间工具
│   │
│   └── log/
│       └── logger.go               # 日志模块
│
├── config/
│   └── godis.conf                  # 配置文件
│
├── scripts/
│   └── build.sh                    # 构建脚本
│
├── Dockerfile
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

---

## 三、核心功能实现规划

### 3.1 RESP 协议实现

**RESP (REdis Serialization Protocol)** 支持以下数据类型：

| 类型 | 标识 | 说明 |
|------|------|------|
| Simple Strings | `+` | 简单字符串，如 `+OK\r\n` |
| Errors | `-` | 错误信息，如 `-Error message\r\n` |
| Integers | `:` | 整数，如 `:1000\r\n` |
| Bulk Strings | `$` | 二进制安全字符串 |
| Arrays | `*` | 数组，命令请求和批量回复 |

### 3.2 数据结构实现

#### 3.2.1 String (字符串)

```
编码方式：
├── int                    # 整数 (≤ long, 8字节)
├── embstr                # 嵌入式字符串 (≤ 44字节)
└── raw                   # 原始字符串 (> 44字节)
```

**核心命令**: SET, GET, MSET, MGET, INCR, DECR, APPEND, GETRANGE, SETRANGE, STRLEN

#### 3.2.2 Hash (哈希)

```
编码方式：
├── ziplist               # 压缩列表 (entry < 512 且 value < 64字节)
└── hashtable             # 哈希表
```

**核心命令**: HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HEXISTS, HINCRBY, HKEYS, HVALS, HLEN

#### 3.2.3 List (列表)

```
编码方式：
├── quicklist             # 快速列表 (ziplist + linkedlist)
└── linkedlist            # 双向链表 (旧版)
```

**核心命令**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LSET, LLEN, LINSERT, LTRIM

#### 3.2.4 Set (集合)

```
编码方式：
├── intset                # 整数集合 (全为整数且 < 512个)
└── hashtable             # 哈希表
```

**核心命令**: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SINTER, SUNION, SDIFF

#### 3.2.5 ZSet (有序集合)

```
编码方式：
├── ziplist               # 压缩列表 (entry < 128 且 value < 64字节)
└── skiplist + hashtable  # 跳表 + 哈希表组合
```

**核心命令**: ZADD, ZREM, ZRANGE, ZREVRANGE, ZRANK, ZSCORE, ZINCRBY, ZCOUNT, ZCARD

### 3.3 过期与淘汰机制

#### 过期策略

1. **被动过期**: 访问时检查过期
2. **主动过期**: 定时扫描删除过期键
   - 每 10ms 随机抽查 20 个键
   - 如果过期键比例 > 25%，加速扫描

#### 淘汰策略

| 策略 | 说明 |
|------|------|
| noeviction | 不淘汰，返回错误 |
| allkeys-lru | LRU 淘汰任意键 |
| volatile-lru | LRU 淘汰设置了过期时间的键 |
| allkeys-lfu | LFU 淘汰任意键 |
| volatile-lfu | LFU 淘汰设置了过期时间的键 |
| allkeys-random | 随机淘汰任意键 |
| volatile-random | 随机淘汰设置了过期时间的键 |
| volatile-ttl | 淘汰即将过期的键 |

### 3.4 持久化机制

#### RDB (快照持久化)

```
保存时机:
├── save 900 1           # 900秒内至少1个key变化
├── save 300 10          # 300秒内至少10个key变化
├── save 60 10000        # 60秒内至少10000个key变化
└── 手动触发: SAVE, BGSAVE
```

**RDB 文件格式**:
- 使用 LZF 压缩
- CRC64 校验和
- 支持增量保存

#### AOF (追加日志)

```
追加策略:
├── always               # 每个写命令都同步
├── everysec             # 每秒同步一次 (默认)
└── no                   # 由操作系统决定
```

**AOF 重写**:
- 当 AOF 文件大小超过指定比例时触发
- 压缩多条命令为单条
- 后台进程执行

### 3.5 事务机制

```
事务流程:
├── MULTI                # 标记事务开始
├── ...命令入队...       # 命令进入队列
├── EXEC                 # 执行事务
└── DISCARD              # 取消事务
```

**WATCH 机制**:
- 乐观锁实现
- 监控 key 在事务执行期间是否被修改
- CAS (Check-And-Set) 语义

---

## 四、实现阶段划分

### 阶段一: 基础框架 (Week 1-2)
- [x] 项目脚手架搭建
- [x] TCP 服务器实现
- [x] RESP 协议编解码
- [x] 命令分发框架
- [x] 配置系统

### 阶段二: 核心数据结构 (Week 3-5)
- [x] String 类型及命令
- [x] Hash 类型及命令
- [x] List 类型及命令
- [x] Set 类型及命令
- [x] ZSet 类型及命令

### 阶段三: 高级功能 (Week 6-7)
- [ ] 过期机制
- [ ] 淘汰策略
- [ ] 发布订阅
- [ ] 事务支持

### 阶段四: 持久化 (Week 8)
- [ ] RDB 快照
- [ ] AOF 日志
- [ ] AOF 重写

### 阶段五: 高级数据结构 (Week 9)
- [ ] Stream 流
- [ ] Bitmap 位图
- [ ] HyperLogLog
- [ ] Geo 地理位置

### 阶段六: 优化与测试 (Week 10)
- [ ] 性能优化
- [ ] 单元测试
- [ ] 集成测试
- [ ] 压力测试

---

## 五、配置文件设计

配置文件位于 `config/godis.conf`，包含以下主要配置:

```conf
# 网络配置
bind 0.0.0.0
port 6379
timeout 0
tcp-keepalive 300

# 通用配置
daemonize no
pidfile /var/run/godis.pid
loglevel notice
logfile ""
databases 16

# 快照配置
save 900 1
save 300 10
save 60 10000
stop-writes-on-bgsave-error yes
rdbcompression yes
rdbchecksum yes
dbfilename dump.rdb
dir ./

# 限制配置
maxclients 10000
maxmemory <bytes>
maxmemory-policy noeviction
maxmemory-samples 5

# AOF 配置
appendonly no
appendfilename "appendonly.aof"
appendfsync everysec
no-appendfsync-on-rewrite no
auto-aof-rewrite-percentage 100
auto-aof-rewrite-min-size 64mb

# 慢查询配置
slowlog-log-slower-than 10000
slowlog-max-len 128

# 高级配置
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-max-ziplist-size -2
list-compress-depth 0
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
```

---

## 六、关键技术点

### 6.1 跳表 (SkipList) 实现

ZSet 的核心数据结构，支持 O(log N) 的插入、删除、查找操作。

```
          +------------+
          |  Header    |
          +-----+------+
                |
       Level 3:  1 ------------------>  NULL
                |                        ^
       Level 2:  1 -------> 4 ------->  6 -> NULL
                |           |            ^
       Level 1:  1 -------> 4 ------->  6 -> NULL
                |           |            |
       Level 0:  1 --> 3 -> 4 -> 5 ->  6 -> NULL
```

### 6.2 时间轮 (Time Wheel) 实现

用于高效的过期时间管理。

```
          +----------------------------------+
          |         Time Wheel               |
          |  +----+----+----+----+----+     |
          |  | 1s | 2s | 3s |... | 60s|     |
          |  +----+----+----+----+----+     |
          |   ↓    ↓    ↓    ↓    ↓        |
          |  [k] [k] [k] [k] [k]  [k]      |
          +----------------------------------+
```

### 6.3 并发控制

- 使用 `sync.RWMutex` 保护数据结构
- 连接级锁避免全局锁竞争
- 分片设计支持多数据库并发

---

## 七、Lua 脚本支持设计

使用 `github.com/yuin/gopher-lua` 作为 Lua 解释器：

```
┌─────────────────────────────────────────────────────────────┐
│                    Lua Script Engine                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Lua VM Pool │  │ Script Cache │  │  Sandbox     │     │
│  │  (并发安全)  │  │  (SHA1 缓存)  │  │  (安全限制)  │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

### Lua API 暴露

```lua
-- Redis 命令调用
redis.call(command, ...)
redis.pcall(command, ...)

-- 返回值处理
redis.error_reply(error)
redis.status_reply(status)
redis.log(level, message)
```

---

## 八、高性能优化策略

### 8.1 内存优化

```
编码优化:
├── int 编码: 小整数直接存储,避免分配
├── embstr: 小字符串内联存储 (< 44字节)
├── ziplist: 压缩列表减少指针开销
└── intset: 整数集合避免哈希表开销
```

### 8.2 并发优化

```
并发策略:
├── 连接池: 每个连接独立处理,避免全局锁
├── 读写锁: 读多写少场景使用 RWMutex
├── 分片锁: 大数据结构使用分片锁
└── 无锁结构: 计数器等使用 atomic
```

### 8.3 I/O 优化

```
网络优化:
├── epoll/kqueue: 高效事件循环
├── 读写缓冲: 减少系统调用
├── 批量回复: 管道支持
└── 零拷贝: 尽可能减少内存拷贝
```

---

## 九、Stream 实现设计

### 9.1 Stream 数据结构

```
Stream 结构:
┌─────────────────────────────────────────────────────────┐
│  master ID (最大消息ID)                                   │
│  last_delivered_id (投递ID)                              │
│  length (消息数量)                                        │
│  radix_tree (基数树索引)                                  │
│  ├─ 1234-0 -> Message {id, fields, ...}                  │
│  ├─ 1234-1 -> Message {id, fields, ...}                  │
│  └─ 1235-0 -> Message {id, fields, ...}                  │
│  consumer_groups:                                         │
│    └─ mygroup: {name, last_id, consumers[]}             │
└─────────────────────────────────────────────────────────┘
```

### 9.2 Stream 核心命令

| 命令 | 功能 |
|------|------|
| XADD | 添加消息 |
| XLEN | 获取消息数量 |
| XRANGE | 获取范围消息 |
| XREVRANGE | 反向获取 |
| XREAD | 读取消息 |
| XGROUP | 管理消费者组 |
| XREADGROUP | 消费者组读取 |
| XACK | 确认消息 |
| XCLAIM | 转移消息所有权 |
| XPENDING | 查看待处理消息 |
