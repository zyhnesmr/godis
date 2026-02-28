# Godis 架构设计文档

> Go 语言实现的 Redis 兼容内存数据库

## 一、项目概述

Godis 是一个使用 Go 语言实现的 Redis 兼容内存数据库，支持完整的 Redis 通讯协议 (RESP2) 和核心功能。

### 核心特性

- **协议兼容**: 完整支持 RESP2 协议
- **数据结构**: String, Hash, List, Set, ZSet, Stream, Bitmap, HyperLogLog, Geo
- **持久化**: RDB 快照 + AOF 日志双持久化
- **高可用**: 事务支持、发布订阅、过期淘汰策略
- **脚本支持**: Lua 脚本 (EVAL/EVALSHA/SCRIPT)
- **并发控制**: sync.RWMutex + per-connection goroutine

---

## 二、系统架构

### 2.1 整体架构图

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
│  │  │   Command    │  │    Command   │  │    Lua       │              │    │
│  │  │  Dispatcher  │──│   Executor   │──│  Script      │              │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                        │
│                                      ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                          Data Structure Layer                        │    │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │    │
│  │  │ String  │  │   Hash  │  │   List  │  │   Set   │  │ ZSet    │   │    │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘   │    │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐                   │    │
│  │  │ Stream  │  │  Bit    │  │  Hyper  │  │  Geo    │                   │    │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                      │                                        │
│                                      ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     Storage & Features Layer                         │    │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │    │
│  │  │   Memory     │  │  Expire      │  │  Eviction    │              │    │
│  │  │  Manager     │  │  Manager     │  │  Policy      │              │    │
│  │  ├──────────────┤  ├──────────────┤  ├──────────────┤              │    │
│  │  │ Transaction  │  │  Pub/Sub     │  │  Persistence │              │    │
│  │  │   (MULTI)    │  │  Manager     │  │  RDB/AOF     │              │    │
│  │  └──────────────┘  └──────────────┘  └──────────────┘              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心模块说明

| 模块 | 职责 |
|------|------|
| **Networking** | TCP 连接管理、RESP 协议编解码 |
| **Command** | 命令分发、执行、路由、Lua 脚本 |
| **Data Structure** | 9 种数据结构实现 |
| **Storage** | 内存管理、过期策略、淘汰算法 |
| **Persistence** | RDB 快照、AOF 日志 |
| **Transaction** | 事务支持、乐观锁 |
| **Pub/Sub** | 发布订阅、模式匹配 |

---

## 三、目录结构

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
│   │   ├── reply.go                # 回复构建器
│   │   │
│   │   └── commands/               # 命令实现
│   │       ├── key.go              # 键管理 (DEL, EXISTS, EXPIRE...)
│   │       ├── string.go           # 字符串 (SET, GET, MSET...)
│   │       ├── hash.go             # 哈希 (HSET, HGET...)
│   │       ├── list.go             # 列表 (LPUSH, LPOP...)
│   │       ├── set.go              # 集合 (SADD, SMEMBERS...)
│   │       ├── zset.go             # 有序集合 (ZADD, ZRANGE...)
│   │       ├── bitmap.go           # 位图 (SETBIT, GETBIT, BITOP...)
│   │       ├── hyperloglog.go      # HyperLogLog (PFADD, PFCOUNT...)
│   │       ├── geo.go              # 地理位置 (GEOADD, GEODIST...)
│   │       ├── stream.go           # 流 (XADD, XREAD...)
│   │       ├── pubsub.go           # 发布订阅 (PUBLISH, SUBSCRIBE...)
│   │       ├── transaction.go      # 事务 (MULTI, EXEC...)
│   │       ├── script.go           # Lua 脚本 (EVAL, EVALSHA...)
│   │       ├── server.go           # 服务器 (PING, INFO...)
│   │       └── persistence.go      # 持久化 (SAVE, BGSAVE, BGREWRITEAOF...)
│   │
│   ├── database/
│   │   ├── db.go                   # 数据库核心
│   │   ├── dict.go                 # 字典实现 (渐进式 rehash)
│   │   ├── object.go               # 数据对象封装
│   │   └── selector.go             # 数据库选择器
│   │
│   ├── datastruct/                 # 数据结构实现
│   │   ├── string/
│   │   │   ├── string.go           # 字符串实现 (含位操作)
│   │   │   └── encoding.go         # 编码方式 (int/embstr/raw)
│   │   ├── hash/
│   │   │   └── hash.go             # 哈希表实现
│   │   ├── list/
│   │   │   └── list.go             # 双向链表实现
│   │   ├── set/
│   │   │   └── set.go              # 集合实现
│   │   ├── zset/
│   │   │   ├── zset.go             # 有序集合实现
│   │   │   └── skiplist.go         # 跳表实现
│   │   ├── stream/
│   │   │   ├── stream.go           # 流数据结构
│   │   │   ├── radix_tree.go       # 基数树索引
│   │   │   └── consumer.go         # 消费者组
│   │   ├── hyperloglog/
│   │   │   └── hll.go              # HyperLogLog (MurmurHash2)
│   │   └── geo/
│   │       └── geohash.go          # Geohash 编码/距离计算
│   │
│   ├── expire/
│   │   ├── expire.go               # 过期管理器
│   │   ├── wheel.go                # 时间轮实现
│   │   └── scheduler.go            # 过期调度器
│   │
│   ├── eviction/
│   │   ├── policy.go               # 淘汰策略 (LRU/LFU/TTL/Random)
│   │   └── manager.go              # 淘汰管理器
│   │
│   ├── persistence/
│   │   ├── rdb/
│   │   │   ├── rdb.go              # RDB 格式
│   │   │   ├── encoder.go          # RDB 编码器
│   │   │   └── decoder.go          # RDB 解码器
│   │   ├── aof/
│   │   │   ├── aof.go              # AOF 实现
│   │   │   ├── rewrite.go          # AOF 重写
│   │   │   └── fsync.go            # Fsync 策略
│   │   └── loader.go               # 数据加载器
│   │
│   ├── pubsub/
│   │   └── manager.go              # 发布订阅管理器
│   │
│   ├── transaction/
│   │   └── manager.go              # 事务管理器 (MULTI/EXEC/WATCH)
│   │
│   └── script/
│       └── manager.go              # Lua 脚本管理器
│
├── pkg/
│   ├── utils/
│   │   ├── bytes.go                # 字节工具
│   │   └── math.go                 # 数学工具
│   └── log/
│       └── logger.go               # 日志模块
│
├── config/
│   └── godis.conf                  # 配置文件
│
├── docs/
│   └── ARCHITECTURE.md             # 架构文档
│
├── Dockerfile
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

---

## 四、数据结构

### 4.1 String (字符串)

**编码方式**:
- `int`: 整数 (≤ long, 8字节)
- `embstr`: 嵌入式字符串 (≤ 44字节)
- `raw`: 原始字符串 (> 44字节)

**核心命令**: SET, GET, MSET, MGET, INCR, DECR, APPEND, GETRANGE, SETRANGE, STRLEN

### 4.2 Hash (哈希)

**核心命令**: HSET, HGET, HMGET, HDEL, HEXISTS, HINCRBY, HKEYS, HVALS, HLEN, HGETALL

### 4.3 List (列表)

**核心命令**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LSET, LLEN, LINSERT, LTRIM, LREM

### 4.4 Set (集合)

**核心命令**: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SMOVE, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SSCAN, SMISMEMBER

### 4.5 ZSet (有序集合)

**数据结构**: 跳表 (SkipList) + 哈希表

**核心命令**: ZADD, ZREM, ZSCORE, ZINCRBY, ZCARD, ZCOUNT, ZRANGE, ZREVRANGE, ZRANK, ZREVRANK, ZPOPMAX, ZPOPMIN, ZRANGEBYSCORE, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZUNION, ZINTER, ZUNIONSTORE, ZINTERSTORE, ZDIFF, ZDIFFSTORE, ZSCAN, ZRANDMEMBER, ZMSCORE

### 4.6 Stream (流)

**数据结构**: 数组存储 + RadixTree 索引 + 消费者组管理

**核心命令**: XADD, XLEN, XRANGE, XREVRANGE, XREAD, XDEL, XTRIM, XGROUP, XREADGROUP, XACK, XCLAIM, XPENDING, XINFO

**StreamID 格式**: `<millisecondsTimestamp>-<sequenceNumber>`

### 4.7 Bitmap (位图)

基于 String 类型扩展，将字符串每一位作为布尔值。

**核心命令**: SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO

**BITOP 操作**: AND, OR, XOR, NOT

### 4.8 HyperLogLog (基数估算)

**算法**: 10-bit precision, 1024 registers, MurmurHash2 哈希

**核心命令**: PFADD, PFCOUNT, PFMERGE

**内存占用**: 1 KB (1024 registers × 1 byte)

**误差率**: < 1%

### 4.9 Geo (地理位置)

**实现**: 基于 ZSet，52-bit Geohash 编码

**核心命令**: GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER

**距离计算**: Haversine 公式 (地球半径 6372797.5608 米)

---

## 五、高级功能

### 5.1 过期机制

**过期策略**:
- **被动过期**: 访问时检查过期
- **主动过期**: 定时扫描删除过期键
  - 每 10ms 随机抽查 20 个键
  - 如果过期键比例 > 25%，加速扫描

**核心命令**: EXPIRE, EXPIREAT, TTL, PTTL, PERSIST, SETEX, PSETEX

### 5.2 淘汰策略

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

**LRU 实现**: 近似 LRU，256 个 bucket 的 EvictionPool

**LFU 实现**: 对数计数器，定期衰减

### 5.3 事务机制

**事务流程**:
```
MULTI → 命令入队 (QUEUED) → EXEC/DISCARD
```

**WATCH 机制** (乐观锁):
- 监控 key 在事务执行期间是否被修改
- 通过 dirty key 回调机制实现
- CAS (Check-And-Set) 语义

**核心命令**: MULTI, EXEC, DISCARD, WATCH, UNWATCH

### 5.4 发布订阅

**订阅类型**:
- **频道订阅**: SUBSCRIBE/UNSUBSCRIBE
- **模式订阅**: PSUBSCRIBE/PUNSUBSCRIBE (支持 Glob 通配符)

**消息格式**:
- 频道订阅: `["message", "channel", "payload"]`
- 模式订阅: `["pmessage", "pattern", "channel", "payload"]`

**核心命令**: PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB

### 5.5 Lua 脚本

**解释器**: gopher-lua

**核心命令**:
| 命令 | 说明 |
|------|------|
| EVAL | 执行 Lua 脚本 |
| EVALSHA | 执行已缓存的脚本 (通过 SHA1) |
| SCRIPT LOAD | 加载脚本并返回 SHA1 |
| SCRIPT EXISTS | 检查脚本是否存在 |
| SCRIPT FLUSH | 清空脚本缓存 |
| SCRIPT KILL | 终止脚本执行 |
| SCRIPT SHOW | 显示脚本内容 |

**Lua API**:
```lua
redis.call(command, ...)    -- 执行 Redis 命令
redis.pcall(command, ...)   -- 安全执行

-- 全局变量
KEYS    -- Redis key 数组
ARGV    -- 参数数组
```

**返回值转换**:
| Lua 类型 | Redis 返回值 |
|----------|-------------|
| number | Integer |
| string | Bulk String |
| boolean | Integer (1=true, 0=false) |
| table | Array |
| nil | Nil |

---

## 六、持久化机制

### 6.1 RDB (快照持久化)

**保存时机**:
- 配置规则: `save 900 1`, `save 300 10`, `save 60 10000`
- 手动触发: SAVE, BGSAVE

**RDB 文件格式**:
- Magic: "REDIS"
- Version: 9 (4 bytes, big endian)
- Opcode: SELECTDB, RESIZEDB, EXPIREMS, EOF
- Value Types: String, List, Hash, Set, ZSet, ZSet2
- CRC64 校验和

**实现文件**: `internal/persistence/rdb/`

### 6.2 AOF (追加日志)

**追加策略**:
| 策略 | 说明 |
|------|------|
| always | 每个写命令都同步 |
| everysec | 每秒同步一次 (默认) |
| no | 由操作系统决定 |

**AOF 文件格式**:
- RESP 格式存储每个写命令
- 启动时自动重放命令恢复数据

**AOF 重写**:
- 当 AOF 文件大小超过指定比例时触发
- 遍历数据库生成最小命令集
- 后台进程执行，不阻塞主服务

**核心命令**: APPENDONLY, BGREWRITEAOF

**实现文件**: `internal/persistence/aof/`

---

## 七、RESP 协议

**RESP (REdis Serialization Protocol)** 数据类型:

| 类型 | 标识 | 格式示例 |
|------|------|----------|
| Simple Strings | `+` | `+OK\r\n` |
| Errors | `-` | `-Error message\r\n` |
| Integers | `:` | `:1000\r\n` |
| Bulk Strings | `$` | `$5\r\nhello\r\n` |
| Arrays | `*` | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

---

## 八、实现进度与问题

### 8.1 已完成功能 ✅

| 模块 | 状态 | 说明 |
|------|------|------|
| 项目框架 | ✅ | 目录结构、配置系统、构建工具 |
| 网络层 | ✅ | TCP 服务器、连接管理 |
| RESP 协议 | ✅ | 完整的 RESP2 编解码 |
| 命令系统 | ✅ | 分发器、执行器、156 个命令 |
| 数据库核心 | ✅ | Dict、DBSelector、Object |
| String | ✅ | int/embstr/raw 编码 |
| Hash | ✅ | 完整命令支持 |
| List | ✅ | 完整命令支持 |
| Set | ✅ | 完整命令支持 |
| ZSet | ✅ | 跳表 + 完整命令支持 |
| Stream | ✅ | RadixTree + 消费者组 |
| Bitmap | ✅ | 位操作、位运算 |
| HyperLogLog | ✅ | MurmurHash2 基数估算 |
| Geo | ✅ | Geohash + Haversine |
| 过期机制 | ✅ | 时间轮 + 主动/被动过期 |
| 淘汰策略 | ✅ | LRU/LFU/TTL/Random |
| 发布订阅 | ✅ | 频道 + 模式订阅 |
| 事务 | ✅ | MULTI/EXEC + WATCH 乐观锁 |
| RDB 持久化 | ✅ | 编码/解码 + CRC64 |
| AOF 持久化 | ✅ | 追加 + 重写 |
| Lua 脚本 | ✅ | EVAL/EVALSHA/SCRIPT |

### 8.2 已修复 Bug

| Bug | 描述 | 修复时间 |
|-----|------|----------|
| ZADD 参数解析 | `break` 只跳出 `switch` 而非 `for` 循环 | 2026-02-27 |
| Dict rehash | rehash 索引越界访问 | 2026-02-28 |
| HyperLogLog 哈希 | FNV-1a 对小输入碰撞严重，改用 MurmurHash2 | 2026-02-28 |
| HyperLogLog alpha | alpha 系数计算公式错误 | 2026-02-28 |
| RDB Stream | Stream 类型未处理导致 SAVE 失败 | 2026-02-28 |
| RDB List/Hash | 解码器类型断言问题 | 2026-02-28 |

### 8.3 已知问题

| 问题 | 描述 | 严重程度 |
|-----|------|----------|
| 并发竞态 | 高并发时偶发性丢失数据 (10 个并发 SET 只有 8-9 个成功) | 中 |

**并发竞态详情**:
```bash
# 测试用例
for i in {1..10}; do redis-cli SET key$i value$i & done
wait
redis-cli DBSIZE  # 可能返回 8-9 而非 10
```

**解决方案**: 需要实现更细粒度的锁机制或使用 `sync.Map` 优化。

---

## 九、配置文件

配置文件位于 `config/godis.conf`：

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
```

---

## 十、关键技术点

### 10.1 跳表 (SkipList)

ZSet 的核心数据结构，支持 O(log N) 的插入、删除、查找操作。

```
        Level 3:  1 ------------------> NULL
                  |                        ^
        Level 2:  1 -------> 4 ------->  6 -> NULL
                  |           |            ^
        Level 1:  1 -------> 4 ------->  6 -> NULL
                  |           |            |
        Level 0:  1 --> 3 -> 4 -> 5 ->  6 -> NULL
```

### 10.2 时间轮 (Time Wheel)

用于高效的过期时间管理。

```
+----------------------------------+
|         Time Wheel               |
|  +----+----+----+----+----+      |
|  | 1s | 2s | 3s |... | 60s|      |
|  +----+----+----+----+----+      |
|   ↓    ↓    ↓    ↓    ↓         |
|  [k] [k] [k] [k] [k]  [k]       |
+----------------------------------+
```

### 10.3 并发控制

- 使用 `sync.RWMutex` 保护数据结构
- 连接级锁避免全局锁竞争
- 分片设计支持多数据库并发

---

## 十一、编译与运行

```bash
# 编译
make build

# 运行
./bin/godis

# 带配置文件运行
./bin/godis -c config/godis.conf

# 测试
make test

# 清理
make clean
```
