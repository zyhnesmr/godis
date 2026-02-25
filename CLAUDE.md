# Godis 项目进度

> Go 语言实现的 Redis 兼容内存数据库

## 当前进度

### ✅ 已完成模块 (15/24 核心任务)

| 模块 | 状态 | 说明 |
|------|------|------|
| 项目目录结构 | ✅ | internal/, pkg/, cmd/, config/ |
| 配置系统 | ✅ | Redis 兼容配置文件解析 |
| RESP 协议 | ✅ | 完整的 RESP2 编解码 |
| TCP 网络层 | ✅ | Server, Conn, Handler |
| 数据库核心 | ✅ | Dict, DBSelector, Object |
| String 数据结构 | ✅ | int/embstr/raw 编码 |
| String 命令 | ✅ | SET, GET, MGET, MSET, INCR, DECR 等 |
| Key 管理命令 | ✅ | DEL, EXISTS, TYPE, KEYS, SCAN, EXPIRE, TTL 等 |
| Server 命令 | ✅ | PING, ECHO, INFO, DBSIZE, TIME |
| 命令分发器 | ✅ | Dispatcher, Reply, Command |
| 主入口和工具 | ✅ | main.go, Makefile, build.sh |
| Hash 数据结构 | ✅ | HSET, HGET, HMGET, HDEL, HEXISTS, HINCRBY 等 |
| List 数据结构 | ✅ | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN 等 |
| Set 数据结构 | ✅ | SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SMOVE, SINTER, SUNION, SDIFF, SINTERSTORE, SUNIONSTORE, SDIFFSTORE, SSCAN, SMISMEMBER 等 |
| ZSet 数据结构 | ✅ | ZADD, ZREM, ZSCORE, ZINCRBY, ZCARD, ZCOUNT, ZRANGE, ZREVRANGE, ZRANK, ZREVRANK, ZPOPMAX, ZPOPMIN, ZRANGEBYSCORE, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZUNION, ZINTER, ZUNIONSTORE, ZINTERSTORE, ZDIFF, ZDIFFSTORE, ZSCAN, ZRANDMEMBER, ZMSCORE 等 |
| 过期机制 | ✅ | 时间轮, 主动/被动过期, Expire/ExpireAt/TTL/Persist/SETEX/PSETEX |
| 淘汰策略 | ✅ | LRU/LFU/TTL/Random/NoEviction, allkeys-volatile变体 |

### ⏳ 待开发模块

| 模块 | 优先级 | 涉及命令 |
|------|--------|----------|
| 发布订阅 | 中 | PUBLISH, SUBSCRIBE, PSUBSCRIBE |
| 事务支持 | 中 | MULTI, EXEC, DISCARD, WATCH |
| RDB 持久化 | 中 | 快照保存，RDB 格式编码/解码 |
| AOF 持久化 | 中 | 追加日志，AOF 重写 |
| Stream 数据结构 | 低 | XADD, XREAD, XGROUP, XACK |
| Bitmap/HyperLogLog | 低 | SETBIT, GETBIT, BITCOUNT, PFADD... |
| 地理位置 | 低 | GEOADD, GEORADIUS, GEOHASH... |
| Lua 脚本 | 低 | EVAL, EVALSHA, SCRIPT LOAD/FLUSH |

## 测试验证

```bash
# 启动服务器
./bin/godis

# 测试 PING
redis-cli -p 6379 PING
# 预期: PONG

# 测试 SET/GET
redis-cli -p 6379 SET key value
redis-cli -p 6379 GET key
# 预期: value

# 测试过期
redis-cli -p 6379 SETEX mykey 3 "hello"
redis-cli -p 6379 TTL mykey
# 预期: 2 或 3
redis-cli -p 6379 GET mykey
# 预期: hello
# 等待3秒后
redis-cli -p 6379 GET mykey
# 预期: (nil)
```

### 淘汰策略测试

```bash
# 使用淘汰策略配置启动
./bin/godis -c config/godis-eviction.conf

# 配置示例
maxmemory 100mb
maxmemory-policy allkeys-lru
maxmemory-samples 5
```

## 技术栈

- **语言**: Go 1.24+
- **协议**: RESP2 (REdis Serialization Protocol)
- **网络**: TCP with keepalive
- **并发**: sync.RWMutex + per-connection goroutine
- **数据结构**: Dict with incremental rehash
- **过期**: 时间轮 + 懒惰删除 + 主动扫描
- **淘汰**: 近似LRU/LFU + EvictionPool (256 buckets)

## 开发笔记

### Arity 规则
- 正数: 命令名 + 参数总数 (如 GET = 2)
- 负数: 至少需要的参数数 (如 SET = -3 表示至少 2 个参数)
- 检查时需要减 1，因为 args 不包含命令名

### RESP 格式
```
Simple Strings: +OK\r\n
Errors:        -ERR message\r\n
Integers:      :123\r\n
Bulk Strings:  $5\r\nhello\r\n
Arrays:        *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
```

### 编译运行
```bash
make build      # 编译
make run        # 运行
make test       # 测试
make clean      # 清理
```

## 下次开发启动

优先级顺序:
1. **Hash** - 常用数据结构，HSET/HGET 使用频繁 ✅
2. **List** - LPUSH/LPOP/RANGE 队列/栈操作 ✅
3. **Set** - 集合去重、交集、并集 ✅
4. **ZSet** - 排行榜、范围查询 ✅
5. **过期机制** - 核心功能，时间轮优化 ✅
6. **淘汰策略** - LRU/LFU 内存管理 ✅
7. **发布订阅** - PUBLISH, SUBSCRIBE, PSUBSCRIBE ← 下一个
