# Godis 项目进度

> Go 语言实现的 Redis 兼容内存数据库

## 当前进度

### ✅ 已完成模块 (23/24 核心任务)

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
| Stream 数据结构 | ✅ | XADD, XLEN, XRANGE, XREVRANGE, XREAD, XDEL, XTRIM, XGROUP, XREADGROUP, XACK, XCLAIM, XPENDING, XINFO |
| 过期机制 | ✅ | 时间轮, 主动/被动过期, Expire/ExpireAt/TTL/Persist/SETEX/PSETEX |
| 淘汰策略 | ✅ | LRU/LFU/TTL/Random/NoEviction, allkeys-volatile变体 |
| 发布订阅 | ✅ | PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB |
| 事务支持 | ✅ | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| RDB 持久化 | ✅ | SAVE, BGSAVE, LASTSAVE, 启动自动加载, CRC64 校验 |
| AOF 持久化 | ✅ | APPENDONLY, BGREWRITEAOF, AOF 重写, Fsync 策略 |
| Bitmap 数据结构 | ✅ | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO |
| HyperLogLog 数据结构 | ✅ | PFADD, PFCOUNT, PFMERGE |
| Geo 地理位置 | ✅ | GEOADD, GEODIST, GEOHASH, GEOPOS, GEORADIUS, GEORADIUSBYMEMBER |

### ⏳ 待开发模块

| 模块 | 优先级 | 涉及命令 |
|------|--------|----------|
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

### 发布订阅测试

```bash
# 终端1: 订阅频道
redis-cli -p 6379 SUBSCRIBE mychannel
# 预期响应:
# *3
# $9
# subscribe
# $9
# mychannel
# :1

# 终端2: 发布消息
redis-cli -p 6379 PUBLISH mychannel "Hello, World!"
# 预期响应: (integer) 1

# 终端1将收到消息:
# *3
# $7
# message
# $9
# mychannel
# $13
# Hello, World!

# PUBSUB 命令
redis-cli -p 6379 PUBSUB CHANNELS      # 列出活跃频道
redis-cli -p 6379 PUBSUB NUMSUB mychannel  # 查看频道订阅数
redis-cli -p 6379 PUBSUB NUMPAT        # 查看模式订阅数
```

### 事务测试

```bash
# 基本事务
printf "MULTI\nSET key1 value1\nSET key2 value2\nEXEC\n" | redis-cli
# 预期: OK, QUEUED, QUEUED, OK (最后返回结果数组)

# DISCARD 取消事务
printf "MULTI\nSET key3 value3\nDISCARD\nGET key3\n" | redis-cli
# 预期: OK, QUEUED, OK, (nil)

# WATCH 乐观锁
redis-cli SET mykey hello
printf "WATCH mykey\nMULTI\nSET mykey world\nEXEC\n" | redis-cli
# 预期: OK, OK, QUEUED, OK (成功执行)

# 并发修改导致事务失败
# 连接1: WATCH key, MULTI, (等待), EXEC
# 连接2: (在EXEC前) SET key new_value
# 预期: EXEC 返回 nil 数组
```

### RDB 持久化测试

```bash
# 添加测试数据
redis-cli SET key1 "hello world"
redis-cli SET key2 "test value"
redis-cli HSET myhash field1 value1 field2 value2
redis-cli LPUSH mylist elem1 elem2 elem3
redis-cli SADD myset member1 member2 member3
redis-cli DBSIZE
# 预期: 5

# 手动保存
redis-cli SAVE
# 预期: OK. Duration: XXX

# 重启服务器验证数据加载
pkill -f "bin/godis"
./bin/godis
redis-cli DBSIZE
# 预期: 5

# 验证各数据类型
redis-cli GET key1         # 预期: hello world
redis-cli HGET myhash field1  # 预期: value1
redis-cli LPOP mylist       # 预期: elem1
redis-cli SMEMBERS myset    # 预期: member1, member2, member3

# 检查 RDB 文件
ls -la dump.rdb
hexdump -C dump.rdb | head -20
```

### Stream 数据结构测试

```bash
# XADD - 添加条目到流
redis-cli XADD mystream 100-0 name Alice age 30
redis-cli XADD mystream 100-1 name Bob age 25
redis-cli XADD mystream 100-2 name Charlie age 35

# XLEN - 获取流长度
redis-cli XLEN mystream
# 预期: 3

# XRANGE - 获取范围条目
redis-cli XRANGE mystream - +
# 预期: 返回所有条目

# XRANGE with COUNT
redis-cli XRANGE mystream - + COUNT 2
# 预期: 返回前2条条目

# XREVRANGE - 反向获取
redis-cli XREVRANGE mystream + -
# 预期: 反序返回所有条目

# XGROUP CREATE - 创建消费者组 (MKSTREAM 自动创建流)
redis-cli XGROUP CREATE newstream mygroup 0 MKSTREAM
# 预期: OK

# XADD 到新流
redis-cli XADD newstream 200-0 message hello

# XINFO STREAM - 查看流信息
redis-cli XINFO STREAM newstream
# 预期: length, groups, last-generated-id 等信息

# XINFO GROUPS - 查看消费者组
redis-cli XINFO GROUPS newstream
# 预期: 消费者组列表

# XDEL - 删除条目
redis-cli XDEL mystream 100-1
redis-cli XLEN mystream
# 预期: 2

# XTRIM - 裁剪流
redis-cli XADD trimstream 1-0 field1 value1
redis-cli XADD trimstream 2-0 field2 value2
redis-cli XADD trimstream 3-0 field3 value3
redis-cli XTRIM trimstream MAXLEN 1
redis-cli XLEN trimstream
# 预期: 1
```

### AOF 持久化测试

```bash
# 启用 AOF
redis-cli APPENDONLY yes
# 预期: OK

# 添加测试数据
redis-cli SET aof_key1 "value1"
redis-cli SET aof_key2 "value2"
redis-cli LPUSH aof_list item1 item2
redis-cli ZADD aof_zset 10 member1 20 member2
redis-cli HSET aof_hash field1 value1

# 查看 AOF 文件
cat appendonly.aof
# 预期: RESP 格式的命令日志
# *2$10APPENDONLY$3yes
# *3$3SET$8aof_key1$6value1...

# AOF 重写
redis-cli BGREWRITEAOF
# 预期: Background append only file rewriting started

# 重启服务器验证 AOF 加载
pkill -f "bin/godis"
./bin/godis
redis-cli DBSIZE
# 预期: 5

# 验证数据
redis-cli GET aof_key1
# 预期: value1
redis-cli LRANGE aof_list 0 -1
# 预期: item2, item1
redis-cli ZRANGE aof_zset 0 -1 WITHSCORES
# 预期: member1, 10, member2, 20
redis-cli HGET aof_hash field1
# 预期: value1

# 禁用 AOF
redis-cli APPENDONLY no
# 预期: OK
```

### Bitmap 数据结构测试

```bash
# SETBIT - 设置位值
redis-cli SETBIT mybits 0 1
redis-cli SETBIT mybits 10 1
redis-cli SETBIT mybits 20 1

# GETBIT - 获取位值
redis-cli GETBIT mybits 0     # 预期: 1
redis-cli GETBIT mybits 1     # 预期: 0
redis-cli GETBIT mybits 10    # 预期: 1

# BITCOUNT - 统计设置为 1 的位数
redis-cli BITCOUNT mybits     # 预期: 3

# BITPOS - 查找指定位的位置
redis-cli BITPOS mybits 1     # 预期: 0 (第一个值为 1 的位)
redis-cli BITPOS mybits 0     # 预期: 1 (第一个值为 0 的位)

# BITOP - 位操作
redis-cli SET bits1 "\xAA"
redis-cli SET bits2 "\x55"
redis-cli BITOP AND dest bits1 bits2
redis-cli BITOP OR dest bits1 bits2
redis-cli BITOP XOR dest bits1 bits2
redis-cli BITOP NOT dest bits1

# BITFIELD - 位字段操作
redis-cli BITFIELD mykey SET u8 0 100 GET u8 0
redis-cli BITFIELD mykey INCRBY u8 0 1
```

### HyperLogLog 数据结构测试

```bash
# PFADD - 添加元素
redis-cli PFADD hll a b c d e f
# 预期: 1 (首次添加)

redis-cli PFADD hll e f g h i j
# 预期: 0 或 1 (取决于是否有新元素)

# PFCOUNT - 估算基数
redis-cli PFCOUNT hll
# 预期: 估算的不重复元素数量

# PFMERGE - 合并多个 HyperLogLog
redis-cli PFADD hll2 x y z
redis-cli PFMERGE hll3 hll hll2
# 预期: OK

redis-cli PFCOUNT hll3
# 预期: 合并后的估算基数

# 多键 PFCOUNT
redis-cli PFCOUNT hll hll2
# 预期: 合并临时计算的估算基数

# 大数据集测试
redis-cli DEL bigset
for i in {1..1000}; do
    redis-cli PFADD bigset "element$i"
done
redis-cli PFCOUNT bigset
# 预期: 接近 1000 的估算值
```

### Geo 地理位置测试

```bash
# GEOADD - 添加地理位置
redis-cli GEOADD Sicily 13.361389 38.115556 "Palermo" \
                         15.087269 37.502669 "Catania"
# 预期: (integer) 2

# GEOPOS - 获取位置坐标
redis-cli GEOPOS Sicily Palermo
# 预期: 1) "13.36138665676117"
# 预期: 2) "38.11555512813572"

# GEODIST - 计算两点距离
redis-cli GEODIST Sicily Palermo Catania
# 预期: "166274.1539" (米)

redis-cli GEODIST Sicily Palermo Catania km
# 预期: "166.2742" (千米)

# GEOHASH - 获取 geohash 字符串
redis-cli GEOHASH Sicily Palermo
# 预期: "sqc8b49rny"

# GEORADIUS - 查找附近位置
redis-cli GEORADIUS Sicily 15 37 200 km WITHDIST
# 预期: 返回 200km 内的位置及其距离

redis-cli GEORADIUS Sicily 15 37 200 km WITHCOORD WITHDIST WITHHASH
# 预期: 返回位置、坐标、距离和 geohash

# GEORADIUSBYMEMBER - 查找指定成员附近的位置
redis-cli GEORADIUSBYMEMBER Sicily Palermo 100 km WITHDIST
# 预期: 返回 Palermo 100km 内的位置

# GEOADD CH 选项 - 更新位置
redis-cli GEOADD Sicily CH 13.36 38.12 "Palermo"
# 预期: 返回更新数量
```

## 技术栈

- **语言**: Go 1.24+
- **协议**: RESP2 (REdis Serialization Protocol)
- **网络**: TCP with keepalive
- **并发**: sync.RWMutex + per-connection goroutine
- **数据结构**: Dict with incremental rehash
- **过期**: 时间轮 + 懒惰删除 + 主动扫描
- **淘汰**: 近似LRU/LFU + EvictionPool (256 buckets)
- **事务**: MULTI/EXEC + WATCH 乐观锁 + dirty key 追踪
- **持久化**: RDB 快照 + AOF 日志 (Redis 格式兼容, CRC64 校验)
- **Bitmap**: String 类型扩展，位操作 (AND/OR/XOR/NOT)
- **HyperLogLog**: 基数估算算法 (10-bit precision, 1024 registers)
- **Geo**: 地理位置，Geohash 编码，Haversine 距离计算

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
4. **ZSet** - 排行榜、范围查询 ✅ (含 ZADD bug 修复)
5. **过期机制** - 核心功能，时间轮优化 ✅
6. **淘汰策略** - LRU/LFU 内存管理 ✅
7. **发布订阅** - PUBLISH, SUBSCRIBE, PSUBSCRIBE ✅
8. **事务支持** - MULTI, EXEC, DISCARD, WATCH ✅
9. **RDB 持久化** - 快照保存，RDB 格式编码/解码 ✅
10. **AOF 持久化** - 追加日志，AOF 重写 ✅
11. **Bitmap** - 位操作，SETBIT/GETBIT/BITOP/BITFIELD ✅
12. **HyperLogLog** - 基数估算，PFADD/PFCOUNT/PFMERGE ✅
13. **Geo 地理位置** - GEOADD/GEODIST/GEOHASH/GEOPOS/GEORADIUS/GEORADIUSBYMEMBER ✅

## 已修复 Bug

| Bug | 描述 | 修复时间 |
|-----|------|----------|
| ZADD 超时 | 参数解析循环中 `break` 只跳出 `switch` 而非 `for` 循环 | 2026-02-27 |

## 新增功能记录

| 功能 | 描述 | 完成时间 |
|------|------|----------|
| Bitmap 模块 | SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD/BITFIELD_RO | 2026-02-28 |
| HyperLogLog 模块 | PFADD/PFCOUNT/PFMERGE 基数估算 (10-bit precision) | 2026-02-28 |
| Geo 地理位置模块 | GEOADD/GEODIST/GEOHASH/GEOPOS/GEORADIUS/GEORADIUSBYMEMBER | 2026-02-28 |
