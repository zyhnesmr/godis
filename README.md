# Godis

[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![License](https://img.shields.io/badge/license-BSD-3-Clause-blue)](LICENSE)

Godis 是一个使用 Go 语言实现的 Redis 兼容内存数据库，支持完整的 Redis 通讯协议 (RESP) 和核心功能。

## 特性

- **完全兼容 RESP 协议** - 支持 Redis 通讯协议
- **核心数据结构** - String, Hash, List, Set, ZSet
- **多数据库支持** - 支持 SELECT 命令切换数据库
- **持久化** - 支持 RDB 快照和 AOF 日志
- **发布订阅** - 支持 Pub/Sub 功能
- **事务支持** - 支持 MULTI/EXEC/WATCH
- **高性能** - 基于 Go 的高效并发模型

## 快速开始

### 安装

从源码编译:

```bash
git clone https://github.com/zyhnesmr/godis.git
cd godis
make build
```

或使用 Docker:

```bash
docker build -t godis .
docker run -p 6379:6379 godis
```

### 运行

```bash
# 使用默认配置运行
./bin/godis

# 使用指定配置文件
./bin/godis -c /path/to/godis.conf

# 指定端口
./bin/godis -p 6380
```

### 使用 redis-cli 测试

```bash
$ redis-cli ping
PONG

$ redis-cli set mykey "Hello Godis"
OK

$ redis-cli get mykey
"Hello Godis"

$ redis-cli incr counter
(integer) 1

$ redis-cli incr counter
(integer) 2
```

## 项目结构

```
godis/
├── cmd/godis/          # 程序入口
├── internal/
│   ├── config/         # 配置管理
│   ├── net/            # TCP 服务器和连接管理
│   ├── protocol/       # RESP 协议实现
│   ├── command/        # 命令处理
│   ├── database/       # 数据库核心
│   ├── datastruct/     # 数据结构实现
│   ├── expire/        # 过期机制
│   ├── eviction/      # 淘汰策略
│   ├── persistence/   # 持久化
│   ├── pubsub/        # 发布订阅
│   └── transaction/   # 事务支持
├── pkg/               # 公共工具库
├── config/            # 配置文件
└── docs/             # 文档
```

## 命令支持

### 服务器命令
- PING, ECHO, QUIT
- SELECT, AUTH
- INFO, TIME
- DBSIZE

### 键管理命令
- SET, GET, MGET, MSET
- SETEX, PSETEX, SETNX
- INCR, DECR, INCRBY, DECRBY
- APPEND, STRLEN, GETRANGE, SETRANGE
- DEL, EXISTS, TYPE
- KEYS, RANDOMKEY
- RENAME, RENAMENX
- EXPIRE, EXPIREAT, TTL, PTTL, PERSIST
- FLUSHDB, FLUSHALL
- SCAN

### 配置

配置文件位于 `config/godis.conf`，主要配置项:

```conf
# 网络配置
bind 0.0.0.0
port 6379

# 通用配置
databases 16
loglevel notice

# 持久化配置
save 900 1
save 300 10
save 60 10000
appendonly yes
appendfsync everysec

# 内存配置
maxmemory 1gb
maxmemory-policy allkeys-lru
```

## 性能

Godis 针对 Go 语言进行了深度优化:

- **零拷贝** - 减少内存分配和数据拷贝
- **连接并发** - 每个连接独立处理，避免全局锁
- **高效编码** - 支持 int/embstr/raw 编码优化

## 开发

```bash
# 安装依赖
go mod download

# 运行测试
make test

# 运行 benchmark
make benchmark

# 代码检查
make fmt
make vet
make lint
```

## 架构文档

详细的架构设计请参考: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

BSD 3-Clause License
