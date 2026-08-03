
# tests — 压测 & 端到端冒烟

本目录提供两类**辅助工具**，与业务代码完全隔离：

| 工具 | 目的 | 入口 |
| --- | --- | --- |
| **seed** | 一次性造出大规模测试数据（用户 / 视频 / 部分互动） | `cmd/seed/main.go` |
| **loadtest** | 直接压 Gateway HTTP 层，采集 QPS / P50 / P95 / P99 / 错误率 | `cmd/loadtest/main.go` |
| **e2e** | 端到端冒烟测试，一条链路走完关键业务 | `go test ./tests/e2e -v` |

所有工具**只用 Go 标准库 + 项目已有依赖**，未引入第三方压测框架。

---

## 目录结构

```
tests/
├── cmd/
│   ├── seed/main.go              # 造数据入口
│   └── loadtest/main.go          # 压测入口（-scenario 选场景）
├── internal/
│   ├── testconfig/               # 命令行参数 + 默认值（10000 用户 / 5000 视频）
│   ├── httpclient/               # HTTP 客户端 + 强类型 API 封装
│   ├── seed/                     # 直连 MySQL 批量灌数据（不走上传接口）
│   ├── metrics/                  # 延迟直方图 / QPS / 分类错误率
│   ├── loadgen/                  # 并发 runner（并发 + 时长控制）
│   └── scenario/                 # 5 个压测场景（点赞/关注/关注流/热榜/发视频）
└── e2e/smoke_test.go             # 冒烟：注册→登录→发视频→点赞→评论→关注→拉Feed
```

---

## 前置：环境准备

1. `docker compose up -d` 起好 MySQL / Redis / Kafka / etcd（见 `deploy/docker-compose.yml`）。
2. 执行 `deploy/sql/*.sql` 建表。
3. 运行所有 gateway / rpc / job 服务（见 `PROJECT_OVERVIEW.md`）。

默认连接串：

| 组件 | 地址 |
| --- | --- |
| Gateway | `http://127.0.0.1:8888` |
| MySQL   | `127.0.0.1:3308` / `root:123456` / db `feedsystem_zero` |
| Redis   | `127.0.0.1:6380` / password `123456` / DB 0 |

---

## Step 1 造数据

```bash
# 默认：10000 用户 + 5000 视频（每个视频作者随机选，播放地址走 file_assets 复用）
go run ./tests/cmd/seed

# 自定义规模
go run ./tests/cmd/seed -users 20000 -videos 8000

# 清空历史 seed/loadtest 业务数据及 fsz:* 派生缓存后再造
# 真实账号、真实视频和真实上传文件不会被删除
go run ./tests/cmd/seed -reset -reset-redis
```

seed 做的事：

1. 直接 `INSERT accounts`，用户名统一 `seed_user_{i}`、邮箱 `seed_user_{i}@loadtest.local`、
   密码 **bcrypt(`LoadTest@123`)**（**所有 seed 用户共用同一份密码**，压测登录只需拿用户名）。
2. 直接 `INSERT file_assets` 造一份可复用的占位视频/封面 URL（走 `/uploads/seed/*.mp4`），
   `ref_count` 与实际引用视频数一致。
3. 直接 `INSERT videos`，`author_id` 在用户中均匀分布，`created_at` 分布在过去 30 天，
   `likes_count / comments_count` 初始为 0（真实互动由压测阶段产生）。

> seed 不向 Redis / Kafka 写入派生业务数据；`-reset-redis` 只在重置时删除 `fsz:*` Key。
> Kafka topic 和消费位点保持不变，新压测事件会从现有位点继续追加和消费。

---

## Step 2 压测

通用参数：

| flag | 默认 | 说明 |
| --- | --- | --- |
| `-scenario` | `like` | 见下表 |
| `-c` | `50` | 并发协程数 |
| `-d` | `30s` | 压测持续时间 |
| `-base` | `http://127.0.0.1:8888` | gateway 地址 |
| `-warmup` | `3s` | 预热时长（不计入统计） |

场景一览：

| scenario | 覆盖接口 | 目标 |
| --- | --- | --- |
| `like`            | `POST /interaction/video/:id/like` | 测点赞热点：Redis lua 分支 + 异步 Kafka |
| `follow`          | `POST /social/users/:id/follow`     | 测粉丝计数原子性 + fanout 触发 |
| `following_feed`  | `GET /feed/following`              | 测推拉分离读侧合并 |
| `hot_feed`        | `GET /feed/hot`                    | 测热榜 zset 快照命中率 |
| `publish_video`   | `POST /video/publish`              | 测视频写扩散 & outbox 落盘 |

**示例**：

```bash
# 点赞压测：100 并发，压 60 秒
go run ./tests/cmd/loadtest -scenario like -c 100 -d 60s

# 热榜压测：500 并发，压 30 秒（游客访问，不需要登录）
go run ./tests/cmd/loadtest -scenario hot_feed -c 500 -d 30s
```

**输出示例**：

```
=== Scenario: like =====================================
Duration       : 60.02s
Concurrency    : 100
Total          : 348,912 requests
Success        : 348,801 (99.97%)
QPS            : 5813.4
Latency (ms)   : P50=12  P95=28  P99=55  Max=203
Errors         :
  4xx-limit    : 111
========================================================
```

---

## Step 3 端到端冒烟

无需 seed，随机生成一个用户跑完整链路，5 秒内结束：

```bash
go test ./tests/e2e -v
```

用途：**新环境部署后一键验证**"注册→登录→发视频→点赞→评论→关注→看关注 Feed→看热榜"整条链路是否通。

---

## 常见问题

**Q. seed 完还没数据？**
A. seed 只写 MySQL，视频进入 Feed 需要等 `outbox` job 消费并写 `feed_timeline` / `feed_author_outbox`；
   默认 outbox job 循环 1 秒，等 3~5 秒即可。热榜由 `hotrank` job 每分钟刷新一次。

**Q. 压测把 Redis 打崩了怎么办？**
A. 项目在 Redis 故障时会**降级直查 MySQL**（见 `PROJECT_OVERVIEW.md` 的降级章节），但
   压测建议先在小并发下热身，不要一上来 1000 并发。

**Q. 想只测某个接口，不要 5 个场景？**
A. `scenario/` 目录下每个文件就是一个独立压测器，可以直接拷贝一个新的 `.go` 文件改。
