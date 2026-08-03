# feedsystem-zero

> 从零重建的**短视频信息流后端**，参考抖音/B 站的读写分离架构。用 Go 单仓多服务的方式，把"账号 / 视频 / 互动 / 社交 / 通知 / Feed"六个业务域拆成独立 RPC，配套 7 个 Job Worker 处理派生数据的最终一致性。

<p align="left">
  <img alt="Go" src="https://img.shields.io/badge/Go-1.25-00ADD8?logo=go">
  <img alt="go-zero" src="https://img.shields.io/badge/go--zero-1.10.2-3178c6">
  <img alt="MySQL" src="https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white">
  <img alt="Redis" src="https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white">
  <img alt="Kafka" src="https://img.shields.io/badge/Kafka-topics%3A6-231F20?logo=apachekafka">
  <img alt="Frontend" src="https://img.shields.io/badge/Web-React%2018%20%2B%20Vite%20%2B%20TS-61DAFB?logo=react">
</p>

---

## 一、项目亮点

- 🧱 **单仓多服务**：`apps/{gateway, account, video, interaction, social, notification, feed}` + `apps/job/*`，同一个 Go module 内共享 `common/*` 与 `model/*`，避免多仓依赖同步的痛苦。
- 🔗 **强一致写路径**：业务写入与派生事件在同一 MySQL 事务中落 `outbox_events`，由 outbox job 通过 `SKIP LOCKED` 抢占后发布到 Kafka，**禁止在业务代码里直接生产消息**。
- 🌊 **最终一致派生数据**：Timeline / 未读数 / 视频计数 / 热榜等全部走 Kafka 事件驱动，消费者用 `processed_events` 唯一键保证幂等，失败进 `dead_letter_events` 旁路，不阻塞 partition。
- ⚙️ **互动事件批量同步**：`interaction_sync` 按 topic+partition 组内保序、组间并发，每 500 条一个幂等事务；事件按视频聚合后升序更新，配合 Redis pending/acked/pending_count 保证最终收敛。
- 🚀 **Feed 推拉分离**：小 V 走 fanout 写扩散到粉丝 inbox，大 V（`is_big_v` 只升不降）只写自己的 outbox，读侧懒加载合并 inbox 与关注的大 V outbox。
- 🧠 **可控降级**：Redis 只作为加速层，Profile / Timeline / 未读数 / 评论首页均采用"版本号 + 惰性重算"，任何时刻 Redis 挂掉都能降级到 MySQL 直查。
- 📦 **文件秒传 + 延迟清理**：分片上传 + 全局 file_hash 秒传，`file_assets.ref_count` 归零后由 asset_cleanup job 延迟物理删除，同时兜底"引用复活"场景。
- 📊 **可复现压测**：内置造数、HTTP 压测和 E2E 工具，已完成 10000 用户、5000 视频规模验证，并对 Outbox、Kafka lag、Redis 增量和 MySQL 聚合做最终一致性验收。
- 🎨 **配套前端**：`web/` 目录内是 Vite + React 18 + TS + Tailwind + TanStack Query 实现的完整前端，覆盖登录、上传、播放、点赞、评论、关注、通知全流程。

---

## 二、总体架构

```mermaid
flowchart LR
    Client["Web / App"] -->|HTTPS + JWT| Gateway["Gateway (go-zero api)"]

    Gateway -->|gRPC| RPCs["Account · Video · Interaction<br/>Social · Notification · Feed"]

    RPCs --> MySQL[("MySQL<br/>业务表 + outbox_events<br/>+ processed_events + dead_letter")]
    RPCs --> Redis[("Redis<br/>版本号缓存 / Timeline ZSet<br/>热榜 / 未读数 / 秒传全局哈希")]

    MySQL -.outbox.-> OutboxJob["outbox job"]
    OutboxJob -->|publish| Kafka[("Kafka · 6 topics")]

    Kafka --> Jobs["interaction_sync · social_sync<br/>feed_timeline · hotrank · notification"]
    MySQL -.轮询.-> AssetCleanup["asset_cleanup job"]

    Jobs --> Redis
    Jobs --> MySQL
```

> 完整版架构图、ER 图、每个模块的时序图（共 17 张 Mermaid）见 [`docs/PROJECT_OVERVIEW.md`](./docs/PROJECT_OVERVIEW.md)。

---

## 三、模块一览

### 3.1 RPC 服务

| 模块 | 主要能力 |
|---|---|
| **account** | 注册 / 登录 / 登出 / 刷新 token / GetProfile / BatchGetProfiles / UpdateProfile |
| **video** | 分片上传 / PublishVideo / GetVideo / BatchGetVideos / ListUserVideos / DeleteVideo / 文件秒传去重 |
| **interaction** | LikeVideo / UnlikeVideo / PublishComment / DeleteComment / ListComments / BatchGetVideoStats |
| **social** | Follow / Unfollow / IsFollowing / BatchIsFollowing / ListFollowers / ListFollowings |
| **notification** | ListNotifications / GetUnreadCount / MarkNotificationRead / MarkAllNotificationsRead |
| **feed** | GetFollowingFeed / GetHotFeed / GetRecommendFeed |

### 3.2 Job 后台

| Job | 消费 topic | 主要动作 |
|---|---|---|
| **outbox** | — | 扫描 `outbox_events`，SKIP LOCKED 抢占后投递到 Kafka |
| **interaction_sync** | `interaction.like.events` / `interaction.comment.events` | topic+partition 并发；500 条批量幂等落库 → Redis eventID ack |
| **social_sync** | `social.follow.events` | 关注状态缓存 & Profile 版本号 bump |
| **feed_timeline** | `feed.video.events` / `social.follow.events` | 推拉分离扇出，ready 丢失时主动 bootstrap |
| **hotrank** | `interaction.like.events` / `interaction.comment.events` | 独立维护 UTC 分钟窗口，Feed 按需构建衰减快照 |
| **notification** | `notification.events` | 通知落库、未读数 version bump、死信旁路 |
| **asset_cleanup** | 无（轮询扫库） | 延迟物理清理 file_assets，抢占超时兜底 + 引用复活 |

---

## 四、快速开始

> 建议环境：Windows / macOS / Linux + Go 1.25 + Docker Desktop + Node 18+（跑前端时）。

### 4.1 起中间件

```bash
cd deploy
docker-compose up -d
# MySQL localhost:3308 (root/123456, db=feedsystem_zero)
# Redis localhost:6380 (password=123456)
# etcd  localhost:23790
# Kafka localhost:9094
```

建表 SQL 通过 `docker-entrypoint-initdb.d` 首次启动自动执行 `deploy/sql/001_*.sql ~ 016_*.sql`。

### 4.2 建 Kafka Topic

```bash
bash deploy/kafka/create_topics.sh
```

### 4.3 起后端

RPC 与 Job 都是独立的 `main.go`，各起一个终端即可：

```bash
# RPC
go run apps/account/account.go             -f apps/account/etc/account.yaml
go run apps/video/video.go                 -f apps/video/etc/video.yaml
go run apps/interaction/interaction.go     -f apps/interaction/etc/interaction.yaml
go run apps/social/social.go               -f apps/social/etc/social.yaml
go run apps/notification/notification.go   -f apps/notification/etc/notification.yaml
go run apps/feed/feed.go                   -f apps/feed/etc/feed.yaml

# Gateway
go run apps/gateway/gateway.go             -f apps/gateway/etc/gateway.yaml

# Jobs
go run apps/job/outbox/outbox.go                     -f apps/job/outbox/etc/outbox.yaml
go run apps/job/interaction_sync/interaction_sync.go -f apps/job/interaction_sync/etc/interaction_sync.yaml
go run apps/job/social_sync/social_sync.go           -f apps/job/social_sync/etc/social_sync.yaml
go run apps/job/feed_timeline/feed_timeline.go       -f apps/job/feed_timeline/etc/feed_timeline.yaml
go run apps/job/hotrank/hotrank.go                   -f apps/job/hotrank/etc/hotrank.yaml
go run apps/job/notification/notification.go        -f apps/job/notification/etc/notification.yaml
go run apps/job/asset_cleanup/asset_cleanup.go       -f apps/job/asset_cleanup/etc/asset_cleanup.yaml
```

### 4.4 起前端

```bash
cd web
pnpm install       # 或 npm install / yarn
pnpm dev           # http://127.0.0.1:5173
```

Vite dev server 会把 `/account`、`/video`、`/interaction`、`/social`、`/feed`、`/notification`、`/uploads` 反向代理到 gateway `127.0.0.1:8888`，因此**前端跑之前 gateway 必须先起**。前端详情见 [`web/README.md`](./web/README.md)。

### 4.5 配置说明

- 所有 `apps/**/etc/*.yaml` 内置的 MySQL / Redis 密码统一为 `123456`，与 `deploy/docker-compose.yml` 匹配，**仅用于本地开发**。部署到线上前请务必修改为强密码并从环境变量或密钥管理系统读取。
- `apps/account/etc/account.yaml` 里的邮件配置是**注册验证码专用**，仓库中已改为占位符：

  ```yaml
  Email:
    Host: smtp.163.com
    Port: 465
    Username: your-email@163.com
    Password: "YOUR_SMTP_AUTH_CODE"    # 邮箱服务商生成的 SMTP 授权码
    From: your-email@163.com
    FromName: FeedSystem 验证码
  ```

  想启用注册邮件验证码时，请填入你自己的邮箱 + 授权码；不填也不影响其他功能。
- JWT `AccessSecret` 默认是 `feedsystem-zero-dev-secret`，**上线前必须替换**。


---

## 五、目录结构

```
feedsystem-zero/
├── apps/
│   ├── gateway/            # HTTP 网关（go-zero api）
│   ├── account/            # 账号 RPC
│   ├── video/              # 视频 RPC
│   ├── interaction/        # 互动 RPC
│   ├── social/             # 社交 RPC
│   ├── notification/       # 通知 RPC
│   ├── feed/               # Feed RPC
│   └── job/
│       ├── outbox/                 # outbox → Kafka 投递
│       ├── interaction_sync/       # 点赞 / 评论落库消费者
│       ├── social_sync/            # 关注缓存 & 版本号维护
│       ├── feed_timeline/          # 推拉分离 Timeline 扇出
│       ├── hotrank/                # 热榜聚合
│       ├── notification/           # 通知落库 & 未读数 bump
│       └── asset_cleanup/          # 文件资产延迟物理清理
├── common/
│   ├── eventx/             # Kafka topic、envelope、payload schema
│   ├── feedx/              # Timeline member 编码 & 大 V 判定
│   ├── gormx/              # GORM 初始化
│   ├── jwtx/               # JWT 签发 / 解析
│   ├── kafkax/             # Kafka producer / consumer 封装
│   ├── rediskey/           # 按业务拆分的 Redis key 常量
│   ├── notificationcache/  # 未读数版本号缓存
│   └── emailx/             # 邮件（注册验证码）
├── deploy/
│   ├── docker-compose.yml  # MySQL / Redis / etcd / Kafka 一键起
│   ├── sql/                # 001 ~ 016 建表、索引与迁移脚本
│   └── kafka/              # topic 创建脚本
├── model/                  # 事件与 GORM 表共享模型
├── docs/                   # 项目文档
│   └── PROJECT_OVERVIEW.md # ⭐ 完整设计说明（架构 / ER / 流程图 / API 汇总）
├── tests/                  # 造数、HTTP 压测、E2E 冒烟与并发测试
├── web/                    # React 前端
└── README.md               # 就是本文件
```

---

## 六、测试与验证

仓库内置 `seed` 和 `loadtest`，可直接通过 Gateway 压测完整后端链路。本轮使用 **10000 用户 + 5000 视频**，所有依赖与服务均运行在同一台本地开发机。

```bash
# 重置压测数据并生成正式规模数据
go run ./tests/cmd/seed -reset -reset-redis -users 10000 -videos 5000

# 点赞/取消点赞正式压测
go run ./tests/cmd/loadtest \
  -scenario like -c 50 -d 60s -warmup 5s \
  -login-pool 500 -target-pool 2000 -v
```

| 场景 | 参数 | 成功率 | QPS | P99 |
|---|---|---:|---:|---:|
| 发布视频 | 5 并发 / 10s | 100% | 318.7 | 24ms |
| 关注 | 10 并发 / 10s | 100% | 354.7 | 54ms |
| 关注流 | 20 并发 / 30s | 100% | 1076.8 | 30ms |
| 热榜缓存命中 | 50 并发 / 30s | 100% | 7503.4 | 16ms |
| 热榜冷快照构建 | 50 并发 / 30s | 100% | 1428.1 | 54ms |
| 点赞正式规模 | 50 并发 / 60s | 100% | 260.4 次循环/s | 374ms |

> 点赞场景一次循环包含 Like + Unlike 两个写请求，`260.4 次循环/s` 约等于 `520.8 HTTP 写请求/s`。优化后的 Kafka 消息在压测结束约 7 秒后排空；最终未投递 Outbox、互动死信、Kafka lag、Redis delta/pending 和三类 MySQL 对账差异均为 0。

关键包已通过 `go test -race`，`go vet` 无输出。完整命令、优化前后对照和一致性 SQL 见 [完整测试记录](./docs/PROJECT_OVERVIEW.md#146-测试压测与一致性验收2026-08-03)。E2E 冒烟目前唯一的环境限制是 `@loadtest.local` 虚构邮箱无法通过真实 163 SMTP 接收验证码。

---

## 七、几个必须知道的约定

1. **身份识别**：所有需要 `user_id` 的 RPC 入参**必须**由 Gateway 从 JWT 中解析后填入，**不接收前端传值**。
2. **幂等键**：视频发布 `(author_id, request_id)`；评论 `(user_id, request_id)`；事件处理 `(event_id, consumer_name)`；通知去重 `business_key`。
3. **软删除**：`videos` / `likes` / `comments` / `follows` / `notifications` 全部软删，配合 `status` + `deleted_at` 字段做状态机。
4. **游标分页**：列表接口一律用 "排序字段 + 主键" 双游标（例如 `(occurred_at, id)`），**禁止 offset 分页**。
5. **批量接口**：Gateway 聚合层禁止 N+1，统一走 `BatchGetProfiles` / `BatchGetVideos` / `BatchGetVideoStats` / `BatchIsFollowing`。
6. **Redis Key**：一律通过 `common/rediskey/*.go` 中的函数生成，禁止业务代码里手写字符串拼接。
7. **写路径闭环**：业务写入 + `outbox_events` 必须同事务，**禁止直接调用 Kafka 生产者**。

---

## 八、深入阅读

- 📘 **一次读懂整个系统** → [`docs/PROJECT_OVERVIEW.md`](./docs/PROJECT_OVERVIEW.md)
  - 总体架构 / ER 图 / 事件契约 / Outbox 模式 / 各模块时序图（共 17 张 Mermaid）
  - Redis Key 命名空间 / 一致性 · 并发 · 幂等设计原则
  - Gateway HTTP API 汇总 / 常见问题排查
- 🎨 **前端指南** → [`web/README.md`](./web/README.md)
- 🗄️ **数据库迁移** → [`deploy/sql/`](./deploy/sql/)
- ✉️ **事件契约** → [`common/eventx/`](./common/eventx/) 与 [`model/event.go`](./model/event.go)

---

## 九、常用命令

```bash
go build ./...       # 全量编译
go vet ./...         # 静态检查
go test ./...        # 单元测试

# 改 proto 后重新生成 RPC
cd apps/account
goctl rpc protoc account.proto --go_out=. --go-grpc_out=. --zrpc_out=. --style=goZero

# 改 gateway.api 后重新生成 API
cd apps/gateway
goctl api go -api gateway.api -dir . --style=goZero
```

---

## License

本项目仅用于个人学习目的，暂未指定开源协议。
