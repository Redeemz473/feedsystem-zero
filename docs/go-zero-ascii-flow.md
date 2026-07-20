# feedsystem-zero ASCII 架构与核心链路图

这份图是为了帮你理解：原来的 Gin 单体项目，改成 go-zero 后到底怎么拆成 `gateway + rpc + job + Kafka + Redis/MySQL`。

核心原则：

```text
gateway 负责对外 HTTP
rpc 负责同步业务能力
job/consumer 负责异步事件处理
MySQL 保存核心状态
Redis 保存缓存、timeline、热榜、token
Kafka 保存领域事件流
```

## 1. go-zero 微服务总架构

```text
┌────────────────┐      HTTP/JSON       ┌──────────────────────────────────────────────────────────────┐
│     Client     │ ───────────────────▶ │                 Gateway (go-zero api)                         │
│ 浏览器 / APP   │                       │                                                              │
└────────────────┘                       │  ┌──────────┐  ┌──────────────┐  ┌──────────────────────┐   │
                                         │  │ Handler  │─▶│ Logic        │─▶│ zrpc Clients          │   │
                                         │  └────┬─────┘  └──────┬───────┘  │ account/video/feed... │   │
                                         │       │               │          └──────────┬───────────┘   │
                                         │       ▼               ▼                     │               │
                                         │  ┌──────────────┐ ┌──────────────┐          │               │
                                         │  │ JWT 中间件    │ │ 限流/参数校验 │          │               │
                                         │  └──────────────┘ └──────────────┘          │               │
                                         └─────────────────────────────────────────────┼───────────────┘
                                                                                       │ zrpc
        ┌──────────────────────────────────────────────────────────────────────────────┼────────────────────────────────┐
        │                                                                              ▼                                │
        │                           RPC 业务服务层 (go-zero zrpc)                                                        │
        │                                                                                                               │
        │  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  ┌────────────────────┐                         │
        │  │ account-rpc    │  │ video-rpc      │  │ feed-rpc       │  │ interaction-rpc    │                         │
        │  │ 注册/登录/资料 │  │ 发布/详情/标签 │  │ 最新流/热榜    │  │ 点赞/评论          │                         │
        │  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘  └─────────┬──────────┘                         │
        │          │                   │                   │                     │                                    │
        │  ┌───────▼────────┐  ┌───────▼────────┐  ┌───────▼────────┐  ┌─────────▼──────────┐                         │
        │  │ Logic          │  │ Logic          │  │ Logic          │  │ Logic              │                         │
        │  │ Repository     │  │ Repository     │  │ Repository     │  │ Repository         │                         │
        │  │ GORM           │  │ GORM           │  │ GORM/Redis     │  │ GORM               │                         │
        │  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘  └─────────┬──────────┘                         │
        │          │                   │                   │                     │                                    │
        │  ┌───────▼────────┐  ┌───────▼────────┐                                                           ┌──────────▼────────┐
        │  │ social-rpc     │  │ notification-rpc│                                                          │ etcd             │
        │  │ 关注/取关      │  │ 通知列表/已读   │                                                          │ 服务注册发现     │
        │  └───────┬────────┘  └───────┬────────┘                                                          └───────────────────┘
        └──────────┼───────────────────┼───────────────────────────────────────────────────────────────────────────────────────┘
                   │                   │
                   │ 同步读写核心状态   │
                   ▼                   ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             存储层                                                                           │
│                                                                                                                              │
│   ┌─────────────────────────┐     ┌──────────────────────────────┐      ┌──────────────────────────────┐                    │
│   │ MySQL 8.0 + GORM         │     │ Redis                         │      │ 文件存储                      │                    │
│   │ accounts/videos/likes    │     │ token/feed timeline/hot rank  │      │ 本地 uploads，后续可换 MinIO  │                    │
│   │ comments/socials         │     │ video cache/chunk session     │      │ 视频/封面                     │                    │
│   │ outbox/processed_events  │     └──────────────────────────────┘      └──────────────────────────────┘                    │
│   └────────────┬────────────┘                                                                                               │
└────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                 │
                 │ 业务事务内写 outbox_events
                 ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                          异步事件层                                                                          │
│                                                                                                                              │
│   ┌──────────────────┐     扫 pending      ┌──────────────────┐      publish       ┌──────────────────────────────┐        │
│   │ outbox_events    │ ───────────────────▶│ job/outbox       │ ──────────────────▶│ Kafka Topics                 │        │
│   │ pending/sent     │                     │ 可靠投递事件     │                    │ video-events                 │        │
│   └──────────────────┘                     └──────────────────┘                    │ interaction-events           │        │
│                                                                                     │ social-events                │        │
│                                                                                     └───────┬─────────┬────────────┘        │
│                                                                                             │         │                     │
│                                                         ┌───────────────────────────┘         │                     │
│                                                         ▼                                     ▼                     ▼
│                                            ┌──────────────────┐                  ┌──────────────────┐   ┌──────────────────┐
│                                            │ job/timeline     │                  │ job/hotrank      │   │ job/notification │
│                                            │ 写 Redis 最新流  │                  │ 写 Redis 热榜    │   │ 写通知表         │
│                                            └──────────────────┘                  └──────────────────┘   └──────────────────┘
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## 2. 原 Gin 单体模块如何映射到 go-zero

```text
原项目 feedsystem_video_go                         新项目 feedsystem-zero
──────────────────────────                         ───────────────────────────────

internal/http/router.go                   ───────▶  apps/gateway
Gin Handler                               ───────▶  gateway/internal/handler
Gin Middleware                            ───────▶  gateway/internal/middleware

internal/account                          ───────▶  apps/account       account-rpc
internal/video                            ───────▶  apps/video         video-rpc
internal/feed                             ───────▶  apps/feed          feed-rpc
internal/video/like + comment             ───────▶  apps/interaction   interaction-rpc
internal/social                           ───────▶  apps/social        social-rpc
internal/message                          ───────▶  可后续拆 message-rpc
internal/worker/notification              ───────▶  apps/notification + apps/job/notification

internal/worker/outboxworker.go           ───────▶  apps/job/outbox
internal/worker/popularityworker.go       ───────▶  apps/job/hotrank
internal/worker/outbox timeline consumer  ───────▶  apps/job/timeline

RabbitMQ                                  ───────▶  Kafka
GORM                                      ───────▶  继续使用 GORM
Redis                                     ───────▶  继续使用 Redis
```

## 3. 发布视频主链路

```text
┌─────────────┐     HTTP/JSON      ┌────────────────────────────────────────────┐
│   Client    │ ─────────────────▶ │ Gateway: POST /video/publish              │
└─────────────┘                    │                                            │
                                   │  ┌──────────┐  ┌──────────────┐           │
                                   │  │ Handler  │─▶│ JWT / 参数校验│           │
                                   │  └────┬─────┘  └──────┬───────┘           │
                                   │       │               │                   │
                                   │       ▼               ▼                   │
                                   │  ┌──────────────────────────────┐         │
                                   │  │ 调用 video-rpc.PublishVideo  │         │
                                   │  └──────────────┬───────────────┘         │
                                   └─────────────────┼─────────────────────────┘
                                                     │ zrpc
                                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                            video-rpc                                         │
│                                                                              │
│  ┌──────────┐   ┌──────────────────┐   ┌──────────────────────────────────┐ │
│  │ Logic    │──▶│ Repository(GORM) │──▶│ MySQL Transaction                │ │
│  └──────────┘   └──────────────────┘   │  1. insert videos                │ │
│                                        │  2. insert tags/video_tags        │ │
│                                        │  3. insert outbox_events          │ │
│                                        │     event_type=video.published    │ │
│                                        └──────────────────┬───────────────┘ │
└───────────────────────────────────────────────────────────┼──────────────────┘
                                                            │ commit 后异步投递
                                                            ▼
┌──────────────────┐     scan pending     ┌──────────────────┐     publish     ┌─────────────────┐
│ outbox_events    │ ───────────────────▶ │ job/outbox       │ ──────────────▶ │ Kafka           │
│ pending          │                      │ 标记 sent/failed │                 │ video-events    │
└──────────────────┘                      └──────────────────┘                 └────────┬────────┘
                                                                                          │ consume
                                                                                          ▼
                                                                             ┌─────────────────────┐
                                                                             │ job/timeline        │
                                                                             │ ZADD Redis timeline │
                                                                             └──────────┬──────────┘
                                                                                        │
                                                                                        ▼
                                                                             ┌─────────────────────┐
                                                                             │ Redis               │
                                                                             │ feed:global_timeline│
                                                                             └─────────────────────┘
```

理解重点：

```text
video-rpc 不直接发 Kafka。
video-rpc 只在事务里写 videos + outbox_events。
job/outbox 负责可靠投递 Kafka。
job/timeline 消费 Kafka 后写 Redis timeline。
```

## 4. 点赞链路

```text
┌─────────────┐     HTTP/JSON      ┌────────────────────────────────────────────┐
│   Client    │ ─────────────────▶ │ Gateway: POST /interaction/like           │
└─────────────┘                    │                                            │
                                   │  Handler -> JWT -> 限流 -> 调用 RPC        │
                                   └─────────────────┬─────────────────────────┘
                                                     │ zrpc
                                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         interaction-rpc                                      │
│                                                                              │
│  ┌──────────┐   ┌──────────────────┐   ┌──────────────────────────────────┐ │
│  │ Logic    │──▶│ Repository(GORM) │──▶│ MySQL Transaction                │ │
│  └──────────┘   └──────────────────┘   │  1. check video exists           │ │
│                                        │  2. insert likes                 │ │
│                                        │  3. update videos.likes_count+1  │ │
│                                        │  4. update videos.popularity+1   │ │
│                                        │  5. insert outbox_events         │ │
│                                        │     event_type=like.created      │ │
│                                        └───────────────┬──────────────────┘ │
└────────────────────────────────────────────────────────┼─────────────────────┘
                                                         │
                                                         ▼
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────────────┐
│ outbox_events    │ ───▶ │ job/outbox       │ ───▶ │ Kafka interaction-events │
└──────────────────┘      └──────────────────┘      └────────────┬─────────────┘
                                                                  │
                         ┌────────────────────────────────────────┼────────────────────────────────────┐
                         ▼                                        ▼                                    ▼
              ┌──────────────────┐                    ┌──────────────────┐                ┌──────────────────┐
              │ job/hotrank      │                    │ job/notification │                │ 其他扩展 consumer│
              │ Redis ZINCRBY    │                    │ 写通知表         │                │ 推荐/统计/风控   │
              │ hot:video:1m     │                    │ processed_events │                │                  │
              └────────┬─────────┘                    └────────┬─────────┘                └──────────────────┘
                       │                                       │
                       ▼                                       ▼
              ┌──────────────────┐                    ┌──────────────────┐
              │ Redis 热榜       │                    │ MySQL 通知表     │
              └──────────────────┘                    └──────────────────┘
```

和原项目相比，你的改进点是：

```text
点赞核心状态同步事务写入:
  likes 表
  videos.likes_count

异步事件只处理派生能力:
  热榜
  通知
  统计
```

这样比“点赞完全丢给 MQ 写库”更稳。

## 5. Feed 查询链路

```text
┌─────────────┐      HTTP/JSON      ┌──────────────────────────────────────────┐
│   Client    │ ──────────────────▶ │ Gateway: POST /feed/latest              │
└─────────────┘                     │                                          │
                                    │  Handler -> SoftJWTAuth -> 调用 feed-rpc │
                                    └────────────────┬─────────────────────────┘
                                                     │ zrpc
                                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                                feed-rpc                                      │
│                                                                              │
│  ┌─────────────────────┐                                                     │
│  │ ListLatest Logic    │                                                     │
│  └──────────┬──────────┘                                                     │
│             │                                                                │
│             ▼                                                                │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ 1. Redis ZREVRANGE feed:global_timeline 获取 video_ids                 │  │
│  │ 2. 批量补齐视频实体                                                     │  │
│  │    L1 本地缓存，可选                                                     │  │
│  │    L2 Redis video:entity:{id}                                            │  │
│  │    L3 MySQL videos                                                       │  │
│  │ 3. BatchGetLiked 补 is_liked                                             │  │
│  │ 4. 返回 video_list / next_cursor / has_more                              │  │
│  └───────┬───────────────────────┬──────────────────────────┬──────────────┘  │
└──────────┼───────────────────────┼──────────────────────────┼─────────────────┘
           │                       │                          │
           ▼                       ▼                          ▼
┌──────────────────────┐ ┌────────────────────────┐ ┌──────────────────────────┐
│ Redis timeline       │ │ Redis video cache       │ │ MySQL videos/likes        │
│ feed:global_timeline │ │ video:entity:{id}       │ │ 冷数据/未命中回源         │
└──────────────────────┘ └────────────────────────┘ └──────────────────────────┘
```

理解重点：

```text
Feed 查询不是直接 SELECT * FROM videos。
它先从 Redis timeline 拿 video_id，再批量补齐视频实体。
这样以后可以把 timeline 换成关注流、推荐流、热榜流，但实体补齐逻辑复用。
```

## 6. go-zero 里每层代码大概写哪里

```text
apps/gateway
  gateway.api
  internal/handler       HTTP 参数绑定、从 header 取 token
  internal/logic         调用 account/video/feed 等 rpc client
  internal/svc           初始化 rpc client、Redis、配置

apps/account
  account.proto
  internal/logic         Register/Login 业务逻辑
  internal/svc           注入 AccountRepository、Redis、JWT 配置

apps/video
  video.proto
  internal/logic         PublishVideo/GetDetail
  internal/svc           注入 VideoRepository、OutboxRepository

apps/feed
  feed.proto
  internal/logic         ListLatest/ListHot/ListFollowing
  internal/svc           注入 Redis、FeedRepository、LikeRepository

apps/interaction
  interaction.proto
  internal/logic         Like/Unlike/Comment
  internal/svc           注入 InteractionRepository、OutboxRepository

apps/job/outbox
  main.go                扫 outbox_events，投递 Kafka

apps/job/timeline
  main.go                消费 video-events，写 Redis timeline

apps/job/hotrank
  main.go                消费 interaction-events，写 Redis 热榜

common/gormx
  db.go                  GORM 初始化

model
  *.go                   GORM model

repository
  *_repo.go              GORM 查询与事务封装
```

## 7. 你应该先写哪几个文件

第一轮只写这些：

```text
go.mod
deploy/docker-compose.yml
deploy/sql/001_schema.sql

common/gormx/db.go
common/jwtx/jwt.go
common/response/response.go
common/xerr/errors.go

model/account.go
repository/account_repo.go

apps/account/account.proto
apps/gateway/gateway.api
```

然后生成：

```bash
goctl rpc protoc apps/account/account.proto \
  --go_out=apps/account \
  --go-grpc_out=apps/account \
  --zrpc_out=apps/account

goctl api go \
  -api apps/gateway/gateway.api \
  -dir apps/gateway
```

第一阶段目标只有一个：

```text
注册、登录、token 鉴权跑通。
```

账号跑通后，再写：

```text
video-rpc
outbox job
timeline consumer
feed-rpc
```
