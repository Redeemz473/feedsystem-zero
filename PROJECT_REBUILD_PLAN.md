# feedsystem-zero 复现与升级路线

目标：用 `go-zero + GORM + Kafka + Redis + MySQL` 重新实现 `feedsystem_video_go`，并把原项目中异步一致性和消费幂等不足的地方补强。

这不是一次“换框架练习”，而是把项目升级为更适合面试的版本：

- Gin 单体升级为 go-zero API + RPC + Job/Consumer。
- RabbitMQ 任务模型升级为 Kafka 事件流模型。
- 保留 GORM，降低你重写数据访问层的学习成本。
- 引入 Outbox 保证“业务写库”和“事件投递”的一致性。
- 引入 processed_events 保证 Kafka 重复消费时不会重复执行副作用。
- Redis 继续承担验证码、缓存、feed timeline、热榜、分片上传会话，以及后续可能加入的 token 黑名单。

## 一、最终项目形态

建议最终目录：

```text
feedsystem-zero/
  README.md
  PROJECT_REBUILD_PLAN.md
  go.mod
  go.work                         # 可选，单仓多服务时方便管理

  apps/
    gateway/                      # go-zero api，对外 HTTP
      api/
        gateway.api
      internal/
        config/
        handler/
        logic/
        svc/
        types/
      gateway.go

    account/                      # go-zero rpc: account-rpc
      account.proto
      account.go
      internal/

    video/                        # go-zero rpc: video-rpc
      video.proto
      video.go
      internal/

    feed/                         # go-zero rpc: feed-rpc
      feed.proto
      feed.go
      internal/

    interaction/                  # go-zero rpc: like/comment-rpc
      interaction.proto
      interaction.go
      internal/

    social/                       # go-zero rpc: follow-rpc
      social.proto
      social.go
      internal/

    notification/                 # go-zero rpc: notification-rpc
      notification.proto
      notification.go
      internal/

    job/
      outbox/                     # 扫 outbox_events，投递 Kafka
      timeline/                   # 消费 video.published，写 Redis timeline
      hotrank/                    # 消费 like/comment，写 Redis 热榜
      notification/               # 消费 like/comment/follow，写通知表

  common/
    ctxdata/                      # 从 context 取 user id
    gormx/                        # GORM 初始化、事务封装
    jwtx/                         # JWT 生成解析
    kafkax/                       # Kafka producer/consumer 封装
    rediskey/                     # Redis key 统一管理
    response/                     # HTTP 统一响应
    xerr/                         # 业务错误码
    xtrace/                       # 日志/trace 预留

  model/
    account.go
    video.go
    like.go
    comment.go
    social.go
    notification.go
    outbox_event.go
    processed_event.go
    tag.go

  repository/
    account_repo.go
    video_repo.go
    interaction_repo.go
    feed_repo.go
    social_repo.go
    notification_repo.go
    outbox_repo.go
    processed_event_repo.go

  deploy/
    docker-compose.yml
    sql/
      001_schema.sql
      002_indexes.sql
    kafka/
      topics.sh

  docs/
    api-flow.md
    architecture.md
```

### 为什么保留 GORM

go-zero 官方常见搭配是 `sqlc` 或自带 model 生成，但你现在对 GORM 熟悉，保留 GORM完全可行。

推荐方式：

```text
go-zero 负责:
  API/RPC 框架、配置、服务注册、依赖注入、限流中间件

GORM 负责:
  MySQL 连接、事务、CRUD、复杂查询

repository 层负责:
  把 GORM 操作封装起来，不让 logic 里到处散落 db.Where(...)
```

不要在每个 logic 里直接写一大堆 GORM。比较好的结构是：

```text
logic -> service/repository -> GORM -> MySQL
```

## 二、技术选型

### 后端框架

- `go-zero api`：对外 HTTP 网关。
- `go-zero zrpc`：内部 RPC 服务。
- `goctl`：生成 api/rpc 代码。

### 数据与中间件

- MySQL 8.0：核心业务数据。
- Redis 7：验证码、缓存、ZSet feed、热榜、分布式锁、分片上传状态、token 黑名单预留。
- Kafka：领域事件流。
- etcd：go-zero RPC 服务注册发现。
- GORM：ORM 和事务。

### Go 工具链

当前本机工具版本可以用：

```text
go                 1.25.0
goctl              1.10.1
protoc             35.1
protoc-gen-go      1.36.11
protoc-gen-go-grpc 1.6.2
```

## 三、核心设计原则

### 原则 1：核心状态同步写，派生状态异步写

核心状态：

```text
accounts
videos
likes
comments
socials
```

派生状态：

```text
Redis feed timeline
Redis hot rank
notifications
cache invalidation
```

点赞时建议这样做：

```text
同步事务:
  insert likes
  update videos.likes_count
  insert outbox_events(like.created)

异步 consumer:
  更新 Redis 热榜
  写通知
```

不要让点赞表本身完全依赖 Kafka consumer 去写。这样能避免“接口返回成功但核心状态还没落库”的体验问题。

### 原则 2：业务服务不直接发 Kafka

业务服务只写 `outbox_events`。

```text
业务事务提交成功
    -> outbox_events 有一条 pending
outbox job 扫描 pending
    -> 投递 Kafka
    -> 成功后标记 sent
```

这样可以保证：

```text
业务数据写库成功，就一定有事件可以最终投递。
```

### 原则 3：所有重要 consumer 都要幂等

consumer 处理消息时：

```text
开启事务
  insert processed_events(event_id, consumer_name)
  如果唯一键冲突，说明处理过，直接跳过
  执行业务副作用
提交事务
提交 Kafka offset
```

这样可以应对：

- Kafka 重平衡导致重复投递。
- consumer 处理成功但提交 offset 前宕机。
- outbox job 重复发送同一个事件。

## 四、第一版服务拆分

不要一开始拆太细。第一版建议先做：

```text
gateway
account-rpc
video-rpc
feed-rpc
interaction-rpc
job/outbox
job/timeline
job/hotrank
```

后续再补：

```text
social-rpc
notification-rpc
job/notification
chunk upload
```

### go-zero 拆分理解图

先看这张图，它展示了用了 go-zero 之后，原来 Gin 单体项目里的 `internal/account`、`internal/video`、`internal/feed`、`internal/social`、`internal/worker` 会怎么拆成 API、RPC 和 Job：

![go-zero 微服务拆分图](docs/go-zero-service-split.svg)

如果你更习惯看手绘/框图风格的文字图，看这份：

[go-zero ASCII 架构与核心链路图](docs/go-zero-ascii-flow.md)

理解这张图时抓住三条线：

```text
同步请求线:
  Client -> gateway(go-zero api) -> account/video/feed/interaction/social RPC -> MySQL/Redis

异步事件线:
  RPC 业务事务 -> outbox_events -> job/outbox -> Kafka -> timeline/hotrank/notification jobs

服务治理线:
  RPC 服务注册到 etcd，gateway 通过 zrpc client 调用 RPC
```

也就是说，go-zero 并不是让你把所有模块都变成独立 HTTP 服务。更合理的拆法是：

```text
对外只暴露 gateway 一个 HTTP 入口；
内部业务能力拆成多个 zrpc 服务；
异步派生能力放到 job/consumer。
```

## 五、数据库表设计

第一版先用 SQL 初始化，不建议 AutoMigrate 作为主方案。你可以在开发时用 GORM model 对齐表结构，但表结构最好写到 `deploy/sql/001_schema.sql`。

### accounts

```sql
CREATE TABLE accounts (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(64) NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  email VARCHAR(128) NOT NULL,
  refresh_token VARCHAR(255) NULL,
  avatar_url VARCHAR(512) DEFAULT '',
  bio VARCHAR(512) DEFAULT '',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_username (username),
  UNIQUE KEY uk_email (email),
  KEY idx_refresh_token (refresh_token)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### videos

```sql
CREATE TABLE videos (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  author_id BIGINT UNSIGNED NOT NULL,
  username VARCHAR(64) NOT NULL,
  title VARCHAR(255) NOT NULL,
  description TEXT NULL,
  play_url VARCHAR(512) NOT NULL,
  cover_url VARCHAR(512) NOT NULL,
  create_time DATETIME NOT NULL,
  likes_count BIGINT NOT NULL DEFAULT 0,
  comments_count BIGINT NOT NULL DEFAULT 0,
  popularity BIGINT NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_author_time (author_id, create_time),
  KEY idx_create_time (create_time),
  KEY idx_likes_id (likes_count, id),
  KEY idx_popularity_time_id (popularity, create_time, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### likes

```sql
CREATE TABLE likes (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  video_id BIGINT UNSIGNED NOT NULL,
  account_id BIGINT UNSIGNED NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_video_account (video_id, account_id),
  KEY idx_account_time (account_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### comments

```sql
CREATE TABLE comments (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  video_id BIGINT UNSIGNED NOT NULL,
  author_id BIGINT UNSIGNED NOT NULL,
  username VARCHAR(64) NOT NULL,
  content TEXT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_video_time (video_id, created_at),
  KEY idx_author_time (author_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### socials

```sql
CREATE TABLE socials (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  follower_id BIGINT UNSIGNED NOT NULL,
  vlogger_id BIGINT UNSIGNED NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_follower_vlogger (follower_id, vlogger_id),
  KEY idx_follower (follower_id),
  KEY idx_vlogger (vlogger_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### tags / video_tags

```sql
CREATE TABLE tags (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE video_tags (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  video_id BIGINT UNSIGNED NOT NULL,
  tag_id BIGINT UNSIGNED NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_video_tag (video_id, tag_id),
  KEY idx_tag (tag_id),
  KEY idx_video (video_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### notifications

```sql
CREATE TABLE notifications (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  recipient_id BIGINT UNSIGNED NOT NULL,
  sender_id BIGINT UNSIGNED NOT NULL,
  type VARCHAR(50) NOT NULL,
  target_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
  content VARCHAR(255) NOT NULL DEFAULT '',
  is_read TINYINT(1) NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_recipient_read_time (recipient_id, is_read, created_at),
  KEY idx_recipient_time (recipient_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### outbox_events

```sql
CREATE TABLE outbox_events (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  event_id VARCHAR(64) NOT NULL,
  topic VARCHAR(128) NOT NULL,
  event_type VARCHAR(128) NOT NULL,
  aggregate_type VARCHAR(64) NOT NULL,
  aggregate_id BIGINT UNSIGNED NOT NULL,
  payload JSON NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  retry_count INT NOT NULL DEFAULT 0,
  next_retry_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_error TEXT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sent_at DATETIME NULL,
  UNIQUE KEY uk_event_id (event_id),
  KEY idx_status_retry (status, next_retry_at),
  KEY idx_aggregate (aggregate_type, aggregate_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

状态建议：

```text
pending
sent
failed
```

不要投递成功后直接删除，保留记录更利于排查。

### processed_events

```sql
CREATE TABLE processed_events (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  event_id VARCHAR(64) NOT NULL,
  consumer_name VARCHAR(128) NOT NULL,
  processed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_event_consumer (event_id, consumer_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 六、GORM Model 设计

示例：`model/outbox_event.go`

```go
package model

import "time"

type OutboxEvent struct {
    ID            uint64     `gorm:"primaryKey"`
    EventID       string     `gorm:"column:event_id;uniqueIndex"`
    Topic         string     `gorm:"column:topic"`
    EventType     string     `gorm:"column:event_type"`
    AggregateType string     `gorm:"column:aggregate_type"`
    AggregateID   uint64     `gorm:"column:aggregate_id"`
    Payload       string     `gorm:"column:payload"`
    Status        string     `gorm:"column:status"`
    RetryCount    int        `gorm:"column:retry_count"`
    NextRetryAt   time.Time  `gorm:"column:next_retry_at"`
    LastError     string     `gorm:"column:last_error"`
    CreatedAt     time.Time  `gorm:"column:created_at"`
    SentAt        *time.Time `gorm:"column:sent_at"`
}

func (OutboxEvent) TableName() string {
    return "outbox_events"
}
```

GORM 注意点：

- 表名用 `TableName()` 明确指定，避免复数规则误差。
- 金额/计数/ID 用 `uint64` 或 `int64`，不要混用太多 `uint`。
- JSON payload 可以先用 `string` 存，简单稳定；后面再升级 datatypes.JSON。
- 所有事务都通过 `db.Transaction(func(tx *gorm.DB) error { ... })`。

## 七、Kafka topic 设计

第一版 topic 不要太多：

```text
video-events
interaction-events
social-events
```

事件类型：

```text
video.published
like.created
like.deleted
comment.created
comment.deleted
social.followed
social.unfollowed
```

consumer group：

```text
timeline-consumer       消费 video-events
hotrank-consumer        消费 interaction-events
notification-consumer   消费 interaction-events + social-events
```

Kafka 关键规则：

```text
同一个 topic 的同一条消息，
同一个 consumer group 内只会被一个 consumer 实例消费；
不同 consumer group 会各自完整消费一遍。
```

所以：

```text
like.created
  -> hotrank-consumer group 消费一次
  -> notification-consumer group 也消费一次
```

## 八、事件消息格式

统一事件 envelope：

```json
{
  "event_id": "uuid",
  "event_type": "video.published",
  "aggregate_type": "video",
  "aggregate_id": 123,
  "occurred_at": 1720000000000,
  "payload": {
    "video_id": 123,
    "author_id": 1,
    "create_time": 1720000000000
  }
}
```

为什么要 envelope：

- `event_id` 用于幂等。
- `event_type` 用于一个 topic 下区分不同事件。
- `aggregate_id` 便于 Kafka key 分区，保证同一视频相关事件尽量有序。
- `payload` 保存业务字段。

## 九、开发顺序总览

严格按这个顺序做：

```text
0. 初始化目录和 go.mod
1. Docker Compose: MySQL / Redis / Kafka / etcd
2. SQL schema
3. common: GORM、JWT、response、xerr、rediskey、kafkax
4. account-rpc + gateway account API
5. video-rpc: PublishVideo + Outbox
6. job/outbox: 扫 outbox_events 投 Kafka
7. job/timeline: 消费 video.published 写 Redis timeline
8. feed-rpc: ListLatest
9. interaction-rpc: Like/Unlike
10. job/hotrank: 消费 like/comment 写 Redis 热榜
11. feed-rpc: ListByPopularity
12. comment
13. social
14. notification
15. chunk upload
16. 测试、压测、文档、简历总结
```

## 十、阶段 0：初始化项目

目录有两类：

```text
go-zero 服务目录:
  目录可以手动创建，里面的 handler/logic/svc 等代码用 goctl 生成

公共/基础设施目录:
  手动 mkdir，比如 common、model、repository、deploy
```

注意：`goctl api new` 和 `goctl rpc new` 很适合快速 demo，但它们会在每个服务目录里生成自己的 `go.mod`。这个项目建议采用“单仓单模块”，也就是根目录只有一个 `go.mod`，所以更推荐：

```text
手动创建目录和 .api/.proto 文件
然后用 goctl api go / goctl rpc protoc 生成服务代码
```

不要手写 go-zero 的 `internal/handler/logic/svc/config` 这些骨架，让 `goctl` 根据 `.api` / `.proto` 生成，后面你只改 logic 和 svc 里的业务代码。

在 `/home/hslam/feedsystem-zero` 下执行：

```bash
go mod init feedsystem-zero
mkdir -p common/{ctxdata,gormx,jwtx,kafkax,rediskey,response,xerr}
mkdir -p model repository deploy/sql deploy/kafka docs
mkdir -p apps/job/{outbox,timeline,hotrank,notification}
mkdir -p apps/{gateway,account,video,feed,interaction,social,notification}
```

然后先手写最小 `.api` / `.proto` 文件，再用 `goctl` 生成代码。第一批只生成这些：

```bash
goctl api go -api apps/gateway/gateway.api -dir apps/gateway
goctl rpc protoc apps/account/account.proto --go_out=apps/account --go-grpc_out=apps/account --zrpc_out=apps/account
```

后面的服务晚点再生成：

```bash
goctl rpc protoc apps/video/video.proto --go_out=apps/video --go-grpc_out=apps/video --zrpc_out=apps/video
goctl rpc protoc apps/feed/feed.proto --go_out=apps/feed --go-grpc_out=apps/feed --zrpc_out=apps/feed
goctl rpc protoc apps/interaction/interaction.proto --go_out=apps/interaction --go-grpc_out=apps/interaction --zrpc_out=apps/interaction
goctl rpc protoc apps/social/social.proto --go_out=apps/social --go-grpc_out=apps/social --zrpc_out=apps/social
goctl rpc protoc apps/notification/notification.proto --go_out=apps/notification --go-grpc_out=apps/notification --zrpc_out=apps/notification
```

先创建：

```text
deploy/docker-compose.yml
deploy/sql/001_schema.sql
```

验收：

```bash
go env GOPATH
goctl --version
protoc --version
```

## 十一、阶段 1：Docker Compose

第一阶段先不要上 Kafka。注册、登录、验证码只需要：

```text
MySQL: 存用户
Redis: 存验证码
etcd: account-rpc 服务注册发现
```

Kafka 等到 `video-rpc + outbox` 阶段再加，否则你现在会同时被 Kafka、go-zero、GORM 三件事分散注意力。

### 1. 创建 deploy/docker-compose.yml

文件路径：

```text
/home/hslam/feedsystem-zero/deploy/docker-compose.yml
```

内容可以先写成：

```yaml
services:
  mysql:
    image: mysql:8.0
    container_name: feedsystem-zero-mysql
    restart: unless-stopped
    environment:
      MYSQL_ROOT_PASSWORD: 123456
      MYSQL_DATABASE: feedsystem_zero
      TZ: Asia/Shanghai
    ports:
      - "3308:3306"
    command:
      - --default-authentication-plugin=mysql_native_password
      - --character-set-server=utf8mb4
      - --collation-server=utf8mb4_0900_ai_ci
    volumes:
      - mysql_data:/var/lib/mysql
      - ./sql:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "mysqladmin ping -h 127.0.0.1 -uroot -p$${MYSQL_ROOT_PASSWORD} --silent"]
      interval: 5s
      timeout: 5s
      retries: 20

  redis:
    image: redis:7-alpine
    container_name: feedsystem-zero-redis
    restart: unless-stopped
    command: ["redis-server", "--appendonly", "yes", "--requirepass", "123456"]
    ports:
      - "6380:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD-SHELL", "redis-cli -a 123456 ping"]
      interval: 5s
      timeout: 3s
      retries: 20

  etcd:
    image: quay.io/coreos/etcd:v3.5.32
    container_name: feedsystem-zero-etcd
    restart: unless-stopped
    command:
      - /usr/local/bin/etcd
      - --name=etcd0
      - --data-dir=/etcd-data
      - --listen-client-urls=http://0.0.0.0:2379
      - --advertise-client-urls=http://0.0.0.0:2379
      - --listen-peer-urls=http://0.0.0.0:2380
      - --initial-advertise-peer-urls=http://0.0.0.0:2380
      - --initial-cluster=etcd0=http://0.0.0.0:2380
      - --initial-cluster-token=etcd-cluster-1
      - --initial-cluster-state=new
    ports:
      - "23790:2379"
    volumes:
      - etcd_data:/etcd-data
    healthcheck:
      test: ["CMD-SHELL", "ETCDCTL_API=3 etcdctl --endpoints=http://127.0.0.1:2379 endpoint health || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 20

volumes:
  mysql_data:
  redis_data:
  etcd_data:
```

端口说明：

```text
MySQL: 3308:3306      # 避免和 feedsystem 原项目的 3307 冲突
Redis: 6380:6379      # 避免和本机已有 Redis 冲突
etcd: 23790:2379     # 宿主机用 23790，容器内仍是 2379
```

### 2. 创建 deploy/sql/001_schema.sql

第一阶段只需要账号表：

```sql
CREATE TABLE IF NOT EXISTS accounts (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(64) NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  email VARCHAR(128) NOT NULL,
  refresh_token VARCHAR(255) NULL,
  avatar_url VARCHAR(512) DEFAULT '',
  bio VARCHAR(512) DEFAULT '',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_username (username),
  UNIQUE KEY uk_email (email),
  KEY idx_refresh_token (refresh_token)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

后面做视频、点赞、Outbox 时，再继续往 SQL 文件里补表，或者新建：

```text
deploy/sql/002_video.sql
deploy/sql/003_interaction.sql
deploy/sql/004_outbox.sql
```

### 3. 启动依赖

在项目根目录执行：

```bash
cd /home/hslam/feedsystem-zero
docker-compose -f deploy/docker-compose.yml up -d
docker-compose -f deploy/docker-compose.yml ps
```

如果你本机没有 `docker compose`，就用 `docker-compose`。你之前机器上是 `docker-compose v2.24.6`，所以这里用 `docker-compose`。

### 4. 验证 MySQL

方式一：进入容器：

```bash
docker exec -it feedsystem-zero-mysql mysql -uroot -p123456 feedsystem_zero
```

进入 MySQL 后执行：

```sql
SHOW TABLES;
DESC accounts;
```

方式二：宿主机如果有 mysql client：

```bash
mysql -h127.0.0.1 -P3308 -uroot -p123456 feedsystem_zero
```

### 5. 验证 Redis

```bash
docker exec -it feedsystem-zero-redis redis-cli -a 123456 ping
```

期望输出：

```text
PONG
```

也可以测试写入：

```bash
docker exec -it feedsystem-zero-redis redis-cli -a 123456 set test:hello world
docker exec -it feedsystem-zero-redis redis-cli -a 123456 get test:hello
```

### 6. 验证 etcd

```bash
docker exec -it feedsystem-zero-etcd etcdctl endpoint health
```

期望类似：

```text
127.0.0.1:2379 is healthy
```

如果容器内命令有差异，也可以先看日志：

```bash
docker logs feedsystem-zero-etcd --tail=50
```

### 7. account-rpc 配置怎么连这些依赖

`apps/account/etc/account.yaml` 先改成：

```yaml
Name: account.rpc
ListenOn: 0.0.0.0:9001

Etcd:
  Hosts:
    - 127.0.0.1:23790
  Key: account.rpc

Mysql:
  DataSource: root:123456@tcp(127.0.0.1:3308)/feedsystem_zero?charset=utf8mb4&parseTime=true&loc=Local

Redis:
  Addr: 127.0.0.1:6380
  Password: "123456"
  DB: 0

Jwt:
  AccessSecret: feedsystem-zero-dev-secret
  AccessExpire: 900
```

其中：

```text
ListenOn 9001: account-rpc 自己监听的端口
Etcd 23790: account-rpc 启动后注册到 etcd
Mysql 3308: 宿主机访问 MySQL 容器
Redis 6380: 宿主机访问 Redis 容器
AccessExpire 900: access token 15 分钟过期
```

### 8. gateway 配置怎么调用 account-rpc

`apps/gateway/etc/gateway.yaml` 后面要加 account-rpc client：

```yaml
Name: gateway
Host: 0.0.0.0
Port: 8888

AccountRpc:
  Etcd:
    Hosts:
      - 127.0.0.1:23790
    Key: account.rpc
```

这表示：

```text
gateway 不直接知道 account-rpc 的 IP:端口
gateway 去 etcd 根据 Key=account.rpc 找 account-rpc
```

### 9. 第一阶段启动顺序

```text
1. docker-compose 启动 MySQL/Redis/etcd
2. 启动 account-rpc
3. 启动 gateway
4. curl/Postman 调 /account/verification、/account/register、/account/login
```

命令：

```bash
cd /home/hslam/feedsystem-zero/apps/account
go run account.go -f etc/account.yaml
```

另开终端：

```bash
cd /home/hslam/feedsystem-zero/apps/gateway
go run gateway.go -f etc/gateway.yaml
```

### 10. 这一阶段做到什么算完成

完成标准：

```text
docker-compose ps 显示 mysql/redis/etcd 都 healthy 或 running
MySQL 里有 accounts 表
Redis ping 返回 PONG
account-rpc 能启动并注册到 etcd
gateway 能启动
/account/verification 能返回验证码
/account/register 能写入 accounts 表
/account/login 能返回 token
```

## 十二、阶段 2：common 基础包

先写这些公共包：

### common/gormx

负责：

```text
根据配置创建 *gorm.DB
设置连接池
提供 Transaction helper
```

建议配置：

```yaml
Mysql:
  DataSource: root:123456@tcp(127.0.0.1:3308)/feedsystem_zero?charset=utf8mb4&parseTime=true&loc=Local
```

### common/jwtx

负责：

```text
GenerateAccessToken
ParseToken
GenerateRefreshToken
```

### common/response

统一 HTTP 返回：

```json
{
  "code": 0,
  "message": "ok",
  "data": {}
}
```

### common/xerr

业务错误码：

```text
ErrInvalidParam
ErrUnauthorized
ErrAccountExists
ErrPasswordWrong
ErrVideoNotFound
ErrAlreadyLiked
ErrNotLiked
```

### common/rediskey

统一 key：

```text
TokenKey(accountID)
VideoDetailKey(videoID)
VideoEntityKey(videoID)
FeedGlobalTimelineKey()
HotVideoWindowKey(minute)
HotVideoMergeKey(asOf)
```

### common/kafkax

第一版可以简单封装：

```text
Producer.Send(ctx, topic, key, value)
Consumer.Run(ctx, topics, group, handler)
```

Kafka Go 客户端可选：

```text
github.com/segmentio/kafka-go
```

它简单易用，适合你当前复现。

## 十三、阶段 3：account-rpc

### proto 能力

`apps/account/account.proto`：

```text
Register
Login
Logout
RefreshToken
GetByID
GetByUsername
UpdateProfile
```

### 生成命令

在根目录 `/home/hslam/feedsystem-zero` 下执行：

```bash
goctl rpc protoc apps/account/account.proto --go_out=apps/account --go-grpc_out=apps/account --zrpc_out=apps/account
```

### GORM 逻辑

Register：

```text
检查 username 是否存在
bcrypt hash password
insert accounts
```

Login：

```text
按 username 查账号
bcrypt 校验
生成 access token / refresh token
MySQL 只保存 refresh_token
返回 token
```

Logout：

```text
清 MySQL refresh_token
后续如引入 access token 黑名单，再写 Redis 黑名单
```

### gateway account API

`apps/gateway/api/gateway.api` 先只写：

```text
POST /account/register
POST /account/login
POST /account/logout
GET  /account/profile
```

生成：

```bash
goctl api go -api gateway.api -dir .
```

验收：

```text
可以注册
可以登录
能拿 token 访问 /account/profile
logout 后旧 token 不能访问
```

## 十四、阶段 4：video-rpc 同步主功能

### video-rpc 第一版接口

```text
PublishVideo
GetVideoDetail
ListByAuthor
DeleteVideo
LikeVideo
UnlikeVideo
IsLiked
ListMyLikedVideos
PublishComment
DeleteComment
ListComments
```

这一阶段先把原项目 video 包的同步功能复现出来，不接 Outbox/Kafka/Feed。

注意：上传视频、上传封面、分片上传属于 HTTP 文件处理能力，第一版放在 gateway handler 里做，不要把大文件通过 zrpc 传给 video-rpc。

建议先做普通上传，再做发布：

```text
UploadVideo:
  gateway 处理 multipart
  上传 mp4，返回 play_url

UploadCover:
  gateway 处理 multipart
  上传 jpg/jpeg/png/webp，返回 cover_url

PublishVideo:
  接收 title/description/play_url/cover_url
  从 JWT 拿 author_id
  gateway 或 video-rpc 内部补齐 author_username
  写 videos
  提取 #tag
  写 tags/video_tags
```

如果一开始不想先做 multipart，也可以先用假 URL 跑通写库：

```json
{
  "title": "test #go",
  "description": "hello",
  "play_url": "http://localhost/static/test.mp4",
  "cover_url": "http://localhost/static/test.jpg"
}
```

### PublishVideo 事务

核心逻辑：

```text
db.Transaction:
  insert videos
  extract #tag
  insert tags
  insert video_tags
```

验收：

```text
POST /video/publish
videos 有记录
tags/video_tags 有记录
GET /video/detail 能查到详情
GET /video/author 能查到作者视频列表
```

### Like / Comment 高并发版

原项目的点赞和评论都在 `internal/video` 包下，所以这一阶段可以继续做：

这一版不再把点赞/评论计数都直接打到 MySQL 热点行上，而是区分：

```text
点赞关系:
  Redis 快路径更新用户状态
  Redis Stream 记录变更日志
  后台 job 批量刷 MySQL likes 表

视频计数:
  Redis Hash 缓冲 likes_count/comments_count/popularity 增量
  详情和列表展示时读取 MySQL 基础值 + Redis delta
  后台 job 定时批量刷回 videos 表

评论正文:
  同步写 MySQL comments 表
  评论计数和热度走 Redis delta
  评论列表读缓存，写评论后删除缓存
```

### 点赞 Redis 快路径

LikeVideo:

```text
1. check video exists，可先查 Redis video cache，miss 再查 MySQL
2. Lua 原子执行:
   - 判断 like:state:{videoID}:{userID} 或 like:video:{videoID}:users 是否已经点赞
   - SADD like:video:{videoID}:users userID
   - SADD like:user:{userID}:videos videoID
   - SET like:state:{videoID}:{userID} 1 EX 7d
   - HINCRBY video:like_delta videoID +1
   - HINCRBY video:popularity_delta videoID +1
   - ZINCRBY hot:video:realtime +1 videoID
   - XADD like:events action=like video_id user_id
3. 返回成功
```

UnlikeVideo:

```text
1. Lua 原子执行:
   - 判断当前是否已点赞
   - SREM like:video:{videoID}:users userID
   - SREM like:user:{userID}:videos videoID
   - SET like:state:{videoID}:{userID} 0 EX 7d
   - HINCRBY video:like_delta videoID -1
   - HINCRBY video:popularity_delta videoID -1
   - ZINCRBY hot:video:realtime -1 videoID
   - XADD like:events action=unlike video_id user_id
2. 返回成功
```

IsLiked:

```text
1. 先查 like:state:{videoID}:{userID}
2. 命中 1 -> true
3. 命中 0 -> false
4. 未命中 -> 查 MySQL likes 表
```

ListMyLikedVideos:

```text
1. 优先读 like:user:{userID}:videos
2. 如果集合为空或未预热，再查 MySQL likes join videos
3. 查到后可回填 Redis Set
```

后台刷库 job：

```text
like-flush job:
  从 Redis Stream like:events 批量读取事件
  按 video_id:user_id 合并，只保留最后一次 action
  action=like   -> INSERT IGNORE INTO likes(video_id,user_id)
  action=unlike -> DELETE FROM likes WHERE video_id=? AND user_id=?

stats-flush job:
  扫 video:like_delta / video:comment_delta / video:popularity_delta
  批量 UPDATE videos
  成功后把对应 delta 扣回去，不能直接 HDEL，避免覆盖刷库期间的新增量
```

注意：这一版用 Redis Stream 作为“点赞关系刷库日志”，不是 Kafka。Kafka/Outbox 下一阶段再接入，用于 Feed、通知、热榜等跨服务事件。

### 评论高并发优化

评论正文这一版仍同步落 MySQL，因为评论是用户明确提交的内容，丢失和延迟可见的代价比点赞更高。

PublishComment:

```text
1. Redis SETNX comment:rate:{userID}:{videoID} 1 EX 2s
   - 防止用户短时间重复刷评论
2. check video exists
3. INSERT comments(video_id,user_id,username,content)
4. HINCRBY video:comment_delta videoID +1
5. HINCRBY video:popularity_delta videoID +1
6. ZINCRBY hot:video:realtime +1 videoID
7. DEL comment:list:{videoID}:0:{limit}
8. DEL video:detail:{videoID}
```

DeleteComment:

```text
1. 查 comment，校验只能作者删除
2. DELETE comments WHERE id=?
3. HINCRBY video:comment_delta videoID -1
4. HINCRBY video:popularity_delta videoID -1
5. DEL comment list cache 和 video detail cache
```

ListComments:

```text
1. 第一页先读 Redis comment:list:{videoID}:0:{limit}
2. miss 后查 MySQL:
   WHERE video_id=? ORDER BY created_at ASC, id ASC LIMIT ?
3. 写 Redis，TTL 30s-60s
```

详情页计数：

```text
display_likes_count = videos.likes_count + HGET video:like_delta videoID
display_comments_count = videos.comments_count + HGET video:comment_delta videoID
display_popularity = videos.popularity + HGET video:popularity_delta videoID
```

这一版先不做：

```text
评论正文先 Kafka 再落库
通知中心异步写入
复杂审核流
```

这三个等 Outbox/Kafka 阶段再做。

## 十五、阶段 5：job/outbox

outbox job 是这个项目的关键工程亮点。

### 扫描逻辑

```text
每 500ms 或 1s 扫描一次
查询 status=pending and next_retry_at <= now
limit 100
逐条投递 Kafka
成功:
  status=sent
  sent_at=now
失败:
  retry_count + 1
  next_retry_at = now + backoff
  last_error = err
  retry_count 超过阈值 -> status=failed
```

### 并发注意

第一版单实例即可。

升级版可以用：

```text
SELECT ... FOR UPDATE SKIP LOCKED
```

或者加一个 `locked_at` 字段，避免多个 outbox job 重复投递。

### 验收

```text
启动 Kafka
启动 job/outbox
发布视频
outbox_events 从 pending 变 sent
Kafka topic video-events 能看到消息
```

## 十六、阶段 6：job/timeline + feed-rpc ListLatest

### timeline consumer

消费：

```text
topic: video-events
group: timeline-consumer
event_type: video.published
```

处理：

```text
幂等检查 processed_events(event_id, timeline-consumer)
ZADD feed:global_timeline score=create_time member=video_id
ZREMRANGEBYRANK 保留最近 1000 条
记录 processed_events
提交 offset
```

### feed-rpc ListLatest 第一版

逻辑：

```text
Redis ZREVRANGE feed:global_timeline 取 videoIDs
video-rpc BatchGetVideos 或 feed-rpc 直接查 DB
返回 video_list
```

第一版为了简单，可以让 feed-rpc 直接读 videos 表。后面再改成调用 video-rpc 或加缓存。

验收：

```text
发布视频
timeline consumer 写 Redis
/feed/latest 能看到新视频
```

这是第一条完整主链路：

```text
发布视频 -> MySQL videos -> outbox_events -> Kafka -> timeline consumer -> Redis timeline -> feed latest
```

## 十七、阶段 7：Feed 缓存优化

补：

```text
video:entity:{id}
video:detail:{id}
singleflight
Redis MGet
```

ListLatest：

```text
先从 Redis timeline 拿 videoIDs
GetVideoByIDs:
  L1 本地缓存，可选
  L2 Redis MGet
  L3 MySQL
回源后写 Redis
BatchGetLiked 补 is_liked
```

验收：

```text
第一次查 feed 走 MySQL
第二次查 feed 命中 Redis video entity
并发查询不会把 DB 打爆
```

## 十八、阶段 8：interaction-rpc 点赞

### Like

逻辑：

```text
db.Transaction:
  检查 video 存在
  insert likes(video_id, account_id)
    如果唯一键冲突 -> already liked
  update videos set likes_count = likes_count + 1
  update videos set popularity = popularity + 1
  insert outbox_events(event_type=like.created, topic=interaction-events)
```

### Unlike

逻辑：

```text
db.Transaction:
  delete likes where video_id=? and account_id=?
  如果 rows_affected=0 -> not liked
  update videos set likes_count = greatest(likes_count - 1, 0)
  update videos set popularity = greatest(popularity - 1, 0)
  insert outbox_events(event_type=like.deleted, topic=interaction-events)
```

### 为什么这样比原项目更好

原项目点赞在 MQ 正常时是异步写 MySQL，存在接口返回后核心状态尚未落库的问题。

你的版本：

```text
likes 表和 likes_count 同步事务提交
热榜和通知异步处理
```

面试更好讲。

验收：

```text
点赞后 likes 有记录
videos.likes_count +1
outbox_events 有 like.created
重复点赞返回 already liked
取消点赞后 likes 删除，likes_count -1
```

## 十九、阶段 9：job/hotrank

消费：

```text
topic: interaction-events
group: hotrank-consumer
```

处理规则：

```text
like.created      -> +1
like.deleted      -> -1
comment.created   -> +2
comment.deleted   -> -2，可选
```

Redis key：

```text
hot:video:1m:{yyyyMMddHHmm}
```

处理：

```text
SETNX processed:hotrank:{event_id} 1 EX 7d
ZINCRBY hot:video:1m:{minute} score video_id
EXPIRE hot:video:1m:{minute} 2h
DEL video:entity:{video_id}
DEL video:detail:{video_id}
```

hotrank 只写 Redis，第一版可以用 Redis SETNX 做幂等。想更严谨也可以用 MySQL `processed_events`。

## 二十、阶段 10：feed-rpc 热榜

ListByPopularity：

```text
as_of = 当前分钟或请求传入分钟
keys = 最近 60 个 hot:video:1m
ZUNIONSTORE hot:video:merge:1m:{as_of}
EXPIRE merge 2min
ZREVRANGE merge offset offset+limit-1
批量补齐视频实体
返回 as_of / next_offset
```

验收：

```text
点赞后 hot ZSet 有分数
/feed/hot 能查到视频
as_of + offset 可以翻页
```

## 二十一、阶段 11：评论

CommentCreate：

```text
db.Transaction:
  检查 video 存在
  insert comments
  update videos.comments_count +1
  update videos.popularity +2
  insert outbox_events(comment.created)
```

CommentDelete：

```text
db.Transaction:
  查 comment
  校验 author_id
  delete comment
  update videos.comments_count -1
  update videos.popularity -2
  insert outbox_events(comment.deleted)
```

验收：

```text
评论后 comments 有记录
videos.comments_count 增加
热榜分数增加
通知 consumer 能生成评论通知
```

## 二十二、阶段 12：social

Follow：

```text
db.Transaction:
  校验 follower/vlogger 存在
  不允许关注自己
  insert socials
  insert outbox_events(social.followed)
```

Unfollow：

```text
db.Transaction:
  delete socials
  insert outbox_events(social.unfollowed)
```

关注流第一版：

```text
fanout-on-read:
  查 socials 得到 vlogger_ids
  查 videos where author_id in (...)
```

升级版：

```text
fanout-on-write:
  video.published consumer 查询作者粉丝
  把 video_id 推到 feed:inbox:{follower_id}
```

第一版先 fanout-on-read，不要过早复杂化。

## 二十三、阶段 13：notification

notification consumer 消费：

```text
interaction-events:
  like.created
  comment.created

social-events:
  social.followed
```

处理：

```text
db.Transaction:
  insert processed_events(event_id, notification-consumer)
  查询 recipient
  insert notifications
```

notification-rpc：

```text
ListNotifications
UnreadCount
MarkRead
```

SSE 第一版可以后做。

验收：

```text
点赞别人的视频后，对方 notifications 有 like 通知
评论别人的视频后，对方有 comment 通知
关注别人后，对方有 follow 通知
```

## 二十四、阶段 14：分片上传

这块最后做。

gateway 提供：

```text
POST /video/chunk/init
POST /video/chunk/upload
POST /video/chunk/status
POST /video/chunk/complete
```

Redis：

```text
chunk_upload:{upload_id}
chunk_upload_hash:{account_id}:{file_hash}
```

本地文件：

```text
.run/uploads/tmp/{upload_id}/{chunk_index}
.run/uploads/videos/{account_id}/{date}/{file}.mp4
```

complete 后返回 play_url，然后前端再调用 `/video/publish`。

升级：

```text
MinIO
秒传
FFmpeg 转码
封面截帧
```

## 二十五、API 路由规划

gateway 第一版：

```text
POST /account/register
POST /account/login
POST /account/logout
GET  /account/profile

POST /video/publish
POST /video/detail
POST /video/listByAuthor

POST /feed/latest
POST /feed/hot
POST /feed/following

POST /interaction/like
POST /interaction/unlike
POST /interaction/comment
POST /interaction/comment/delete

POST /social/follow
POST /social/unfollow

POST /notification/list
POST /notification/unreadCount
POST /notification/markRead
```

## 二十六、验收用例顺序

每次只验一条链路。

### 用例 1：账号

```text
注册 userA
登录 userA
拿 token 获取 profile
logout
旧 token 失效
```

### 用例 2：视频发布进最新流

```text
userA 登录
发布 videoA
检查 videos 表
检查 outbox_events pending -> sent
检查 Kafka video-events
检查 Redis feed:global_timeline
调用 /feed/latest 查到 videoA
```

### 用例 3：点赞和热榜

```text
userB 登录
userB 点赞 videoA
likes 表有记录
videos.likes_count = 1
outbox_events 有 like.created
hotrank consumer 更新 Redis hot ZSet
/feed/hot 查到 videoA
重复点赞返回 already liked
```

### 用例 4：消费幂等

```text
手动用同一个 event_id 重放 Kafka 消息
notification 只写一条
hotrank 不重复加分
processed_events 有记录
```

### 用例 5：评论通知

```text
userB 评论 userA 的 videoA
comments 表有记录
notifications 给 userA 写 comment 通知
```

### 用例 6：关注流

```text
userB follow userA
userB 调用 /feed/following
能看到 userA 的视频
unfollow 后关注流不再显示
```

## 二十七、面试亮点对应实现

### go-zero 微服务

对应实现：

```text
gateway + account-rpc + video-rpc + feed-rpc + interaction-rpc
```

面试表达：

> 我用 go-zero 拆分 API 网关和内部 RPC 服务，网关只负责 HTTP、鉴权和聚合，核心业务逻辑放在 RPC 服务中。

### Kafka 事件驱动

对应实现：

```text
outbox job -> Kafka topics -> timeline/hotrank/notification consumers
```

面试表达：

> Kafka 用来承接领域事件，不同 consumer group 分别更新时间线、热榜和通知，实现事件的一次发布、多方消费。

### Outbox 一致性

对应实现：

```text
业务表 + outbox_events 同事务提交
outbox job 可靠投递 Kafka
```

面试表达：

> 避免业务写库成功但消息发送失败的问题。

### 消费幂等

对应实现：

```text
processed_events(event_id, consumer_name) 唯一索引
```

面试表达：

> Kafka 至少一次投递可能导致重复消费，所以每个重要 consumer 都基于 event_id 做幂等控制。

### Feed 高性能读取

对应实现：

```text
Redis ZSet timeline
Redis video entity cache
MySQL cursor fallback
singleflight
```

面试表达：

> 信息流先拿 video_id，再批量补齐实体，利用 Redis 和本地缓存降低 DB 压力。

## 二十八、当前进度

账号模块已经跑通，当前完成内容：

```text
account-rpc:
  SendVerification
  Register
  Login
  Logout
  RefreshToken
  GetProfile
  UpdateProfile

gateway:
  /account/verification
  /account/register
  /account/login
  /account/refresh_token
  /account/logout
  /account/profile GET
  /account/profile PUT

基础能力:
  MySQL accounts 表
  Redis 验证码
  Redis access token 白名单
  MySQL refresh token
  JWT + TokenAuth 双层鉴权
  gateway 从 JWT context 获取 user_id，不信任前端传 user_id
```

这说明项目的“用户身份地基”已经完成。接下来不要继续扩账号小功能，应该先把原项目 video 域的同步功能完整复现出来，再进入 Outbox/Kafka/Feed。

## 二十九、下一步开发顺序

现在开始做第二阶段：

```text
1. video-rpc
2. gateway video API
3. videos/tags/video_tags/likes/comments 表
4. 本地文件存储
5. 普通上传视频/封面
6. 发布视频/删除视频/详情/作者列表
7. 分片上传
8. 点赞/取消点赞/isLiked/我点赞的视频
9. 评论发布/删除/列表
10. 下一阶段再做 Outbox + Kafka + Feed 最新流
```

### 为什么先做 video-rpc

视频是整个项目的核心资源。点赞、评论、Feed、热榜、通知都围绕视频展开。

如果先写点赞或 Feed，就会缺少稳定的视频数据来源。正确顺序是：

```text
先能上传视频/封面，拿到 play_url/cover_url
再能发布视频，把元数据写入 videos
再能查询视频
再能分片上传
再围绕视频做点赞评论
最后再把发布、点赞、评论事件接到 Outbox/Kafka/Feed
```

### video-rpc 第一版方法

先定义最少但完整的方法：

```protobuf
service Video {
  rpc PublishVideo(PublishVideoReq) returns (PublishVideoResp);
  rpc GetVideo(GetVideoReq) returns (GetVideoResp);
  rpc ListUserVideos(ListUserVideosReq) returns (ListUserVideosResp);
  rpc DeleteVideo(DeleteVideoReq) returns (DeleteVideoResp);
  rpc LikeVideo(LikeVideoReq) returns (LikeVideoResp);
  rpc UnlikeVideo(UnlikeVideoReq) returns (UnlikeVideoResp);
  rpc IsLiked(IsLikedReq) returns (IsLikedResp);
  rpc ListMyLikedVideos(ListMyLikedVideosReq) returns (ListMyLikedVideosResp);
  rpc PublishComment(PublishCommentReq) returns (PublishCommentResp);
  rpc DeleteComment(DeleteCommentReq) returns (DeleteCommentResp);
  rpc ListComments(ListCommentsReq) returns (ListCommentsResp);
}
```

第一版不要把视频审核、转码、MinIO 都塞进来。先做本地文件存储：

```text
gateway 接收 multipart file
gateway 保存到 uploads/videos、uploads/covers
发布时 gateway 调 video-rpc 写 videos 表
video-rpc 同事务写 videos + tags/video_tags
```

这样既能跑通真实上传，也不会一开始就被对象存储和转码拖住。

### videos 表第一版字段

建议先落这些字段：

```text
id
author_id
title
description
play_url
cover_url
likes_count
comments_count
popularity
created_at
updated_at
```

`author_id` 从 JWT 里拿，不允许前端传。

### gateway video API 第一版

建议先做：

```text
POST /video/upload        需要 JWT，上传 mp4，返回 play_url
POST /video/cover         需要 JWT，上传封面，返回 cover_url
POST /video/publish       需要 JWT，JSON 传 title/play_url/cover_url
GET  /video/:id           可不登录，查视频详情
GET  /video/user          需要 JWT，查我的视频列表
DELETE /video/:id         需要 JWT，只允许作者删除
POST /video/chunk/init    需要 JWT，初始化分片上传
POST /video/chunk/upload  需要 JWT，上传分片
GET  /video/chunk/status  需要 JWT，查询分片状态
POST /video/chunk/complete 需要 JWT，合并分片，返回 play_url
```

如果 goctl 生成的 handler 不方便直接表达 multipart，handler 里可以手写读取：

```go
file, header, err := r.FormFile("video")
```

这是正常做法，不影响 go-zero 的整体结构。

## 三十、三天冲刺计划

三天内完成“面试可展示 MVP”是可行的，但不是完成所有高级功能。建议目标定义为：

```text
账号登录态完整
视频发布和查询完整
Feed 最新流可用
点赞/取消点赞可用
Kafka + Outbox + 消费幂等至少跑通一条完整链路
文档和接口测试整理清楚
```

暂时不纳入三天范围：

```text
视频转码
MinIO/OSS
关注流
通知中心完整实现
推荐算法
压测调优
多设备登录态
复杂权限系统
```

### 第 1 天：视频主链路

目标：用户登录后可以上传、发布、删除、查询视频。

任务：

```text
1. 补 deploy/sql:
   videos
   tags
   video_tags
   likes
   comments

2. 新建 apps/video:
   video.proto
   goctl rpc protoc 生成 video-rpc

3. 新建 video model:
   Video
   Tag
   VideoTag
   Like
   Comment

4. video-rpc:
   PublishVideo
   GetVideo
   ListUserVideos
   DeleteVideo

5. gateway:
   增加 VideoRpc client
   增加 /video/upload
   增加 /video/cover
   增加 /video/publish
   增加 /video/:id
   增加 /video/user
   增加 DELETE /video/:id

6. 本地文件存储:
   uploads/videos
   uploads/covers
   保存文件后把相对 URL 写入 videos 表

7. 联调:
   登录 -> 上传视频 -> 查详情 -> 查我的视频列表
```

验收标准：

```text呢

第 1 天不要急着写 Kafka consumer。先把视频同步主链路写稳。

### 第 2 天：Outbox + Kafka + Feed 最新流

目标：视频发布后，通过 Kafka 异步写入 Redis timeline，Feed 能读出来。

任务：

```text
1. docker-compose 增加 Kafka
2. common/kafkax:
   producer 初始化
   consumer 初始化
   event envelope 定义

3. job/outbox:
   扫描 pending outbox_events
   publish 到 Kafka topic video-events
   成功后标记 sent
   失败后记录 retry_count / last_error

4. job/timeline:
   消费 video.published
   processed_events 做幂等
   Redis ZADD feed:global_timeline

5. feed-rpc:
   ListLatest
   先读 Redis ZSet 拿 video_id
   批量查 MySQL videos
   返回 cursor/has_more

6. gateway:
   GET /feed/latest
```

验收标准：

```text
发布视频 -> outbox_events pending
job/outbox -> Kafka -> outbox_events sent
job/timeline -> Redis timeline 有 video_id
GET /feed/latest 能看到刚发布的视频
重复消费同一个 event_id 不会重复处理
```

第 2 天结束时，这个项目已经有“微服务 + Kafka + Redis Feed + 最终一致性”的面试亮点。

### 第 3 天：互动能力 + 热榜 + 收尾文档

目标：补上短视频项目最关键的互动能力，并把面试材料整理好。

任务：

```text
1. 新建 apps/interaction:
   LikeVideo
   UnlikeVideo
   CommentVideo
   ListComments

2. SQL:
   likes
   comments

3. 点赞事务:
   check video exists
   insert likes
   update videos.likes_count + 1
   update videos.popularity + 1
   insert outbox_events like.created

4. 取消点赞事务:
   delete likes
   update videos.likes_count - 1
   update videos.popularity - 1
   insert outbox_events like.deleted

5. 评论事务:
   insert comments
   update videos.comments_count + 1
   update videos.popularity + 评论权重
   insert outbox_events comment.created

6. job/hotrank:
   消费 interaction-events
   processed_events 幂等
   Redis ZINCRBY hot:video

7. feed-rpc:
   ListPopular

8. gateway:
   POST /interaction/like
   DELETE /interaction/like
   POST /interaction/comment
   GET /interaction/comments
   GET /feed/popular

9. 收尾:
   curl/Postman 测试脚本
   README 启动方式
   面试项目亮点文档
```

验收标准：

```text
点赞不能重复点赞
取消点赞具备幂等性
评论能增加 comments_count
热榜 Redis ZSet 有分数变化
Feed 最新流和热榜都能查询
Outbox 和 processed_events 能讲清楚
```

## 三十一、三天后项目应该达到的面试状态

三天后最理想的项目演示链路：

```text
1. 注册 / 登录
2. 拿 access_token
3. 发布视频
4. 查询视频详情
5. Feed 最新流出现该视频
6. 点赞视频
7. 热榜分数变化
8. 评论视频
9. 查看评论列表
10. logout 后旧 token 失效
```

面试时可以这样总结：

```text
我用 go-zero 将原 Gin 单体项目拆成 gateway + account/video/feed/interaction RPC，
核心状态用 MySQL + GORM 同步落库，派生状态通过 Outbox + Kafka 异步分发，
Feed timeline 和热榜使用 Redis ZSet 承载高频读。

为了解决分布式场景下的一致性问题，我没有在业务逻辑里直接发 Kafka，
而是把业务表和 outbox_events 放在同一个 MySQL 事务里提交；
consumer 侧用 processed_events 的唯一索引做幂等，保证 Kafka 至少一次投递下不会重复执行副作用。

认证部分使用 access token + refresh token，
access token 写 Redis 白名单，logout 可以立即失效，
gateway 从 JWT 中解析 user_id 后调用后端 RPC，避免前端篡改用户身份。
```

这就是三天内最值得追求的版本。它不一定功能最多，但技术主线最清楚，面试官也更容易看到后端能力。
