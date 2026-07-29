# feedsystem-zero-web

FeedSystem Zero 的前端（Vite + React 18 + TypeScript + Tailwind + TanStack Query + Zustand + axios）。

## 启动

```powershell
# 首次安装依赖
pnpm install    # 或 npm install / yarn

# 开发服务
pnpm dev        # 默认监听 http://127.0.0.1:5173

# 生产构建
pnpm build
```

Vite dev 服务会把以下路径反代到后端 gateway（`127.0.0.1:8888`）：

- `/account/**`
- `/video/**`
- `/interaction/**`
- `/social/**`
- `/feed/**`
- `/notification/**`
- `/uploads/**`（静态资源：封面/视频文件）

所以本地必须先起 gateway 服务，前端才能真正跑通。

## 目录结构

```
web/src/
├── main.tsx            # 入口，装配 React Query / Router / Toaster
├── App.tsx             # 全局路由表
├── index.css           # Tailwind + 公共 .input 样式
├── api/
│   ├── request.ts       # axios 实例 + JWT 注入 + 401 刷新（并发合并）
│   ├── account.ts
│   ├── video.ts
│   ├── interaction.ts
│   ├── social.ts
│   ├── feed.ts
│   └── notification.ts
├── types/
│   └── api.ts          # 与 gateway.api / types.go 完全对齐
├── stores/
│   └── auth.ts         # zustand 持久化登录态
├── hooks/
│   ├── useCurrentUser.ts
│   └── useUnreadCount.ts  # 通知红点 30s 轮询
├── utils/
│   ├── hash.ts         # 大文件分片 MD5，用于秒传
│   └── time.ts         # 时间格式化
├── components/
│   ├── UserAvatar.tsx
│   ├── FollowButton.tsx
│   ├── VideoCard.tsx
│   ├── CommentSection.tsx
│   └── InfiniteLoader.tsx  # 交叉观察器实现的无限滚动
├── router/
│   └── RequireAuth.tsx
├── layouts/
│   └── MainLayout.tsx
└── pages/
    ├── LoginPage.tsx
    ├── RegisterPage.tsx
    ├── HomePage.tsx           # 推荐/关注/热榜 三 Tab
    ├── ProfilePage.tsx        # 我的资料编辑
    ├── UserPage.tsx           # 他人主页 / 自己主页
    ├── UploadPage.tsx         # 分片上传 + 秒传 + 封面
    ├── VideoDetailPage.tsx    # 播放器 + 点赞 + 评论
    ├── LikesPage.tsx          # 我的喜欢
    ├── FollowListPage.tsx     # 粉丝 / 关注 列表
    ├── NotificationsPage.tsx  # 消息中心
    └── ComingSoon.tsx
```

## 主要功能

- ✅ 登录 / 注册 / 邮箱验证码（60s 冷却） / Token 自动刷新
- ✅ 个人资料查看与编辑
- ✅ 视频上传：分片上传 + 秒传 + 断点续传 + 进度条
- ✅ 封面上传
- ✅ 视频列表：推荐 / 关注 / 热榜三 Tab，全部无限滚动
- ✅ 视频详情：播放器 + 点赞（乐观更新） + 评论（分页 + 发表 + 删除）
- ✅ 我的喜欢
- ✅ 用户主页 + 关注/取消关注
- ✅ 粉丝列表 / 关注列表
- ✅ 消息通知：未读红点（30s 轮询）+ 单条已读 + 一键全部已读

## 与后端契约的对齐

`src/types/api.ts` 里的 TS interface 与 `apps/gateway/gateway.api` 的 `type` 保持严格一致：字段名、字段类型、可选性完全对齐。改后端 API 时**必须同步改此文件**。
