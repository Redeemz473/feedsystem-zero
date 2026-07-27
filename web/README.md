# feedsystem-zero-web

FeedSystem Zero 的前端（Vite + React 18 + TypeScript + Tailwind + TanStack Query + Zustand + axios）。

## 启动

```powershell
# 首次安装依赖
pnpm install    # 或 npm install / yarn

# 开发服务
pnpm dev        # 默认监听 http://127.0.0.1:5173
```

Vite dev 服务会把以下路径反代到后端 gateway（`127.0.0.1:8888`）：

- `/account/**`
- `/video/**`
- `/interaction/**`
- `/social/**`
- `/uploads/**`（静态资源：封面/视频文件）

所以本地必须先起 gateway 服务，前端才能真正跑通。

## 目录结构

```
web/
├── index.html
├── package.json
├── vite.config.ts
├── tailwind.config.js
├── postcss.config.js
├── tsconfig.json
└── src/
    ├── main.tsx            # 入口，装配 React Query / Router / Toaster
    ├── App.tsx             # 全局路由表
    ├── index.css           # Tailwind + 公共 .input 样式
    ├── api/
    │   ├── request.ts      # axios 实例 + JWT 注入 + 401 刷新（并发合并）
    │   └── account.ts      # 账号相关请求
    ├── types/
    │   └── api.ts          # 与 gateway.api / types.go 一致的 TS 类型
    ├── stores/
    │   └── auth.ts         # zustand 持久化登录态
    ├── hooks/
    │   └── useCurrentUser.ts
    ├── router/
    │   └── RequireAuth.tsx # 路由守卫
    ├── layouts/
    │   └── MainLayout.tsx  # 顶部导航 + 主内容
    └── pages/
        ├── LoginPage.tsx
        ├── RegisterPage.tsx
        ├── HomePage.tsx
        ├── ProfilePage.tsx
        └── ComingSoon.tsx
```

## 里程碑

- **M1 完成**：脚手架、登录/注册（含邮箱验证码 60s 冷却）、Token 自动刷新、个人资料查看/编辑、退出登录、路由守卫。
- **M2 计划**：视频上传（分片 + 秒传 + 进度）、我的视频列表、视频删除。
- **M3 计划**：视频详情页 + 播放器、点赞、评论、我的喜欢。
- **M4 计划**：关注/取关、粉丝/关注列表、用户主页。
- **M5 计划**：Feed / 热榜 / 通知（等后端 feed rpc 就绪）。
