import { Link } from "react-router-dom";

// M1 阶段占位：真正的 Feed / 推荐 / 关注流会在 M5 里接入 feed rpc
export default function HomePage() {
  return (
    <div className="max-w-3xl mx-auto px-4 py-12">
      <h1 className="text-2xl font-semibold text-gray-800">欢迎来到 FeedSystem</h1>
      <p className="mt-3 text-gray-500">
        当前 M1 阶段已经完成基础脚手架、登录/注册、路由守卫、Token 自动刷新。
      </p>

      <div className="mt-8 grid grid-cols-1 sm:grid-cols-2 gap-4">
        <Card
          title="上传视频"
          desc="M2 里程碑：支持分片上传 + 秒传"
          to="/upload"
        />
        <Card
          title="我的资料"
          desc="查看和编辑个人信息"
          to="/profile"
        />
      </div>
    </div>
  );
}

function Card({
  title,
  desc,
  to,
}: {
  title: string;
  desc: string;
  to: string;
}) {
  return (
    <Link
      to={to}
      className="block p-5 bg-white rounded-lg border border-gray-200 hover:border-brand-400 hover:shadow transition"
    >
      <div className="text-brand-600 font-medium">{title}</div>
      <div className="mt-1 text-sm text-gray-500">{desc}</div>
    </Link>
  );
}
