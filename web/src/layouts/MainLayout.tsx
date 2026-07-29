import { Link, NavLink, Outlet, useNavigate } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Bell, LogOut, User } from "lucide-react";

import { logout } from "@/api/account";
import { useCurrentUser } from "@/hooks/useCurrentUser";
import { useUnreadCount } from "@/hooks/useUnreadCount";
import { useAuthStore } from "@/stores/auth";
import { extractErrMsg } from "@/api/request";

// 顶部导航栏 + 主内容区域的骨架
export default function MainLayout() {
  const { data: me } = useCurrentUser();
  const { data: unread } = useUnreadCount();
  const navigate = useNavigate();
  const qc = useQueryClient();

  async function handleLogout() {
    try {
      await logout();
    } catch (err) {
      // 即便后端返回失败，也在前端清理 token
      toast.error(extractErrMsg(err, "登出失败，将强制清除本地登录态"));
    } finally {
      useAuthStore.getState().clear();
      qc.clear();
      navigate("/login", { replace: true });
    }
  }

  const unreadCount = unread?.unread_count ?? 0;

  return (
    <div className="min-h-full flex flex-col">
      <header className="h-14 bg-white border-b border-gray-200 sticky top-0 z-20">
        <div className="max-w-6xl mx-auto h-full px-4 flex items-center justify-between">
          <Link to="/" className="font-bold text-brand-600 text-lg">
            FeedSystem
          </Link>

          <nav className="flex items-center gap-4 text-sm">
            <NavItem to="/">首页</NavItem>
            <NavItem to="/upload">上传</NavItem>
            <NavItem to="/likes">我的喜欢</NavItem>
          </nav>

          <div className="flex items-center gap-3 text-sm">
            {me ? (
              <>
                <Link
                  to="/notifications"
                  className="relative text-gray-500 hover:text-brand-600"
                  title="消息"
                >
                  <Bell size={18} />
                  {unreadCount > 0 ? (
                    <span className="absolute -top-1 -right-1 min-w-[16px] h-4 px-1 rounded-full bg-red-500 text-white text-[10px] leading-4 text-center">
                      {unreadCount > 99 ? "99+" : unreadCount}
                    </span>
                  ) : null}
                </Link>
                <Link
                  to={`/users/${me.user_id}`}
                  className="flex items-center gap-1 text-gray-700 hover:text-brand-600"
                >
                  <User size={16} />
                  {me.username}
                </Link>
                <button
                  onClick={handleLogout}
                  className="flex items-center gap-1 text-gray-500 hover:text-red-500"
                >
                  <LogOut size={16} />
                  退出
                </button>
              </>
            ) : (
              <Link
                to="/login"
                className="px-3 py-1.5 rounded-md bg-brand-600 text-white hover:bg-brand-700"
              >
                登录
              </Link>
            )}
          </div>
        </div>
      </header>

      <main className="flex-1">
        <Outlet />
      </main>
    </div>
  );
}

function NavItem({ to, children }: { to: string; children: React.ReactNode }) {
  return (
    <NavLink
      to={to}
      end
      className={({ isActive }) =>
        isActive
          ? "text-brand-600 font-medium"
          : "text-gray-600 hover:text-brand-600"
      }
    >
      {children}
    </NavLink>
  );
}
