import { Navigate, useLocation } from "react-router-dom";

import { useAuthStore } from "@/stores/auth";

/**
 * 需要登录才能访问的页面守卫。
 * 未登录时把当前 pathname 记到 state.from，登录后回跳。
 */
export function RequireAuth({ children }: { children: React.ReactNode }) {
  const accessToken = useAuthStore((s) => s.accessToken);
  const location = useLocation();

  if (!accessToken) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }
  return <>{children}</>;
}
