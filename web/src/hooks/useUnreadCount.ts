import { useQuery } from "@tanstack/react-query";

import { getUnreadCount } from "@/api/notification";
import { useAuthStore } from "@/stores/auth";

// 通知未读数：登录后每 30 秒轮询一次，用于顶栏红点提示。
// 后端使用版本号+缓存，成本极低。
export function useUnreadCount() {
  const accessToken = useAuthStore((s) => s.accessToken);

  return useQuery({
    queryKey: ["notification", "unread-count"],
    queryFn: getUnreadCount,
    enabled: Boolean(accessToken),
    staleTime: 15_000,
    refetchInterval: 30_000,
    refetchOnWindowFocus: true,
  });
}
