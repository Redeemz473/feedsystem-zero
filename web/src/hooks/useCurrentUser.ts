import { useQuery } from "@tanstack/react-query";

import { getProfile } from "@/api/account";
import { useAuthStore } from "@/stores/auth";

// 通过 accessToken 是否存在 + /account/profile 是否成功 双重判定登录态
export function useCurrentUser() {
  const accessToken = useAuthStore((s) => s.accessToken);

  return useQuery({
    queryKey: ["me", accessToken],
    queryFn: getProfile,
    enabled: Boolean(accessToken),
    staleTime: 60_000,
  });
}
