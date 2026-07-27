import { create } from "zustand";
import { persist } from "zustand/middleware";

// 全局登录态：只存 token；用户资料由 react-query 单独缓存
interface AuthState {
  accessToken: string | null;
  refreshToken: string | null;
  setTokens: (access: string, refresh: string) => void;
  clear: () => void;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      accessToken: null,
      refreshToken: null,
      setTokens: (access, refresh) =>
        set({ accessToken: access, refreshToken: refresh }),
      clear: () => set({ accessToken: null, refreshToken: null }),
    }),
    { name: "fsz-auth" }
  )
);

// 允许在非 React 上下文（比如 axios 拦截器里）访问 store
export const authStore = {
  get: () => useAuthStore.getState(),
  set: (access: string | null, refresh: string | null) => {
    if (access && refresh) {
      useAuthStore.getState().setTokens(access, refresh);
    } else {
      useAuthStore.getState().clear();
    }
  },
};
