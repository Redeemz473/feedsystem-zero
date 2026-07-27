import axios, {
  AxiosError,
  type AxiosRequestConfig,
  type InternalAxiosRequestConfig,
} from "axios";

import { authStore } from "@/stores/auth";
import type { RefreshTokenResp } from "@/types/api";

// 后端 gateway 通过 vite proxy 转发；生产环境请通过 nginx 反向代理同源部署
const request = axios.create({
  baseURL: "/",
  timeout: 30_000,
});

/* ------------------------------- 请求拦截器 ------------------------------- */
request.interceptors.request.use((config: InternalAxiosRequestConfig) => {
  const { accessToken } = authStore.get();
  if (accessToken) {
    config.headers.Authorization = `Bearer ${accessToken}`;
  }
  return config;
});

/* ---------------------- 响应拦截器：401 刷新 token ---------------------- */

// 并发 401 时，只发一次刷新请求，其它请求排队等待
let refreshPromise: Promise<string | null> | null = null;

async function doRefresh(): Promise<string | null> {
  const { refreshToken } = authStore.get();
  if (!refreshToken) return null;

  try {
    const resp = await axios.post<RefreshTokenResp>(
      "/account/refresh_token",
      { refresh_token: refreshToken }
    );
    authStore.set(resp.data.access_token, resp.data.refresh_token);
    return resp.data.access_token;
  } catch {
    authStore.set(null, null);
    return null;
  }
}

request.interceptors.response.use(
  (resp) => resp,
  async (error: AxiosError) => {
    const original = error.config as
      | (AxiosRequestConfig & { _retry?: boolean })
      | undefined;

    // 未鉴权 → 尝试刷新一次；刷新接口本身 401 直接抛
    const isAuthCall =
      original?.url?.includes("/account/login") ||
      original?.url?.includes("/account/refresh_token") ||
      original?.url?.includes("/account/register");

    if (
      error.response?.status === 401 &&
      original &&
      !original._retry &&
      !isAuthCall
    ) {
      original._retry = true;

      if (!refreshPromise) {
        refreshPromise = doRefresh().finally(() => {
          refreshPromise = null;
        });
      }

      const newToken = await refreshPromise;
      if (newToken) {
        original.headers = original.headers ?? {};
        (original.headers as Record<string, string>).Authorization =
          `Bearer ${newToken}`;
        return request(original);
      }

      // 刷新失败：跳登录页（用 hash 让路由感知）
      if (typeof window !== "undefined") {
        window.location.href = "/login";
      }
    }

    return Promise.reject(error);
  }
);

/* --------------------------------- 工具 --------------------------------- */

// 从 axios error 中抽出人类可读的错误信息
export function extractErrMsg(err: unknown, fallback = "请求失败"): string {
  if (err instanceof AxiosError) {
    const data = err.response?.data as { msg?: string; message?: string } | undefined;
    return data?.msg || data?.message || err.message || fallback;
  }
  if (err instanceof Error) return err.message;
  return fallback;
}

export default request;
