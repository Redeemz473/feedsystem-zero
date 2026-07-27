import { useState } from "react";
import { useForm } from "react-hook-form";
import { Link, useLocation, useNavigate } from "react-router-dom";
import { toast } from "sonner";

import { login } from "@/api/account";
import { extractErrMsg } from "@/api/request";
import { useAuthStore } from "@/stores/auth";

interface FormValues {
  username: string;
  password: string;
}

export default function LoginPage() {
  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<FormValues>();
  const [submitting, setSubmitting] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const from = (location.state as { from?: string } | null)?.from ?? "/";

  async function onSubmit(values: FormValues) {
    setSubmitting(true);
    try {
      const resp = await login(values);
      useAuthStore.getState().setTokens(resp.access_token, resp.refresh_token);
      toast.success("登录成功");
      navigate(from, { replace: true });
    } catch (err) {
      toast.error(extractErrMsg(err, "登录失败"));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="min-h-full flex items-center justify-center bg-gray-50 py-12">
      <div className="w-full max-w-sm bg-white shadow rounded-lg p-8">
        <h1 className="text-xl font-semibold text-center mb-6">登录</h1>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          <Field label="用户名" error={errors.username?.message}>
            <input
              type="text"
              autoComplete="username"
              className="input"
              {...register("username", { required: "请输入用户名" })}
            />
          </Field>

          <Field label="密码" error={errors.password?.message}>
            <input
              type="password"
              autoComplete="current-password"
              className="input"
              {...register("password", { required: "请输入密码" })}
            />
          </Field>

          <button
            type="submit"
            disabled={submitting}
            className="w-full py-2 rounded-md bg-brand-600 hover:bg-brand-700 text-white font-medium disabled:opacity-60"
          >
            {submitting ? "登录中…" : "登录"}
          </button>
        </form>

        <p className="mt-6 text-sm text-gray-500 text-center">
          还没有账号？{" "}
          <Link to="/register" className="text-brand-600 hover:underline">
            注册
          </Link>
        </p>
      </div>
    </div>
  );
}

function Field({
  label,
  error,
  children,
}: {
  label: string;
  error?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="block">
      <span className="text-sm text-gray-700">{label}</span>
      <div className="mt-1">{children}</div>
      {error ? <p className="mt-1 text-xs text-red-500">{error}</p> : null}
    </label>
  );
}
