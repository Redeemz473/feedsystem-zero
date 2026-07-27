import { useState } from "react";
import { useForm } from "react-hook-form";
import { Link, useNavigate } from "react-router-dom";
import { toast } from "sonner";

import { register as registerApi, sendVerification } from "@/api/account";
import { extractErrMsg } from "@/api/request";

interface FormValues {
  username: string;
  password: string;
  email: string;
  verification: string;
}

export default function RegisterPage() {
  const {
    register,
    handleSubmit,
    getValues,
    formState: { errors },
  } = useForm<FormValues>();
  const [submitting, setSubmitting] = useState(false);
  const [sending, setSending] = useState(false);
  const [cooldown, setCooldown] = useState(0);
  const navigate = useNavigate();

  async function handleSendCode() {
    const email = getValues("email");
    if (!email) {
      toast.error("请先输入邮箱");
      return;
    }
    setSending(true);
    try {
      await sendVerification({ email });
      toast.success("验证码已发送，请查收邮箱");
      let left = 60;
      setCooldown(left);
      const timer = setInterval(() => {
        left -= 1;
        setCooldown(left);
        if (left <= 0) clearInterval(timer);
      }, 1000);
    } catch (err) {
      toast.error(extractErrMsg(err, "验证码发送失败"));
    } finally {
      setSending(false);
    }
  }

  async function onSubmit(values: FormValues) {
    setSubmitting(true);
    try {
      await registerApi(values);
      toast.success("注册成功，请登录");
      navigate("/login", { replace: true });
    } catch (err) {
      toast.error(extractErrMsg(err, "注册失败"));
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="min-h-full flex items-center justify-center bg-gray-50 py-12">
      <div className="w-full max-w-sm bg-white shadow rounded-lg p-8">
        <h1 className="text-xl font-semibold text-center mb-6">注册</h1>

        <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">
          <Field label="用户名" error={errors.username?.message}>
            <input
              type="text"
              autoComplete="username"
              className="input"
              {...register("username", {
                required: "请输入用户名",
                minLength: { value: 3, message: "至少 3 个字符" },
              })}
            />
          </Field>

          <Field label="密码" error={errors.password?.message}>
            <input
              type="password"
              autoComplete="new-password"
              className="input"
              {...register("password", {
                required: "请输入密码",
                minLength: { value: 6, message: "至少 6 位" },
              })}
            />
          </Field>

          <Field label="邮箱" error={errors.email?.message}>
            <input
              type="email"
              autoComplete="email"
              className="input"
              {...register("email", {
                required: "请输入邮箱",
                pattern: {
                  value: /^[^\s@]+@[^\s@]+\.[^\s@]+$/,
                  message: "邮箱格式不正确",
                },
              })}
            />
          </Field>

          <Field label="邮箱验证码" error={errors.verification?.message}>
            <div className="flex gap-2">
              <input
                type="text"
                className="input flex-1"
                {...register("verification", { required: "请输入验证码" })}
              />
              <button
                type="button"
                disabled={sending || cooldown > 0}
                onClick={handleSendCode}
                className="whitespace-nowrap px-3 py-2 rounded-md border border-gray-300 text-sm text-gray-700 hover:bg-gray-50 disabled:opacity-60"
              >
                {cooldown > 0 ? `${cooldown}s` : sending ? "发送中" : "发送验证码"}
              </button>
            </div>
          </Field>

          <button
            type="submit"
            disabled={submitting}
            className="w-full py-2 rounded-md bg-brand-600 hover:bg-brand-700 text-white font-medium disabled:opacity-60"
          >
            {submitting ? "注册中…" : "注册"}
          </button>
        </form>

        <p className="mt-6 text-sm text-gray-500 text-center">
          已有账号？{" "}
          <Link to="/login" className="text-brand-600 hover:underline">
            去登录
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
