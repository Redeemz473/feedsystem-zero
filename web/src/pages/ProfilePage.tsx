import { useEffect } from "react";
import { useForm } from "react-hook-form";
import { toast } from "sonner";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { updateProfile } from "@/api/account";
import { extractErrMsg } from "@/api/request";
import { useCurrentUser } from "@/hooks/useCurrentUser";
import type { UpdateProfileReq } from "@/types/api";

interface FormValues {
  username: string;
  avatar_url: string;
  bio: string;
}

export default function ProfilePage() {
  const { data: me, isLoading } = useCurrentUser();
  const qc = useQueryClient();

  const { register, handleSubmit, reset, formState: { errors } } =
    useForm<FormValues>({
      defaultValues: { username: "", avatar_url: "", bio: "" },
    });

  useEffect(() => {
    if (me) {
      reset({
        username: me.username,
        avatar_url: me.avatar_url,
        bio: me.bio,
      });
    }
  }, [me, reset]);

  const mutation = useMutation({
    mutationFn: (body: UpdateProfileReq) => updateProfile(body),
    onSuccess: () => {
      toast.success("已保存");
      qc.invalidateQueries({ queryKey: ["me"] });
    },
    onError: (err) => toast.error(extractErrMsg(err, "保存失败")),
  });

  if (isLoading || !me) {
    return <div className="p-8 text-gray-500">加载中…</div>;
  }

  function onSubmit(values: FormValues) {
    mutation.mutate({
      username: values.username,
      avatar_url: values.avatar_url,
      bio: values.bio,
    });
  }

  return (
    <div className="max-w-xl mx-auto px-4 py-10">
      <h1 className="text-xl font-semibold mb-6">个人资料</h1>

      <div className="mb-6 p-4 bg-white rounded-lg border border-gray-200">
        <div className="text-sm text-gray-500">用户 ID</div>
        <div className="text-gray-800">{me.user_id}</div>
        <div className="mt-2 text-sm text-gray-500">邮箱</div>
        <div className="text-gray-800">{me.email}</div>
      </div>

      <form
        onSubmit={handleSubmit(onSubmit)}
        className="space-y-4 bg-white p-6 rounded-lg border border-gray-200"
      >
        <label className="block">
          <span className="text-sm text-gray-700">用户名</span>
          <input
            className="input mt-1"
            {...register("username", { required: "用户名不能为空" })}
          />
          {errors.username ? (
            <p className="mt-1 text-xs text-red-500">{errors.username.message}</p>
          ) : null}
        </label>

        <label className="block">
          <span className="text-sm text-gray-700">头像 URL</span>
          <input className="input mt-1" {...register("avatar_url")} />
        </label>

        <label className="block">
          <span className="text-sm text-gray-700">简介</span>
          <textarea
            className="input mt-1 min-h-[80px]"
            {...register("bio")}
          />
        </label>

        <button
          type="submit"
          disabled={mutation.isPending}
          className="px-4 py-2 rounded-md bg-brand-600 hover:bg-brand-700 text-white text-sm disabled:opacity-60"
        >
          {mutation.isPending ? "保存中…" : "保存"}
        </button>
      </form>
    </div>
  );
}
