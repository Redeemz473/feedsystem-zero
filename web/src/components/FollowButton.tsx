import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import clsx from "clsx";

import { follow, unfollow } from "@/api/social";
import { extractErrMsg } from "@/api/request";
import { useCurrentUser } from "@/hooks/useCurrentUser";

interface Props {
  targetUserID: number;
  following: boolean;
  onChange?: (following: boolean) => void;
  size?: "sm" | "md";
  className?: string;
}

// 关注/取关按钮：受控 following 状态，成功后通过 onChange 回调告诉父组件更新
export default function FollowButton({
  targetUserID,
  following,
  onChange,
  size = "md",
  className,
}: Props) {
  const { data: me } = useCurrentUser();
  const qc = useQueryClient();

  const mutation = useMutation({
    mutationFn: async () => {
      if (following) return unfollow(targetUserID);
      return follow(targetUserID);
    },
    onSuccess: (data) => {
      const nowFollowing =
        "followed" in data ? Boolean(data.followed) : !("unfollowed" in data && data.unfollowed);
      onChange?.(nowFollowing);
      qc.invalidateQueries({ queryKey: ["following-status", targetUserID] });
      qc.invalidateQueries({ queryKey: ["batch-following"] });
      qc.invalidateQueries({ queryKey: ["me"] });
      toast.success(nowFollowing ? "已关注" : "已取消关注");
    },
    onError: (err) => toast.error(extractErrMsg(err, "操作失败")),
  });

  // 不能关注自己
  if (me?.user_id === targetUserID) return null;

  const base =
    size === "sm"
      ? "px-3 py-1 text-xs rounded"
      : "px-4 py-1.5 text-sm rounded-md";

  return (
    <button
      type="button"
      disabled={mutation.isPending}
      onClick={(e) => {
        e.stopPropagation();
        e.preventDefault();
        if (!me) {
          toast.info("请先登录");
          return;
        }
        mutation.mutate();
      }}
      className={clsx(
        base,
        "transition font-medium disabled:opacity-60",
        following
          ? "bg-gray-100 text-gray-600 hover:bg-gray-200"
          : "bg-brand-600 text-white hover:bg-brand-700",
        className
      )}
    >
      {mutation.isPending ? "…" : following ? "已关注" : "关注"}
    </button>
  );
}
