import { Link } from "react-router-dom";
import { useInfiniteQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { CheckCheck, Heart, MessageCircle, UserPlus } from "lucide-react";
import clsx from "clsx";

import {
  listNotifications,
  markAllNotificationsRead,
  markNotificationRead,
} from "@/api/notification";
import { extractErrMsg } from "@/api/request";
import UserAvatar from "@/components/UserAvatar";
import InfiniteLoader from "@/components/InfiniteLoader";
import { timeAgo } from "@/utils/time";
import type { ListNotificationsResp, NotificationItem } from "@/types/api";

export default function NotificationsPage() {
  const qc = useQueryClient();

  const q = useInfiniteQuery<ListNotificationsResp>({
    queryKey: ["notifications"],
    queryFn: ({ pageParam }) =>
      listNotifications(pageParam as {
        cursor_occurred_at?: number;
        cursor_notification_id?: number;
        page_size?: number;
      }),
    initialPageParam: { page_size: 20 } as {
      cursor_occurred_at?: number;
      cursor_notification_id?: number;
      page_size?: number;
    },
    getNextPageParam: (last) =>
      last.has_more
        ? {
            cursor_occurred_at: last.next_cursor_occurred_at,
            cursor_notification_id: last.next_cursor_notification_id,
            page_size: 20,
          }
        : undefined,
  });

  const markOne = useMutation({
    mutationFn: (id: number) => markNotificationRead(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["notifications"] });
      qc.invalidateQueries({ queryKey: ["notification", "unread-count"] });
    },
    onError: (err) => toast.error(extractErrMsg(err, "标记失败")),
  });

  const markAll = useMutation({
    mutationFn: () => markAllNotificationsRead(),
    onSuccess: (data) => {
      toast.success(`已标记 ${data.changed_count} 条为已读`);
      qc.invalidateQueries({ queryKey: ["notifications"] });
      qc.invalidateQueries({ queryKey: ["notification", "unread-count"] });
    },
    onError: (err) => toast.error(extractErrMsg(err, "操作失败")),
  });

  const items: NotificationItem[] = q.data?.pages.flatMap((p) => p.notifications) ?? [];

  return (
    <div className="max-w-2xl mx-auto px-4 py-6">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-lg font-semibold">消息通知</h1>
        <button
          onClick={() => markAll.mutate()}
          disabled={markAll.isPending}
          className="inline-flex items-center gap-1 text-sm text-gray-600 hover:text-brand-600 disabled:opacity-50"
        >
          <CheckCheck size={16} />
          全部已读
        </button>
      </div>

      {q.isLoading ? (
        <p className="py-10 text-center text-gray-500 text-sm">加载中…</p>
      ) : items.length === 0 ? (
        <p className="py-10 text-center text-gray-500 text-sm">暂无消息</p>
      ) : (
        <ul className="space-y-2">
          {items.map((n) => (
            <NotificationRow
              key={n.notification_id}
              item={n}
              onMarkRead={() => markOne.mutate(n.notification_id)}
            />
          ))}
        </ul>
      )}

      <InfiniteLoader
        hasMore={Boolean(q.hasNextPage)}
        loading={q.isFetchingNextPage}
        onLoadMore={() => q.fetchNextPage()}
      />
    </div>
  );
}

function NotificationRow({
  item,
  onMarkRead,
}: {
  item: NotificationItem;
  onMarkRead: () => void;
}) {
  const unread = item.status === "unread";
  const icon =
    item.type === "like" ? (
      <Heart size={14} className="text-red-500" />
    ) : item.type === "comment" ? (
      <MessageCircle size={14} className="text-blue-500" />
    ) : (
      <UserPlus size={14} className="text-brand-600" />
    );
  const actionText =
    item.type === "like"
      ? "赞了你的视频"
      : item.type === "comment"
      ? "评论了你的视频"
      : "关注了你";

  const target =
    item.type === "follow"
      ? `/users/${item.actor.user_id}`
      : item.video?.video_id
      ? `/videos/${item.video.video_id}`
      : `/users/${item.actor.user_id}`;

  return (
    <li
      className={clsx(
        "p-3 bg-white border rounded-lg flex items-start gap-3",
        unread ? "border-brand-200 bg-brand-50/40" : "border-gray-200"
      )}
    >
      <UserAvatar
        userID={item.actor.user_id}
        username={item.actor.username}
        avatarUrl={item.actor.avatar_url}
        size={40}
      />
      <div className="flex-1 min-w-0">
        <div className="text-sm text-gray-800">
          <Link
            to={`/users/${item.actor.user_id}`}
            className="font-medium hover:text-brand-600"
          >
            {item.actor.username}
          </Link>
          <span className="inline-flex items-center gap-1 mx-1">{icon}</span>
          {actionText}
        </div>

        {item.video ? (
          <Link
            to={`/videos/${item.video.video_id}`}
            className="mt-1 inline-flex items-center gap-2 text-xs text-gray-500 hover:text-brand-600"
          >
            {item.video.cover_url ? (
              <img
                src={item.video.cover_url}
                alt=""
                className="w-10 h-6 object-cover rounded"
              />
            ) : null}
            <span className="truncate max-w-[240px]">{item.video.title}</span>
          </Link>
        ) : null}

        <div className="mt-1 flex items-center gap-3 text-xs text-gray-400">
          <span>{timeAgo(item.occurred_at)}</span>
          {target ? (
            <Link to={target} className="hover:text-brand-600">
              查看
            </Link>
          ) : null}
          {unread ? (
            <button
              onClick={onMarkRead}
              className="ml-auto hover:text-brand-600"
            >
              标记已读
            </button>
          ) : null}
        </div>
      </div>
    </li>
  );
}
