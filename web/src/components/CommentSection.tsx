import { useState } from "react";
import { useInfiniteQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Trash2 } from "lucide-react";

import { deleteComment, listComments, publishComment } from "@/api/interaction";
import { extractErrMsg } from "@/api/request";
import { useCurrentUser } from "@/hooks/useCurrentUser";
import { timeAgo } from "@/utils/time";
import UserAvatar from "./UserAvatar";
import type { CommentInfo, ListCommentsResp } from "@/types/api";

interface Props {
  videoID: number;
  onCommentsCountChange?: (count: number) => void;
}

// 视频评论区：游标分页 + 发表 + 删除
export default function CommentSection({ videoID, onCommentsCountChange }: Props) {
  const { data: me } = useCurrentUser();
  const qc = useQueryClient();
  const [content, setContent] = useState("");

  const listQuery = useInfiniteQuery<ListCommentsResp>({
    queryKey: ["comments", videoID],
    queryFn: ({ pageParam }) =>
      listComments(videoID, pageParam as {
        cursor_created_at?: number;
        cursor_comment_id?: number;
        page_size?: number;
      }),
    initialPageParam: { page_size: 20 } as {
      cursor_created_at?: number;
      cursor_comment_id?: number;
      page_size?: number;
    },
    getNextPageParam: (last) =>
      last.has_more
        ? {
            cursor_created_at: last.next_cursor_created_at,
            cursor_comment_id: last.next_cursor_comment_id,
            page_size: 20,
          }
        : undefined,
  });

  const publishMutation = useMutation({
    mutationFn: () =>
      publishComment(videoID, {
        content: content.trim(),
        request_id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
      }),
    onSuccess: (data) => {
      toast.success("已发表");
      setContent("");
      onCommentsCountChange?.(data.comments_count);
      qc.invalidateQueries({ queryKey: ["comments", videoID] });
    },
    onError: (err) => toast.error(extractErrMsg(err, "发表失败")),
  });

  const deleteMutation = useMutation({
    mutationFn: (commentID: number) => deleteComment(commentID),
    onSuccess: (data) => {
      toast.success("已删除");
      onCommentsCountChange?.(data.comments_count);
      qc.invalidateQueries({ queryKey: ["comments", videoID] });
    },
    onError: (err) => toast.error(extractErrMsg(err, "删除失败")),
  });

  const comments: CommentInfo[] =
    listQuery.data?.pages.flatMap((p) => p.comments) ?? [];

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4">
      <h2 className="text-base font-semibold mb-3">评论</h2>

      {me ? (
        <div className="flex gap-2 items-start mb-4">
          <UserAvatar
            userID={me.user_id}
            username={me.username}
            avatarUrl={me.avatar_url}
            size={32}
            clickable={false}
          />
          <div className="flex-1">
            <textarea
              value={content}
              onChange={(e) => setContent(e.target.value)}
              className="input min-h-[60px]"
              placeholder="留下你的看法…"
              maxLength={500}
            />
            <div className="mt-2 flex justify-end">
              <button
                disabled={!content.trim() || publishMutation.isPending}
                onClick={() => publishMutation.mutate()}
                className="px-3 py-1.5 rounded-md bg-brand-600 text-white text-sm hover:bg-brand-700 disabled:opacity-50"
              >
                {publishMutation.isPending ? "发表中…" : "发表"}
              </button>
            </div>
          </div>
        </div>
      ) : (
        <p className="mb-4 text-sm text-gray-500">登录后即可评论</p>
      )}

      {listQuery.isLoading ? (
        <p className="text-sm text-gray-500">加载中…</p>
      ) : comments.length === 0 ? (
        <p className="text-sm text-gray-500">还没有评论，来抢沙发～</p>
      ) : (
        <ul className="space-y-4">
          {comments.map((c) => (
            <li key={c.comment_id} className="flex gap-2 items-start">
              <UserAvatar userID={c.user_id} username={c.username} size={32} />
              <div className="flex-1 min-w-0">
                <div className="text-sm">
                  <span className="font-medium text-gray-800">{c.username}</span>
                  <span className="ml-2 text-xs text-gray-400">
                    {timeAgo(c.created_at)}
                  </span>
                </div>
                <div className="text-sm text-gray-700 mt-0.5 break-words whitespace-pre-wrap">
                  {c.content}
                </div>
              </div>
              {c.can_delete ? (
                <button
                  onClick={() => {
                    if (confirm("确认删除该评论？")) deleteMutation.mutate(c.comment_id);
                  }}
                  className="text-gray-400 hover:text-red-500"
                  title="删除"
                >
                  <Trash2 size={16} />
                </button>
              ) : null}
            </li>
          ))}
        </ul>
      )}

      {listQuery.hasNextPage ? (
        <div className="mt-4 text-center">
          <button
            onClick={() => listQuery.fetchNextPage()}
            disabled={listQuery.isFetchingNextPage}
            className="text-sm text-brand-600 hover:underline"
          >
            {listQuery.isFetchingNextPage ? "加载中…" : "加载更多评论"}
          </button>
        </div>
      ) : null}
    </div>
  );
}
