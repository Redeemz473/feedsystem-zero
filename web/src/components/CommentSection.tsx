import { useState } from "react";
import { useInfiniteQuery, useMutation, useQueryClient, type InfiniteData } from "@tanstack/react-query";
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

// Optimistic marker for pending comments: negative comment_id encodes a
// client-generated placeholder that hasn't reached the server yet.
const OPTIMISTIC_ID_PREFIX = -1_000_000; // any negative < -1_000_000 is optimistic

function isOptimistic(c: CommentInfo): boolean {
  return c.comment_id <= OPTIMISTIC_ID_PREFIX;
}

// 视频评论区：游标分页 + 发表 + 删除（发表和删除均走乐观 UI）
export default function CommentSection({ videoID, onCommentsCountChange }: Props) {
  const { data: me } = useCurrentUser();
  const qc = useQueryClient();
  const [content, setContent] = useState("");

  const commentsKey = ["comments", videoID] as const;

  const listQuery = useInfiniteQuery<ListCommentsResp>({
    queryKey: commentsKey,
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

  // Helper: mutate cached comment pages in place. We only ever touch the
  // first page because new comments always land at the head of the list.
  function patchFirstPage(fn: (comments: CommentInfo[]) => CommentInfo[]) {
    qc.setQueryData<InfiniteData<ListCommentsResp>>(commentsKey, (old) => {
      if (!old || old.pages.length === 0) return old;
      const [first, ...rest] = old.pages;
      return {
        ...old,
        pages: [{ ...first, comments: fn(first.comments) }, ...rest],
      };
    });
  }

  const publishMutation = useMutation({
    mutationFn: (payload: { content: string; requestID: string; optimisticID: number }) =>
      publishComment(videoID, {
        content: payload.content,
        request_id: payload.requestID,
      }),
    onMutate: (payload) => {
      // Build a placeholder comment that shows up instantly in the list.
      if (!me) return;
      const optimistic: CommentInfo = {
        comment_id: payload.optimisticID,
        video_id: videoID,
        user_id: me.user_id,
        username: me.username,
        content: payload.content,
        created_at: Math.floor(Date.now() / 1000),
        updated_at: Math.floor(Date.now() / 1000),
        can_delete: true,
      };
      patchFirstPage((comments) => [optimistic, ...comments]);
    },
    onSuccess: (data, payload) => {
      // Replace the optimistic placeholder with the authoritative comment.
      patchFirstPage((comments) =>
        comments.map((c) => (c.comment_id === payload.optimisticID ? data.comment : c))
      );
      onCommentsCountChange?.(data.comments_count);
      toast.success("已发表");
    },
    onError: (err, payload) => {
      // Roll back: drop the optimistic placeholder from the cache.
      patchFirstPage((comments) => comments.filter((c) => c.comment_id !== payload.optimisticID));
      toast.error(extractErrMsg(err, "发表失败"));
    },
  });

  function handlePublish() {
    const trimmed = content.trim();
    if (!trimmed || publishMutation.isPending) return;
    const optimisticID = OPTIMISTIC_ID_PREFIX - Math.floor(Math.random() * 1_000_000);
    const requestID = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    // Clear the textarea immediately for that "sent" feel.
    setContent("");
    publishMutation.mutate({ content: trimmed, requestID, optimisticID });
  }

  const deleteMutation = useMutation({
    mutationFn: (commentID: number) => deleteComment(commentID),
    onMutate: (commentID) => {
      // Snapshot for rollback.
      const snapshot = qc.getQueryData<InfiniteData<ListCommentsResp>>(commentsKey);
      patchFirstPage((comments) => comments.filter((c) => c.comment_id !== commentID));
      return { snapshot };
    },
    onSuccess: (data) => {
      onCommentsCountChange?.(data.comments_count);
      toast.success("已删除");
    },
    onError: (err, _commentID, context) => {
      // Restore the previous cache exactly, in case the deleted comment was
      // buried on a non-first page.
      if (context?.snapshot) {
        qc.setQueryData<InfiniteData<ListCommentsResp>>(commentsKey, context.snapshot);
      }
      toast.error(extractErrMsg(err, "删除失败"));
    },
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
                onClick={handlePublish}
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
          {comments.map((c) => {
            const pending = isOptimistic(c);
            return (
              <li
                key={c.comment_id}
                className={`flex gap-2 items-start ${pending ? "opacity-60" : ""}`}
              >
                <UserAvatar userID={c.user_id} username={c.username} size={32} />
                <div className="flex-1 min-w-0">
                  <div className="text-sm">
                    <span className="font-medium text-gray-800">{c.username}</span>
                    <span className="ml-2 text-xs text-gray-400">
                      {pending ? "发送中…" : timeAgo(c.created_at)}
                    </span>
                  </div>
                  <div className="text-sm text-gray-700 mt-0.5 break-words whitespace-pre-wrap">
                    {c.content}
                  </div>
                </div>
                {c.can_delete && !pending ? (
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
            );
          })}
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
