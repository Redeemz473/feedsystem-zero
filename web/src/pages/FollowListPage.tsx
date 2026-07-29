import { useMemo, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";
import { useInfiniteQuery } from "@tanstack/react-query";
import clsx from "clsx";

import { listFollowers, listFollowings } from "@/api/social";
import { useCurrentUser } from "@/hooks/useCurrentUser";
import UserAvatar from "@/components/UserAvatar";
import FollowButton from "@/components/FollowButton";
import InfiniteLoader from "@/components/InfiniteLoader";
import type {
  FollowRelationInfo,
  ListFollowersResp,
  ListFollowingsResp,
} from "@/types/api";

type Mode = "followers" | "followings";

// 使用同一个组件呈现"TA 的粉丝" / "TA 关注的"，通过 URL 后缀区分
export default function FollowListPage() {
  const { id } = useParams<{ id: string }>();
  const userID = Number(id);
  const location = useLocation();
  const mode: Mode = location.pathname.endsWith("/followers")
    ? "followers"
    : "followings";
  const { data: me } = useCurrentUser();

  const q = useInfiniteQuery<ListFollowersResp | ListFollowingsResp>({
    queryKey: ["follow-list", mode, userID],
    queryFn: ({ pageParam }) => {
      const params = pageParam as {
        cursor_updated_at?: number;
        cursor_follow_id?: number;
        page_size?: number;
      };
      return mode === "followers"
        ? listFollowers(userID, params)
        : listFollowings(userID, params);
    },
    enabled: Number.isFinite(userID) && userID > 0,
    initialPageParam: { page_size: 20 } as {
      cursor_updated_at?: number;
      cursor_follow_id?: number;
      page_size?: number;
    },
    getNextPageParam: (last) =>
      last.has_more
        ? {
            cursor_updated_at: last.next_cursor_updated_at,
            cursor_follow_id: last.next_cursor_follow_id,
            page_size: 20,
          }
        : undefined,
  });

  // 展平所有页 -> 二选一读取字段
  const relations: FollowRelationInfo[] = useMemo(() => {
    const pages = q.data?.pages ?? [];
    return pages.flatMap((p) =>
      mode === "followers"
        ? (p as ListFollowersResp).followers
        : (p as ListFollowingsResp).followings
    );
  }, [q.data, mode]);

  // 本地覆盖：关注按钮点击后的即时更新
  const [overrides, setOverrides] = useState<Record<number, boolean>>({});

  return (
    <div className="max-w-2xl mx-auto px-4 py-6">
      <h1 className="text-lg font-semibold mb-4">
        {mode === "followers" ? "粉丝列表" : "关注列表"}
      </h1>

      <div className="mb-4 flex items-center gap-4 border-b border-gray-200 text-sm">
        <TabLink to={`/users/${userID}/followings`} active={mode === "followings"}>
          TA 关注的
        </TabLink>
        <TabLink to={`/users/${userID}/followers`} active={mode === "followers"}>
          粉丝
        </TabLink>
      </div>

      {q.isLoading ? (
        <p className="py-10 text-center text-gray-500 text-sm">加载中…</p>
      ) : relations.length === 0 ? (
        <p className="py-10 text-center text-gray-500 text-sm">还没有相关用户</p>
      ) : (
        <ul className="space-y-2">
          {relations.map((r) => {
            const uid = r.user.user_id;
            const following =
              overrides[uid] ?? Boolean(r.viewer_is_following);
            const isMe = me?.user_id === uid;
            return (
              <li
                key={r.relation_id}
                className="p-3 bg-white border border-gray-200 rounded flex items-center gap-3"
              >
                <UserAvatar
                  userID={uid}
                  username={r.user.username}
                  avatarUrl={r.user.avatar_url}
                  size={40}
                />
                <div className="flex-1 min-w-0">
                  <Link
                    to={`/users/${uid}`}
                    className="text-sm font-medium text-gray-800 hover:text-brand-600 truncate"
                  >
                    {r.user.username}
                  </Link>
                  {r.user.bio ? (
                    <p className="text-xs text-gray-500 truncate">{r.user.bio}</p>
                  ) : null}
                </div>
                {me && !isMe ? (
                  <FollowButton
                    targetUserID={uid}
                    following={following}
                    size="sm"
                    onChange={(v) =>
                      setOverrides((prev) => ({ ...prev, [uid]: v }))
                    }
                  />
                ) : null}
              </li>
            );
          })}
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

function TabLink({
  to,
  active,
  children,
}: {
  to: string;
  active: boolean;
  children: React.ReactNode;
}) {
  return (
    <Link
      to={to}
      replace
      className={clsx(
        "px-1 py-2 border-b-2 -mb-px transition",
        active
          ? "border-brand-600 text-brand-600 font-medium"
          : "border-transparent text-gray-500 hover:text-gray-800"
      )}
    >
      {children}
    </Link>
  );
}
