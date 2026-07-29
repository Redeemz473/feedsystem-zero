import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";

import { listUserVideos } from "@/api/video";
import { isFollowing as apiIsFollowing } from "@/api/social";
import { getProfile } from "@/api/account";
import { useCurrentUser } from "@/hooks/useCurrentUser";
import VideoCard from "@/components/VideoCard";
import UserAvatar from "@/components/UserAvatar";
import FollowButton from "@/components/FollowButton";
import InfiniteLoader from "@/components/InfiniteLoader";
import type { ListUserVideosResp } from "@/types/api";

// 用户主页：他人视图 or 自己视图
// 后端目前"公开资料"接口暂未单独暴露，这里通过视频列表里的 author_username 兜底；
// 如果 :id 与当前登录用户相同，直接复用 /account/profile 拿到完整资料。
export default function UserPage() {
  const { id } = useParams<{ id: string }>();
  const userID = Number(id);
  const { data: me } = useCurrentUser();
  const navigate = useNavigate();
  const isSelf = me?.user_id === userID;

  // 视频列表
  const videosQuery = useInfiniteQuery<ListUserVideosResp>({
    queryKey: ["user-videos", userID],
    queryFn: ({ pageParam }) =>
      listUserVideos(userID, pageParam as {
        cursor_created_at?: number;
        cursor_video_id?: number;
        page_size?: number;
      }),
    enabled: Number.isFinite(userID) && userID > 0,
    initialPageParam: { page_size: 12 } as {
      cursor_created_at?: number;
      cursor_video_id?: number;
      page_size?: number;
    },
    getNextPageParam: (last) =>
      last.has_more
        ? {
            cursor_created_at: last.next_cursor_created_at,
            cursor_video_id: last.next_cursor_video_id,
            page_size: 12,
          }
        : undefined,
  });

  // 自己视图直接用 /account/profile；他人视图从首条视频取 author_username 兜底
  const selfQuery = useQuery({
    queryKey: ["me-profile-detail"],
    queryFn: getProfile,
    enabled: isSelf,
  });

  // 关注状态
  const followingQuery = useQuery({
    queryKey: ["following-status", userID],
    queryFn: () => apiIsFollowing(userID),
    enabled: Boolean(me && !isSelf && userID > 0),
  });
  const [followingLocal, setFollowingLocal] = useState<boolean | null>(null);
  const following =
    followingLocal ?? Boolean(followingQuery.data?.following);

  const videos = videosQuery.data?.pages.flatMap((p) => p.videos) ?? [];
  const inferredUsername = videos[0]?.author_username || `用户 ${userID}`;
  const displayUsername = isSelf ? me?.username || inferredUsername : inferredUsername;
  const displayAvatar = isSelf ? me?.avatar_url : undefined;
  const displayBio = isSelf ? me?.bio : "";
  const followersCount = isSelf ? selfQuery.data?.followers_count ?? 0 : undefined;
  const followingsCount = isSelf ? selfQuery.data?.followings_count ?? 0 : undefined;

  return (
    <div className="max-w-6xl mx-auto px-4 py-6">
      {/* 用户信息卡片 */}
      <div className="bg-white p-5 rounded-lg border border-gray-200 flex items-start gap-4">
        <UserAvatar
          userID={userID}
          username={displayUsername}
          avatarUrl={displayAvatar}
          size={64}
          clickable={false}
        />
        <div className="flex-1 min-w-0">
          <h1 className="text-lg font-semibold text-gray-900 truncate">
            {displayUsername}
          </h1>
          {displayBio ? (
            <p className="mt-1 text-sm text-gray-600">{displayBio}</p>
          ) : null}
          {isSelf ? (
            <div className="mt-2 flex items-center gap-4 text-sm text-gray-500">
              <Link
                to={`/users/${userID}/followings`}
                className="hover:text-brand-600"
              >
                关注 <b className="text-gray-800">{followingsCount ?? 0}</b>
              </Link>
              <Link
                to={`/users/${userID}/followers`}
                className="hover:text-brand-600"
              >
                粉丝 <b className="text-gray-800">{followersCount ?? 0}</b>
              </Link>
            </div>
          ) : (
            <div className="mt-2 flex items-center gap-4 text-sm text-gray-500">
              <Link
                to={`/users/${userID}/followings`}
                className="hover:text-brand-600"
              >
                TA 关注的
              </Link>
              <Link
                to={`/users/${userID}/followers`}
                className="hover:text-brand-600"
              >
                粉丝
              </Link>
            </div>
          )}
        </div>
        {isSelf ? (
          <button
            onClick={() => navigate("/profile")}
            className="px-4 py-1.5 rounded-md border border-gray-300 text-sm hover:bg-gray-50"
          >
            编辑资料
          </button>
        ) : me ? (
          <FollowButton
            targetUserID={userID}
            following={following}
            onChange={(v) => setFollowingLocal(v)}
          />
        ) : null}
      </div>

      {/* 视频列表 */}
      <h2 className="mt-6 mb-3 text-sm font-medium text-gray-700">TA 发布的视频</h2>
      {videosQuery.isLoading ? (
        <p className="py-10 text-center text-gray-500 text-sm">加载中…</p>
      ) : videos.length === 0 ? (
        <p className="py-10 text-center text-gray-500 text-sm">还没有发布过视频</p>
      ) : (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {videos.map((v) => (
              <VideoCard key={v.video_id} video={v} />
            ))}
          </div>
          <InfiniteLoader
            hasMore={Boolean(videosQuery.hasNextPage)}
            loading={videosQuery.isFetchingNextPage}
            onLoadMore={() => videosQuery.fetchNextPage()}
          />
        </>
      )}
    </div>
  );
}
