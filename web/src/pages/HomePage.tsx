import { useState } from "react";
import { NavLink, useNavigate } from "react-router-dom";
import { useInfiniteQuery } from "@tanstack/react-query";
import clsx from "clsx";

import { getFollowingFeed, getHotFeed, getRecommendFeed } from "@/api/feed";
import { useAuthStore } from "@/stores/auth";
import VideoCard from "@/components/VideoCard";
import InfiniteLoader from "@/components/InfiniteLoader";
import type {
  GetFollowingFeedResp,
  GetHotFeedResp,
  GetRecommendFeedResp,
  HotFeedVideo,
  VideoInfo,
} from "@/types/api";

type Tab = "recommend" | "following" | "hot";

export default function HomePage() {
  const accessToken = useAuthStore((s) => s.accessToken);
  const navigate = useNavigate();
  const [tab, setTab] = useState<Tab>("recommend");

  return (
    <div className="max-w-6xl mx-auto px-4 py-6">
      {/* Tabs */}
      <div className="mb-4 flex items-center gap-4 border-b border-gray-200">
        <TabButton current={tab} value="recommend" onClick={setTab}>
          推荐
        </TabButton>
        <TabButton
          current={tab}
          value="following"
          onClick={(v) => {
            if (!accessToken) {
              navigate("/login");
              return;
            }
            setTab(v);
          }}
        >
          关注
        </TabButton>
        <TabButton current={tab} value="hot" onClick={setTab}>
          热榜
        </TabButton>

        <NavLink
          to="/upload"
          className="ml-auto px-3 py-1.5 rounded-md bg-brand-600 text-white text-sm hover:bg-brand-700"
        >
          发布视频
        </NavLink>
      </div>

      {tab === "recommend" ? <RecommendPane /> : null}
      {tab === "following" ? <FollowingPane /> : null}
      {tab === "hot" ? <HotPane /> : null}
    </div>
  );
}

function TabButton({
  current,
  value,
  onClick,
  children,
}: {
  current: Tab;
  value: Tab;
  onClick: (v: Tab) => void;
  children: React.ReactNode;
}) {
  const active = current === value;
  return (
    <button
      onClick={() => onClick(value)}
      className={clsx(
        "px-1 py-2 text-sm border-b-2 -mb-px transition",
        active
          ? "border-brand-600 text-brand-600 font-medium"
          : "border-transparent text-gray-500 hover:text-gray-800"
      )}
    >
      {children}
    </button>
  );
}

/* ============ 推荐流：允许游客 ============ */
function RecommendPane() {
  const q = useInfiniteQuery<GetRecommendFeedResp>({
    queryKey: ["feed", "recommend"],
    queryFn: ({ pageParam }) =>
      getRecommendFeed(pageParam as {
        cursor_published_at?: number;
        cursor_video_id?: number;
        page_size?: number;
      }),
    initialPageParam: { page_size: 12 } as {
      cursor_published_at?: number;
      cursor_video_id?: number;
      page_size?: number;
    },
    getNextPageParam: (last) =>
      last.has_more
        ? {
            cursor_published_at: last.next_cursor_published_at,
            cursor_video_id: last.next_cursor_video_id,
            page_size: 12,
          }
        : undefined,
  });
  const videos: VideoInfo[] = q.data?.pages.flatMap((p) => p.videos) ?? [];
  return (
    <FeedGrid
      videos={videos}
      loading={q.isLoading}
      empty="暂无推荐"
      hasMore={Boolean(q.hasNextPage)}
      fetchingMore={q.isFetchingNextPage}
      onLoadMore={() => q.fetchNextPage()}
    />
  );
}

/* ============ 关注流：需要登录 ============ */
function FollowingPane() {
  const q = useInfiniteQuery<GetFollowingFeedResp>({
    queryKey: ["feed", "following"],
    queryFn: ({ pageParam }) =>
      getFollowingFeed(pageParam as {
        cursor_published_at?: number;
        cursor_video_id?: number;
        page_size?: number;
      }),
    initialPageParam: { page_size: 12 } as {
      cursor_published_at?: number;
      cursor_video_id?: number;
      page_size?: number;
    },
    getNextPageParam: (last) =>
      last.has_more
        ? {
            cursor_published_at: last.next_cursor_published_at,
            cursor_video_id: last.next_cursor_video_id,
            page_size: 12,
          }
        : undefined,
  });
  const videos: VideoInfo[] = q.data?.pages.flatMap((p) => p.videos) ?? [];
  return (
    <FeedGrid
      videos={videos}
      loading={q.isLoading}
      empty="还没有关注的作者更新，去发现更多创作者吧"
      hasMore={Boolean(q.hasNextPage)}
      fetchingMore={q.isFetchingNextPage}
      onLoadMore={() => q.fetchNextPage()}
    />
  );
}

/* ============ 热榜：允许游客，快照 + offset 分页 ============ */
function HotPane() {
  const q = useInfiniteQuery<GetHotFeedResp>({
    queryKey: ["feed", "hot"],
    queryFn: ({ pageParam }) => {
      const p = pageParam as { snapshot_at?: number; offset?: number };
      return getHotFeed({ ...p, page_size: 20 });
    },
    initialPageParam: {} as { snapshot_at?: number; offset?: number },
    getNextPageParam: (last) =>
      last.has_more
        ? { snapshot_at: last.snapshot_at, offset: last.next_offset }
        : undefined,
  });
  const items: HotFeedVideo[] = q.data?.pages.flatMap((p) => p.items) ?? [];
  const videos: VideoInfo[] = items.map((it) => it.video);
  const badges = items.map((it) => ({ rank: it.rank, hotScore: it.hot_score }));
  return (
    <FeedGrid
      videos={videos}
      badges={badges}
      loading={q.isLoading}
      empty="暂无热榜数据"
      hasMore={Boolean(q.hasNextPage)}
      fetchingMore={q.isFetchingNextPage}
      onLoadMore={() => q.fetchNextPage()}
    />
  );
}

/* ============ 网格容器 ============ */
function FeedGrid({
  videos,
  badges,
  loading,
  empty,
  hasMore,
  fetchingMore,
  onLoadMore,
}: {
  videos: VideoInfo[];
  badges?: { rank: number; hotScore: number }[];
  loading: boolean;
  empty: string;
  hasMore: boolean;
  fetchingMore: boolean;
  onLoadMore: () => void;
}) {
  if (loading) {
    return <p className="py-10 text-center text-gray-500 text-sm">加载中…</p>;
  }
  if (videos.length === 0) {
    return <p className="py-10 text-center text-gray-500 text-sm">{empty}</p>;
  }
  return (
    <>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
        {videos.map((v, i) => (
          <VideoCard key={v.video_id} video={v} hotBadge={badges?.[i]} />
        ))}
      </div>
      <InfiniteLoader
        hasMore={hasMore}
        loading={fetchingMore}
        onLoadMore={onLoadMore}
      />
    </>
  );
}
