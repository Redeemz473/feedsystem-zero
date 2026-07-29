import { useInfiniteQuery } from "@tanstack/react-query";

import { listMyLikedVideos } from "@/api/interaction";
import VideoCard from "@/components/VideoCard";
import InfiniteLoader from "@/components/InfiniteLoader";
import type { ListMyLikedVideosResp } from "@/types/api";

export default function LikesPage() {
  const q = useInfiniteQuery<ListMyLikedVideosResp>({
    queryKey: ["my-liked-videos"],
    queryFn: ({ pageParam }) =>
      listMyLikedVideos(pageParam as {
        cursor_created_at?: number;
        cursor_like_id?: number;
        page_size?: number;
      }),
    initialPageParam: { page_size: 12 } as {
      cursor_created_at?: number;
      cursor_like_id?: number;
      page_size?: number;
    },
    getNextPageParam: (last) =>
      last.has_more
        ? {
            cursor_created_at: last.next_cursor_created_at,
            cursor_like_id: last.next_cursor_like_id,
            page_size: 12,
          }
        : undefined,
  });

  const items = q.data?.pages.flatMap((p) => p.liked_videos) ?? [];

  return (
    <div className="max-w-6xl mx-auto px-4 py-6">
      <h1 className="text-lg font-semibold mb-4">我的喜欢</h1>

      {q.isLoading ? (
        <p className="py-10 text-center text-gray-500 text-sm">加载中…</p>
      ) : items.length === 0 ? (
        <p className="py-10 text-center text-gray-500 text-sm">
          还没有点赞任何视频
        </p>
      ) : (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {items.map((it) => (
              <VideoCard key={it.like_id} video={it.video} />
            ))}
          </div>
          <InfiniteLoader
            hasMore={Boolean(q.hasNextPage)}
            loading={q.isFetchingNextPage}
            onLoadMore={() => q.fetchNextPage()}
          />
        </>
      )}
    </div>
  );
}
