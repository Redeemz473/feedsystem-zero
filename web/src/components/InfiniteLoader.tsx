import { useEffect, useRef } from "react";

interface Props {
  hasMore: boolean;
  loading: boolean;
  onLoadMore: () => void;
  end?: string;
}

// 通用无限滚动触发器：靠近视口底部时自动调用 onLoadMore
export default function InfiniteLoader({
  hasMore,
  loading,
  onLoadMore,
  end = "— 到底啦 —",
}: Props) {
  const sentinelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!hasMore || loading) return;
    const el = sentinelRef.current;
    if (!el) return;
    const io = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) onLoadMore();
      },
      { rootMargin: "200px" }
    );
    io.observe(el);
    return () => io.disconnect();
  }, [hasMore, loading, onLoadMore]);

  return (
    <div ref={sentinelRef} className="py-6 text-center text-xs text-gray-400">
      {loading ? "加载中…" : hasMore ? "上滑加载更多" : end}
    </div>
  );
}
