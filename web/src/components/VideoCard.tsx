import { Link } from "react-router-dom";
import { Heart, MessageCircle } from "lucide-react";

import UserAvatar from "./UserAvatar";
import type { VideoInfo } from "@/types/api";
import { timeAgo } from "@/utils/time";

interface Props {
  video: VideoInfo;
  // 是否显示右下角热度徽章（首页热榜用）
  hotBadge?: { rank: number; hotScore: number };
}

// 通用视频卡片：封面 + 标题 + 作者 + 计数
export default function VideoCard({ video, hotBadge }: Props) {
  return (
    <Link
      to={`/videos/${video.video_id}`}
      className="group block bg-white border border-gray-200 rounded-lg overflow-hidden hover:shadow-md transition"
    >
      <div className="relative aspect-video bg-gray-100">
        {video.cover_url ? (
          <img
            src={video.cover_url}
            alt={video.title}
            className="w-full h-full object-cover group-hover:scale-[1.02] transition"
            loading="lazy"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center text-gray-400 text-sm">
            无封面
          </div>
        )}
        {hotBadge ? (
          <div className="absolute top-2 left-2 bg-red-500/90 text-white text-xs px-2 py-0.5 rounded">
            No.{hotBadge.rank}
          </div>
        ) : null}
      </div>

      <div className="p-3">
        <h3 className="text-sm font-medium text-gray-900 line-clamp-2 min-h-[2.5rem]">
          {video.title}
        </h3>

        <div className="mt-2 flex items-center gap-2 text-xs text-gray-500">
          <UserAvatar
            userID={video.author_id}
            username={video.author_username}
            size={20}
            clickable={false}
          />
          <span className="truncate">{video.author_username}</span>
          <span className="ml-auto">{timeAgo(video.created_at)}</span>
        </div>

        <div className="mt-2 flex items-center gap-3 text-xs text-gray-500">
          <span className="flex items-center gap-1">
            <Heart size={13} className={video.is_liked ? "text-red-500 fill-red-500" : ""} />
            {video.likes_count}
          </span>
          <span className="flex items-center gap-1">
            <MessageCircle size={13} />
            {video.comments_count}
          </span>
        </div>
      </div>
    </Link>
  );
}
