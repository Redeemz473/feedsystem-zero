import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { ArrowLeft, Heart, MessageCircle, Trash2 } from "lucide-react";

import { getVideo, deleteVideo } from "@/api/video";
import { isFollowing as apiIsFollowing } from "@/api/social";
import { extractErrMsg } from "@/api/request";
import { useCurrentUser } from "@/hooks/useCurrentUser";
import { useOptimisticLike } from "@/hooks/useOptimisticLike";
import UserAvatar from "@/components/UserAvatar";
import FollowButton from "@/components/FollowButton";
import CommentSection from "@/components/CommentSection";
import { formatDateTime } from "@/utils/time";
import type { VideoInfo } from "@/types/api";

export default function VideoDetailPage() {
  const { id } = useParams<{ id: string }>();
  const videoID = Number(id);
  const { data: me } = useCurrentUser();
  const navigate = useNavigate();
  const qc = useQueryClient();

  const videoQuery = useQuery({
    queryKey: ["video", videoID],
    queryFn: () => getVideo(videoID),
    enabled: Number.isFinite(videoID) && videoID > 0,
  });

  const video: VideoInfo | undefined = videoQuery.data?.video;
  const isOwner = me && video && me.user_id === video.author_id;

  const followingQuery = useQuery({
    queryKey: ["following-status", video?.author_id],
    queryFn: () => apiIsFollowing(video!.author_id),
    enabled: Boolean(me && video && !isOwner),
  });
  const [followingLocal, setFollowingLocal] = useState<boolean | null>(null);
  const following =
    followingLocal ?? Boolean(followingQuery.data?.following);

  // Optimistic like: click updates UI immediately, backend value only kicks
  // in when drift is large enough for the user to notice inconsistency.
  const {
    liked: isLikedFinal,
    likesCount: likesFinal,
    pending: likePending,
    toggle: toggleLike,
  } = useOptimisticLike({
    videoID,
    initialLiked: video?.is_liked ?? false,
    initialCount: video?.likes_count ?? 0,
    loggedIn: Boolean(me),
    onSuccess: () => {
      // Refresh the personal liked-videos list on the profile page so it
      // reflects the new like/unlike next time the user opens it.
      qc.invalidateQueries({ queryKey: ["my-liked-videos"] });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteVideo(videoID),
    onSuccess: () => {
      toast.success("已删除");
      qc.invalidateQueries({ queryKey: ["feed"] });
      navigate(-1);
    },
    onError: (err) => toast.error(extractErrMsg(err, "删除失败")),
  });

  if (videoQuery.isLoading) {
    return <p className="py-10 text-center text-gray-500 text-sm">加载中…</p>;
  }
  if (videoQuery.isError || !video) {
    return (
      <div className="max-w-3xl mx-auto px-4 py-10 text-center text-gray-500">
        <p>视频不存在或已被删除</p>
        <button
          onClick={() => navigate(-1)}
          className="mt-4 text-brand-600 text-sm hover:underline"
        >
          返回
        </button>
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto px-4 py-6">
      <button
        onClick={() => navigate(-1)}
        className="mb-4 inline-flex items-center gap-1 text-sm text-gray-500 hover:text-gray-800"
      >
        <ArrowLeft size={16} />
        返回
      </button>

      <div className="bg-black rounded-lg overflow-hidden aspect-video">
        {video.play_url ? (
          <video
            src={video.play_url}
            poster={video.cover_url}
            controls
            className="w-full h-full"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center text-gray-400">
            视频源不可用
          </div>
        )}
      </div>

      <div className="mt-4">
        <h1 className="text-xl font-semibold text-gray-900">{video.title}</h1>
        {video.description ? (
          <p className="mt-1 text-sm text-gray-600 whitespace-pre-wrap">
            {video.description}
          </p>
        ) : null}
        <div className="mt-2 text-xs text-gray-400">
          发布于 {formatDateTime(video.created_at)}
          {video.tags?.length ? (
            <span className="ml-3">
              {video.tags.map((t) => (
                <span
                  key={t}
                  className="ml-1 px-2 py-0.5 bg-gray-100 text-gray-600 rounded"
                >
                  #{t}
                </span>
              ))}
            </span>
          ) : null}
        </div>
      </div>

      {/* 作者卡片 + 关注按钮 */}
      <div className="mt-4 p-4 bg-white border border-gray-200 rounded-lg flex items-center gap-3">
        <UserAvatar
          userID={video.author_id}
          username={video.author_username}
          size={44}
        />
        <div className="flex-1 min-w-0">
          <Link
            to={`/users/${video.author_id}`}
            className="text-sm font-medium text-gray-900 hover:text-brand-600"
          >
            {video.author_username}
          </Link>
        </div>
        {me && !isOwner ? (
          <FollowButton
            targetUserID={video.author_id}
            following={following}
            onChange={(v) => setFollowingLocal(v)}
          />
        ) : null}
        {isOwner ? (
          <button
            onClick={() => {
              if (confirm("确定删除该视频？")) deleteMutation.mutate();
            }}
            className="text-sm px-3 py-1.5 rounded border border-red-200 text-red-500 hover:bg-red-50"
          >
            <Trash2 size={14} className="inline -mt-0.5 mr-1" />
            删除
          </button>
        ) : null}
      </div>

      {/* 互动栏 */}
      <div className="mt-4 flex items-center gap-4">
        <button
          onClick={toggleLike}
          disabled={likePending}
          className="inline-flex items-center gap-1 text-sm text-gray-700 hover:text-red-500 disabled:opacity-60"
        >
          <Heart
            size={18}
            className={isLikedFinal ? "text-red-500 fill-red-500" : ""}
          />
          {likesFinal}
        </button>
        <span className="inline-flex items-center gap-1 text-sm text-gray-500">
          <MessageCircle size={18} />
          {video.comments_count}
        </span>
      </div>

      {/* 评论区 */}
      <div className="mt-6">
        <CommentSection
          videoID={videoID}
          onCommentsCountChange={() => {
            qc.invalidateQueries({ queryKey: ["video", videoID] });
          }}
        />
      </div>
    </div>
  );
}
