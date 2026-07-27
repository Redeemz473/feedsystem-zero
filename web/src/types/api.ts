// 与 gateway.api / types.go 严格对齐的 TypeScript 类型定义
// 一处修改，全站生效

/* ==================== Account ==================== */

export interface VerificationReq { email: string }
export interface VerificationResp { verification: string }

export interface RegisterReq {
  username: string;
  password: string;
  email: string;
  verification: string;
}
export interface RegisterResp { msg: string }

export interface LoginReq {
  username: string;
  password: string;
}
export interface LoginResp {
  access_token: string;
  refresh_token: string;
}

export interface LogoutResp { msg: string }

export interface RefreshTokenReq { refresh_token: string }
export interface RefreshTokenResp {
  access_token: string;
  refresh_token: string;
}

export interface GetProfileResp {
  user_id: number;
  username: string;
  email: string;
  avatar_url: string;
  bio: string;
}

export interface UpdateProfileReq {
  username?: string;
  avatar_url?: string;
  bio?: string | null;
}
export interface UpdateProfileResp { msg: string }

/* ==================== Video ==================== */

export interface VideoInfo {
  video_id: number;
  author_id: number;
  author_username: string;
  title: string;
  description: string;
  play_url: string;
  cover_url: string;
  likes_count: number;
  comments_count: number;
  popularity: number;
  status: number;
  created_at: number;
  updated_at: number;
  is_liked: boolean;
  tags: string[];
}

export interface UploadVideoResp {
  msg: string;
  play_url: string;
}

export interface InitVideoUploadReq {
  filename: string;
  file_hash: string;
  file_size: number;
  chunk_size?: number;
  total_chunks?: number;
}
export interface InitVideoUploadResp {
  msg: string;
  upload_id: string;
  need_upload: boolean;
  need_chunk: boolean;
  play_url: string;
  uploaded_chunks: number[];
  chunk_size: number;
  chunk_threshold_bytes: number;
}

export interface UploadVideoChunkResp {
  msg: string;
  upload_id: string;
  chunk_index: number;
  uploaded_chunks: number[];
}

export interface VideoUploadStatusResp {
  upload_id: string;
  uploaded_chunks: number[];
  total_chunks: number;
  completed: boolean;
  play_url: string;
}

export interface CompleteVideoUploadReq {
  upload_id: string;
  filename: string;
  file_hash: string;
  file_size: number;
  total_chunks: number;
}
export interface CompleteVideoUploadResp {
  msg: string;
  play_url: string;
  file_hash: string;
}

export interface UploadCoverResp {
  msg: string;
  cover_url: string;
}

export interface PublishVideoReq {
  title: string;
  description?: string;
  play_url: string;
  cover_url: string;
  tags?: string[];
  request_id?: string;
}
export interface PublishVideoResp {
  msg: string;
  video: VideoInfo;
}

export interface GetVideoResp { video: VideoInfo }

export interface ListUserVideosReq {
  cursor_created_at?: number;
  cursor_video_id?: number;
  page_size?: number;
}
export interface ListUserVideosResp {
  videos: VideoInfo[];
  next_cursor_created_at: number;
  next_cursor_video_id: number;
  has_more: boolean;
}

export interface DeleteVideoResp { msg: string }

/* ==================== Interaction ==================== */

export interface LikeVideoResp {
  msg: string;
  liked: boolean;
  likes_count: number;
}
export interface UnlikeVideoResp {
  msg: string;
  liked: boolean;
  likes_count: number;
}
export interface IsLikedResp { liked: boolean }

export interface LikedVideoInfo {
  like_id: number;
  liked_at: number;
  video: VideoInfo;
}
export interface ListMyLikedVideosReq {
  cursor_created_at?: number;
  cursor_like_id?: number;
  page_size?: number;
}
export interface ListMyLikedVideosResp {
  liked_videos: LikedVideoInfo[];
  next_cursor_created_at: number;
  next_cursor_like_id: number;
  has_more: boolean;
}

export interface CommentInfo {
  comment_id: number;
  video_id: number;
  user_id: number;
  username: string;
  content: string;
  created_at: number;
  updated_at: number;
  can_delete: boolean;
}
export interface PublishCommentReq {
  content: string;
  request_id?: string;
}
export interface PublishCommentResp {
  msg: string;
  comment: CommentInfo;
  comments_count: number;
}
export interface DeleteCommentResp {
  msg: string;
  deleted: boolean;
  comments_count: number;
}
export interface ListCommentsReq {
  cursor_created_at?: number;
  cursor_comment_id?: number;
  page_size?: number;
}
export interface ListCommentsResp {
  comments: CommentInfo[];
  next_cursor_created_at: number;
  next_cursor_comment_id: number;
  has_more: boolean;
}

/* ==================== Social ==================== */

export interface SocialUserInfo {
  user_id: number;
  username: string;
  avatar_url: string;
  bio: string;
}

export interface FollowRelationInfo {
  relation_id: number;
  user: SocialUserInfo;
  followed_at: number;
  viewer_is_following: boolean;
}

export interface FollowResp {
  msg: string;
  followed: boolean;
}
export interface UnfollowResp {
  msg: string;
  unfollowed: boolean;
}
export interface IsFollowingResp { following: boolean }

export interface ListFollowRelationReq {
  cursor_updated_at?: number;
  cursor_follow_id?: number;
  page_size?: number;
}
export interface ListFollowersResp {
  followers: FollowRelationInfo[];
  next_cursor_updated_at: number;
  next_cursor_follow_id: number;
  has_more: boolean;
}
export interface ListFollowingsResp {
  followings: FollowRelationInfo[];
  next_cursor_updated_at: number;
  next_cursor_follow_id: number;
  has_more: boolean;
}
export interface GetFollowStatsResp {
  followers_count: number;
  followings_count: number;
}
