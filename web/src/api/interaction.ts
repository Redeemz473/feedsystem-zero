import request from "./request";
import type {
  DeleteCommentResp,
  IsLikedResp,
  LikeVideoResp,
  ListCommentsReq,
  ListCommentsResp,
  ListMyLikedVideosReq,
  ListMyLikedVideosResp,
  PublishCommentReq,
  PublishCommentResp,
  UnlikeVideoResp,
} from "@/types/api";

/* ---------------- 点赞 ---------------- */

export const likeVideo = (videoID: number) =>
  request.post<LikeVideoResp>(`/interaction/video/${videoID}/like`).then((r) => r.data);

export const unlikeVideo = (videoID: number) =>
  request.delete<UnlikeVideoResp>(`/interaction/video/${videoID}/like`).then((r) => r.data);

export const isLiked = (videoID: number) =>
  request.get<IsLikedResp>(`/interaction/video/${videoID}/liked`).then((r) => r.data);

export const listMyLikedVideos = (params: ListMyLikedVideosReq = {}) =>
  request
    .get<ListMyLikedVideosResp>("/interaction/likes", { params })
    .then((r) => r.data);

/* ---------------- 评论 ---------------- */

export const publishComment = (videoID: number, body: PublishCommentReq) =>
  request
    .post<PublishCommentResp>(`/interaction/video/${videoID}/comments`, body)
    .then((r) => r.data);

export const deleteComment = (commentID: number) =>
  request
    .delete<DeleteCommentResp>(`/interaction/comments/${commentID}`)
    .then((r) => r.data);

export const listComments = (videoID: number, params: ListCommentsReq = {}) =>
  request
    .get<ListCommentsResp>(`/interaction/video/${videoID}/comments`, { params })
    .then((r) => r.data);
