import request from "./request";
import type {
  BatchIsFollowingReq,
  BatchIsFollowingResp,
  FollowResp,
  IsFollowingResp,
  ListFollowersResp,
  ListFollowingsResp,
  ListFollowRelationReq,
  UnfollowResp,
} from "@/types/api";

export const follow = (targetUserID: number) =>
  request.post<FollowResp>(`/social/users/${targetUserID}/follow`).then((r) => r.data);

export const unfollow = (targetUserID: number) =>
  request
    .delete<UnfollowResp>(`/social/users/${targetUserID}/follow`)
    .then((r) => r.data);

export const isFollowing = (targetUserID: number) =>
  request
    .get<IsFollowingResp>(`/social/users/${targetUserID}/following-status`)
    .then((r) => r.data);

export const batchIsFollowing = (body: BatchIsFollowingReq) =>
  request
    .post<BatchIsFollowingResp>("/social/users/following/batch", body)
    .then((r) => r.data);

export const listFollowers = (userID: number, params: ListFollowRelationReq = {}) =>
  request
    .get<ListFollowersResp>(`/social/users/${userID}/followers`, { params })
    .then((r) => r.data);

export const listFollowings = (userID: number, params: ListFollowRelationReq = {}) =>
  request
    .get<ListFollowingsResp>(`/social/users/${userID}/followings`, { params })
    .then((r) => r.data);
