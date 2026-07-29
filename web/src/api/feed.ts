import request from "./request";
import type {
  GetFollowingFeedReq,
  GetFollowingFeedResp,
  GetHotFeedReq,
  GetHotFeedResp,
  GetRecommendFeedReq,
  GetRecommendFeedResp,
} from "@/types/api";

export const getRecommendFeed = (params: GetRecommendFeedReq = {}) =>
  request.get<GetRecommendFeedResp>("/feed/recommend", { params }).then((r) => r.data);

export const getFollowingFeed = (params: GetFollowingFeedReq = {}) =>
  request.get<GetFollowingFeedResp>("/feed/following", { params }).then((r) => r.data);

export const getHotFeed = (params: GetHotFeedReq = {}) =>
  request.get<GetHotFeedResp>("/feed/hot", { params }).then((r) => r.data);
