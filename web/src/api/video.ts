import request from "./request";
import type {
  CompleteVideoUploadReq,
  CompleteVideoUploadResp,
  DeleteVideoResp,
  GetVideoResp,
  InitVideoUploadReq,
  InitVideoUploadResp,
  ListUserVideosReq,
  ListUserVideosResp,
  PublishVideoReq,
  PublishVideoResp,
  UploadCoverResp,
  UploadVideoChunkResp,
  UploadVideoResp,
  VideoUploadStatusResp,
} from "@/types/api";

/* ---------------- 元数据 ---------------- */

export const getVideo = (videoID: number) =>
  request.get<GetVideoResp>(`/video/${videoID}`).then((r) => r.data);

export const listUserVideos = (authorID: number, params: ListUserVideosReq = {}) =>
  request
    .get<ListUserVideosResp>(`/video/user/${authorID}`, { params })
    .then((r) => r.data);

export const publishVideo = (body: PublishVideoReq) =>
  request.post<PublishVideoResp>("/video/publish", body).then((r) => r.data);

export const deleteVideo = (videoID: number) =>
  request.delete<DeleteVideoResp>(`/video/${videoID}`).then((r) => r.data);

/* ---------------- 一次性上传（小文件） ---------------- */

export const uploadVideo = (file: File, onProgress?: (percent: number) => void) => {
  const form = new FormData();
  form.append("file", file);
  return request
    .post<UploadVideoResp>("/video/upload", form, {
      headers: { "Content-Type": "multipart/form-data" },
      onUploadProgress: (e) => {
        if (onProgress && e.total) {
          onProgress(Math.round((e.loaded / e.total) * 100));
        }
      },
    })
    .then((r) => r.data);
};

/* ---------------- 分片上传（大文件） ---------------- */

export const initVideoUpload = (body: InitVideoUploadReq) =>
  request.post<InitVideoUploadResp>("/video/upload/init", body).then((r) => r.data);

export const uploadVideoChunk = (params: {
  upload_id: string;
  chunk_index: number;
  chunk_hash: string;
  chunk: Blob;
  onProgress?: (percent: number) => void;
}) => {
  const form = new FormData();
  form.append("upload_id", params.upload_id);
  form.append("chunk_index", String(params.chunk_index));
  form.append("chunk_hash", params.chunk_hash);
  form.append("chunk", params.chunk);
  return request
    .post<UploadVideoChunkResp>("/video/upload/chunk", form, {
      headers: { "Content-Type": "multipart/form-data" },
      onUploadProgress: (e) => {
        if (params.onProgress && e.total) {
          params.onProgress(Math.round((e.loaded / e.total) * 100));
        }
      },
    })
    .then((r) => r.data);
};

export const videoUploadStatus = (params: {
  upload_id: string;
  file_hash?: string;
}) =>
  request
    .get<VideoUploadStatusResp>("/video/upload/status", { params })
    .then((r) => r.data);

export const completeVideoUpload = (body: CompleteVideoUploadReq) =>
  request
    .post<CompleteVideoUploadResp>("/video/upload/complete", body)
    .then((r) => r.data);

/* ---------------- 封面上传 ---------------- */

export const uploadCover = (file: File) => {
  const form = new FormData();
  form.append("file", file);
  return request
    .post<UploadCoverResp>("/video/cover", form, {
      headers: { "Content-Type": "multipart/form-data" },
    })
    .then((r) => r.data);
};
