import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useForm } from "react-hook-form";
import { toast } from "sonner";
import { UploadCloud, Image as ImageIcon } from "lucide-react";

import {
  completeVideoUpload,
  initVideoUpload,
  publishVideo,
  uploadCover,
  uploadVideoChunk,
} from "@/api/video";
import { extractErrMsg } from "@/api/request";
import { computeChunkHash, computeFileHash } from "@/utils/hash";

interface FormValues {
  title: string;
  description: string;
  tags: string; // 逗号分隔
}

// 大于该阈值默认使用分片上传（后端会返回真实阈值，前端只做兜底判断）
const CHUNK_THRESHOLD_DEFAULT = 32 * 1024 * 1024;

export default function UploadPage() {
  const navigate = useNavigate();
  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<FormValues>({
    defaultValues: { title: "", description: "", tags: "" },
  });

  const [videoFile, setVideoFile] = useState<File | null>(null);
  const [coverFile, setCoverFile] = useState<File | null>(null);
  const [playUrl, setPlayUrl] = useState<string>("");
  const [coverUrl, setCoverUrl] = useState<string>("");
  const [uploading, setUploading] = useState(false);
  const [progressText, setProgressText] = useState<string>("");
  const [progressPercent, setProgressPercent] = useState<number>(0);
  const [publishing, setPublishing] = useState(false);

  /* -------------------- 视频上传 -------------------- */
  async function handleUploadVideo() {
    if (!videoFile) {
      toast.info("请先选择视频文件");
      return;
    }
    setUploading(true);
    setProgressText("正在计算文件指纹…");
    setProgressPercent(0);
    try {
      const fileHash = await computeFileHash(
        videoFile,
        5 * 1024 * 1024,
        (p) => setProgressPercent(p)
      );

      setProgressText("正在初始化上传…");
      setProgressPercent(0);
      const initResp = await initVideoUpload({
        filename: videoFile.name,
        file_hash: fileHash,
        file_size: videoFile.size,
      });

      // 后端返回 need_upload=false 表示秒传命中
      if (!initResp.need_upload && initResp.play_url) {
        setPlayUrl(initResp.play_url);
        setProgressText("秒传命中 ✓");
        setProgressPercent(100);
        toast.success("秒传成功");
        return;
      }

      const chunkSize = initResp.chunk_size || 5 * 1024 * 1024;
      const threshold = initResp.chunk_threshold_bytes || CHUNK_THRESHOLD_DEFAULT;

      if (!initResp.need_chunk || videoFile.size <= threshold) {
        // 小文件直接一整包走 complete 前后端会自然处理，为简化前端只用分片路径
        // （分片路径对小文件也完全兼容）
      }

      const totalChunks = Math.ceil(videoFile.size / chunkSize);
      const uploaded = new Set(initResp.uploaded_chunks || []);

      for (let i = 0; i < totalChunks; i++) {
        if (uploaded.has(i)) continue;
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, videoFile.size);
        const blob = videoFile.slice(start, end);
        const chunkHash = await computeChunkHash(blob);
        await uploadVideoChunk({
          upload_id: initResp.upload_id,
          chunk_index: i,
          chunk_hash: chunkHash,
          chunk: blob,
        });
        setProgressText(`正在上传分片 ${i + 1}/${totalChunks}`);
        setProgressPercent(Math.round(((i + 1) / totalChunks) * 100));
      }

      setProgressText("正在合并文件…");
      const completeResp = await completeVideoUpload({
        upload_id: initResp.upload_id,
        filename: videoFile.name,
        file_hash: fileHash,
        file_size: videoFile.size,
        total_chunks: totalChunks,
      });
      setPlayUrl(completeResp.play_url);
      setProgressText("上传完成 ✓");
      setProgressPercent(100);
      toast.success("视频上传成功");
    } catch (err) {
      toast.error(extractErrMsg(err, "上传失败"));
      setProgressText("");
    } finally {
      setUploading(false);
    }
  }

  /* -------------------- 封面上传 -------------------- */
  async function handleUploadCover() {
    if (!coverFile) {
      toast.info("请先选择封面图");
      return;
    }
    try {
      const resp = await uploadCover(coverFile);
      setCoverUrl(resp.cover_url);
      toast.success("封面上传成功");
    } catch (err) {
      toast.error(extractErrMsg(err, "封面上传失败"));
    }
  }

  /* -------------------- 发布 -------------------- */
  async function onSubmit(values: FormValues) {
    if (!playUrl) {
      toast.info("请先上传视频");
      return;
    }
    if (!coverUrl) {
      toast.info("请先上传封面");
      return;
    }
    setPublishing(true);
    try {
      const tags = values.tags
        .split(/[,，]/)
        .map((t) => t.trim())
        .filter(Boolean)
        .slice(0, 10);
      const resp = await publishVideo({
        title: values.title.trim(),
        description: values.description.trim(),
        play_url: playUrl,
        cover_url: coverUrl,
        tags,
        request_id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
      });
      toast.success("发布成功");
      navigate(`/videos/${resp.video.video_id}`);
    } catch (err) {
      toast.error(extractErrMsg(err, "发布失败"));
    } finally {
      setPublishing(false);
    }
  }

  return (
    <div className="max-w-2xl mx-auto px-4 py-8">
      <h1 className="text-xl font-semibold mb-6">发布视频</h1>

      <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
        {/* 视频文件 */}
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <label className="text-sm font-medium text-gray-800 flex items-center gap-2">
            <UploadCloud size={16} />
            视频文件
          </label>
          <input
            type="file"
            accept="video/*"
            onChange={(e) => setVideoFile(e.target.files?.[0] || null)}
            className="mt-3 block w-full text-sm text-gray-600
                       file:mr-3 file:py-2 file:px-3 file:rounded
                       file:border-0 file:text-sm file:bg-brand-50
                       file:text-brand-700 hover:file:bg-brand-100"
          />
          {videoFile ? (
            <p className="mt-2 text-xs text-gray-500">
              {videoFile.name}（{(videoFile.size / 1024 / 1024).toFixed(2)} MB）
            </p>
          ) : null}

          <button
            type="button"
            onClick={handleUploadVideo}
            disabled={!videoFile || uploading}
            className="mt-3 px-4 py-1.5 rounded-md bg-brand-600 text-white text-sm hover:bg-brand-700 disabled:opacity-60"
          >
            {uploading ? "上传中…" : playUrl ? "重新上传" : "开始上传"}
          </button>

          {progressText ? (
            <div className="mt-3">
              <div className="h-1.5 bg-gray-100 rounded overflow-hidden">
                <div
                  className="h-full bg-brand-500 transition-all"
                  style={{ width: `${progressPercent}%` }}
                />
              </div>
              <p className="mt-1 text-xs text-gray-500">
                {progressText}
                {progressPercent > 0 ? ` · ${progressPercent}%` : ""}
              </p>
            </div>
          ) : null}
          {playUrl ? (
            <p className="mt-2 text-xs text-green-600 break-all">
              ✓ {playUrl}
            </p>
          ) : null}
        </div>

        {/* 封面 */}
        <div className="bg-white p-4 rounded-lg border border-gray-200">
          <label className="text-sm font-medium text-gray-800 flex items-center gap-2">
            <ImageIcon size={16} />
            视频封面
          </label>
          <input
            type="file"
            accept="image/*"
            onChange={(e) => setCoverFile(e.target.files?.[0] || null)}
            className="mt-3 block w-full text-sm text-gray-600
                       file:mr-3 file:py-2 file:px-3 file:rounded
                       file:border-0 file:text-sm file:bg-brand-50
                       file:text-brand-700 hover:file:bg-brand-100"
          />
          <button
            type="button"
            onClick={handleUploadCover}
            disabled={!coverFile}
            className="mt-3 px-4 py-1.5 rounded-md bg-brand-600 text-white text-sm hover:bg-brand-700 disabled:opacity-60"
          >
            上传封面
          </button>
          {coverUrl ? (
            <div className="mt-3">
              <img
                src={coverUrl}
                alt="cover"
                className="w-40 h-24 object-cover rounded border border-gray-200"
              />
            </div>
          ) : null}
        </div>

        {/* 元数据 */}
        <div className="bg-white p-4 rounded-lg border border-gray-200 space-y-3">
          <label className="block">
            <span className="text-sm text-gray-700">标题</span>
            <input
              className="input mt-1"
              maxLength={80}
              {...register("title", { required: "标题必填" })}
            />
            {errors.title ? (
              <p className="mt-1 text-xs text-red-500">{errors.title.message}</p>
            ) : null}
          </label>

          <label className="block">
            <span className="text-sm text-gray-700">简介</span>
            <textarea
              className="input mt-1 min-h-[80px]"
              maxLength={500}
              {...register("description")}
            />
          </label>

          <label className="block">
            <span className="text-sm text-gray-700">标签（逗号分隔，最多 10 个）</span>
            <input
              className="input mt-1"
              placeholder="例如：技术,后端,go"
              {...register("tags")}
            />
          </label>
        </div>

        <button
          type="submit"
          disabled={!playUrl || !coverUrl || publishing}
          className="px-6 py-2 rounded-md bg-brand-600 text-white text-sm hover:bg-brand-700 disabled:opacity-60"
        >
          {publishing ? "发布中…" : "立即发布"}
        </button>
      </form>
    </div>
  );
}
