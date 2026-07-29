import SparkMD5 from "spark-md5";

// 大文件分片 MD5，用于秒传：先计算 hash，再走 init 接口。
// chunkSize 与后端约定的分片粒度保持一致（默认 5MB）。
export function computeFileHash(
  file: File,
  chunkSize = 5 * 1024 * 1024,
  onProgress?: (percent: number) => void
): Promise<string> {
  return new Promise((resolve, reject) => {
    const total = Math.ceil(file.size / chunkSize);
    const spark = new SparkMD5.ArrayBuffer();
    const reader = new FileReader();
    let current = 0;

    reader.onload = (e) => {
      spark.append(e.target!.result as ArrayBuffer);
      current++;
      if (onProgress) onProgress(Math.min(100, Math.round((current / total) * 100)));
      if (current < total) {
        loadNext();
      } else {
        resolve(spark.end());
      }
    };
    reader.onerror = () => reject(new Error("读取文件失败"));

    function loadNext() {
      const start = current * chunkSize;
      const end = Math.min(start + chunkSize, file.size);
      reader.readAsArrayBuffer(file.slice(start, end));
    }
    loadNext();
  });
}

// 分片 hash（用于 uploadVideoChunk 的 chunk_hash 校验）
export function computeChunkHash(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      const spark = new SparkMD5.ArrayBuffer();
      spark.append(e.target!.result as ArrayBuffer);
      resolve(spark.end());
    };
    reader.onerror = () => reject(new Error("读取分片失败"));
    reader.readAsArrayBuffer(blob);
  });
}
