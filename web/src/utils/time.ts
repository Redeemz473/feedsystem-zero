// 简单的时间格式化辅助：把秒级时间戳转成"多久前"

export function timeAgo(sec: number): string {
  if (!sec || sec <= 0) return "";
  const now = Math.floor(Date.now() / 1000);
  const diff = now - sec;
  if (diff < 0) return "刚刚";
  if (diff < 60) return `${diff}秒前`;
  if (diff < 3600) return `${Math.floor(diff / 60)}分钟前`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}小时前`;
  if (diff < 30 * 86400) return `${Math.floor(diff / 86400)}天前`;
  const d = new Date(sec * 1000);
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(
    d.getDate()
  ).padStart(2, "0")}`;
}

// 更详细的日期时间：yyyy-MM-dd HH:mm
export function formatDateTime(sec: number): string {
  if (!sec || sec <= 0) return "";
  const d = new Date(sec * 1000);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(
    d.getHours()
  )}:${pad(d.getMinutes())}`;
}
