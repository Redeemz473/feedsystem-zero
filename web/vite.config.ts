import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// gateway 后端监听在 8888，静态资源在 /uploads
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
  server: {
    port: 5173,
    host: "0.0.0.0",
    proxy: {
      "/account": { target: "http://127.0.0.1:8888", changeOrigin: true },
      "/video": { target: "http://127.0.0.1:8888", changeOrigin: true },
      "/interaction": { target: "http://127.0.0.1:8888", changeOrigin: true },
      "/social": { target: "http://127.0.0.1:8888", changeOrigin: true },
      "/uploads": { target: "http://127.0.0.1:8888", changeOrigin: true },
    },
  },
});
