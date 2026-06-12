import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const apiProxyTarget = process.env.VITE_PAAS_API_PROXY_TARGET ?? "http://127.0.0.1:18088";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 8090,
    proxy: {
      "/api": {
        target: apiProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
});
