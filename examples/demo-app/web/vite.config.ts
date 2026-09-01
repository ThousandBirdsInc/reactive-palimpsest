import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";

export default defineConfig({
  plugins: [react(), wasm(), topLevelAwait()],
  // pglite ships its own wasm + worker assets; esbuild pre-bundling
  // breaks their relative URLs, so leave the package alone.
  optimizeDeps: { exclude: ["@electric-sql/pglite"] },
  server: { port: 5173, host: true },
  preview: { port: 8080, host: true },
});
