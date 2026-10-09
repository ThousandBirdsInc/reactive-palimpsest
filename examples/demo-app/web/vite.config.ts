import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import wasm from "vite-plugin-wasm";

export default defineConfig({
  // Vite's default build target (baseline-widely-available) supports
  // top-level await natively, so the wasm plugin needs no companion.
  plugins: [react(), wasm()],
  // pglite ships its own wasm + worker assets; esbuild pre-bundling
  // breaks their relative URLs, so leave the package alone.
  optimizeDeps: { exclude: ["@electric-sql/pglite"] },
  server: { port: 5173, host: true },
  preview: { port: 8080, host: true },
});
