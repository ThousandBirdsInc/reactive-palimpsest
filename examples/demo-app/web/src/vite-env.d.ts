/// <reference types="vite/client" />

// Ambient declaration for the wasm-pack output. The real types come from
// `pkg/palimpsest_client_js.d.ts` once `build-wasm.sh` runs; this stub
// lets `tsc` typecheck before the bundle is built (e.g. in CI's
// install → typecheck step before the wasm build kicks in).
declare module "../pkg/palimpsest_client_js" {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const wasm: any;
  // eslint-disable-next-line import/no-default-export
  export default wasm;
  export const Client: unknown;
  export const Subscription: unknown;
}
