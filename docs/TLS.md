# TLS termination

Palimpsest's `palimpsest-server` is a tonic gRPC service. v1 ships
**without** in-process TLS. The deployment story is "terminate TLS at an
upstream proxy and forward plaintext h2 / h2c to Palimpsest." This page
explains why, what we recommend, and how to plug it in.

## Why no in-process TLS in v1

Three reasons:

1. **Operationally, TLS lives where the cert does.** Almost every
   Palimpsest deployment we have in mind runs behind something that
   already terminates TLS for unrelated services (an ingress controller,
   an L7 load balancer, an envoy sidecar). Terminating *again* in
   Palimpsest doubles the cert-rotation surface for no upside.
2. **gRPC-Web bridging belongs at the edge.** Palimpsest enables
   `tonic-web` so browser clients can talk to it via a CORS-friendly
   HTTP/1.1 framing. That bridge is where the cert needs to live: the
   browser's TLS stack speaks to the edge proxy, and the proxy speaks
   plaintext h2/h2c to the server.
3. **Mutual TLS ≠ identity.** Palimpsest's identity model is JWT-based
   (`palimpsest_server::JwtAuthenticator`), not certificate-based. mTLS
   between proxy and server can still be useful as a network-layer
   restriction, but it doesn't replace JWT auth — and the JWT path is
   tested end-to-end.

## Recommended topology

```
┌────────────────┐  TLS 1.3   ┌────────────────────┐  h2c   ┌──────────────┐
│ browser / CLI  │──────────▶ │ ingress / proxy    │ ──────▶│ palimpsest   │
│ (TLS, gRPC-Web │  + JWT     │ (envoy, nginx,     │   JWT  │ (tonic h2c)  │
│  or gRPC)      │            │  ingress-nginx,    │ passes │ 50051        │
└────────────────┘            │  cloudflare, etc.) │ through└──────────────┘
                              │ TLS terminated     │
                              │ here. Cert rotated │
                              │ by cert-manager /  │
                              │ ACME / your CDN.   │
                              └────────────────────┘
```

Two specifics that matter:

- **The proxy must speak h2 to the server.** gRPC requires HTTP/2; many
  proxies default to HTTP/1.1 between proxy and origin. In nginx that's
  `grpc_pass` (not `proxy_pass`); in envoy it's `http2_protocol_options`
  on the upstream cluster; in ingress-nginx it's the
  `nginx.ingress.kubernetes.io/backend-protocol: GRPC` annotation.
- **Forward the `authorization` header verbatim.** JWT auth depends on
  the header surviving the proxy hop. Most proxies do this by default,
  but if you have header-allowlist filtering enabled, ensure
  `authorization` is on the list.

## What if I really need TLS in-process?

We're keeping the door open. tonic supports `Server::tls_config(...)`
out of the box. If a deployment has no edge proxy (single-tenant
internal service, no load balancer), we'll add an opt-in
`PalimpsestBuilder::with_tls(TlsConfig)` knob in v1.x. File an issue
with the deployment shape if you hit this case — it's not architectural,
it's just not on the v1 critical path.

## mutual TLS / SPIFFE / service mesh

For zero-trust deployments where the proxy is itself unauthenticated
to the server, run a **service mesh** (Istio, Linkerd, Cilium service
mesh) with mTLS between sidecars. Palimpsest stays plaintext-h2c on
localhost; the mesh handles cert rotation and identity. The same JWT
auth still gates *user* identity end-to-end.

## Browser / WASM clients

Browsers cannot speak raw gRPC, so they go through `tonic-web` (which
the server already enables). The proxy in front of Palimpsest must:

- present a valid TLS cert to the browser,
- handle CORS preflight (`OPTIONS`) with the `grpc-web-*` headers
  permitted,
- proxy `Content-Type: application/grpc-web*` to the server unmodified.

The `palimpsest-client-js` quickstart in `docs/` covers the matching
client-side configuration.
