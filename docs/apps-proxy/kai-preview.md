# Kai-Preview Iframe-Auth: Operator Guide

## 1. Overview

apps-proxy exposes a dev-mode-only authentication path that lets the kbc-ui SPA
embed a running data app inside an `<iframe>` without prompting the user with the
app's configured OAuth or Basic auth flow. The flow uses a short-lived HMAC
handshake JWT (60 s) that the SPA mints via a Storage API token and then exchanges
for a host-only session cookie. Direct (non-iframe) access to the app still goes
through the configured `AuthRules` as before; the kai-preview path is invisible to
end-users browsing directly.

---

## 2. When Does It Activate?

The kai-preview path is active only when **both** of the following are true:

1. The app's `App` CRD has `spec.devMode.enabled: true`.
2. The kbc-ui SPA calls `POST /_proxy/kai-preview/handshake-token` with a valid
   Storage API token in the `X-StorageApi-Token` header.

When `spec.devMode.enabled` is `false` (or absent) every kai-preview endpoint
returns `404`. No config flag disables the path at the proxy level — dev-mode on
the CRD is the single gate.

---

## 3. Endpoints

All endpoints live under the prefix `/_proxy/kai-preview`. They return `404` on
apps that do not have dev-mode enabled.

| Endpoint | Method | Auth | Notes |
|---|---|---|---|
| `/_proxy/kai-preview/handshake-token` | `POST` | `X-StorageApi-Token` header (CORS) | Mint a 60 s handshake JWT after verifying the Storage token against Storage API |
| `/_proxy/kai-preview/bootstrap`   | `GET`  | none | Return the postMessage handshake shim HTML; sets `Content-Security-Policy: frame-ancestors <allowed-origins>` |
| `/_proxy/kai-preview/exchange`    | `POST` | JWT in JSON body `{"token":"..."}` | Verify handshake JWT, set the `kbc-kai-preview-session` session cookie |
| `/_proxy/kai-preview/refresh`     | `POST` | session cookie (CORS) | Re-mint and slide the session cookie; returns `204 No Content` |

**CORS note.** `handshake-token` and `refresh` enforce an origin allowlist
(`kaiPreview.allowedOrigins`). Requests from origins not in the list are
rejected with `403` before any business logic runs. `bootstrap` and `exchange` are
not cross-origin calls (they are frame navigations / same-origin fetch from inside
the frame).

---

## 4. Configuration

### 4.1 Config keys

| Key | Default | Notes |
|---|---|---|
| `kaiPreview.handshakeSigningKey` | *(required)* | HMAC-SHA256 key for the 60 s handshake JWT |
| `kaiPreview.sessionSigningKey`   | *(required)* | HMAC-SHA256 key for the session cookie JWT |
| `kaiPreview.sessionTTL`          | `4h`          | Sliding session cookie lifetime |
| `kaiPreview.allowedOrigins`      | *(required)* | Origins permitted to call `handshake-token` and `refresh`, e.g. `https://connection.keboola.com` |
| `storageApiUrl`                  | *(required)*                     | Storage API base URL used to verify Storage tokens in `handshake-token`. Must be set per stack — there is no default value and the proxy will refuse to start without it |

### 4.2 Provisioning new signing keys

Two new Kubernetes / Helm secrets must be provisioned **per stack**. Generate each
key with:

```bash
openssl rand -hex 32   # run once for handshakeSigningKey
openssl rand -hex 32   # run once for sessionSigningKey
```

Store the output as separate secret values and mount them via the appropriate Helm
values path. Do **not** share keys between stacks or between the two key roles.

---

## 5. Routing Decision Tree

The following shows the order in which `appHandler.ServeHTTP` evaluates an
incoming request. Earlier steps short-circuit; later steps are only reached if
all prior steps pass.

```
Incoming request
│
├─1─ Host != canonical host?
│       └─ YES → 308 redirect to canonical URL
│
├─2─ App has dev-mode enabled?
│     │
│     ├─ YES + path starts with /_proxy/kai-preview/*
│     │       └─ kai-preview composite handler (handshake-token / bootstrap / exchange / refresh)
│     │
│     ├─ YES + request has a valid kbc-kai-preview-session cookie
│     │       └─ forward to upstream app
│     │             (if cookie is past its midpoint TTL, slide the cookie on the way out)
│     │
│     └─ YES + Sec-Fetch-Dest=iframe|frame, Accept=text/html, no valid session cookie
│               └─ serve bootstrap shim (rewrites path to /_proxy/kai-preview/bootstrap)
│
├─3─ Path starts with /_proxy/* (internal auth URLs)?
│       └─ existing auth handler (OAuth2 Proxy / Basic)
│
└─4─ AuthRules matching
        └─ matching rule found → apply configured auth (OAuth / Basic / none), forward to upstream
           no match → 404
```

---

## 6. Multi-Replica Behavior

All JWTs (both handshake and session cookie) are **stateless HMAC**. There is no
shared cache, Redis, or database involved. Any replica that holds the same signing
key can verify a token minted by any other replica. This means:

- Mint (`handshake-token`) and exchange can land on different replicas — no affinity
  required.
- Cookie validation is fully per-request and stateless — works identically across
  all replicas.
- Sliding refresh (`refresh`) re-mints a fresh JWT on every call — also stateless
  and replica-agnostic.

The only operational requirement is that **all replicas share the same signing
keys** (injected via Kubernetes secret).

---

## 7. Known Limitations

- **No user identity headers.** Apps that read `X-Kbc-User-*` headers to determine
  the acting user will not receive them in kai-preview iframe sessions — the session
  cookie carries only an app-scoped JWT with no user identity. Apps should fall back
  to `KBC_TOKEN` (the app's own service token) for user identity, or implement their
  own identity resolution via the Storage API.

- **Safari ITP.** Safari's Intelligent Tracking Prevention may purge partitioned
  cookies aggressively under low-traffic conditions. The SPA's sliding-refresh
  heartbeat (the `/_proxy/kai-preview/refresh` call made periodically by kbc-ui)
  compensates by re-minting the cookie before it is evicted. Operators should
  ensure the heartbeat interval in kbc-ui is shorter than the Safari ITP eviction
  window (typically 7 days for partitioned cookies, but may vary).

- **Dev-mode only.** The entire kai-preview path is disabled for production apps
  (`spec.devMode.enabled: false`). There is no override. Do not rely on this path
  for production embedding scenarios.
