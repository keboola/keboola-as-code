# Dev-Mode App Preview Links: Operator Guide

## 1. Overview

sandboxes-service mints a 60 s `ES256` link: `POST /apps/{appId}/preview-link`
returns `url` = `https://<app host>/_proxy/preview#t=<JWT>`. apps-proxy
verifies and redeems that JWT for the `__Host-kbc-app-preview-session` cookie.
The session lets the request skip the app's configured `AuthRules` — but only
while the workload's `App`/`Sandbox` CRD has dev mode enabled; if dev mode is
later turned off, an existing cookie stops working.

The session cookie's idle timeout and hard cap are code constants in the
`preview/session` package (`session.IdleTTL`, `session.MaxTTL`), not
configuration. The idle timeout is `4h`
and the hard cap is `12h` (see [§8](#8-configuration)).

The feature is off by default and turned on per stack by setting
`preview.jwksURL`; see [§8](#8-configuration).

---

## 2. Endpoints

Both live at the single path `/_proxy/preview` (`config.InternalPrefix +
"/preview"`). It is matched right after the canonical-host redirect and before
every other routing step ([§7](#7-routing-order)), so no auth handler,
`AuthRule` or upstream app ever intercepts it. The feature returns `404` on
every method when `preview.jwksURL` is unset.

| Method | Behavior |
|---|---|
| `GET`, `HEAD` | Returns the preview landing page. |
| `POST` | Redeems the link token from form field `token`. |
| other | `405`, header `Allow: GET, HEAD, POST`. |

**Landing page response headers:** `Cache-Control: no-store`,
`Referrer-Policy: no-referrer`, and a `Content-Security-Policy` built by
`preview.LandingCSP(nonce, frameAncestors)`:

```
default-src 'none'; script-src 'nonce-<nonce>'; form-action 'self'; base-uri 'none'; frame-ancestors <ancestors or 'none'>
```

`<ancestors>` is `preview.allowedFrameAncestors` space-joined, or `'none'` when
empty. The nonce is a fresh 24-character random value per request.

**Redeem (`POST`):**
- Requires header `Sec-Fetch-Site: same-origin`; anything else — including a
  missing header — gets `403` with body "The preview link was not submitted
  from its landing page." The landing page sends `Referrer-Policy:
  no-referrer`, so a same-origin form post from it still arrives with
  `Origin: null` in Chrome; the check uses `Sec-Fetch-Site`, never `Origin`,
  for that reason.
- On success: `303` with `Location: /` and `Set-Cookie` for the session.
- On any verification failure (unreadable form, workload not in dev mode,
  invalid/expired token): `401` with body "Preview link is invalid or expired.
  Get a new link." The proxy does not distinguish the failure reasons in the
  response — see [§9](#9-logging) for what is logged instead.
- Body is capped at 16 KiB (`preview.MaxRedeemBodySize`).

---

## 3. Link Verification

`preview.LinkVerifier.Verify` rejects a link unless every one of these holds:

- Signing algorithm is `ES256`, and only `ES256`.
- The key is selected by the JWT header's `kid`, looked up in the pinned JWKS
  ([§5](#5-jwks)). `jku`, `x5u` and `jwk` headers are never read.
- `iss` matches `preview.issuer`.
- `aud` contains `apps-proxy`.
- `sub` equals this host's canonical origin exactly — see
  [§4](#4-sub-derivation).
- `purpose` is `app-preview-link`.
- `ver` is `1`.
- `jti` is present (non-empty).
- `iat` is present.
- Clock skew of `30s` (`preview.ClockSkew`) is allowed on `exp`/`iat`.
- `exp − iat` is at most `90s` (the link's 60 s lifetime plus the 30 s skew
  allowance).

Any failure returns the same outcome to the caller — `401` with the generic
message from [§2](#2-endpoints) — with the specific reason only in the log
([§9](#9-logging)).

---

## 4. `sub` Derivation

The link's `sub` is **never parsed or normalised** — it is compared
byte-for-byte against the origin apps-proxy computes for itself. Only the
server side is normalised, through `preview.NormalizeOrigin`:

- Parse as a URL. Scheme must be `http` or `https`, lowercased. Host must be
  non-empty, lowercased. Port is dropped when it is the scheme's default
  (`443` on `https`, `80` on `http`). Path must be `""` or `/`. A query,
  fragment, userinfo or opaque part is rejected outright.
- Result: `scheme://host[:port]`, no trailing slash.

**App host** (legacy app, served by an `App` CRD): `NormalizeOrigin(baseURL.Scheme
+ "://" + baseURL.Host)`, where `baseURL = app.BaseURL(cfg.API.PublicURL)`. A
request on a non-canonical host is 308-redirected to `baseURL.Host` before
`/_proxy/preview` is ever reached, so the link only has to match the one
canonical origin.

**Sandbox host** (draft, served by a `Sandbox` CRD): `NormalizeOrigin(publicURL.Scheme
+ "://" + AppInfo.PublicHost[:port])`, where `publicURL = cfg.API.PublicURL`
and the port is appended only when `publicURL.Port()` is set. A Sandbox is
reached only by an exact hostname match, so there is no canonical-redirect
step and no alternate host a link could target.

A link minted for an App is rejected on its Sandbox and vice versa, because
the two origins are never equal.

---

## 5. JWKS

- The JWKS URL (`preview.jwksURL`) is pinned in config; the HTTP client
  follows no redirects (`CheckRedirect` returns `http.ErrUseLastResponse`).
- The response body is capped at 64 KiB; the fetch itself times out after 10 s.
- The set is refreshed every `jwks.RefreshInterval` (10 min).
- An unknown `kid` triggers an out-of-cycle refetch, but at most once a minute
  (`unknownKidRefetchInterval`).
- A fetched set is only usable for `jwks.MaxStaleness` (1 h) after it
  was fetched; once stale, key lookups fail closed (as if the key were
  missing) until a refresh succeeds.
- Only entries with `kty=EC`, `crv=P-256`, `use=sig`, `alg` absent or `ES256`,
  a non-empty `kid`, and `x`/`y` as unpadded base64url of exactly 32 bytes are
  accepted. Anything else is skipped and logged with the sanitized `kid`
  ([§9](#9-logging)).
- Boot never waits for JWKS: the refresh loop is started in its own goroutine
  when the service is constructed, so startup itself does not depend on the
  first fetch succeeding. A redeem that hits an unknown `kid` can still wait
  for that request's own refetch (up to the 10 s fetch timeout) before it
  fails.

---

## 6. Session

The redeemed session is a stateless HS256 JWT in the `__Host-kbc-app-preview-session`
cookie (`Secure`, `HttpOnly`, `SameSite=None`, `Partitioned`, `Path=/`).

**Claims:** `sub` (the same canonical origin as the link), `jti` (fresh random
id), `iat`, `exp`, `ver=1`, `purpose=app-preview-session`, `authTime` (Unix
seconds of the original redeem — fixed for the life of the session, even
across slides), and `linkJti` (the redeemed link's `jti`).

**Expiry:** `exp = min(now + session.IdleTTL, authTime + session.MaxTTL)`. A
valid request re-mints the cookie with a fresh `iat`/`exp` once the cookie is at
least 5 minutes old (`now − iat ≥ session.SlideInterval`) — but only when that
recomputed `exp` is later than the cookie's current one. The session therefore
ends 4h after the last request (at most 5 minutes earlier), and at most one
`Set-Cookie` is sent per 5 minutes. Once the hard cap from `authTime` already
bounds `exp`, no further request moves it, so a session can never outlive
`authTime + session.MaxTTL` regardless of how often it slides.

**No identity.** The claims carry no user or provider information — the
session only proves "this link was redeemed for this origin," nothing about
who redeemed it.

**Removed before the upstream.** The cookie is stripped from the `Cookie`
header (`session.TakeCookie`) immediately after the `/_proxy/preview`
path check and before any other routing decision, so the app itself, every
auth handler and the kai-preview path never see it.

**The upgrade slides, open frames don't.** A websocket upgrade is an ordinary
request/response pair — the slid `Set-Cookie` reaches the client on the `101`
response the same as on any other request (`TestPreviewSession/websocket`
asserts this). Once the connection is open, frames exchanged on it never
trigger a slide, so a long-lived connection can still outlive the idle window
between upgrades — see [§10](#10-known-limits).

**Clock tolerance.** The session check does not reject a cookie whose `iat` is
slightly in the future — this happens for a cookie minted on a replica whose
clock is a little ahead. `exp` and the `authTime + session.MaxTTL` hard cap are
still enforced strictly.

---

## 7. Routing Order

`appHandler.ServeHTTP` evaluates, in order:

```
Incoming request
│
├─1─ Host != canonical host (App only, not Sandbox)?
│       └─ YES → 308 redirect to canonical URL
│
├─2─ Path == /_proxy/preview?
│       └─ YES → preview landing page (GET/HEAD) or redeem (POST)
│
│   (the preview session cookie is stripped from every request here,
│    before any further routing — see §6; a request to /_proxy/sign_out
│    clears it too, whether or not the app has an auth handler)
│
├─3─ App has dev-mode enabled + path starts with /_proxy/kai-preview/*?
│       └─ YES → kai-preview composite handler
│
├─4─ Path starts with /_proxy/* and the app has an auth handler?
│       └─ YES → existing auth handler (sign-out also ends the
│                 sessions-manager session, then OAuth2 Proxy / Basic)
│
├─5─ App has dev-mode enabled + valid preview session cookie for this origin?
│       └─ YES → forward to upstream (skips AuthRules; slides the cookie)
│
├─6─ App has dev-mode enabled + valid kai-preview session cookie?
│       └─ YES → forward to upstream (skips AuthRules; slides the cookie)
│
├─7─ App has dev-mode enabled + iframe document load, no session?
│       └─ YES → serve kai-preview bootstrap shim
│
└─8─ AuthRules matching
        └─ matching rule found → apply configured auth, forward to upstream
           no match → 404
```

Step 2 runs before any auth handler, `AuthRule` or upstream forwarding — see
[§2](#2-endpoints), and clears the preview session cookie on sign-out
regardless of whether the app has an auth handler. Step 4 is checked ahead of
step 5, when the app does have an auth handler, so sign-out and the OIDC
callback always reach it rather than being shadowed by a live preview
session; on a sign-out it additionally ends the sessions-manager session. See
[kai-preview.md](kai-preview.md) for steps 3, 6 and 7.

---

## 8. Configuration

### 8.1 Config keys

| Key | Env var | Default | Notes |
|---|---|---|---|
| `preview.jwksURL` | `APPS_PROXY_PREVIEW_JWKS_URL` | `""` (disabled — every `/_proxy/preview` request gets `404`) | In-cluster URL of the sandboxes-service JWKS. |
| `preview.issuer` | `APPS_PROXY_PREVIEW_ISSUER` | `""` (required when `jwksURL` is set) | Expected `iss` claim of preview links, e.g. `https://apps.<suffix>`. |
| `preview.sessionSigningKey` | `APPS_PROXY_PREVIEW_SESSION_SIGNING_KEY` | `""` (required when `jwksURL` is set, ≥ 32 chars) | HMAC key for the session cookie. Generate with `openssl rand -hex 32`. |
| `preview.allowedFrameAncestors` | `APPS_PROXY_PREVIEW_ALLOWED_FRAME_ANCESTORS` | `""` → `frame-ancestors 'none'` | Comma-separated list of origins allowed to frame the landing page. |

### 8.2 Code constants (not configurable)

| Constant | Value | Notes |
|---|---|---|
| `session.IdleTTL` | `4h` | |
| `session.MaxTTL` | `12h` | |
| `session.SlideInterval` | `5m` | |
| `jwks.RefreshInterval` | `10m` | |
| `jwks.MaxStaleness` | `1h` | |
| `preview.ClockSkew` | `30s` | Link verification only ([§3](#3-link-verification)). |

---

## 9. Logging

**Logged:** `preview.linkJti` (the redeemed link's `jti`), `preview.kid`
(passed through `log.Sanitize`, which escapes line breaks; apps-proxy logs
are JSON, which escapes the rest), and the rejection
reason (as a log message, e.g. "preview: redeem rejected: ...").

**Never logged:** the link token, the session cookie value, or any redeem
request body.

---

## 10. Known Limits

- **Frames on an open websocket do not slide the session** ([§6](#6-session)).
  Only the upgrade request does, so a long-lived connection can outlive the
  idle window between upgrades.
- **An open websocket is not closed when the session ends.** Nothing revokes
  an in-flight connection; it is only new requests that re-check the cookie.
- **An expired or missing session is indistinguishable from one that was
  never established.** `previewSessionValid` returns `false` in both cases,
  and the request falls through to whatever the app's normal `AuthRules`
  produce — its own login, a password prompt, or `404` — with nothing telling
  the visitor their preview session lapsed. The generic `401` in
  [§2](#2-endpoints) is a separate case: it is only returned when redeeming a
  link fails, never when an existing session expires.
- **A link fails across a slug rename.** If the app's canonical host changes
  (proxy-config slug update) between minting and redeeming, `sub` no longer
  matches and the link is rejected with the generic `401`; the caller must
  mint a new link.
- **A valid preview session skips all of the app's `AuthRules`,** the same as
  a kai-preview session. A path that no `AuthRule` matches (`404` without a
  session) is forwarded to the app instead.
- **A preview link works for anyone who holds it, and any page can make a
  browser redeem it.** The landing page submits the link on its own, so a page
  that sends a user to someone else's link gives that user the other person's
  preview session. The session carries no user identity, so this grants nothing
  beyond the link itself.
