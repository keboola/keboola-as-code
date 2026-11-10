# App User Token

## 1. Overview

apps-proxy signs a short-lived JWT for every request that an OAuth/OIDC provider
authenticated, and passes it to the app in the `X-Kbc-App-User-Token` header.
The app forwards the header unchanged to Keboola services (Query Service first).
They verify it offline against the public keys that apps-proxy publishes.

The token means **"user X, acting through app Y"**. It is not a generic user
token and it is not specific to Query Service.

A WebSocket app gets a new token through the refresh endpoint, see §6.

Related tickets: [AJDA-3464](https://linear.app/keboola/issue/AJDA-3464),
[AJDA-3584](https://linear.app/keboola/issue/AJDA-3584) (refresh).

## 2. Header

The header name is the same on both hops: apps-proxy → app, and app → Keboola
service. It is not `Authorization`, because Query Service already uses that
header to authenticate the caller.

A browser cannot send the token: apps-proxy deletes every inbound `X-Kbc-*`
header before routing, and deletes `X-Kbc-App-User-Token`,
`X-Kbc-App-User-Refresh-Token` and `X-Kbc-App-Public-Url` again right before it
sets its own.

A token is minted only when an OAuth/OIDC provider admitted the request and the
user id is known. There is no token for:

- password (basic) auth,
- paths with an anonymous rule (`authRequired: false`), also when the user is logged in,
- app preview-link sessions,
- the hub host and the `/_proxy/*` paths, which never reach the app.

A new token is minted for every proxied request.

## 3. Token format

JOSE header: `alg=ES256`, `kid=<active kid>`, `typ=kbc-app-user+jwt`. A verifier
must check `typ`, so that it never takes this token for a different kind of JWT.

| Claim | Value |
|---|---|
| `iss` | apps-proxy public URL (`https://hub.<stack>`), exact string |
| `aud` | `keboola` (a JSON string, not an array) |
| `sub` | IdP subject (for GitHub: the login) |
| `idp` | Issuer URL of the IdP, trailing `/` removed. GitHub: `https://github.com`, or the GitHub Enterprise URL. |
| `email` | Only when verified, see §4 |
| `groups` | IdP groups. Omitted when unknown, see §4 |
| `kbc.project_id` | Project id of the app |
| `kbc.app_id` | App id |
| `kbc.workspace_id` | Workspace id of the app. Omitted when unknown, see §4 |
| `jti` | Random, 32 hex characters, unique per token |
| `sid` | Session id, only when [session tracking](sessions.md) is on |
| `iat`, `exp` | `exp = iat + TTL` (default and maximum 5 minutes) |

`sub` is unique only per issuer, so the pair `(idp, sub)` is the primary user key.

**GitHub caveat.** GitHub issues no ID token, so for GitHub `sub` is the
account login, not a stable subject. Its owner can rename it, and once it is
released another account can take it. A verifier must not key durable grants
on a GitHub `(idp, sub)`: after a rename the user loses them, and the next owner
of the login inherits them. GitHub's numeric user id is stable, but oauth2-proxy
does not pass it on today.

**Trust in `idp`.** `idp` is the issuer that the app's owner configured. The
apps-proxy signature proves which IdP authenticated the user, not that the IdP
is honest. Anyone who can edit an app's auth providers can point it at an IdP
they control, and that IdP can assert any `sub`, any `email` with
`email_verified: true`, and any `groups`. A verifier must therefore allowlist
`idp`, or bind it to the project (`kbc.project_id`), before it honors `sub`,
`email` or `groups`. Otherwise a rule that names someone's email can be met
through a self-hosted IdP.

App data comes only from the Sandboxes proxy config and the Kubernetes CRs,
never from the request.

## 4. Omitted claims

A missing claim never stops the mint: the token is minted without it, and the
verifier decides. For `groups` and `kbc.workspace_id`, "missing" means
"unknown", and the verifier must fail closed. An empty `groups` claim would
wrongly mean "known, no groups", so it is never sent.

- **`email`** is best effort. It is in the token only when the IdP's ID token
  has `email_verified: true`, for every provider type. Missing or `false`:
  omitted.

  oauth2-proxy treats a **missing** `email_verified` as verified, and Entra ID
  usually omits it. So Entra ID users get a token without `email`. GitHub
  issues no ID token, so GitHub users never get `email`. Rules that name one
  person by email do not match these users; `groups` rules and `*` do.

- **`groups`** is omitted when the IdP sends no groups, or when the groups
  joined with `,` are longer than 4096 bytes (large Entra group lists). The
  header from oauth2-proxy is comma-joined, so a group name that contains `,` is
  split into two groups.

- **`kbc.workspace_id`** comes from `status.workspace.id` of the Sandbox CR that
  runs the app: the draft Sandbox itself, or the App's `status.productionSandbox`.
  It is omitted when that Sandbox has no workspace or is not cached. This is a
  valid state and is not logged. E2B apps do not run as Sandboxes yet, so they
  get no `kbc.workspace_id`.

## 5. JWKS

The public keys are at `https://hub.<stack>/.well-known/jwks.json`, without
authentication. Only the hub host serves it; on an app host the path goes to the
app.

- `Content-Type: application/jwk-set+json`, `Cache-Control: public, max-age=300`.
- Every configured key is listed: `kty=EC`, `crv=P-256`, `use=sig`, `alg=ES256`, `kid`, `x`, `y`.
- When no key is configured: `{"keys":[]}`.

## 6. Refresh for WebSocket apps

A WebSocket app (Streamlit) makes one HTTP request, the handshake, and then
talks only over the socket. It sees only the handshake headers. So on a
WebSocket handshake that gets a token, apps-proxy also sets:

| Header | Value |
|---|---|
| `X-Kbc-App-User-Refresh-Token` | Refresh token, valid until apps-proxy closes this websocket |
| `X-Kbc-App-Public-Url` | Public origin of the app, e.g. `https://my-app-123.hub.<stack>`, no trailing `/`. For a Sandbox, its own host. |

A plain HTTP request gets neither header: it gets a new token every time.
When refresh keys are not configured, or the Sandbox has no public host, the
handshake gets neither header.

The app calls:

```
POST <X-Kbc-App-Public-Url>/_proxy/app-user-token
Authorization: Bearer <refresh token>
```

The answer is `200`, `Content-Type: application/json`, `Cache-Control: no-store`:

```json
{"token": "<app user token>", "expiresAt": "2026-11-10T12:05:00Z"}
```

- The credential is the `Authorization` header, never an `X-Kbc-*` header:
  apps-proxy deletes those before routing.
- The app must take the URL only from `X-Kbc-App-Public-Url`. apps-proxy passes
  an inbound `X-Forwarded-Host` on to the app, so a client controls it, and
  `Host` is the internal address.
- Any replica answers. The refresh token is self-contained.
- No cookie and no login run on this path. It never reaches the app, so a
  refresh call is neither [session](sessions.md) nor
  [idle-suspend](idle-suspend.md) activity.
- No CORS headers. A browser never holds the refresh token.
- Other methods: `405`. Refresh keys not configured: `404`.

The new token is minted like the injected one: the identity from the
handshake, a new `jti`, `iat` and `exp`, and `kbc.workspace_id` read again. Its
`exp` is never later than the refresh token's `exp`.

Every failed check answers `401` with `WWW-Authenticate: Bearer` and no detail
in the body:

- no `Authorization: Bearer` value;
- the signature does not match a configured refresh key, or `alg` is not HS256;
- `typ`, `iss` or `aud` is wrong, or the token has expired;
- `kbc.app_id`, `kbc.project_id` or `kbc.sandbox` differ from the app and the
  workload the host routes to (the App and each of its Sandboxes are separate);
- the auth provider in the token is no longer configured for the app, no longer
  guards any path that requires login, or has another issuer now;
- the signing keys of the app user token are not configured.

After a `401` the app gets no new token until the websocket reconnects.

### Refresh token

A JWT signed with a separate HS256 key. The refresh keys are never in the JWKS,
and only apps-proxy verifies refresh tokens.

JOSE header: `alg=HS256`, `kid=<active refresh kid>`, `typ=kbc-app-user-refresh+jwt`.

| Claim | Value |
|---|---|
| `iss` | apps-proxy public URL, as in the app user token |
| `aud` | `apps-proxy` |
| `sub`, `idp`, `email`, `groups` | As in the app user token of the handshake |
| `kbc.project_id`, `kbc.app_id` | As in the app user token |
| `kbc.sandbox` | Name of the Sandbox the host routes to. Omitted for the App. |
| `kbc.auth_provider_id` | Id of the auth provider that admitted the handshake |
| `jti` | Random, 32 hex characters |
| `sid` | Session id, only when session tracking is on |
| `iat` | Time of the handshake |
| `exp` | When apps-proxy closes this websocket, see [login-session.md](login-session.md): the earliest of the next session refresh, the cookie expiry and `wsTimeout` (6 hours) after the handshake |

The refresh token has no `kbc.workspace_id`; each refresh reads it again.

## 7. Configuration

| Config key | Env | Default |
|---|---|---|
| `appUserToken.signingKeys` | `APPS_PROXY_APP_USER_TOKEN_SIGNING_KEYS` | *(empty — feature off)* |
| `appUserToken.activeKid` | `APPS_PROXY_APP_USER_TOKEN_ACTIVE_KID` | *(empty — feature off)* |
| `appUserToken.ttl` | `APPS_PROXY_APP_USER_TOKEN_TTL` | `5m` (maximum `5m`) |
| `appUserToken.refreshKeys` | `APPS_PROXY_APP_USER_TOKEN_REFRESH_KEYS` | *(empty — refresh off)* |
| `appUserToken.refreshActiveKid` | `APPS_PROXY_APP_USER_TOKEN_REFRESH_ACTIVE_KID` | *(empty — refresh off)* |

`signingKeys` is a JSON list of `{"kid": "...", "pem": "..."}` entries, each an
EC P-256 private key (`openssl ecparam -name prime256v1 -genkey -noout`). It is a
secret. `activeKid` names the key that signs. Both empty turn the feature off:
no tokens, empty JWKS. Only one of them set is a startup error.

Startup refuses: invalid JSON, an entry without `kid` or `pem`, a `kid` that does
not match `^[A-Za-z0-9._-]{1,64}$`, a duplicate `kid`, a key that is not EC
P-256, an active kid that is not in the list, and a TTL above 5 minutes. Error
messages never contain a PEM.

`refreshKeys` is a JSON list of `{"kid": "...", "secret": "..."}` entries. It
is a secret. `secret` is standard base64 (with padding) of at least 32 random
bytes (`openssl rand -base64 32`). `refreshActiveKid` names the key that signs
new refresh tokens. Both empty turn the refresh off: no refresh header, the
endpoint answers `404`. Only one of them set is a startup error. Startup also
refuses invalid JSON, an unknown field, a `kid` that does not match the pattern
above, a duplicate `kid`, a secret that is not standard base64 of at least 32
bytes, and an active kid that is not in the list. Error messages never contain a
secret.

In kbc-stacks the keys are `appUserToken.signingKeyPems` (secrets) and
`appUserToken.activeSigningKid` (values), the same model as the sandboxes-service
app preview links.

## 8. Key rotation

The full runbook is in `kbc-stacks/apps/apps-proxy/README.md`. In short:

1. Add the new key; keep the active kid at the old one. Check that the JWKS lists both kids.
2. Activate the new key at least 10 minutes after step 1 is rolled out (JWKS
   max-age 300 s plus the verifiers' refetch).
3. Remove the old key at least 6 minutes after step 2 is rolled out (token TTL
   5 minutes plus clock skew).

Never remove the active key. For a leaked key, do all three steps in one change;
verifiers refetch the JWKS on an unknown `kid`.

Refresh keys rotate on their own, and only apps-proxy reads them, so there is no
JWKS wait:

1. Add the new key and make it the active refresh kid in the same change.
2. Remove the old key once the longest refresh token life has passed after step
   1 is rolled out: `wsTimeout` (6 hours) plus clock skew.

A refresh token signed by a removed key gets `401` until its websocket
reconnects. Rotating the refresh keys does not touch the app user token keys,
and the other way round.

## 9. Monitoring

There are no metrics. Missing claims are valid states, not errors. The only
runtime error is a failed signing: apps-proxy logs `cannot sign the app user
token, the request continues without it` at error level and passes the request
on without a token. A bad key config stops apps-proxy at startup.

A refused refresh is logged at info level as `app user token refresh refused:
<reason>`, with the app attributes. A handshake that gets a token but no refresh
token because the Sandbox has no public host logs `no public URL known for
"<workload>", the websocket gets no app user refresh token` at warning level. A
failed refresh signing logs `cannot sign the app user refresh token, the
websocket continues without it` at error level.

The token, the refresh token, the claims and the keys are never logged. The
`Authorization` header is redacted in traces.

## 10. Limits in v1

- **WebSockets.** The token on the handshake expires after 5 minutes. The app
  must call the refresh endpoint (§6) for a new one.
- **Refresh outlives the tab.** After the user closes the tab, the app can still
  refresh until the refresh token expires: about 1 hour for a session with a
  refresh token, up to 6 hours without one. Sign-out does not revoke a refresh
  token already issued.
- **Refresh identity.** A refreshed token carries the identity of the
  handshake. Group or email changes at the IdP reach it only after the next
  handshake.
- **Drafts.** A draft gets no OIDC login of its own yet, so it gets no token.
  When it does, its token carries the production `kbc.app_id` and the draft's own
  `kbc.workspace_id`.
- **Header size.** With a large group list the token reaches about 6 KB. App
  servers with a small header limit (Node.js: 16 KiB for all headers) may need a
  higher limit.
- **Group freshness.** The token's groups come from the session cookie. They
  can be up to about 1 hour old for a session with a refresh token, and up to
  24 hours old for one without, see [login-session.md](login-session.md).
- **Replay.** App code can reuse a token of a user who visited the app until it
  expires, and so can anyone the app sends it to.
