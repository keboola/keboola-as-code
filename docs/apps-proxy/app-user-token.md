# App User Token

## 1. Overview

apps-proxy signs a short-lived JWT for every request that an OAuth/OIDC provider
authenticated, and passes it to the app in the `X-Kbc-App-User-Token` header.
The app forwards the header unchanged to Keboola services (Query Service first).
They verify it offline against the public keys that apps-proxy publishes.

The token means **"user X, acting through app Y"**. It is not a generic user
token and it is not specific to Query Service.

Related ticket: [AJDA-3464](https://linear.app/keboola/issue/AJDA-3464).

## 2. Header

The header name is the same on both hops: apps-proxy → app, and app → Keboola
service. It is not `Authorization`, because Query Service already uses that
header to authenticate the caller.

A browser cannot send the token: apps-proxy deletes every inbound `X-Kbc-*`
header before routing, and deletes `X-Kbc-App-User-Token` again right before it
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

## 6. Configuration

| Config key | Env | Default |
|---|---|---|
| `appUserToken.signingKeys` | `APPS_PROXY_APP_USER_TOKEN_SIGNING_KEYS` | *(empty — feature off)* |
| `appUserToken.activeKid` | `APPS_PROXY_APP_USER_TOKEN_ACTIVE_KID` | *(empty — feature off)* |
| `appUserToken.ttl` | `APPS_PROXY_APP_USER_TOKEN_TTL` | `5m` (maximum `5m`) |

`signingKeys` is a JSON list of `{"kid": "...", "pem": "..."}` entries, each an
EC P-256 private key (`openssl ecparam -name prime256v1 -genkey -noout`). It is a
secret. `activeKid` names the key that signs. Both empty turn the feature off:
no tokens, empty JWKS. Only one of them set is a startup error.

Startup refuses: invalid JSON, an entry without `kid` or `pem`, a `kid` that does
not match `^[A-Za-z0-9._-]{1,64}$`, a duplicate `kid`, a key that is not EC
P-256, an active kid that is not in the list, and a TTL above 5 minutes. Error
messages never contain a PEM.

In kbc-stacks the keys are `appUserToken.signingKeyPems` (secrets) and
`appUserToken.activeSigningKid` (values), the same model as the sandboxes-service
app preview links.

## 7. Key rotation

The full runbook is in `kbc-stacks/apps/apps-proxy/README.md`. In short:

1. Add the new key; keep the active kid at the old one. Check that the JWKS lists both kids.
2. Activate the new key at least 10 minutes after step 1 is rolled out (JWKS
   max-age 300 s plus the verifiers' refetch).
3. Remove the old key at least 6 minutes after step 2 is rolled out (token TTL
   5 minutes plus clock skew).

Never remove the active key. For a leaked key, do all three steps in one change;
verifiers refetch the JWKS on an unknown `kid`.

## 8. Monitoring

There are no metrics. Missing claims are valid states, not errors. The only
runtime error is a failed signing: apps-proxy logs `cannot sign the app user
token, the request continues without it` at error level and passes the request
on without a token. A bad key config stops apps-proxy at startup.

The token, the claims and the keys are never logged.

## 9. Limits in v1

- **WebSockets.** The token is minted once, on the handshake. It expires after
  5 minutes, but the socket can stay open for hours. Streamlit reads the token
  from the handshake headers, so Keboola calls that use it fail after 5 minutes.
- **Drafts.** A draft gets no OIDC login of its own yet, so it gets no token.
  When it does, its token carries the production `kbc.app_id` and the draft's own
  `kbc.workspace_id`.
- **Header size.** With a large group list the token reaches about 6 KB. App
  servers with a small header limit (Node.js: 16 KiB for all headers) may need a
  higher limit.
- **Rollout.** `email_verified` is read from the oauth2-proxy session cookie,
  which apps-proxy does not refresh. OIDC users who logged in before the release
  get tokens without `email` until they log in again or the cookie expires
  (7 days).
- **Replay.** App code can reuse a token of a user who visited the app until it
  expires, and so can anyone the app sends it to.
