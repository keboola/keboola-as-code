# Headers the App Receives

apps-proxy sets the `X-Kbc-*` headers itself. A client never controls them: every
inbound header whose name starts with `X-Kbc-` is deleted before routing.

## Where each header is set and deleted

A request passes these steps in this order. Each row says what a step does to
the `X-Kbc-*` headers.

| Step | Code | Deletes | Sets | When |
|---|---|---|---|---|
| 1. Inbound | `stripInternalHeaders` in `proxy/server.go` | every `X-Kbc-*` header | — | every request |
| 2. OAuth login check | oauth2-proxy, `InjectRequestHeaders` in `oauthproxy/config.go` | — | `X-Kbc-User-Id`, `X-Kbc-User-Name`, `X-Kbc-User-Email`, `X-Kbc-User-Roles`, `X-Kbc-User-Email-Verified` | a request with a valid OAuth login session; a header is set only when its claim has a value |
| 3. Identity capture | `apptoken.WithIdentity` in `dataapps/apptoken/identity.go` | `X-Kbc-User-Email-Verified` | — | every request through an OAuth provider |
| 4. Session tracking | `sessions.Middleware` in `dataapps/sessions/sessions.go` | `X-Kbc-User-Id` | — | every request that reaches the app |
| 5. App user token | `apptoken.Minter.Inject` in `dataapps/apptoken/middleware.go` | `X-Kbc-App-User-Token`, `X-Kbc-App-User-Refresh-Token`, `X-Kbc-App-Public-Url` | `X-Kbc-App-User-Token` | deletes on every request; sets on a request that passed step 3 with a user id, when the signing keys are set |
| 5b. Refresh token | `apptoken.Minter.injectRefresh` in `dataapps/apptoken/refreshissue.go` | — | `X-Kbc-App-User-Refresh-Token`, `X-Kbc-App-Public-Url` | additionally on a WebSocket handshake with a valid ticket cookie and a known public host |

Steps 3 to 5 run only for a request that reaches the app. A path under `/_proxy/`
is answered by apps-proxy itself and never reaches them.

## What the app receives

| Header | From | Details |
|---|---|---|
| `X-Kbc-User-Name` | step 2 | [login-session.md](login-session.md) |
| `X-Kbc-User-Email` | step 2 | [login-session.md](login-session.md) |
| `X-Kbc-User-Roles` | step 2 | The groups, joined with `,`. A group name that contains a comma cannot be told apart. |
| `X-Kbc-App-User-Token` | step 5 | [app-user-token.md](app-user-token.md) |
| `X-Kbc-App-User-Refresh-Token` | step 5b | [app-user-token.md](app-user-token.md) §6 |
| `X-Kbc-App-Public-Url` | step 5b | [app-user-token.md](app-user-token.md) §6 |

`X-Kbc-User-Id` and `X-Kbc-User-Email-Verified` are internal: apps-proxy reads
them and deletes them before the app.

## Forwarded headers

The reverse proxy (`upstream.newReverseProxy` in
`proxy/apphandler/upstream/upstream.go`) sets the standard forwarded headers:

- `X-Forwarded-For`: the inbound chain plus this hop.
- `X-Forwarded-Host`: the inbound value when there is one, else the request
  host. A client controls it, so an app must not build URLs from it; use
  `X-Kbc-App-Public-Url`.
- `X-Forwarded-Proto`: the inbound `X-Forwarded-Proto`, else the inbound
  `X-Forwarded-Scheme`, else the scheme of this hop.

Cookies are listed in [app-cookies.md](app-cookies.md).
