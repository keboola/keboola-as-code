# Cookies the App Receives

The app receives every cookie of its own, unchanged. It receives no cookie
that apps-proxy sets.

## Cookies apps-proxy sets

| Cookie | Set by | Purpose |
|---|---|---|
| `_oauth2_proxy` | oauth2-proxy | The login session: tokens, email, groups. See [login-session.md](login-session.md). |
| `_oauth2_proxy_0`, `_oauth2_proxy_1`, … | oauth2-proxy | The same session, split into parts when it is larger than 4 KiB. |
| `_oauth2_proxy_csrf` | oauth2-proxy | CSRF state of a login in progress. Deleted at the callback. |
| `_oauth2_provider` | provider selector | The login provider the user chose. |
| `proxyBasicAuth` | password provider | The password login. |
| `_kbc_dasid` | session tracking | The session id. See [sessions.md](sessions.md). |
| `__Host-kbc-app-preview-session` | preview link | The dev-mode preview session. See [preview-link.md](preview-link.md). |

apps-proxy reads these cookies for authentication, session tracking and
preview. Then it removes them from the `Cookie` header of the request it
forwards to the app. This applies to every request that reaches the app,
the WebSocket handshake included, whichever auth handler served it.

## How the cookies are matched

The names are matched exactly. For the split session cookie, the match is
`_oauth2_proxy_` followed by digits only. A cookie of the app with a similar
name, for example `_streamlit_xsrf` or `_oauth2_proxy_app`, reaches the app
unchanged.

A `Cookie` header line that contains no apps-proxy cookie is forwarded byte
for byte. A line that contains one is rebuilt from the remaining cookies,
joined with `; `. A line that contains only apps-proxy cookies is removed.

## What the app has instead

- The user's identity: the `X-Kbc-User-*` headers and the app user token,
  see [app-user-token.md](app-user-token.md).
- Sign-out: a browser navigation to `/_proxy/sign_out`. The app does not need
  the session cookie for it.

## Code and tests

- `proxy/apphandler/cookies.go`: the list of names and the middleware. It is
  the last step of the upstream chain, after session tracking and the app
  user token.
- `proxy/cookies_test.go` (`TestUpstreamCookies`): runs every flow that sets a
  cookie (OIDC login with a small and a split session, sign-out, password
  login, preview link), records every `Set-Cookie` name, sends all of them
  back and checks that the app receives only its own cookies, for HTTP and the
  WebSocket handshake. A new cookie that apps-proxy sets in one of these flows
  and does not remove makes this test fail.
