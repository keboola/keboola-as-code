# Cookies the App Receives

The app receives every cookie of its own, unchanged. It receives no cookie
that apps-proxy sets.

## Cookies apps-proxy sets

| Cookie | Set by | Purpose |
|---|---|---|
| `_oauth2_proxy_ticket` | oauth2-proxy | The login session ticket. See [login-session.md](login-session.md) and [session-store.md](session-store.md). |
| `_oauth2_proxy_ticket_csrf` | oauth2-proxy | CSRF state of a login in progress. Deleted at the callback. |
| `_oauth2_provider` | provider selector | The login provider the user chose. |
| `proxyBasicAuth` | password provider | The password login. |
| `_kbc_dasid` | session tracking | The session id. See [sessions.md](sessions.md). |
| `__Host-kbc-app-preview-session` | preview link | The dev-mode preview session. See [preview-link.md](preview-link.md). |

apps-proxy reads these cookies for authentication, session tracking and
preview. Then it removes them from the `Cookie` header of the request it
forwards to the app. This applies to every request that reaches the app,
the WebSocket handshake included, whichever auth handler served it.

## How the cookies are matched

`_oauth2_proxy` and every name that starts with `_oauth2_proxy_` are removed.
This covers every cookie oauth2-proxy sets: the session ticket, the CSRF
cookie, and the per-request CSRF cookies. It also covers the session cookies
of a cookie session store (`_oauth2_proxy`, `_oauth2_proxy_0`, `_1`, …), which
browsers can still send.
An app cookie whose name starts with `_oauth2_proxy_` therefore does not reach
the app. The other names are matched exactly. An app cookie with a similar
name, for example `_streamlit_xsrf` or `_oauth2_proxyapp`, reaches the app
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
  cookie (OIDC login, sign-out, password login, preview link), records every
  `Set-Cookie` name, sends all of them back together with the cookie session
  store names, and checks that the app receives only its own cookies, for HTTP and the
  WebSocket handshake. A new cookie that apps-proxy sets in one of these flows
  and does not remove makes this test fail.
