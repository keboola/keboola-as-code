# Apps proxy Architecture Overview

- Serves for data apps authentication and authorization.
- Typical usage is to perform OIDC login through some OIDC provider (e.g. Microsoft login, Google login etc.)
- Has possibility to add basic authorization which consists of password prompt on a web page.


## Entrypoint

[cmd/apps-proxy/main.go](../../cmd/apps-proxy/main.go)

## Authentication provider endpoints

The proxy calls the endpoints of an app's authentication provider itself (OIDC discovery, token, userinfo, JWKS).

- Configured provider URLs must use `https` and a public host name. IP addresses, single-label hosts, `localhost`, and the `.localhost`, `.local` (incl. `.cluster.local`), `.internal` and `.svc` suffixes are rejected. The URLs are checked before the first request to the provider.
- Requests to providers connect only to public unicast addresses. The address is checked after DNS resolution. Private, loopback, link-local, CGNAT, multicast and cloud metadata addresses (incl. Azure WireServer `168.63.129.16`) are refused, as are IPv6 forms that carry an IPv4 address (IPv4-mapped, IPv4-compatible, NAT64, 6to4, Teredo).
- The backend logout request is sent by oauth2-proxy through the Go default client. The URL check applies to it, but the connection is not checked: a name that resolves to a non-public address, or a redirect, is followed.
- A provider that fails to initialize shows a generic error page. The detail is logged with the exception ID.

## Apps Proxy Options

## Operations

**Note**: Apps Proxy deployment provisioning has been migrated to GitOps (PAT-868). The legacy deployment scripts and Kubernetes manifests have been removed from `provisioning/apps-proxy/`. Local development directories (`provisioning/apps-proxy/dev` and `provisioning/apps-proxy/docker`) are retained for local testing. For production deployment instructions, please refer to the platform GitOps repository.

## Further docs

- [login-session.md](login-session.md) — how long a login lasts, refresh, and websocket closes.
- [login-scope.md](login-scope.md) — the OIDC login scope, when `groups` is requested, and its known limits.
- [session-store.md](session-store.md) — the login session store (cookie or Valkey): config, metrics, the 503 page, failover, switch and rollback.
- [app-cookies.md](app-cookies.md) — the cookies the app receives, and the apps-proxy cookies removed before it.
- [app-headers.md](app-headers.md) — the `X-Kbc-*` headers: where apps-proxy sets and deletes each one, and what the app receives.
- [app-user-token.md](app-user-token.md) — the signed `X-Kbc-App-User-Token` passed to apps, and its JWKS.
- [idle-suspend.md](idle-suspend.md) — what counts as activity for idle suspend, and how to check it.
- [preview-link.md](preview-link.md) — dev-mode preview links minted by sandboxes-service, and the page a frame gets without a preview session.
- [sessions.md](sessions.md) — end-user session tracking for data apps.
