# Apps proxy Architecture Overview

- Serves for data apps authentication and authorization.
- Typical usage is to perform OIDC login through some OIDC provider (e.g. Microsoft login, Google login etc.)
- Has possibility to add basic authorization which consists of password prompt on a web page.


## Entrypoint

[cmd/apps-proxy/main.go](../../cmd/apps-proxy/main.go)

## Authentication provider endpoints

The proxy calls the endpoints of an app's authentication provider itself (OIDC discovery, token, userinfo, JWKS).

- Configured provider URLs must use `https` and a public host name. IP addresses, single-label hosts, `localhost`, and the `.localhost`, `.local`, `.internal`, `.svc` and `.cluster.local` suffixes are rejected.
- Requests to providers connect only to public unicast addresses. The address is checked after DNS resolution.
- A provider that fails to initialize shows a generic error page. The detail is logged with the exception ID.

## Apps Proxy Options

## Operations

**Note**: Apps Proxy deployment provisioning has been migrated to GitOps (PAT-868). The legacy deployment scripts and Kubernetes manifests have been removed from `provisioning/apps-proxy/`. Local development directories (`provisioning/apps-proxy/dev` and `provisioning/apps-proxy/docker`) are retained for local testing. For production deployment instructions, please refer to the platform GitOps repository.

## Further docs

- [kai-preview.md](kai-preview.md) — dev-mode iframe-auth for the kbc-ui SPA.
- [preview-link.md](preview-link.md) — dev-mode preview links minted by sandboxes-service.
- [sessions.md](sessions.md) — end-user session tracking for data apps.
