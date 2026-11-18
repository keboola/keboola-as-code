# Allowed Roles

Who may open an app behind an OIDC provider, decided by the `groups` claim of
the user's ID token. Applies to the `oidc` and `jumpcloud` provider types.
GitLab uses its own `groups` and `projects` fields and ignores `allowedRoles`,
the empty list included; GitHub has no such field.

## 1. The `allowedRoles` field

The field is part of each provider in the proxy-config
(`authProviders[].allowedRoles`).

| Value | Meaning |
|---|---|
| `null` or missing | Every user the IdP logs in may open the app |
| `["a", "b"]` | Only a member of at least one listed group |
| `[]` | Nobody who logs in through this provider. The login works, every request to the app gets a 403 |

The list belongs to one provider. Paths without required authentication,
another provider of the same app and a preview session are not affected.

Group values are compared exactly, as the IdP sends them. Entra ID sends group
object IDs, not names.

The non-empty list is enforced by oauth2-proxy (`AllowedGroups`), at the login
callback and again on every request. The empty list is enforced by apps-proxy:
oauth2-proxy reads an empty list as "allow all", so the app upstream is
replaced by a handler that always answers 403
(`proxy/apphandler/authproxy/oauthproxy/handler.go`).

## 2. Scope

apps-proxy requests `openid email profile`, plus `offline_access` when the
IdP's discovery document lists it. It never requests `groups` on its own. The
`groups` claim usually does not depend on that scope, and an authorization
server without it rejects the whole login with `invalid_scope` (Okta
`/oauth2/default`).

What the IdP must do instead:

- **Okta:** add a `groups` claim to the ID token of the authorization server
  ("Always" include, not only when the scope is requested).
- **Entra ID:** add the groups claim to the app registration, limited to
  "Groups assigned to the application". A user in more than about 200 groups
  otherwise gets no `groups` claim at all.

An IdP that sends groups only when asked for a scope gets it through the
provider's `scopes` field, which is added to the default. Without it, such an
IdP sends no groups and every user of an app with `allowedRoles` is denied.
Until AJDA-3233, `groups` was requested whenever `allowedRoles` was set, so an
existing app at such an IdP needs the field:

```json
{ "id": "oidc-1", "type": "oidc", "scopes": ["groups"], "allowedRoles": ["admin"] }
```

## 3. Denied access

A denied user gets the 403 page with the reason and, when they have a session,
a link to sign in with a different account. The decision is logged with these
attributes:

| Attribute | Value |
|---|---|
| `proxy.access.decision` | `deny` |
| `proxy.access.reason` | see below |
| `proxy.access.provider` | provider ID |
| `proxy.access.email` | the user's email, when the session is known |
| `proxy.app.id`, `proxy.app.projectId` | from the request context |

| Reason | When |
|---|---|
| `deny_all` | `allowedRoles` is `[]` |
| `not_in_groups` | An existing session has groups, none of them allowed |
| `no_groups_claim` | An existing session has no groups: the IdP did not send the claim |
| `unknown` | The login callback refused the user: not in an allowed group, or no groups sent. oauth2-proxy does not expose the session there, so neither the email nor the exact reason is known |

Allowed requests are not logged per decision. Other login failures (CSRF, an
error returned by the IdP) are not access decisions and keep the plain 403
page.
