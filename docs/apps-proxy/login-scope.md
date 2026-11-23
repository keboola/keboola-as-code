# Login Scope

The OAuth `scope` that apps-proxy sends to an OIDC identity provider (IdP) at
login. Applies to the `oidc` and `jumpcloud` provider types. GitLab and GitHub
use the oauth2-proxy default for their type. The scope is not configurable.

## 1. How the scope is chosen

apps-proxy reads the issuer's `/.well-known/openid-configuration` when it builds
an app's handler. It keeps the result until the handler is rebuilt (see
[login-session.md](login-session.md) §5). `scopes_supported` in that document
decides the optional scopes.

| `allowedRoles` | `scopes_supported` | Requested scope |
|---|---|---|
| not set | any, missing, or unreadable | `openid email profile` |
| set | lists `groups` | `openid email profile groups` |
| set | does not list `groups`, missing, or unreadable | `openid email profile` |
| any | lists `offline_access` | the above + `offline_access` |

apps-proxy always sends the scope. The oauth2-proxy default is never used for
OIDC, because that default adds `groups` whenever `allowedRoles` is set.

If the document cannot be read, apps-proxy logs a warning:

```
cannot read OIDC discovery of "<issuer>", "offline_access" and "groups" scopes not requested: <error>
```

## 2. `allowedRoles` and the `groups` claim

`allowedRoles` is checked against the `groups` claim. The scope only asks the
IdP for that claim. oauth2-proxy reads the claim from the ID token. If the ID
token does not have it, oauth2-proxy reads it from the IdP's `userinfo`
endpoint. A user without a matching group gets 403.

Many IdPs send the claim without the `groups` scope, because the claim is set
up on the app in the IdP:

- **Okta custom authorization server** (for example `/oauth2/default`): does not
  list `groups`, and rejects it with `invalid_scope`. Add a `groups` claim to
  the server with "Include in: Any scope".
- **Microsoft Entra ID, JumpCloud, Auth0, Keycloak (mapper on a default
  scope)**: the claim comes from the app or mapper config, not from a scope.
- **Okta org authorization server, OneLogin**: list `groups` and need it.
  apps-proxy requests it.

## 3. Known limits

There is no workaround for these in apps-proxy.

- **The IdP sends groups only for a `groups` scope that it does not list.**
  apps-proxy does not request the scope, the token has no groups, and every
  user gets 403. Examples: an Okta custom server whose groups claim is tied to
  a `groups` scope not published in metadata, and Fedora (Ipsilon), which uses
  the URI scope `https://id.fedoraproject.org/scope/groups`. Fix it in the IdP:
  send the claim for any scope.
- **The IdP lists `groups` but refuses it for this client.** Keycloak lists
  every realm client scope, also those not linked to the client. If the realm
  has a `groups` scope that is not linked to the client, the login fails with
  `invalid_scope`. Fix it in Keycloak: link the `groups` scope to the client.
