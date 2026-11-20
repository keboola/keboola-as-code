# Access Denied

What a signed-in user sees when the provider's rules refuse them, and what is logged.
Applies to `allowedRoles` on the `oidc` and `jumpcloud` provider types, and to the
`groups` / `projects` rules of the `gitlab` type.

## 1. Where the denial happens

oauth2-proxy checks the user's groups against the allowed list in two places:

| Stage | When | Session |
|---|---|---|
| `callback` | At `/_proxy/callback`, right after the login. Every first denial happens here. | Not saved. |
| `session` | On a request with an existing session whose groups no longer match, for example after a refresh. | The session cookie is cleared. |

In both places oauth2-proxy calls the `OnAccessDenied` option of the Keboola fork
(`github.com/keboola/go-oauth2-proxy`) with the refused session. apps-proxy sets it in
`oauthproxy/config.go` (`accessDenied.handle`). With `ForceJSONErrors` the fork answers a
JSON 403 instead; apps-proxy does not set that option.

Other 403s (missing CSRF cookie, an `error` from the IdP, `email_verified: false`) keep the
generic error page.

## 2. The page

HTTP 403, HTML title `Access denied`, heading "You don't have access to this app", then:

- "You are signed in as **&lt;email&gt;** (&lt;provider name&gt;)."
- One sentence for the reason:

| Reason | When | Text |
|---|---|---|
| `not_in_groups` | `oidc` / `jumpcloud`, the session has groups, none is allowed | Your account is not in a group that is allowed to open this application. Ask the person who shared it with you to give your account access. |
| `no_groups_claim` | `oidc` / `jumpcloud`, the session has no groups | Your identity provider did not send any groups for your account, so your access could not be checked. Ask your IT administrator to send the groups claim to this application. |
| `not_allowed` | `gitlab` | Your account is not allowed by the sign-in rules of this application. Ask the person who shared it with you to give your account access. |

- A **Sign in with a different account** button.
- Technical details: `Access denied (<reason>)`, app, project, status, exception ID.

The page never names a group: neither the allowed groups nor the user's groups.

## 3. Sign in with a different account

The button starts a new login with `prompt=login`:

| App | Button URL |
|---|---|
| One provider | `/_proxy/sign_in?prompt=login` |
| More providers | `/_proxy/selection?prompt=login`; the selection page passes `prompt=login` on to the chosen provider |

At the `session` stage the URL also has `rd=<path>`, so the user returns to the page they opened.

`prompt=login` is the only value a sign-in URL may override the provider's default `prompt`
with (`provider.PromptLogin`, the `Allow` rule of the `prompt` login parameter). Any other value
is dropped and the default is sent (`select_account` for `oidc`, `login` for `jumpcloud` and
`gitlab`).

`select_account` is not enough here: Keycloak does not support it (its discovery lists only
`none`, `login`, `consent`), and Okta is reported not to. With an IdP session they sign the
same account in again without asking. `/_proxy/sign_out` does not help either: a refused user
has no session, and `logoutUrl` is a server-side call that cannot end the IdP session in the
browser.

A refused user who opens the app again is sent to the IdP login again, with the default `prompt`.
No "denied" marker is kept.

## 4. Log

One `warn` line per denial, component `oauth2proxy`, with the request context:

```json
{"level":"warn","message":"access denied","component":"oauth2proxy",
 "proxy.access.provider":"oidc","proxy.access.email":"bob@example.com",
 "proxy.access.reason":"not_in_groups","proxy.access.stage":"callback",
 "proxy.app.id":"oidc","proxy.app.projectId":"123","proxy.app.name":"…","proxy.app.upstream":"…",
 "context.appId":"oidc","http.request_id":"GX6SaiV56ceuonv"}
```

| Field | Value |
|---|---|
| `proxy.access.provider` | provider ID from the app config |
| `proxy.access.email` | the refused user's email |
| `proxy.access.reason` | `not_in_groups`, `no_groups_claim` or `not_allowed` |
| `proxy.access.stage` | `callback` or `session` |

The line has no groups and no group count.

oauth2-proxy also writes its own auth line for the same denial (`[AuthFailure] Invalid
authentication via OAuth2: unauthorized` at the callback). At the `session` stage that line
prints the whole session, groups included (`Invalid authorization via session (unauthorized):
removing session Session{… groups:[…]}`), and so does the success line at every login
(`Authenticated via OAuth2: Session{…}`).
