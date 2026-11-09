# Login Session

How long a login to a data app lasts, and how a user removed or changed in the
identity provider (IdP) loses access. Applies to the OAuth providers (OIDC,
JumpCloud, GitLab, GitHub). The password provider has its own 1-hour cookie and
is not covered here.

## 1. Summary

| Session | Refresh | Ends | A removed user keeps access |
|---|---|---|---|
| With a refresh token | every 1 hour | 24 hours after the last refresh | about 1 hour |
| Without a refresh token | never | 24 hours after login | up to 24 hours |
| GitLab (has a refresh token) | never | 24 hours after login | up to 24 hours |

Both values are constants in
`proxy/apphandler/authproxy/oauthproxy/config.go` (`sessionRefresh`,
`sessionExpire`). They are not configurable.

## 2. The session cookie

The session is the oauth2-proxy cookie. There is no server-side store. The
cookie carries the tokens, the email and the groups, and it is signed with the
session's creation time. A cookie older than 24 hours is rejected and the user
logs in again.

## 3. Refresh

Refresh is lazy. It runs only when a request with the cookie arrives and the
session is older than 1 hour. apps-proxy then asks the IdP for new tokens with
the refresh token:

- Success: the cookie is saved again with new groups and a new creation time,
  so its 24-hour window starts again. An active user is never logged out.
- `invalid_grant` (user disabled, token revoked): the cookie is deleted and the
  user must log in.
- Any other error (IdP down): the session is checked as before the refresh and
  kept if its tokens are still valid. The next request tries again.

Allowed groups are checked on every request against the groups in the cookie,
so a user removed from an allowed group loses access at the next refresh.

## 4. Sessions without a refresh token

Such a session is never refreshed and never re-validated (rule in the
oauth2-proxy fork, `pkg/middleware/stored_session.go`). It ends 24 hours after login, whatever the
lifetime of its ID and access tokens.

This covers:

- **GitHub.** GitHub OAuth gives no refresh token. Organization, team and
  repository rules are checked only at login, so a removed user keeps access up
  to 24 hours.
- **OIDC IdPs that give no refresh token** (see section 5).

GitLab sessions have a refresh token but are not refreshed either, see
section 6.

## 5. The `offline_access` scope

An OIDC IdP gives a refresh token only when the login asks for the
`offline_access` scope. apps-proxy reads the issuer's
`/.well-known/openid-configuration` when it builds an app's handler, and asks
for the scope only when `scopes_supported` lists it. An IdP that does not know
the scope therefore cannot reject the login. If the document cannot be read,
the login uses the default scopes.

- **Okta:** the customer's Okta app must also allow the "Refresh Token" grant
  type. Without it Okta logs the user in but returns no refresh token, and the
  session falls back to section 4.
- **GitLab** is not asked: it always gives a refresh token, but apps-proxy
  does not use it (section 6).
- **Google** needs `access_type=offline` instead, which apps-proxy does not
  send. Google sessions have no refresh token.

## 6. GitLab: no refresh

`Cookie.Refresh` is 0 for every provider whose `RefreshTokensSingleUse()`
returns true (`dataapps/auth/provider`, applied in `oauthproxy/config.go`).
Only GitLab does. A GitLab session ends 24 hours after login, like a session without a refresh token. A
user removed in GitLab, or removed from a required group or project, keeps
access up to 24 hours.

Do not turn the refresh on for GitLab without solving the race below first.

- GitLab gives a refresh token on every login (`use_refresh_token` in
  [GitLab's Doorkeeper initializer](https://gitlab.com/gitlab-org/gitlab/-/blob/master/config/initializers/doorkeeper.rb)).
- A GitLab refresh token is single-use, with no grace period. The initializer
  does not set `refresh_token_revoked_on_use`, so Doorkeeper revokes the old
  token as soon as it is used. Reuse detection, which needs the
  `previous_refresh_token` column, is not enabled
  ([gitlab-org/gitlab#364111](https://gitlab.com/gitlab-org/gitlab/-/issues/364111)).
- The cookie session store has no lock, and apps-proxy runs several replicas.
  A page load sends many requests at once, and a Streamlit reconnect sends
  `/_stcore/health`, `/_stcore/host-config` and the websocket upgrade together.
  When the session is due, each of them refreshes with the same token.
- The first refresh wins. The others get `invalid_grant`, which oauth2-proxy
  treats as fatal: it clears the session and sends a cookie deletion. The
  browser keeps whichever cookie it applies last, so users are logged out at
  random, about once an hour. GitLab logins use `prompt=login`, so the user has
  to sign in again.

IdPs that rotate refresh tokens with a grace period (Okta 30 s by default,
Pocket ID 1 min) are not affected: parallel requests arrive well inside it. An
Okta admin can set that grace period to 0, which brings the same race back.
This is not handled.

Revisit this when one of these is true:

- GitLab accepts a used refresh token for a grace period (for example
  `refresh_token_revoked_on_use`, or reuse detection with
  `previous_refresh_token`).
- apps-proxy stores sessions server-side with a lock shared by all replicas,
  so only one request refreshes.

## 7. Websockets

After the upgrade request, a websocket carries no cookie, so no refresh can run
on it. apps-proxy therefore closes every websocket at the earliest of:

- the next refresh, plus 5 seconds (only for a session that is refreshed),
- the cookie expiry,
- 6 hours after it opened (`upstream.wsTimeout`).

The browser reconnects at once. The upgrade request goes through the normal
cookie check: a due refresh runs and its new cookie is sent on the `101`
response, or the user is sent to log in. apps-proxy never refreshes inside a
websocket: an IdP that rotates refresh tokens would cancel the token the cookie
still holds.

If a refresh failed without ending the session, the next close is 1 hour later.

Each close writes one info log line:

```
websocket closed at its deadline: <reason>
```

`<reason>` is `refresh due`, `cookie expiry` or `ws timeout`. The line carries
the app attributes (`proxy.app.id`, `proxy.app.projectId`, …). A websocket closed
by the browser or the app writes no such line.

Side effects of the close:

- Auto-suspend: the reconnect is not idle-suspend activity. The upgrade does
  not count, only websocket data frames do, so a forgotten tab does not keep
  the app awake. See [idle-suspend.md](idle-suspend.md) §1 and §2.
- Session tracking records a heartbeat at each close (see
  [sessions.md](sessions.md)).

### Symptoms and how to check

- **Users see the app reconnect (a short "CONNECTING" label in Streamlit) about
  once an hour.** This is expected for a session with a refresh token. Check
  the logs for `websocket closed at its deadline: refresh due` with the app's
  `proxy.app.id`, about 1 hour apart. If the reconnect fails, look next for
  `Unable to refresh session` or `Fatal refresh error` (oauth2-proxy) for the
  same app: the IdP refused the refresh, and the user must log in.
- **The close repeats much more often than once an hour.** Look for many
  `refresh due` lines for one app within minutes. The likely cause is a refresh
  that does not reset the session time, for example clock skew between
  replicas larger than the 5-second grace.
- **An app with a tab open never auto-suspends.** Compare the times of the
  `websocket closed at its deadline` lines for the app with
  `spec.lastRequestAt` of its `IdleTimer` CR. If `lastRequestAt` moves at each
  close, the reconnect is counted as activity, against the rule. Otherwise
  follow [idle-suspend.md](idle-suspend.md) §3 (b).
- **Users are logged out after 24 hours.** Expected for sessions without a
  refresh token and for GitHub and GitLab. The close line says
  `cookie expiry`, and the reconnect is sent to the login page.
