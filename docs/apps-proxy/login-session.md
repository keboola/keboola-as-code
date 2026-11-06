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

Such a session is never refreshed and never re-validated (fork change in
`pkg/middleware/stored_session.go`). It ends 24 hours after login, whatever the
lifetime of its ID and access tokens.

This covers:

- **GitHub.** GitHub OAuth gives no refresh token. Organization, team and
  repository rules are checked only at login, so a removed user keeps access up
  to 24 hours.
- **OIDC IdPs that give no refresh token** (see section 5).

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
- **GitLab** always gives a refresh token, so GitLab sessions are refreshed.
- **Google** needs `access_type=offline` instead, which apps-proxy does not
  send. Google sessions have no refresh token.

## 6. Websockets

After the upgrade request, a websocket carries no cookie, so no refresh can run
on it. apps-proxy therefore closes every websocket at the earliest of:

- the next refresh, plus 5 seconds (only for a session with a refresh token),
- the cookie expiry,
- 6 hours after it opened (`upstream.wsTimeout`).

The browser reconnects at once. The upgrade request goes through the normal
cookie check: a due refresh runs and its new cookie is sent on the `101`
response, or the user is sent to log in. apps-proxy never refreshes inside a
websocket: an IdP that rotates refresh tokens would cancel the token the cookie
still holds.

If a refresh failed without ending the session, the next close is 1 hour later.

Side effects of the close:

- The reconnect counts as activity for auto-suspend. A forgotten tab with a
  refresh-token session keeps an app with an auto-suspend threshold over 1 hour
  awake.
- Session tracking records a heartbeat at each close (see
  [sessions.md](sessions.md)).

## 7. Rollout

Cookies issued before the 24-hour limit are older than 24 hours and have no
refresh token. They are rejected once and the users log in again.
