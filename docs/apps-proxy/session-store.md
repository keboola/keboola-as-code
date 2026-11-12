# Login Session Store

Where apps-proxy keeps the OAuth/OIDC login session (OIDC, JumpCloud, GitLab,
GitHub providers), and how to run, watch and switch it. The password provider,
public apps and preview links do not use the store.

How long a session lasts and how refresh works is the same in both modes, see
[login-session.md](login-session.md).

## 1. Modes

The mode is set per stack by `sessionStore.type` in the kbc-stacks chart
(`apps/apps-proxy`). The default is `cookie`.

| | `cookie` | `redis` |
|---|---|---|
| Where the session is | in the browser cookie (tokens, email, groups) | in Valkey, under the key `_oauth2_proxy_ticket-<id>` with a TTL of 24 hours |
| Cookie | `_oauth2_proxy`, split into `_oauth2_proxy_0`, `_1`, … above 4 KiB | `_oauth2_proxy_ticket`: a ticket under 512 B, the same size for every user |
| CSRF cookie of a login in progress | `_oauth2_proxy_csrf` | `_oauth2_proxy_ticket_csrf` |
| Refresh lock | none: parallel requests each refresh | one refresh per session, under a Valkey lock (`<key>.lock`, 2 s) |
| Depends on Valkey | no | yes, for OAuth/OIDC apps only |

In `redis` mode the browser cookie stays small whatever the number of groups,
so large group lists do not break the ingress or the cookie size limit. The
`X-Kbc-User-Roles` header still grows with the groups.

The two cookie names differ on purpose. The cookie store clears every cookie
that matches `^_oauth2_proxy(_\d+)?$` when it cannot read it. With one shared
name, replicas in different modes would log each other's users out during the
rolling restart that switches a stack. The ticket name is also the key prefix
that the Valkey ACL of the `appsproxy` user allows (`~_oauth2_proxy_ticket-*`),
so it must not be renamed on its own.

Both cookies, like every cookie that starts with `_oauth2_proxy_`, are removed
before the request reaches the app, see [app-cookies.md](app-cookies.md).

## 2. Topology (redis mode)

Valkey runs as a subchart of `apps/apps-proxy` in the `apps-proxy` namespace:
one primary and two replicas (StatefulSet `apps-proxy-valkey`), three Sentinels
with quorum 2, AOF persistence (`appendfsync everysec`), `maxmemory` with the
`volatile-lru` policy. A primary without a connected replica refuses writes
(`min-replicas-to-write 1`).

apps-proxy builds one go-redis Sentinel client at startup and shares it with
every app handler. It asks the Sentinels for the current primary and follows
`+switch-master` events. The client is closed after the HTTP server has
drained at shutdown.

Valkey ACL users: `appsproxy` (session keys only), `sentinel` (Sentinel; the
Sentinel `default` user is off), `default` (replication and Sentinel
monitoring), `datadog` (the Datadog check, no key access). The passwords are
in the Secret `apps-proxy-valkey-credentials`, keys `default`, `appsproxy`,
`datadog`, `sentinel`.

## 3. Configuration

ENV prefix `APPS_PROXY_`. The chart sets the first seven keys only when
`sessionStore.type` is `redis`.

| Config key | ENV | Default | Set by the chart |
|---|---|---|---|
| `sessionStore.type` | `SESSION_STORE_TYPE` | `cookie` | `redis` |
| `sessionStore.sentinelAddrs` | `SESSION_STORE_SENTINEL_ADDRS` | – (required in redis mode) | `apps-proxy-valkey-sentinel:26379` |
| `sessionStore.sentinelMasterName` | `SESSION_STORE_SENTINEL_MASTER_NAME` | – (required in redis mode) | `mymaster` |
| `sessionStore.sentinelUsername` | `SESSION_STORE_SENTINEL_USERNAME` | `sentinel` | `sentinel` |
| `sessionStore.sentinelPassword` | `SESSION_STORE_SENTINEL_PASSWORD` | – | `sessionStore.sentinelPassword` |
| `sessionStore.username` | `SESSION_STORE_USERNAME` | – | `appsproxy` |
| `sessionStore.password` | `SESSION_STORE_PASSWORD` | – | `sessionStore.appsproxyPassword` |
| `sessionStore.dialTimeout` | `SESSION_STORE_DIAL_TIMEOUT` | `500ms` | – |
| `sessionStore.readTimeout` | `SESSION_STORE_READ_TIMEOUT` | `500ms` | – |
| `sessionStore.writeTimeout` | `SESSION_STORE_WRITE_TIMEOUT` | `500ms` | – |
| `sessionStore.poolSize` | `SESSION_STORE_POOL_SIZE` | `20` | – |
| `sessionStore.maxRetries` | `SESSION_STORE_MAX_RETRIES` | `1` | – |

`sentinelAddrs` is a comma-separated `host:port` list; one Service address is
enough. `maxRetries: 0` means no retry. The short timeouts and the single retry
make a request fail to the 503 page within about a second instead of waiting.

The startup log shows the mode: `Cookie settings: name:_oauth2_proxy_ticket …`
in redis mode, `name:_oauth2_proxy …` in cookie mode. It is written when an
app's handler is built, so it appears with the first request to an OAuth app.

## 4. When the store is unavailable

A request with a ticket cookie that cannot read, lock or write its session
(connection refused, timeout, no primary, `NOREPLICAS`) gets an HTTP 503 page:
"Sign-in is temporarily unavailable. Please try again in a moment." The cookie
is kept. When the store answers again, the same cookie works; nobody is logged
out. This also covers the OAuth callback (a login cannot be saved) and the save
after a successful token refresh.

Not affected by a store outage: `/health-check` (the store is not part of it,
so pods stay Ready), password apps, public apps, preview links.

A session that is missing from the store (evicted, deleted, expired) is not an
outage: the user is sent to the login page and the cookie is cleared.

### Failover times measured on a dev stack

1 request per second with a logged-in user during each case:

| Case | New primary after | 503s seen by the user | Logouts |
|---|---|---|---|
| Graceful delete of the primary pod | 3 s | 0 | 0 |
| Hard kill of the primary pod (`--grace-period=0 --force`) | 3 s | 0 | 0 |
| Drain of the primary's node | 9 s | 0 | 0 |
| Rolling restart of the Valkey StatefulSet | 1 s, then 75 s | 0 | 0 |
| Primary cut off by a NetworkPolicy for 60 s | 40 s | 0 | 0 |
| All 6 Valkey and Sentinel pods deleted at once | none (same primary) | 1 | 0 |
| All 3 Sentinels deleted, then the primary | 140 s | 117 s of 503 | 0 |
| Valkey scaled to 0 (pods gone ~3 min) | none (same primary) | 96 s of 503 | 0 |

Reads keep working while a new primary is chosen; writes (logins, refreshes)
fail for 1–8 s and get the 503 page. No acknowledged write was lost in any case.

Worst case: losing all Sentinels and then the primary costs about 2 minutes of
store outage. A Sentinel failover attempt that stalls waits for the Sentinel
`failover-timeout` (60 s by default), and the next attempt starts no sooner than
twice that time after the first one began.

### Lock contention

A session older than 1 hour is refreshed under a Valkey lock. Parallel requests
of the same user that find the lock taken poll for it every 10 ms (up to 5 s)
and then read the refreshed session. Every poll is a store command with the
500 ms read timeout, so a store that stalls for more than 500 ms while several
requests wait gives some of them the 503 page. The cookie is kept, and the next
request works.

A lock that cannot be released because the store went down stays until its
2-second TTL ends. Until then a request of that user that needs a refresh waits
for the lock.

### Refresh tokens

If the save after a successful token refresh fails, the user gets the 503 page
and the store keeps the session with the previous refresh token. An IdP that
rotates refresh tokens and detects reuse then rejects that token at the next
refresh, and the user logs in again.

## 5. Metrics

OpenTelemetry names. In Prometheus and Datadog the dots are underscores and
counters end in `_total`, for example `keboola_go_appsproxy_sessionstore_up`.
All of them exist only in redis mode.

| Metric | Type | Meaning |
|---|---|---|
| `keboola.go.appsproxy.sessionstore.up` | gauge | 1 if the last ping of the store succeeded, 0 otherwise. Pinged at startup and every 10 s. `min` over pods below 1 means some replica cannot reach the store. |
| `keboola.go.appsproxy.sessionstore.unavailable` | counter | Requests answered with the 503 page because the store was unavailable. What users saw. |
| `keboola.go.appsproxy.sessionstore.errors` | counter, `command` | Failed store commands. A missing key is not an error. |
| `keboola.go.appsproxy.sessionstore.duration` | histogram (ms), `command`, `result` | Duration of each store command. `command`: `get` (load), `set` (save), `del` (sign-out, clear), `lock` (lock scripts), `other` (ping, connection setup). `result`: `ok`, `miss` (no such session), `error`. |
| `keboola.go.appsproxy.sessionstore.pool.connections` | gauge, `state` | Connections in the client pool, `total` and `idle`. Should stay flat; handler rebuilds share the pool. |
| `keboola.go.appsproxy.sessionstore.pool.timeouts` | gauge | Times a command waited for a free pool connection and timed out, since start. |

Logs (component `sessionstore`): `session store is down: <error>` (warning) and
`session store is up` (info), only when the state changes. oauth2-proxy logs
`Error loading cookied session: … keeping session` for each 503. go-redis
writes some lines of its own to stderr, starting with `redis:`.

## 6. Switch a stack

Prerequisites: Valkey is running on the stack (`valkey.enabled: true`, the four
passwords in `secrets.yaml`), all 6 pods are Ready, and
`SENTINEL ckquorum mymaster` returns OK (see section 8).

1. Set `sessionStore.type: redis` in `<stack>/apps-proxy/values.yaml` and sync.
2. apps-proxy pods restart one by one. During the rollout old and new pods serve
   side by side; neither clears the other's cookie.
3. Every OAuth/OIDC user logs in once, because a cookie-store session cannot be
   read in redis mode. With a live IdP session this is usually one click.
4. A login started on an old pod and finished on a new pod fails with
   "Unable to find a valid CSRF token" (the CSRF cookie name differs). Trying
   again works.
5. Check: the log shows `name:_oauth2_proxy_ticket`, `sessionstore.up` is 1 on
   every pod, `sessionstore.unavailable` stays flat, and no `NOPERM` appears in
   the apps-proxy log after the first login.

Users who could not open an app because of a too-large session cookie may keep
the old cookie until it expires (up to 24 hours). Clearing the cookies of the
app's domain fixes it at once.

## 7. Roll back

- Fast: set `sessionStore.type: cookie` and sync. Users whose cookie-store
  session is less than 24 hours old keep it; the others log in once. The
  cookie-mode pods do not touch Valkey or the ticket cookie, so switching back
  to `redis` within 24 hours restores the Valkey sessions without a login.
- Full: also set `valkey.enabled: false`. ArgoCD does not prune, so the Valkey
  pods, Services and PVCs keep running until the app is synced with prune by
  hand. Delete the PVCs only when Valkey is not coming back.

## 8. Troubleshooting

Commands run in the stack's cluster, namespace `apps-proxy`. Read a password
inside the pod, never print it:

```bash
kubectl -n apps-proxy get pods -l app.kubernetes.io/instance=apps-proxy -o wide
# Sentinel: quorum and current primary
kubectl -n apps-proxy exec <sentinel-pod> -- sh -c \
  'valkey-cli -p 26379 --user sentinel --pass "$(cat <sentinel-password-file>)" SENTINEL ckquorum mymaster'
# Primary: role and replicas
kubectl -n apps-proxy exec <valkey-pod> -- sh -c \
  'valkey-cli --user default --pass "$(cat <default-password-file>)" INFO replication'
```

| Symptom | Likely cause | Check |
|---|---|---|
| OAuth apps show "Sign-in is temporarily unavailable", `sessionstore.up` 0 on all pods | No reachable primary, or Sentinel down | Pods Ready? `SENTINEL ckquorum mymaster`; `INFO replication` on the pods; section 4 for expected times |
| `sessionstore.up` 0 on one pod only | That pod cannot reach Valkey or Sentinel | NetworkPolicy (the pod needs the label `app: apps-proxy`); the pod's log `session store is down: …` |
| `session store is down: WRONGPASS …` or `NOAUTH` | Password or user does not match the Secret | `APPS_PROXY_SESSION_STORE_*` in the `apps-proxy-config` Secret vs. `apps-proxy-valkey-credentials` |
| `NOPERM` in the apps-proxy log | The `appsproxy` ACL misses a command or key pattern | ACL in the chart values; the session keys must match `_oauth2_proxy_ticket-*` |
| Short bursts of 503 for logins and refreshes, reads fine | Writes refused during a failover (`NOREPLICAS`) | Replica `master_link_status`; resolves when a replica reattaches |
| Users logged out after their session was evicted | Valkey reached `maxmemory` | `evicted_keys` in `INFO stats`; memory alerts |
| All OAuth users log in once | A switch between `cookie` and `redis` | Expected, section 6 |
| "Unable to find a valid CSRF token" | Login spanned old and new pods during a switch | Expected during the rollout; try again |
| A user is logged out while the IdP returns 5xx | A refresh failed and the session's tokens had expired; not the store | apps-proxy log `Unable to refresh session: …`; see [login-session.md](login-session.md) |

## 9. Code Map

| What | Where |
|---|---|
| Config | `config/sessionstore.go` |
| Client, checker, metrics | `sessionstore/` |
| Client in the service scope, shutdown | `dependencies/dependencies.go` |
| Store selection, cookie name, 503 hook | `proxy/apphandler/authproxy/oauthproxy/config.go` |
| Redis store, lock, store errors | `github.com/keboola/go-oauth2-proxy` (`pkg/sessions/redis`, `pkg/middleware/stored_session.go`) |
