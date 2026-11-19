# Login Session Store

Where apps-proxy keeps the OAuth/OIDC login session (OIDC, JumpCloud, GitLab,
GitHub providers), and how to run and watch it. The password provider, public
apps and preview links do not use the store.

How long a session lasts and how refresh works, see
[login-session.md](login-session.md).

## 1. The store

| | |
|---|---|
| Where the session is | in Valkey, under the key `_oauth2_proxy_ticket-<id>` with a TTL of 24 hours |
| Cookie | `_oauth2_proxy_ticket`: a ticket under 512 B, the same size for every user |
| CSRF cookie of a login in progress | `_oauth2_proxy_ticket_csrf` |
| Refresh lock | one refresh per session, under a Valkey lock (`<key>.lock`, 30 s) |
| Depends on Valkey | OAuth/OIDC apps only |

The browser cookie stays small whatever the number of groups, so large group
lists do not break the ingress or the cookie size limit. The
`X-Kbc-User-Roles` header still grows with the groups.

The ticket name is also the key prefix that the Valkey ACL of the `appsproxy`
user allows (`~_oauth2_proxy_ticket-*`), so it must not be renamed on its own.
Renaming it logs every user out.

The ticket, its CSRF cookie and every other cookie that starts with
`_oauth2_proxy`, like the `_oauth2_proxy`, `_oauth2_proxy_0`, … cookies of a
cookie session store, are removed before the request reaches the app, see
[app-cookies.md](app-cookies.md).

## 2. Topology

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

ENV prefix `APPS_PROXY_`. apps-proxy does not start without `sentinelAddrs`
and `sentinelMasterName`. It starts when the store is not reachable; see
section 4.

| Config key | ENV | Default | Set by the chart |
|---|---|---|---|
| `sessionStore.sentinelAddrs` | `SESSION_STORE_SENTINEL_ADDRS` | – (required) | `apps-proxy-valkey-sentinel:26379` |
| `sessionStore.sentinelMasterName` | `SESSION_STORE_SENTINEL_MASTER_NAME` | – (required) | `mymaster` |
| `sessionStore.sentinelUsername` | `SESSION_STORE_SENTINEL_USERNAME` | `sentinel` | `sentinel` |
| `sessionStore.sentinelPassword` | `SESSION_STORE_SENTINEL_PASSWORD` | – | `sessionStore.sentinelPassword` |
| `sessionStore.redisUsername` | `SESSION_STORE_REDIS_USERNAME` | – | `appsproxy` |
| `sessionStore.redisPassword` | `SESSION_STORE_REDIS_PASSWORD` | – | `sessionStore.appsproxyPassword` |

`sentinelAddrs` is a comma-separated `host:port` list; one Service address is
enough.

The client timeouts are fixed in code (`sessionstore/client.go`): dial, read
and write 500 ms, one retry, a pool of 20 connections. They make a request fail
to the 503 page within about a second instead of waiting.

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

A request that the client aborts while it waits for the store (a closed tab, a
cancelled fetch) is not an outage either. It is answered with status 499, gets
no 503 page and is not counted in `sessionstore.unavailable`.

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

### Sentinel pauses (TILT)

A Sentinel sometimes stops answering for about 5 s. When it wakes up, it logs
`+tilt` and makes no decisions for the next 30 s; the other Sentinels may log a
short `+sdown`/`-sdown` for it. This happens every few hours per Sentinel on
every stack. It does not affect users: apps-proxy talks to the primary
directly, skips a Sentinel that does not answer within 0.5 s, and the two other
Sentinels still form the quorum of 2.

Likely cause: the chart enables `sentinel resolve-hostnames`, and Sentinel
resolves peer and node host names with a blocking DNS lookup on its main thread
(`src/sentinel.c`, `anetResolve`). A lost DNS packet waits for the 5 s default
resolver timeout. The pauses measured on a dev stack are almost exactly 5.0 s.

Known effects:

- The Sentinel probes (`sentinel-ping.sh`) have a 1 s timeout and are not
  configurable in chart `1.0.0`. A pause fails one probe; three failed liveness
  probes in a row restart the Sentinel container.
- A failover is delayed only if the primary fails while at least two Sentinels
  are paused or restarting at the same time. Users then see the 503 page for
  the length of the pause (5–30 s); nobody is logged out.
- On GKE 1.35+, exec probes without an explicit `timeoutSeconds` can raise the
  GKE Recommender exec-probe-timeout alert, which may pause automatic cluster
  upgrades (upstream `valkey-io/valkey-helm` issue #169; the Valkey container
  probes are configurable, the Sentinel probes are not).

Possible fixes, not applied: a pod `dnsConfig` with `timeout: 1` and
`attempts: 2` on the Sentinel pods, and a higher Sentinel liveness
`failureThreshold`. Both need a chart change.

### Lock contention

A session older than 1 hour is refreshed under a Valkey lock. The request that
takes the lock asks the IdP for new tokens and saves the session; the IdP call
is cut off after 24 s, and a cut-off call counts as an IdP outage: the user gets
the 503 page and keeps the cookie (see [login-session.md](login-session.md) §3).
Parallel requests of the same user that find the lock taken poll for it, first
every 10 ms and then less often, up to every 100 ms, and then read the
refreshed session.

| Setting | Value |
|---|---|
| Lock duration (TTL) | 30 s |
| Wait for the lock | 35 s (lock duration + 5 s) |
| IdP refresh call cut off | 24 s (4/5 of the lock duration) |

These are the oauth2-proxy fork defaults; apps-proxy does not change them. They
only matter if an IdP is unusually slow: with a normal IdP the refresh takes well
under a second and nobody waits for long.

A request that does not get the lock within the wait gets the 503 page. So does
a waiting request whose poll is not answered within the 500 ms read timeout. In
both cases the cookie is kept, and the next request works.

A lock that cannot be released because the store went down stays until its TTL
ends (30 s). Until then a request of that user that needs a refresh waits for
the lock.

### Refresh tokens

If the save after a successful token refresh fails, the user gets the 503 page
and the store keeps the session with the previous refresh token. An IdP that
rotates refresh tokens and detects reuse then rejects that token at the next
refresh, and the user logs in again.

Known limit, not related to the store: an IdP 5xx during a refresh logs the
user out when the session's tokens have expired (AJDA-3829).

## 5. Metrics

OpenTelemetry names. In Prometheus and Datadog the dots are underscores and
counters end in `_total`, for example `keboola_go_appsproxy_sessionstore_up`.

| Metric | Type | Meaning |
|---|---|---|
| `keboola.go.appsproxy.sessionstore.up` | gauge | 1 if the last ping of the store succeeded, 0 otherwise. Pinged at startup and every 10 s. `min` over pods below 1 means some replica cannot reach the store. |
| `keboola.go.appsproxy.sessionstore.unavailable` | counter | Requests answered with the 503 page because the store was unavailable. What users saw. |
| `keboola.go.appsproxy.sessionstore.errors` | counter, `command` | Failed store commands. A missing key and a command cancelled with its request are not errors. |
| `keboola.go.appsproxy.sessionstore.duration` | histogram (ms), `command`, `result` | Duration of each store command. `command`: `get` (load), `set` (save), `del` (sign-out, clear), `lock` (lock scripts), `other` (ping, connection setup). `result`: `ok`, `miss` (no such session), `canceled` (the client aborted the request), `error`. |
| `keboola.go.appsproxy.sessionstore.pool.connections` | gauge, `state` | Connections in the client pool, `total` and `idle`. Should stay flat; handler rebuilds share the pool. |
| `keboola.go.appsproxy.sessionstore.pool.timeouts` | gauge | Times a command waited for a free pool connection and timed out, since start. |

Logs (component `sessionstore`): `session store is down: <error>` (warning) and
`session store is up` (info), only when the state changes. oauth2-proxy logs
`Error loading cookied session: … keeping session` for each 503, and also for
a request the client aborted (`… context canceled, keeping session`). go-redis
writes some lines of its own to stderr, starting with `redis:`.

## 6. Troubleshooting

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
| Sentinel logs `+tilt`, peers log `+sdown`/`-sdown` for it, no failover | A Sentinel pause, usually a slow DNS lookup | Section 4, "Sentinel pauses (TILT)"; act only on `+switch-master`/`+odown` or repeated restarts |
| Sentinel container restarted, event "failed liveness probe" | Several Sentinel pauses close together | Section 4, "Sentinel pauses (TILT)"; check that the other two Sentinels are Ready |

## 7. Code Map

| What | Where |
|---|---|
| Config | `config/sessionstore.go` |
| Client, checker, metrics | `sessionstore/` |
| Client in the service scope, shutdown | `dependencies/dependencies.go` |
| Store options, cookie name, 503 hook | `proxy/apphandler/authproxy/oauthproxy/config.go` |
| Redis store, lock, store errors | `github.com/keboola/go-oauth2-proxy` (`pkg/sessions/redis`, `pkg/middleware/stored_session.go`) |
