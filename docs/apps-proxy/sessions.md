# Data App Session Tracking: Operator Guide

## 1. Overview

apps-proxy tracks end-user sessions in data apps and writes them to a Keboola
Storage table through a [Stream](../stream) HTTP source. This answers questions
that nothing in the platform could answer before: who used an app, when, and
for how long.

Related ticket: [AJDA-3219](https://linear.app/keboola/issue/AJDA-3219).

Why the proxy and not the app: it is the only component that sees every request,
knows who the user is (after oauth2-proxy), lives outside the app pod — so it
survives auto-suspend, crashes and re-deploys — and works no matter what the app
author did or did not implement.

Why not Sandboxes Service: nothing about a session needs to be queried at request
time, so no service state is involved.

---

## 2. What Gets Recorded

A session is identified by the `_kbc_dasid` cookie, minted by the proxy on the
first request that has none. Everything durable lives either in the cookie or in
the emitted rows; the proxy keeps only a throttling cache in memory.

Three event types, one row each:

| Event | When |
|---|---|
| `session_start` | The proxy minted a new cookie — the actual start of a visit. One per session for a browser that keeps cookies (see [§6](#6-known-limitations) for clients that do not). |
| `heartbeat` | Periodically while the session is active (default every 5 min). Carries identity, so a session that starts anonymous and authenticates later still gets its user. |
| `session_end` | Websocket close, or an explicit `/_proxy/sign_out`. **Best-effort, and a websocket close is not final** — see [§6](#6-known-limitations). |

Identity is the email and name from the `X-Kbc-User-Email` / `X-Kbc-User-Name`
headers that oauth2-proxy injects. Both are empty for a shared-password app and
for a path with `authRequired: false`: those sessions carry a session id and
nothing else.

A sign-out ends the session **and clears the cookie**, so the next person to use
that browser starts a session of their own rather than being attributed to the
previous one.

`auth_provider_id` / `auth_provider_type` name the provider that admitted the
request. oauth2-proxy does not pass this on and one app can offer several
providers, so each per-provider auth handler stamps it into the request context
(`sessions.WithAuthProvider`). It is exact for shared-password apps too, which
never reach oauth2-proxy at all.

---

## 3. Why an Event Model

Stream is append-only. There is no way to fill in `session_end` on a row that
has already been written, so a session cannot be one mutable row — it has to be
a series of events, rebuilt by grouping on `session_id`:

```sql
SELECT
    session_id,
    MIN(session_start)                              AS started_at,
    MAX(event_time)                                 AS last_activity_at,
    MAX(CASE WHEN event_type = 'session_end'
             THEN event_time END)                   AS ended_at,
    MAX(CASE WHEN event_type = 'session_end'
             THEN end_reason END)                   AS end_reason,
    MAX(app_id)                                     AS app_id,
    MAX(project_id)                                 AS project_id,
    MAX(NULLIF(user_email, ''))                     AS user_email,
    MAX(NULLIF(user_name, ''))                      AS user_name,
    MAX(NULLIF(auth_provider_type, ''))             AS auth_provider_type,
    SUM(requests)                                   AS requests,
    SUM(ws_frames)                                  AS ws_frames
FROM "in.c-data-apps"."sessions"
WHERE app_id <> 'setup-script'
GROUP BY session_id
```

Note `MAX(event_time)` rather than the first `session_end`: a websocket close
emits `session_end` but does **not** end the visit, because Streamlit reconnects
routinely (its ~20 min reconnect cycle, a network blip, the 6 h upstream
timeout) and the user keeps working on the same session id. So one session can
carry several `session_end` rows, and activity can follow one of them. Only
`end_reason = 'sign_out'` is final.

A session whose `ended_at` is null ended without any `session_end` row. Either
way, close it with the same idle window the proxy uses
(`sessions.idleTimeout`, default 30 min): treat it as ended at
`last_activity_at` when that is more than 30 minutes old, and as still open
otherwise.

`requests` and `ws_frames` are **deltas** since the previous event of the same
session, which is why they are summed rather than taken with `MAX`.

---

## 4. Table Schema

Created by `scripts/stream-sessions-setup.sh`. Default table
`in.c-data-apps.sessions`.

| Column | Source | Note |
|---|---|---|
| `received_at` | Stream | Arrival time. Kept next to `event_time` so queueing delay is visible. |
| `event_id` | `eventId` | Row identifier (UUIDv7). |
| `event_type` | `eventType` | `session_start` \| `heartbeat` \| `session_end` |
| `event_time` | `eventTime` | Stamped by the proxy. Fixed-precision UTC (`2006-01-02T15:04:05.000000Z`), so that `MIN`/`MAX` order correctly even though the column is text. |
| `session_id` | `sessionId` | UUIDv7. Its timestamp prefix *is* the session start. |
| `session_start` | `sessionStart` | Decoded from `session_id`, identical on every row of a session. |
| `app_id`, `app_name`, `project_id` | app config | |
| `auth_provider_id`, `auth_provider_type` | request context | Empty when no auth was required. |
| `user_email`, `user_name` | `X-Kbc-User-*` | Empty for password / no-auth apps. |
| `user_agent` | request | |
| `requests`, `ws_frames` | proxy counters | Deltas, not totals. |
| `end_reason` | `endReason` | `ws_close` \| `sign_out`. Only on `session_end`; only `sign_out` is final. |

The Stream `ip` and `headers` column types are deliberately **not** used: they
describe the request Stream received, which comes from apps-proxy, not from the
end user. Everything about the user is sent explicitly in the body.

---

## 5. Setup

### 5.1 Create the source and sink

One source per stack, in a Keboola-internal project on that stack — not in the
customer project that owns the app.

```bash
export KEBOOLA_TOKEN=<sapi-token-of-the-internal-project>
bash scripts/stream-sessions-setup.sh
```

`KEBOOLA_BRANCH_ID` defaults to `default`. Stream takes a branch id or that
literal — unlike the Storage API it rejects `0`, and does so with a misleading
"Branch id:\"0\" was not found".

The script is idempotent and resumable: state goes to
`./stream-sessions-state.env` (mode 600 — the ingest URL embeds the write
secret) after each step, and re-running it picks up where it stopped. It ends by
sending one test event with `app_id = setup-script`; filter that out when
querying. `CLEANUP=true` deletes the source again.

### 5.2 Configure apps-proxy

| Config key | Env | Default |
|---|---|---|
| `sessions.streamUrl` | `APPS_PROXY_SESSIONS_STREAM_URL` | *(empty — tracking off)* |
| `sessions.maxSessionLength` | `APPS_PROXY_SESSIONS_MAX_SESSION_LENGTH` | `12h` |
| `sessions.heartbeatInterval` | `APPS_PROXY_SESSIONS_HEARTBEAT_INTERVAL` | `5m` |
| `sessions.idleTimeout` | `APPS_PROXY_SESSIONS_IDLE_TIMEOUT` | `30m` |
| `sessions.queueSize` | `APPS_PROXY_SESSIONS_QUEUE_SIZE` | `4096` |
| `sessions.workers` | `APPS_PROXY_SESSIONS_WORKERS` | `4` |
| `sessions.sendTimeout` | `APPS_PROXY_SESSIONS_SEND_TIMEOUT` | `5s` |

`streamUrl` contains the write secret, so it belongs in the encrypted kbc-stacks
secrets, not in `values.yaml`.

**Leaving `streamUrl` unset disables the whole feature.** That is how stacks
without Stream stay unaffected — as of this writing apps-proxy runs on 20 stacks
and Stream on 12, so eight stacks (all the single-tenant customer clouds,
`cloud-keboola-cs` among them) cannot run this yet.

### 5.3 Cookie lifetime

The cookie carries a **deadline that the proxy signs alongside the session id**,
and that deadline moves with the visitor's activity. `Max-Age` is derived from
it, but `Max-Age` is only a hint a client may ignore — the deadline in the
signed value is what the proxy enforces, and because it is inside the HMAC only
the proxy can move it.

| Issued on | Deadline | Why |
|---|---|---|
| an ordinary HTTP request | `now + idleTimeout` (30 min) | An abandoned tab stops counting in half an hour. |
| a websocket handshake | `now + upstream.wsTimeout + 10 min` (≈6 h 10 min) | The one case where the visitor may legitimately be active for hours without another HTTP request. |

Both are then clamped to `session start + maxSessionLength` (12 h), and a
deadline is never moved backwards — an ordinary request arriving during a live
websocket must not cut short the deadline that handshake was granted.

The expiry slides: **every request buys another full idle window**, so a visitor
who keeps returning inside 30 minutes never loses the session, whatever the
spacing of their requests. That means the cookie is re-issued on essentially
every ordinary request; leaving a deadline in place to save a `Set-Cookie`
header does not work, because it would silently still be the previous request's
and a visitor returning inside the window could find it already expired.

The handshake case is the whole point. Streamlit does nearly everything over one
long-lived websocket, and after the upgrade there is no further HTTP response
that could carry a `Set-Cookie` — so a 30-minute cookie would expire
mid-connection and split one visit into several sessions. The proxy already
watches that connection's frames to decide whether the app may be auto-suspended;
the cookie deadline is sized off the same signal, so as long as the connection
can still be alive, its session cookie is still valid. This works because
`Set-Cookie` written before the proxy runs survives the 101 response: Go's
`handleUpgradeResponse` merges the upstream's headers *into* the
`ResponseWriter`'s and writes that set.

For an app that talks plain HTTP rather than websockets — a React SPA calling
its backend, a PythonJS app — only the first row of that table applies, and the
result is the ordinary idle-session model: any request inside 30 minutes keeps
the session, and the visit ends 30 minutes after the last one. Nothing special
is needed. Long-lived HTTP *responses* are a different matter, but they do not
arise here: apps-proxy cancels any non-websocket request after
`upstream.httpTimeout` (30 s), so server-sent events are not a working transport
through the proxy at all. If that ever changes, such a response needs the same
treatment as a websocket handshake — the deadline must cover the connection,
because nothing else will arrive to refresh it.

`maxSessionLength` is validated to be longer than `idleTimeout`, and
`heartbeatInterval` shorter than it; the proxy refuses to start otherwise. It
should also exceed `upstream.wsTimeout` — that is not settable through
configuration, so the proxy only warns if a code change ever broke it.

`SameSite` is `Lax`, not `Strict`, for a related reason: the request landing
back on the app after an OAuth redirect is a cross-site top-level navigation.
Under `Strict` the cookie would not be sent on it and a second session would be
minted immediately after every login.

---

## 6. Known Limitations

- **Delivery is at-most-once.** Events are queued and sent fire-and-forget with
  no retries: a retried heartbeat would land as a second row carrying the same
  delta and inflate the counts. A full queue drops events (logged, counted).
  Losing a heartbeat is invisible; losing a `session_end` is covered by the idle
  window.
- **`session_end` is best-effort.** A proxy restart or a hard client
  disconnection loses it. Always apply the idle-window fallback.
- **Counters can undercount.** Deltas accumulated since the last heartbeat are
  lost when the entry is evicted or the process restarts. Session start and end
  times are unaffected.
- **A session can be seen by more than one replica.** apps-proxy runs 2 replicas
  on production stacks with no session affinity, so consecutive requests of one
  session land on either of them. Everything a session needs is in the signed
  cookie — its id, its start time (from the UUIDv7) and its deadline — so the
  in-memory state is a per-replica cache with no cross-replica invariants, and
  nothing has to be shared or coordinated. What follows from that:
    - `session_start` is emitted once, because it is gated on the cookie being
      absent, not on the cache being empty.
    - The heartbeat throttle is per-replica, so a session can emit up to one
      heartbeat per replica per interval. Counters are deltas rather than
      totals precisely so they still sum correctly across replicas.
    - A websocket is pinned to the replica that accepted it, while HTTP
      requests of the same session may go to the other one. Both track the
      same session id independently.
    - A restart or rollout drops the cache: pending deltas are lost, and the
      session continues on the next request with no spurious `session_start`.
- **A websocket close does not end the visit.** It emits `session_end` and drops
  the proxy's cached state, but later activity on the same cookie continues the
  same session id — which is correct for Streamlit, whose connection closes and
  reopens routinely. Consequence for queries: take `MAX(event_time)`, not the
  first `session_end`.
- **A visit longer than `maxSessionLength` is reported as several sessions.**
  A browser left open on a dashboard indefinitely rolls onto a new session id
  every 12 h, which is deliberate: the alternative is a single session measured
  in weeks.
- **An HTTP app that polls keeps its session alive.** Only Streamlit's two poll
  paths are recognised as non-activity. A frontend polling its own
  `/api/status` every few seconds looks exactly like a working user, so an
  abandoned tab reports a session up to the 12 h cap. This is the same blind
  spot auto-suspend already has — that poll keeps the app running too — so the
  fix, if one is wanted, belongs in `frameworkpoll` and benefits both.
- **Clients that ignore cookies produce one session per request.** An uptime
  monitor, a `curl` loop or a crawler hitting an app with
  `authRequired: false` never sends the cookie back, so every request mints a
  session and emits a `session_start`. Only the two Streamlit poll paths are
  excluded. Filter such traffic out (by `user_agent`, or by sessions with a
  single `session_start` and nothing else) before counting users.
- **Dev-mode previews are tracked too.** A data app opened through the
  kai-preview iframe path in the Connection UI reaches the upstream like any
  other request, so it produces sessions in the same table with no marker to
  tell it apart. Nothing distinguishes an internal preview from a real visit.
- **Activity excludes framework background polls.** `/_stcore/health` and
  `/_stcore/host-config` fire on every Streamlit websocket reconnect
  independently of the user, and are already excluded from the auto-suspend
  notification. Session activity reuses that same rule (`frameworkpoll.Is`) so
  there is exactly one definition of "the user did something".

---

## 7. Code Map

| What | Where |
|---|---|
| Manager, middleware, event building | `dataapps/sessions/sessions.go` |
| Cookie signing, deadline, UUIDv7 handling | `dataapps/sessions/cookie.go` |
| In-memory state and eviction | `dataapps/sessions/store.go` |
| Stream sender | `dataapps/sessions/writer.go` |
| Event / table row shape | `dataapps/sessions/event.go` |
| Middleware wired between auth and upstream | `proxy/apphandler/manager.go` |
| Auth provider stamp | `proxy/apphandler/authproxy/manager.go` |
| HTTP + websocket activity, websocket close | `proxy/apphandler/upstream/upstream.go` |
| Background-poll definition (shared with auto-suspend) | `dataapps/frameworkpoll/frameworkpoll.go` |
| Sign-out end | `proxy/apphandler/apphandler.go` |
| Source and sink provisioning | `scripts/stream-sessions-setup.sh` |
