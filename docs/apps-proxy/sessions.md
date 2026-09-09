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
| `heartbeat` | A flush: on the throttle interval while the session is active (default 5 min), and additionally when a websocket closes or the proxy shuts down gracefully. Carries identity, so a session that starts anonymous and authenticates later still gets its user. |
| `session_end` | The user signed out (`/_proxy/sign_out`). **The only end there is, and terminal** — at most one per session id, because signing out clears the cookie. |

Identity is **how the provider names the user**, injected as `X-Kbc-User-Id`:
the OIDC **subject claim** for every provider that issues an ID token, and the
**account login** for GitHub, which issues none. Neither is taken from the
e-mail claim, and no display name is recorded. An OIDC issuer is free to use
the e-mail address as its subject, so the value can still look like one — that
is the issuer's choice, not something this asks for.

The two differ in how long they hold. A subject claim is stable for the life of
the account, across e-mail and display-name changes. A GitHub login is not: the
owner can change it, and a released login can be taken over by a different
account. Treat a GitHub user id as correct at the time it was recorded, not as a
permanent key.

It is empty for a shared-password app and for a path with `authRequired: false`.
Those sessions carry a session id and nothing else.

The value is only unique within one provider, so distinct users must be counted
over `(auth_provider_id, provider_user_id)`, never `provider_user_id` alone.

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
             THEN event_time END)                   AS signed_out_at,
    MAX(SAFE_CAST(idle_timeout_seconds AS INT64))   AS idle_timeout_seconds,
    MAX(app_id)                                     AS app_id,
    MAX(project_id)                                 AS project_id,
    MAX(NULLIF(provider_user_id, ''))               AS provider_user_id,
    MAX(NULLIF(auth_provider_id, ''))               AS auth_provider_id,
    MAX(NULLIF(auth_provider_type, ''))             AS auth_provider_type,
    SUM(requests)                                   AS requests,
    SUM(ws_frames)                                  AS ws_frames
FROM "in.c-data-apps"."sessions"
WHERE app_id <> 'setup-script'
GROUP BY session_id
```

`session_end` means one thing only: the user signed out. It is terminal and
there is at most one per session id, because signing out clears the cookie.

**Every other way a visit stops produces no event at all** — closing the tab,
losing the network, walking away. A websocket closing is not one of them either:
Streamlit reconnects routinely (its ~20 min cycle, a network blip, the 6 h
upstream timeout) and the same cookie carries the session on, so that close is a
`heartbeat`. So `signed_out_at` is null for almost every session, and the end
has to be computed:

```
ended_at = COALESCE(signed_out_at, last_activity_at + idle_timeout_seconds)
```

and a session is still open when `last_activity_at + idle_timeout_seconds` is in
the future. `idle_timeout_seconds` is on every row rather than assumed, because
it is settable per stack: a query spanning stacks, or one spanning a change to
the setting, cannot use a single number.

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
| `event_type` | `eventType` | `session_start` \| `heartbeat` \| `session_end`. Only a sign-out ends a session; see [§3](#3-why-an-event-model). |
| `event_time` | `eventTime` | Stamped by the proxy. Fixed-precision UTC (`2006-01-02T15:04:05.000000Z`), so that `MIN`/`MAX` order correctly even though the column is text. |
| `session_id` | `sessionId` | UUIDv7. Its timestamp prefix *is* the session start. |
| `session_start` | `sessionStart` | Decoded from `session_id`, identical on every row of a session. |
| `app_id`, `app_name`, `project_id` | app config | |
| `auth_provider_id`, `auth_provider_type` | request context | Empty when no auth was required. |
| `provider_user_id` | `X-Kbc-User-Id` | OIDC subject claim, or the account login for GitHub, which issues no ID token. Never taken from the e-mail claim. Empty for password / no-auth apps. Unique only within one provider — pair it with `auth_provider_id`. |
| `user_agent` | request | |
| `requests`, `ws_frames` | proxy counters | Deltas, not totals. |
| `idle_timeout_seconds` | `idleTimeoutSeconds` | The idle window in force when the row was written. Needed to close a session that never signed out, and recorded per row because the setting is per stack. |

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

### 5.2 Changing the event format later

The sink's column mapping is what turns an event into a row: each column names
a JSON path in the event. Change a field name in `Event` and the mapping stops
matching — and nothing fails. Stream simply finds no value at the old path and
writes the column's `defaultValue`, so the column fills with empty strings and
no error appears in the proxy, in Stream, or in Storage.

The script refuses to run against a sink whose mapping no longer matches and
prints which column drifted. Updating the mapping alone is not enough either:
the Storage table still has the old columns. To migrate a stack:

```bash
# 1. Delete the sink (async — poll the returned task).
curl -X DELETE -H "X-StorageApi-Token: $KEBOOLA_TOKEN" \
  "$STREAM_API/v1/branches/default/sources/data-app-sessions/sinks/session-events"

# 2. Delete the Storage table. Its rows go with it, so export them first if
#    the history matters.
curl -X DELETE -H "X-StorageApi-Token: $KEBOOLA_TOKEN" \
  "$STORAGE_API/v2/storage/tables/in.c-data-apps.sessions"

# 3. Clear SINK_ID from the state file and re-run. (In-place sed is not
#    portable between GNU and BSD, and this runs on both.)
perl -pi -e 's/^SINK_ID=.*/SINK_ID=""/' ./stream-sessions-state.env
bash scripts/stream-sessions-setup.sh
```

Keep `SOURCE_ID` — deleting the source would issue a new ingest URL and every
stack's `APPS_PROXY_SESSIONS_STREAM_URL` would have to be rotated with it.

Deploy the proxy that emits the new field **after** the sink is recreated. In
between, the new columns stay empty; the other way round, the old ones do.

### 5.3 Configure apps-proxy

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

### 5.4 Cookie lifetime

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

`maxSessionLength` is validated to be longer than both `idleTimeout` and
`upstream.wsTimeout`, and `heartbeatInterval` shorter than `idleTimeout`; with
tracking enabled the proxy refuses to start otherwise. A cap below the
websocket timeout would cut long visits into several sessions and inflate the
counts.

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
- **`session_end` is best-effort.** A proxy restart between the sign-out and the
  send loses it, and the session then closes through the idle window like any
  other. Always apply the fallback rather than relying on the row being there.
- **Counters can undercount, but only on a hard exit.** A graceful shutdown
  flushes every live session's pending deltas as a heartbeat before the queue
  drains, so an ordinary deploy keeps them; a SIGKILL or a crash does not.
  Eviction does not lose them either: an entry is kept for as long as the
  longest deadline a cookie could have been granted — the websocket window, not
  the idle one — because `lastSeen` moves only on a data frame, so a live
  connection whose user stepped away would otherwise lose its entry while still
  open. Session start and end times are unaffected.
- **A deploy does not end a session.** The shutdown flush emits a heartbeat, not
  an end: the cookie survives the restart and the browser reconnects, so ending
  there would split one visit into as many sessions as there are deploys.
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
    - A restart or rollout drops the cache. A graceful one flushes the pending
      deltas first; either way the session continues on the next request with
      no spurious `session_start`.
- **A websocket close does not end the visit.** It flushes what the connection
  counted as a `heartbeat` and drops the proxy's cached state, but later
  activity on the same cookie continues the same session id — which is correct
  for Streamlit, whose connection closes and reopens routinely. This is why the
  close is not an end event: calling it one would over-count sessions and
  under-report their length for anyone querying the table directly.
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
- **Only a navigation or a websocket handshake starts a session.** A page load
  fires the document plus its subresources at once, none of them yet carrying a
  cookie, so letting any request mint one reported a single visit as several —
  this was observed on canary before the check existed. `Sec-Fetch-Mode`
  decides; clients that predate it fall back to asking for `text/html`. A
  subresource still *joins* a session it has a cookie for, and is counted
  against it. The same check keeps a client that ignores `Set-Cookie` — an
  uptime monitor, a crawler — from reporting a session per request, though one
  that sends `Accept: text/html` and drops cookies still would.
- **A websocket handshake can still open a duplicate session.** The handshake is
  deliberately allowed to mint one, so that a tab whose cookie has expired keeps
  being tracked when Streamlit reconnects without reloading the page. The cost
  is that a cookieless handshake arriving alongside a page load — a reconnect
  landing at the same moment as a fresh visit — produces a second session for
  the same person. Verified against canary: a handshake with no cookie does
  return a `Set-Cookie`, and it does so even for a handshake the app then
  rejects.

  This is a deliberate trade-off, not an oversight: the alternative loses the
  activity of anyone still working in a tab that has been idle past
  `idleTimeout`. Sessions counted from this table are therefore an upper bound.
  Where an exact count matters, drop sessions whose only event is a
  `session_start` — a duplicate never gets a second event, because the browser
  keeps just one cookie and every later request carries it.
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
