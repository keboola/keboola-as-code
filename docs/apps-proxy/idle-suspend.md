# Idle Suspend: What Counts as Activity

Apps proxy suspends a running data app when nobody has used it for longer than
the workload's `autoSuspendAfterSeconds`. The proxy is the only place that sees
the traffic, so it decides what "used" means. The activity record lives in the
workload's `IdleTimer` CR (`spec.lastRequestAt`).

Code: `internal/pkg/service/appsproxy/proxy/apphandler/upstream/upstream.go`
(`trace()` and `newWebsocketProxy`), recorder and suspender in
`internal/pkg/service/appsproxy/dataapps/idletimer/`.

## 1. The Rule

| Traffic | Activity? |
|---|---|
| An HTTP request proxied to the app | Yes |
| A websocket data frame, in either direction | Yes |
| `/_stcore/health`, `/_stcore/host-config` (`streamlit.IsBackgroundPoll`) | No |
| A websocket upgrade request (the handshake) | No |
| Websocket control frames (ping, pong, close) | No |

A real new visitor is counted: the page load is an HTTP request, and the
browser sends its first websocket data frame right after the upgrade.

This rule is only for idle suspend. Session tracking (`dataapps/sessions`)
counts the upgrade, see [sessions.md](sessions.md).

A request to an app that is **already suspended** is handled before the rule.
A background poll gets a 503 "suspended" page and does not wake the app. Any
other request wakes it, a websocket upgrade included. Streamlit is safe because
it polls `/_stcore/health` before it opens the websocket. A non-Streamlit app
that reconnects its websocket without a poll first will wake a suspended app.

## 2. Why the Upgrade Does Not Count

The browser reconnects the websocket without any user input every time the
connection closes. Apps proxy itself closes it:

- when the upstream websocket timeout (`upstream.wsTimeout`, default `6h`)
  ends it,
- 1 hour after it opened, when the handshake got an app user token,
- at the login session deadline: when the session is due for refresh (about
  every hour for a session that is refreshed) or when the session cookie
  expires. See [login-session.md](login-session.md) §7.

Each of these closes writes the log line
`websocket closed at its deadline: <reason>` (`refresh due`, `cookie expiry`,
`app user token cap` or `ws timeout`).

If the reconnect counted, a forgotten tab would reset the idle timer on every
reconnect. An app with a threshold over the reconnect interval would never
suspend.

### Measurement

Streamlit 1.51.0 (the version data apps use), headless Chrome, a proxy that cut
the websocket with a TCP close and logged every data frame:

- An open tab with no input: 0 data frames in 60 s. Streamlit has no
  application-level heartbeat.
- After a reconnect, with no input for 90 s: 0 data frames in either
  direction. The only traffic was the upgrade on `/_stcore/stream`, plus
  `/_stcore/health` and `/_stcore/host-config`.
- First connect: the browser sends a 65-byte frame within 10 ms of the open,
  then the server answers.
- A real click: one browser frame (about 195 bytes) plus about 10 server
  frames.

So for Streamlit, data frames are exactly "the user did something", and the
upgrade alone is not.

## 3. Symptoms and How to Check

Available signals:

- The `IdleTimer` CR of the workload:
  `kubectl -n <ns> get idletimers <name> -o yaml`. `spec.lastRequestAt` is the
  last recorded activity. `spec.observers.<replica>.aliveAt` is when each
  replica last wrote its entry.
- Log line `suspended idle workload "<ref>"` (info, component `idletimer`). It
  does not carry the idle time or the threshold.
- Metrics:
  - `keboola.go.appsproxy.idletimer.suspends{outcome}` — suspends performed.
  - `keboola.go.appsproxy.idletimer.woke_soon_after_suspend` — workloads that
    started again within a minute of a suspend.
  - `keboola.go.appsproxy.idletimer.workloads{state}` — `candidate`,
    `no_threshold`, `unresolved`, `unvouched`.
  - `keboola.go.appsproxy.idletimer.write_failures{reason}`.

### (a) An app suspends while a user has it open and working

Likely cause: the app's real work does not produce data frames or HTTP
requests through the proxy. For example, a non-Streamlit app that only
reconnects its websocket, or that sends only control frames.

Check:

1. Find the `suspended idle workload` log line for the workload and its time.
2. Look at `woke_soon_after_suspend`. A rise means users wake apps right
   after a suspend.
3. Compare `spec.lastRequestAt` with what the user did. If the user was
   clicking but `lastRequestAt` did not move, the app's traffic is not seen as
   activity.
4. If the app is Streamlit, this rule is not the cause: a click is always a
   data frame. Check `write_failures` and `workloads{state="unvouched"}`
   instead.

### (b) An app never suspends with a tab open

Likely cause: something without user input is counted as activity.

Check:

1. Watch `spec.lastRequestAt` of the workload with the tab open and no input.
   If it moves on the websocket reconnect interval, a reconnect is counted
   as activity, against the rule in §1. The `websocket closed at its
   deadline` log lines for the app show when apps proxy closed the websocket.
2. If it moves more often, the app itself sends data frames or HTTP requests
   on a timer (a frontend heartbeat or poll). The proxy counts that by design.
3. Check `workloads{state}`: an app in `no_threshold` has no
   `autoSuspendAfterSeconds` and never suspends. An app in `unvouched` or
   `unresolved` is skipped by the suspender.
