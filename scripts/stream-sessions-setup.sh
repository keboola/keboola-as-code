#!/usr/bin/env bash
# scripts/stream-sessions-setup.sh
#
# Creates the Stream HTTP source and table sink that apps-proxy writes data app
# session events to (AJDA-3219). The ingest URL, which embeds the write secret,
# is written to the state file (mode 600) rather than to stdout; put it into the
# apps-proxy config as APPS_PROXY_SESSIONS_STREAM_URL.
#
# One source per stack, in a Keboola-internal project on that stack — not in
# the customer project that owns the app. The project id is therefore fixed
# per stack and the URL is a static piece of configuration.
#
# The sink writes one row per event, never updating an earlier one: Stream is
# append-only, so a session is reconstructed by grouping rows on session_id.
# See docs/apps-proxy/sessions.md.
#
# State is saved to ./stream-sessions-state.env after each step, so a partial
# failure can be resumed by re-running the script.
#
# Usage:
#   export KEBOOLA_TOKEN=<sapi-token-of-the-internal-project>
#   bash scripts/stream-sessions-setup.sh
#
# Optional overrides:
#   KEBOOLA_BRANCH_ID — branch id, or "default" (the default). Note this is not
#                       the Storage API convention: Stream rejects "0".
#   STREAM_API_HOST — defaults to stream.keboola.com
#   SOURCE_NAME     — defaults to "Data App Sessions"
#   TABLE_ID        — defaults to in.c-data-apps.sessions
#   CLEANUP         — set to "true" to delete the source instead

set -euo pipefail

command -v jq &>/dev/null || {
  echo "  ✗ jq is required but not installed. Install via your package manager (apt, brew, dnf, …)." >&2
  exit 1
}
command -v curl &>/dev/null || {
  echo "  ✗ curl is required but not installed." >&2
  exit 1
}

TOKEN="${KEBOOLA_TOKEN:?Set KEBOOLA_TOKEN}"
# Stream takes a numeric branch id or the literal "default". It rejects "0",
# which is the Storage API's alias for the default branch, with a confusing
# "Branch id:\"0\" was not found".
BRANCH_ID="${KEBOOLA_BRANCH_ID:-default}"
STREAM_API_HOST="${STREAM_API_HOST:-stream.keboola.com}"
SOURCE_NAME="${SOURCE_NAME:-Data App Sessions}"
SINK_NAME="${SINK_NAME:-Session Events}"
TABLE_ID="${TABLE_ID:-in.c-data-apps.sessions}"
STATE_FILE="${STATE_FILE:-./stream-sessions-state.env}"

STREAM_API="https://${STREAM_API_HOST}/v1"

pretty() { jq . 2>/dev/null || cat; }
header() { echo; echo "══════════════════════════════════════════════════════"; echo "  $*"; echo "══════════════════════════════════════════════════════"; }
ok()     { echo "  ✓ $*"; }
info()   { echo "  → $*"; }
warn()   { echo "  ! $*"; }
fail()   { echo "  ✗ $*" >&2; exit 1; }

# api_post <path> <body> → prints response body; sets API_CODE and API_BODY
api_post() {
  local path="$1" body="$2"
  local raw
  raw=$(curl -s --max-time 30 -w "\n%{http_code}" -X POST \
    "${STREAM_API}${path}" \
    -H "Content-Type: application/json" \
    -H "X-StorageApi-Token: ${TOKEN}" \
    -d "${body}")
  API_CODE="${raw##*$'\n'}"
  API_BODY="${raw%$'\n'*}"
  echo "${API_BODY}" | pretty
}

# api_get <path> → sets API_CODE and API_BODY
api_get() {
  local path="$1"
  local raw
  raw=$(curl -s --max-time 30 -w "\n%{http_code}" \
    "${STREAM_API}${path}" \
    -H "X-StorageApi-Token: ${TOKEN}")
  API_CODE="${raw##*$'\n'}"
  API_BODY="${raw%$'\n'*}"
}

poll_task() {
  local task_url="$1"
  local elapsed=0 response status
  while true; do
    response=$(curl -s --max-time 30 "${task_url}" -H "X-StorageApi-Token: ${TOKEN}")
    status=$(echo "${response}" | jq -r '.status')
    if [[ "${status}" != "processing" ]]; then
      echo "${response}"
      [[ "${status}" == "success" ]] || fail "Task failed: $(echo "${response}" | jq -r '.error // .result // "unknown"')"
      return 0
    fi
    elapsed=$((elapsed + 2))
    [[ ${elapsed} -lt 60 ]] || fail "Task timed out after 60s"
    sleep 2
  done
}

save_state() {
  # Restrictive perms — INGEST_URL embeds the write secret, and the default
  # umask on a shared machine may otherwise leave it world-readable.
  ( umask 077; : > "${STATE_FILE}" )
  chmod 600 "${STATE_FILE}"
  cat > "${STATE_FILE}" <<EOF
# stream-sessions-state.env — $(date -u +%Y-%m-%dT%H:%M:%SZ)
STREAM_API_HOST="${STREAM_API_HOST}"
BRANCH_ID="${BRANCH_ID}"
SOURCE_ID="${SOURCE_ID:-}"
SINK_ID="${SINK_ID:-}"
INGEST_URL="${INGEST_URL:-}"
EOF
  info "State → ${STATE_FILE}"
}

# ── Load existing state ───────────────────────────────────────────────────────

SOURCE_ID="" SINK_ID="" INGEST_URL=""
if [[ -f "${STATE_FILE}" ]]; then
  # Load into a subshell and take only the object ids. Sourcing directly would
  # also overwrite STREAM_API_HOST and BRANCH_ID, so a resume against a
  # different stack or branch would silently use the persisted ones while
  # STREAM_API had already been built from the environment.
  state=$(
    # shellcheck disable=SC1090
    source "${STATE_FILE}"
    printf '%s\n%s\n%s\n%s\n%s\n' \
      "${SOURCE_ID:-}" "${SINK_ID:-}" "${INGEST_URL:-}" "${STREAM_API_HOST:-}" "${BRANCH_ID:-}"
  )
  { read -r SOURCE_ID; read -r SINK_ID; read -r INGEST_URL; read -r state_host; read -r state_branch; } <<< "${state}"

  if [[ -n "${state_host}" && "${state_host}" != "${STREAM_API_HOST}" ]]; then
    fail "${STATE_FILE} was written for stack '${state_host}', but STREAM_API_HOST is '${STREAM_API_HOST}'. Use a different STATE_FILE."
  fi
  if [[ -n "${state_branch}" && "${state_branch}" != "${BRANCH_ID}" ]]; then
    fail "${STATE_FILE} was written for branch '${state_branch}', but KEBOOLA_BRANCH_ID is '${BRANCH_ID}'. Use a different STATE_FILE."
  fi
  [[ -z "${SOURCE_ID}" ]] || warn "Resuming from existing state (SOURCE_ID=${SOURCE_ID})"
fi

# ── Cleanup mode ──────────────────────────────────────────────────────────────

if [[ "${CLEANUP:-false}" == "true" ]]; then
  [[ -n "${SOURCE_ID}" ]] || fail "No SOURCE_ID in ${STATE_FILE}"
  header "Cleanup — deleting source ${SOURCE_ID}"
  raw=$(curl -s --max-time 30 -w "\n%{http_code}" -X DELETE \
    "${STREAM_API}/branches/${BRANCH_ID}/sources/${SOURCE_ID}" \
    -H "X-StorageApi-Token: ${TOKEN}")
  http_code="${raw##*$'\n'}"
  delete_body="${raw%$'\n'*}"
  echo "${delete_body}" | pretty
  # 404 — already gone, treat as success.
  if [[ "${http_code}" == "404" ]]; then
    rm -f "${STATE_FILE}"
    ok "Already deleted"
    exit 0
  fi
  [[ "${http_code}" -lt 400 ]] || fail "Delete failed HTTP ${http_code}"
  # Deletion is asynchronous: poll the task before dropping the state file so a
  # delayed failure can still be retried.
  delete_task_url=$(echo "${delete_body}" | jq -r '.url // empty')
  if [[ -n "${delete_task_url}" ]]; then
    info "Polling delete task…"
    poll_task "${delete_task_url}" > /dev/null
    ok "Delete task completed"
  fi
  rm -f "${STATE_FILE}"
  ok "Done"
  exit 0
fi

# ── 1. Create HTTP source ─────────────────────────────────────────────────────

header "1. Create HTTP source"

if [[ -n "${SOURCE_ID}" ]]; then
  ok "Already exists: ${SOURCE_ID} — skipping"
else
  # Build the payload with jq so a name containing quotes or backslashes is
  # escaped correctly.
  create_payload=$(jq -nc --arg name "${SOURCE_NAME}" \
    '{name: $name, type: "http", description: "Data app end-user session events from apps-proxy (AJDA-3219)."}')
  api_post "/branches/${BRANCH_ID}/sources" "${create_payload}"

  if [[ "${API_CODE}" == "409" ]]; then
    warn "409 — fetching existing source by name…"
    api_get "/branches/${BRANCH_ID}/sources"
    # Match on type as well, so an unrelated source that happens to share the
    # name is never reused (and never deleted by CLEANUP).
    SOURCE_ID=$(echo "${API_BODY}" | jq -r --arg n "${SOURCE_NAME}" \
      'first(.sources[] | select(.name==$n and .type=="http") | .sourceId) // empty')
    if [[ -z "${SOURCE_ID}" ]]; then
      conflicting_type=$(echo "${API_BODY}" | jq -r --arg n "${SOURCE_NAME}" \
        '[.sources[] | select(.name==$n) | .type] | first // empty')
      if [[ -n "${conflicting_type}" ]]; then
        fail "A source named '${SOURCE_NAME}' already exists with type '${conflicting_type}'. Pick a different SOURCE_NAME."
      fi
      fail "HTTP source '${SOURCE_NAME}' not found in list"
    fi
    ok "Reusing source: ${SOURCE_ID}"
  elif [[ "${API_CODE}" -ge 400 ]]; then
    fail "Create source failed HTTP ${API_CODE}"
  else
    TASK_URL=$(echo "${API_BODY}" | jq -r '.url')
    info "Polling…"
    TASK=$(poll_task "${TASK_URL}")
    SOURCE_ID=$(echo "${TASK}" | jq -r '.outputs.sourceId')
    ok "Created: ${SOURCE_ID}"
  fi
  save_state
fi

# ── 2. Fetch ingest URL ───────────────────────────────────────────────────────

header "2. Get ingest URL"

if [[ -n "${INGEST_URL}" ]]; then
  ok "Already have it"
else
  api_get "/branches/${BRANCH_ID}/sources/${SOURCE_ID}"
  INGEST_URL=$(echo "${API_BODY}" | jq -r '.http.url')
  [[ -n "${INGEST_URL}" && "${INGEST_URL}" != "null" ]] || fail "Missing .http.url"
  ok "Ingest URL obtained (contains the write secret)"
  save_state
fi

# ── 3. Create table sink ──────────────────────────────────────────────────────

header "3. Create table sink → ${TABLE_ID}"

# Column order is the table's column order. Every field of the event payload
# gets its own column so nothing has to be parsed out of a JSON blob later.
#
# received_at is Stream's own arrival timestamp, kept alongside event_time
# (which apps-proxy stamps) so a queueing delay is visible in the data.
#
# Note the deliberate absence of the `ip` and `headers` column types: those
# describe the request Stream received, which comes from apps-proxy — not from
# the end user. Anything about the user is sent explicitly in the body.
COLUMNS=$(jq -nc '[
  {type: "datetime", name: "received_at"},
  {type: "path", name: "event_id",           path: "eventId",          rawString: true, defaultValue: ""},
  {type: "path", name: "event_type",         path: "eventType",        rawString: true, defaultValue: ""},
  {type: "path", name: "event_time",         path: "eventTime",        rawString: true, defaultValue: ""},
  {type: "path", name: "session_id",         path: "sessionId",        rawString: true, defaultValue: ""},
  {type: "path", name: "session_start",      path: "sessionStart",     rawString: true, defaultValue: ""},
  {type: "path", name: "app_id",             path: "appId",            rawString: true, defaultValue: ""},
  {type: "path", name: "app_name",           path: "appName",          rawString: true, defaultValue: ""},
  {type: "path", name: "project_id",         path: "projectId",        rawString: true, defaultValue: ""},
  {type: "path", name: "auth_provider_id",   path: "authProviderId",   rawString: true, defaultValue: ""},
  {type: "path", name: "auth_provider_type", path: "authProviderType", rawString: true, defaultValue: ""},
  {type: "path", name: "user_email",         path: "userEmail",        rawString: true, defaultValue: ""},
  {type: "path", name: "user_name",          path: "userName",         rawString: true, defaultValue: ""},
  {type: "path", name: "user_agent",         path: "userAgent",        rawString: true, defaultValue: ""},
  {type: "path", name: "requests",           path: "requests",         rawString: true, defaultValue: "0"},
  {type: "path", name: "ws_frames",          path: "wsFrames",         rawString: true, defaultValue: "0"},
  {type: "path", name: "end_reason",         path: "endReason",        rawString: true, defaultValue: ""}
]')

if [[ -n "${SINK_ID}" ]]; then
  ok "Already exists: ${SINK_ID} — skipping"
else
  body=$(jq -nc \
    --arg name "${SINK_NAME}" \
    --arg tableId "${TABLE_ID}" \
    --argjson columns "${COLUMNS}" \
    '{name: $name, type: "table", table: {type: "keboola", tableId: $tableId, mapping: {columns: $columns}}}')

  api_post "/branches/${BRANCH_ID}/sources/${SOURCE_ID}/sinks" "${body}"

  if [[ "${API_CODE}" == "409" ]]; then
    warn "409 — fetching existing sink…"
    api_get "/branches/${BRANCH_ID}/sources/${SOURCE_ID}/sinks"
    matched=$(echo "${API_BODY}" | jq -c --arg n "${SINK_NAME}" \
      '[.sinks[] | select(.name==$n)] | first // empty')
    [[ -n "${matched}" ]] || fail "Sink '${SINK_NAME}' not found in list"
    SINK_ID=$(jq -r '.sinkId' <<< "${matched}")
    existing_table=$(jq -r '.table.tableId // empty' <<< "${matched}")
    # An existing sink with the same name may point at a different table.
    # Refuse to reuse it rather than silently routing events elsewhere.
    if [[ "${existing_table}" != "${TABLE_ID}" ]]; then
      fail "Sink '${SINK_NAME}' (${SINK_ID}) targets table '${existing_table}', expected '${TABLE_ID}'. Refusing to reuse."
    fi
    ok "Reusing sink: ${SINK_ID}"
  elif [[ "${API_CODE}" -ge 400 ]]; then
    fail "Create sink failed HTTP ${API_CODE}"
  else
    task_url=$(echo "${API_BODY}" | jq -r '.url')
    info "Polling…"
    task=$(poll_task "${task_url}")
    SINK_ID=$(echo "${task}" | jq -r '.outputs.sinkId')
    ok "Created: ${SINK_ID}"
  fi
  save_state
fi

# ── 4. Smoke test ─────────────────────────────────────────────────────────────

header "4. Send a test event"

# Fixed fractional precision, matching what the proxy emits. The column is text
# in Storage, so a row with a different precision would not order correctly
# against the real ones.
test_event=$(jq -nc --arg now "$(date -u +%Y-%m-%dT%H:%M:%S.000000Z)" '{
  eventId: "00000000-0000-7000-8000-000000000000",
  eventType: "session_start",
  eventTime: $now,
  sessionId: "00000000-0000-7000-8000-000000000001",
  sessionStart: $now,
  appId: "setup-script",
  appName: "setup-script",
  projectId: "0",
  authProviderId: "",
  authProviderType: "",
  userEmail: "",
  userName: "",
  userAgent: "stream-sessions-setup.sh",
  requests: 0,
  wsFrames: 0,
  endReason: ""
}')

raw=$(curl -s --max-time 30 -w "\n%{http_code}" -X POST "${INGEST_URL}" \
  -H "Content-Type: application/json" -d "${test_event}")
http_code="${raw##*$'\n'}"
[[ "${http_code}" -lt 400 ]] || fail "Test event rejected with HTTP ${http_code}: ${raw%$'\n'*}"
ok "Test event accepted (appId=setup-script — filter it out when querying)"

# ── Done ──────────────────────────────────────────────────────────────────────

header "Done"
cat <<EOF
  Source: ${SOURCE_ID}
  Sink:   ${SINK_ID} → ${TABLE_ID}

  Put the ingest URL into the apps-proxy config. It contains the write secret,
  so it belongs in the encrypted kbc-stacks secrets, not in values.yaml:

    APPS_PROXY_SESSIONS_STREAM_URL=<see ${STATE_FILE}>

  Leaving it unset keeps session tracking switched off, which is how stacks
  without Stream stay unaffected.
EOF
