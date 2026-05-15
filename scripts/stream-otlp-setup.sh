#!/usr/bin/env bash
# scripts/stream-otlp-setup.sh
#
# Creates an OTLP source + three signal-specific sinks (logs, metrics, traces)
# on a Keboola Stream stack. Every field from the OTLP flatten step gets its
# own column so nothing is dropped.
#
# State is saved incrementally to ./stream-otlp-state.env (relative to the
# current working directory) after each step so a partial failure can be
# resumed by re-running the script.
#
# Usage:
#   export KEBOOLA_TOKEN=<sapi-token>
#   export KEBOOLA_BRANCH_ID=<branch-id>    # 0 = default
#   bash scripts/stream-otlp-setup.sh
#
# Optional overrides:
#   STREAM_API_HOST     — defaults to stream.keboola.com
#   SOURCE_NAME         — defaults to "OTLP Source"
#   BUCKET              — storage bucket prefix, defaults to in.c-otlp
#   CLEANUP             — set to "true" to delete the source instead

set -euo pipefail

# Upfront dependency check — the script relies on jq for response parsing,
# state extraction, and payload construction. Fail fast instead of half-creating
# resources on a system without it.
command -v jq &>/dev/null || {
  echo "  ✗ jq is required but not installed. Install via your package manager (apt, brew, dnf, …)." >&2
  exit 1
}
command -v curl &>/dev/null || {
  echo "  ✗ curl is required but not installed." >&2
  exit 1
}

TOKEN="${KEBOOLA_TOKEN:?Set KEBOOLA_TOKEN}"
BRANCH_ID="${KEBOOLA_BRANCH_ID:?Set KEBOOLA_BRANCH_ID (0 = default branch)}"
STREAM_API_HOST="${STREAM_API_HOST:-stream.keboola.com}"
SOURCE_NAME="${SOURCE_NAME:-OTLP Source}"
BUCKET="${BUCKET:-in.c-otlp}"
STATE_FILE="${STATE_FILE:-./stream-otlp-state.env}"

STREAM_API="https://${STREAM_API_HOST}/v1"

pretty()  { jq .; }
header()  { echo; echo "══════════════════════════════════════════════════════"; echo "  $*"; echo "══════════════════════════════════════════════════════"; }
ok()      { echo "  ✓ $*"; }
info()    { echo "  → $*"; }
warn()    { echo "  ! $*"; }
fail()    { echo "  ✗ $*" >&2; exit 1; }

# api_post <path> <body> → prints response body; sets API_CODE and API_BODY
api_post() {
  local path="$1" body="$2"
  local raw
  raw=$(curl -s --max-time 30 -w "\n%{http_code}" -X POST \
    "${STREAM_API}${path}" \
    -H "Content-Type: application/json" \
    -H "X-StorageApi-Token: ${TOKEN}" \
    -d "${body}")
  API_CODE=$(tail -1 <<< "${raw}")
  API_BODY=$(head -n -1 <<< "${raw}")
  echo "${API_BODY}" | pretty
}

# api_get <path> → sets API_CODE and API_BODY
api_get() {
  local path="$1"
  local raw
  raw=$(curl -s --max-time 30 -w "\n%{http_code}" \
    "${STREAM_API}${path}" \
    -H "X-StorageApi-Token: ${TOKEN}")
  API_CODE=$(tail -1 <<< "${raw}")
  API_BODY=$(head -n -1 <<< "${raw}")
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
  # Create with restrictive perms — OTLP_URL embeds the write secret and the
  # default umask on shared machines may otherwise leave it readable to others.
  ( umask 077; : > "${STATE_FILE}" )
  chmod 600 "${STATE_FILE}"
  cat > "${STATE_FILE}" <<EOF
# stream-otlp-state.env — $(date -u +%Y-%m-%dT%H:%M:%SZ)
STREAM_API_HOST="${STREAM_API_HOST}"
BRANCH_ID="${BRANCH_ID}"
SOURCE_ID="${SOURCE_ID:-}"
SINK_ID_LOGS="${SINK_ID_LOGS:-}"
SINK_ID_METRICS="${SINK_ID_METRICS:-}"
SINK_ID_TRACES="${SINK_ID_TRACES:-}"
OTLP_URL="${OTLP_URL:-}"
EOF
  info "State → ${STATE_FILE}"
}

# ── Load existing state ───────────────────────────────────────────────────────

SOURCE_ID="" SINK_ID_LOGS="" SINK_ID_METRICS="" SINK_ID_TRACES="" OTLP_URL=""
if [[ -f "${STATE_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${STATE_FILE}"
  [[ -z "${SOURCE_ID}" ]] || warn "Resuming from existing state (SOURCE_ID=${SOURCE_ID})"
fi

# ── Cleanup mode ──────────────────────────────────────────────────────────────

if [[ "${CLEANUP:-false}" == "true" ]]; then
  [[ -n "${SOURCE_ID}" ]] || fail "No SOURCE_ID in ${STATE_FILE}"
  header "Cleanup — deleting source ${SOURCE_ID}"
  local_raw=$(curl -s --max-time 30 -w "\n%{http_code}" -X DELETE \
    "${STREAM_API}/branches/${BRANCH_ID}/sources/${SOURCE_ID}" \
    -H "X-StorageApi-Token: ${TOKEN}")
  http_code=$(tail -1 <<< "${local_raw}")
  delete_body=$(head -n -1 <<< "${local_raw}")
  echo "${delete_body}" | pretty
  # 404 — already gone, treat as success.
  if [[ "${http_code}" == "404" ]]; then
    rm -f "${STATE_FILE}"
    ok "Already deleted"
    exit 0
  fi
  [[ "${http_code}" -lt 400 ]] || fail "Delete failed HTTP ${http_code}"
  # Source deletion is asynchronous: the API returns 202 with a task URL.
  # Poll it before removing the state file so a delayed failure can be retried.
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

# ── 1. Create OTLP source ─────────────────────────────────────────────────────

header "1. Create OTLP source"

if [[ -n "${SOURCE_ID}" ]]; then
  ok "Already exists: ${SOURCE_ID} — skipping"
else
  # Build the payload with jq so SOURCE_NAME values containing quotes, backslashes,
  # or newlines are escaped correctly. Direct string interpolation would produce
  # invalid JSON for any non-trivial name.
  create_payload=$(jq -nc --arg name "${SOURCE_NAME}" '{name: $name, type: "otlp"}')
  api_post "/branches/${BRANCH_ID}/sources" "${create_payload}"

  if [[ "${API_CODE}" == "409" ]]; then
    warn "409 — fetching existing OTLP source by name…"
    api_get "/branches/${BRANCH_ID}/sources"
    # Filter by both name AND type so we never accidentally reuse (and later
    # delete during cleanup) an unrelated HTTP source that happens to share
    # the name.
    SOURCE_ID=$(echo "${API_BODY}" | jq -r --arg n "${SOURCE_NAME}" \
      '.sources[] | select(.name==$n and .type=="otlp") | .sourceId' | head -1)
    if [[ -z "${SOURCE_ID}" ]]; then
      # A non-OTLP source matched the name → bail out instead of reusing it.
      conflicting_type=$(echo "${API_BODY}" | jq -r --arg n "${SOURCE_NAME}" \
        '[.sources[] | select(.name==$n) | .type] | first // empty')
      if [[ -n "${conflicting_type}" ]]; then
        fail "A source named '${SOURCE_NAME}' already exists with type '${conflicting_type}'. Pick a different SOURCE_NAME."
      fi
      fail "OTLP source '${SOURCE_NAME}' not found in list"
    fi
    ok "Reusing OTLP source: ${SOURCE_ID}"
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

# ── 2. Fetch OTLP URL ─────────────────────────────────────────────────────────

header "2. Get OTLP ingestion URL"

if [[ -n "${OTLP_URL}" ]]; then
  ok "Already have: ${OTLP_URL}"
else
  api_get "/branches/${BRANCH_ID}/sources/${SOURCE_ID}"
  OTLP_URL=$(echo "${API_BODY}" | jq -r '.otlp.url')
  [[ -n "${OTLP_URL}" && "${OTLP_URL}" != "null" ]] || fail "Missing .otlp.url"
  ok "OTLP URL: ${OTLP_URL}"
  save_state
fi

# ── Sink helper ───────────────────────────────────────────────────────────────

create_sink() {
  local sink_var="$1" sink_name="$2" table_id="$3" columns_json="$4" signals_json="$5"

  header "Create sink: ${sink_name} → ${table_id}"

  local current_id="${!sink_var}"
  if [[ -n "${current_id}" ]]; then
    ok "Already exists: ${current_id} — skipping"
    return
  fi

  local body
  body=$(jq -nc \
    --arg name "${sink_name}" \
    --arg tableId "${table_id}" \
    --argjson columns "${columns_json}" \
    --argjson signals "${signals_json}" \
    '{name: $name, type: "table", allowedSignals: $signals, table: {type: "keboola", tableId: $tableId, mapping: {columns: $columns}}}')

  api_post "/branches/${BRANCH_ID}/sources/${SOURCE_ID}/sinks" "${body}"

  local sink_id
  if [[ "${API_CODE}" == "409" ]]; then
    warn "409 — fetching existing sink…"
    api_get "/branches/${BRANCH_ID}/sources/${SOURCE_ID}/sinks"
    # An existing sink with the same name might point at a different table
    # or carry a different allowedSignals filter. Verify both before reusing
    # so a stale fixture doesn't silently route to the wrong table/signal.
    matched=$(echo "${API_BODY}" | jq -c --arg n "${sink_name}" \
      '[.sinks[] | select(.name==$n)] | first // empty')
    if [[ -z "${matched}" ]]; then
      fail "Sink '${sink_name}' not found in list"
    fi
    sink_id=$(jq -r '.sinkId' <<< "${matched}")
    existing_table=$(jq -r '.table.tableId // empty' <<< "${matched}")
    existing_signals=$(jq -c '.allowedSignals // []' <<< "${matched}")
    expected_signals=$(jq -c '.' <<< "${signals_json}")
    if [[ "${existing_table}" != "${table_id}" ]]; then
      fail "Sink '${sink_name}' (${sink_id}) targets table '${existing_table}', expected '${table_id}'. Refusing to reuse."
    fi
    if [[ "${existing_signals}" != "${expected_signals}" ]]; then
      fail "Sink '${sink_name}' (${sink_id}) has allowedSignals=${existing_signals}, expected ${expected_signals}. Refusing to reuse."
    fi
    ok "Reusing sink with matching mapping: ${sink_id}"
  elif [[ "${API_CODE}" -ge 400 ]]; then
    fail "Create sink failed HTTP ${API_CODE}"
  else
    local task_url
    task_url=$(echo "${API_BODY}" | jq -r '.url')
    info "Polling…"
    local task
    task=$(poll_task "${task_url}")
    sink_id=$(echo "${task}" | jq -r '.outputs.sinkId')
    ok "Created: ${sink_id}"
  fi

  # Write back to the named variable and persist
  printf -v "${sink_var}" '%s' "${sink_id}"
  save_state
}

# ── 3. Logs sink ──────────────────────────────────────────────────────────────
# One row per log record. Every field from flatten_logs.go gets its own column.

LOGS_COLUMNS=$(cat <<'EOF'
[
  {"type":"datetime","name":"datetime"},
  {"type":"template","name":"signal",             "template":{"language":"jsonnet","content":"\"logs\""}},
  {"type":"template","name":"timestamp",          "template":{"language":"jsonnet","content":"Body(\"timestamp\", \"\")"}},
  {"type":"template","name":"observed_timestamp", "template":{"language":"jsonnet","content":"Body(\"observed_timestamp\", \"\")"}},
  {"type":"template","name":"severity_number",    "template":{"language":"jsonnet","content":"Body(\"severity_number\", \"\")"}},
  {"type":"template","name":"severity_text",      "template":{"language":"jsonnet","content":"Body(\"severity_text\", \"\")"}},
  {"type":"template","name":"body",               "template":{"language":"jsonnet","content":"Body(\"body\", \"\")"}},
  {"type":"template","name":"flags",              "template":{"language":"jsonnet","content":"Body(\"flags\", \"\")"}},
  {"type":"template","name":"trace_id",           "template":{"language":"jsonnet","content":"Body(\"trace_id\", \"\")"}},
  {"type":"template","name":"span_id",            "template":{"language":"jsonnet","content":"Body(\"span_id\", \"\")"}},
  {"type":"template","name":"attributes",         "template":{"language":"jsonnet","content":"Body(\"attributes\", {})"}},
  {"type":"template","name":"resource",           "template":{"language":"jsonnet","content":"Body(\"resource\", {})"}},
  {"type":"template","name":"scope_name",         "template":{"language":"jsonnet","content":"Body(\"scope\", {})[\"name\"]"}},
  {"type":"template","name":"scope_version",      "template":{"language":"jsonnet","content":"Body(\"scope\", {})[\"version\"]"}}
]
EOF
)

create_sink "SINK_ID_LOGS" "OTLP Logs" "${BUCKET}.logs" "${LOGS_COLUMNS}" '["logs"]'

# ── 4. Metrics sink ───────────────────────────────────────────────────────────
# One row per data point. Covers gauge, sum, histogram, exp-histogram, summary.

METRICS_COLUMNS=$(cat <<'EOF'
[
  {"type":"datetime","name":"datetime"},
  {"type":"template","name":"signal",                  "template":{"language":"jsonnet","content":"\"metrics\""}},
  {"type":"template","name":"timestamp",               "template":{"language":"jsonnet","content":"Body(\"timestamp\", \"\")"}},
  {"type":"template","name":"start_timestamp",         "template":{"language":"jsonnet","content":"Body(\"start_timestamp\", \"\")"}},
  {"type":"template","name":"metric_name",             "template":{"language":"jsonnet","content":"Body(\"metric_name\", \"\")"}},
  {"type":"template","name":"metric_description",      "template":{"language":"jsonnet","content":"Body(\"metric_description\", \"\")"}},
  {"type":"template","name":"metric_unit",             "template":{"language":"jsonnet","content":"Body(\"metric_unit\", \"\")"}},
  {"type":"template","name":"metric_type",             "template":{"language":"jsonnet","content":"Body(\"metric_type\", \"\")"}},
  {"type":"template","name":"value",                   "template":{"language":"jsonnet","content":"Body(\"value\", \"\")"}},
  {"type":"template","name":"count",                   "template":{"language":"jsonnet","content":"Body(\"count\", \"\")"}},
  {"type":"template","name":"sum",                     "template":{"language":"jsonnet","content":"Body(\"sum\", \"\")"}},
  {"type":"template","name":"min",                     "template":{"language":"jsonnet","content":"Body(\"min\", \"\")"}},
  {"type":"template","name":"max",                     "template":{"language":"jsonnet","content":"Body(\"max\", \"\")"}},
  {"type":"template","name":"bucket_counts",           "template":{"language":"jsonnet","content":"Body(\"bucket_counts\", [])"}},
  {"type":"template","name":"explicit_bounds",         "template":{"language":"jsonnet","content":"Body(\"explicit_bounds\", [])"}},
  {"type":"template","name":"is_monotonic",            "template":{"language":"jsonnet","content":"Body(\"is_monotonic\", \"\")"}},
  {"type":"template","name":"aggregation_temporality", "template":{"language":"jsonnet","content":"Body(\"aggregation_temporality\", \"\")"}},
  {"type":"template","name":"scale",                   "template":{"language":"jsonnet","content":"Body(\"scale\", \"\")"}},
  {"type":"template","name":"zero_count",              "template":{"language":"jsonnet","content":"Body(\"zero_count\", \"\")"}},
  {"type":"template","name":"quantile_values",         "template":{"language":"jsonnet","content":"Body(\"quantile_values\", [])"}},
  {"type":"template","name":"attributes",              "template":{"language":"jsonnet","content":"Body(\"attributes\", {})"}},
  {"type":"template","name":"resource",                "template":{"language":"jsonnet","content":"Body(\"resource\", {})"}},
  {"type":"template","name":"scope_name",              "template":{"language":"jsonnet","content":"Body(\"scope\", {})[\"name\"]"}},
  {"type":"template","name":"scope_version",           "template":{"language":"jsonnet","content":"Body(\"scope\", {})[\"version\"]"}}
]
EOF
)

create_sink "SINK_ID_METRICS" "OTLP Metrics" "${BUCKET}.metrics" "${METRICS_COLUMNS}" '["metrics"]'

# ── 5. Traces sink ────────────────────────────────────────────────────────────
# One row per span. Events and links stored as JSON arrays.

TRACES_COLUMNS=$(cat <<'EOF'
[
  {"type":"datetime","name":"datetime"},
  {"type":"template","name":"signal",         "template":{"language":"jsonnet","content":"\"traces\""}},
  {"type":"template","name":"timestamp",      "template":{"language":"jsonnet","content":"Body(\"timestamp\", \"\")"}},
  {"type":"template","name":"end_timestamp",  "template":{"language":"jsonnet","content":"Body(\"end_timestamp\", \"\")"}},
  {"type":"template","name":"trace_id",       "template":{"language":"jsonnet","content":"Body(\"trace_id\", \"\")"}},
  {"type":"template","name":"span_id",        "template":{"language":"jsonnet","content":"Body(\"span_id\", \"\")"}},
  {"type":"template","name":"parent_span_id", "template":{"language":"jsonnet","content":"Body(\"parent_span_id\", \"\")"}},
  {"type":"template","name":"trace_state",    "template":{"language":"jsonnet","content":"Body(\"trace_state\", \"\")"}},
  {"type":"template","name":"name",           "template":{"language":"jsonnet","content":"Body(\"name\", \"\")"}},
  {"type":"template","name":"kind",           "template":{"language":"jsonnet","content":"Body(\"kind\", \"\")"}},
  {"type":"template","name":"flags",          "template":{"language":"jsonnet","content":"Body(\"flags\", \"\")"}},
  {"type":"template","name":"status_code",    "template":{"language":"jsonnet","content":"Body(\"status_code\", \"\")"}},
  {"type":"template","name":"status_message", "template":{"language":"jsonnet","content":"Body(\"status_message\", \"\")"}},
  {"type":"template","name":"attributes",     "template":{"language":"jsonnet","content":"Body(\"attributes\", {})"}},
  {"type":"template","name":"events",         "template":{"language":"jsonnet","content":"Body(\"events\", [])"}},
  {"type":"template","name":"links",          "template":{"language":"jsonnet","content":"Body(\"links\", [])"}},
  {"type":"template","name":"resource",       "template":{"language":"jsonnet","content":"Body(\"resource\", {})"}},
  {"type":"template","name":"scope_name",     "template":{"language":"jsonnet","content":"Body(\"scope\", {})[\"name\"]"}},
  {"type":"template","name":"scope_version",  "template":{"language":"jsonnet","content":"Body(\"scope\", {})[\"version\"]"}}
]
EOF
)

create_sink "SINK_ID_TRACES" "OTLP Traces" "${BUCKET}.traces" "${TRACES_COLUMNS}" '["traces"]'

# ── Done ──────────────────────────────────────────────────────────────────────

header "Done — OTLP pipeline ready"
echo
echo "  Source:         ${SOURCE_ID}"
echo "  Sink (logs):    ${SINK_ID_LOGS}    → ${BUCKET}.logs"
echo "  Sink (metrics): ${SINK_ID_METRICS} → ${BUCKET}.metrics"
echo "  Sink (traces):  ${SINK_ID_TRACES}  → ${BUCKET}.traces"
echo
echo "══════════════════════════════════════════════════════"
echo "  Step 1 — Smoke-test the endpoint"
echo "══════════════════════════════════════════════════════"
echo
echo "  Send a minimal logs batch with curl (protobuf payload omitted for brevity —"
echo "  any OTel SDK will produce valid encodings):"
echo
echo "    curl -i -X POST \"${OTLP_URL}/v1/logs\" \\"
echo "      -H 'Content-Type: application/json' \\"
echo "      -d '{\"resourceLogs\":[{\"scopeLogs\":[{\"logRecords\":[{\"body\":{\"stringValue\":\"hello\"}}]}]}]}'"
echo
echo "  Expect HTTP 200 with an empty/partial-success body."
echo
echo "══════════════════════════════════════════════════════"
echo "  Step 2 — Configure your OTel SDK"
echo "══════════════════════════════════════════════════════"
echo
echo "  Set these two environment variables in every service:"
echo
echo "    OTEL_EXPORTER_OTLP_ENDPOINT=${OTLP_URL}"
echo "    OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf"
echo
echo "  The secret is embedded in the URL — no extra auth header needed."
echo
echo "  Language quick-starts:"
echo
echo "  Go (using OTLP env vars — works with any OTel Go SDK exporter):"
echo "    import _ \"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp\""
echo "    // Set env vars above, then use sdk.WithEnv() on the provider."
echo
echo "  Python:"
echo "    pip install opentelemetry-exporter-otlp"
echo "    # Env vars are picked up automatically by OTLPLogExporter(), etc."
echo
echo "  Node.js:"
echo "    npm install @opentelemetry/exporter-logs-otlp-http"
echo "    // Env vars are read by @opentelemetry/exporter-*-otlp-http packages."
echo
echo "  Java / Spring Boot:"
echo "    management.otlp.logging.endpoint=\${OTEL_EXPORTER_OTLP_ENDPOINT}/v1/logs"
echo "    # or set OTEL_EXPORTER_OTLP_ENDPOINT as a JVM system property."
echo
echo "══════════════════════════════════════════════════════"
echo "  Step 3 — Verify rows in Keboola Storage"
echo "══════════════════════════════════════════════════════"
echo
echo "  Wait ~30 s after the first payload, then run in Keboola Transformations:"
echo
echo "  Snowflake:"
echo "    SELECT datetime, timestamp, severity_text, body,"
echo "           attributes:\"user.id\"::string AS user_id"
echo "    FROM \"${BUCKET}\".\"logs\""
echo "    ORDER BY datetime DESC LIMIT 20;"
echo
echo "  BigQuery:"
echo "    SELECT datetime, timestamp, severity_text, body,"
echo "           JSON_VALUE(attributes, '$.user.id') AS user_id"
echo "    FROM \`${BUCKET//./_}.logs\`"
echo "    ORDER BY datetime DESC LIMIT 20;"
echo
echo "══════════════════════════════════════════════════════"
echo "  Tear down"
echo "══════════════════════════════════════════════════════"
echo
echo "    CLEANUP=true bash scripts/stream-otlp-setup.sh"
