import { check } from 'k6';
import http from "k6/http";
import { TextEncoder } from "https://raw.githubusercontent.com/inexorabletash/text-encoding/master/index.js"
import { Counter } from 'k6/metrics';
import { randomItem } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import { URL } from 'https://jslib.k6.io/url/1.0.0/index.js';
import { Api, randomPayloads } from "./api.js";

const SCENARIO = __ENV.K6_SCENARIO || "constant"; // constant or ramping
const TABLE_MAPPING = __ENV.K6_TABLE_MAPPING || "static"; // static or template

// Target
const API_TOKEN = __ENV.K6_API_TOKEN;
const API_HOST = __ENV.K6_API_HOST || "http://localhost:8001";
const OVERWRITE_SOURCE_HOST = __ENV.K6_OVERWRITE_SOURCE_HOST; // to call directly k8s service

// Common for all scenarios
const PARALLEL_REQS_PER_USER = __ENV.K6_PARALLEL_REQS_PER_USER || 1;

// Constant VUs / iterations scenario
const CONST_VIRTUAL_USERS = __ENV.K6_CONST_VIRTUAL_USERS || 1000;
const CONST_TOTAL_REQUESTS = __ENV.K6_CONST_TOTAL_REQUESTS || 1000000;
const CONST_TIMEOUT = __ENV.K6_CONST_TIMEOUT || "20m";

// Ramping scenario
const RAMPING_MAX_VIRTUAL_USERS = __ENV.K6_RAMPING_MAX_VIRTUAL_USERS || 1000;
const RAMPING_UP_DURATION = __ENV.K6_RAMPING_UP_DURATION || "2m";
const RAMPING_STABLE_DURATION = __ENV.K6_RAMPING_STABLE_DURATION || "10m";
const RAMPING_DOWN_DURATION = __ENV.K6_RAMPING_DOWN_DURATION || "2m";

// Stream configuration
const SYNC_MODE = __ENV.STREAM_SYNC_MODE || "disk"; // cache / disk
const SYNC_WAIT = __ENV.STREAM_SYNC_WAIT || "1"; // 1 = enabled, 0 = disabled

// Payload configuration
const PAYLOAD_SIZE = __ENV.STREAM_PAYLOAD_SIZE || 1 // 1 = 54B, 1024 = ~1KB (1077B), 1048500 = ~1MB (1048576)

// Available scenarios, use K6_SCENARIO env to select one
const scenarios = {
  constant: {
    executor: "shared-iterations",
    vus: CONST_VIRTUAL_USERS,
    iterations: CONST_TOTAL_REQUESTS / PARALLEL_REQS_PER_USER,
    maxDuration: CONST_TIMEOUT,
  },
  ramping: {
    executor: 'ramping-vus',
    startVUs: 0,
    stages: [
      { target: RAMPING_MAX_VIRTUAL_USERS, duration: RAMPING_UP_DURATION },
      { target: RAMPING_MAX_VIRTUAL_USERS, duration: RAMPING_STABLE_DURATION },
      { target: 0, duration: RAMPING_DOWN_DURATION },
    ],
  }
}

const mappings = {
  static: {
    columns: [
      { type: "uuid", name: "id", primaryKey: true },
      { type: "datetime", name: "datetime" },
      { type: "ip", name: "ip" },
      { type: "body", name: "body" },
      { type: "headers", name: "headers" },
    ],
  },
  template: {
    columns: [
      { type: "uuid", name: "id", primaryKey: true },
      {
        type: "template",
        name: "template",
        template: {
          language: "jsonnet",
          undefinedValueStrategy: "null",
          content: `Body('a')+":"+Body('c.f.g')`,
        },
      },
    ],
  }
}

// K6 options
export const options = {
  systemTags: ['status', 'group'],
  discardResponseBodies: true, // we are checking only status codes
  teardownTimeout: '120s',
  batch: PARALLEL_REQS_PER_USER,
  batchPerHost: PARALLEL_REQS_PER_USER,
  scenarios: {
    [SCENARIO]: scenarios[SCENARIO]
  },
  // Improve results table
  // Workaround: https://k6.io/docs/using-k6/workaround-to-calculate-iteration_duration/
  thresholds: {
    [`http_req_duration{scenario:${SCENARIO}}`]: [`max>=0`],
    'http_req_duration{group:::setup}': [`max>=0`],
    'http_req_duration{group:::teardown}': [`max>=0`],
    [`http_reqs{scenario:${SCENARIO}}`]: [`rate>=0`],
    'http_reqs{group:::setup}': [`rate>=0`],
    'http_reqs{group:::teardown}': [`rate>=0`],
    [`iteration_duration{scenario:${SCENARIO}}`]: [`max>=0`],
    'iteration_duration{group:::setup}': [`max>=0`],
    'iteration_duration{group:::teardown}': [`max>=0`],
  },
};

const api = new Api(API_HOST, API_TOKEN)

const errors_metrics = new Counter("failed_imports");

const payloads = randomPayloads(PAYLOAD_SIZE)

export function setup() {
  // Create source
  const source = api.createHTTPSource();

  // Modify source URL hostname if enabled
  let sourceUrl = source.url
  if (OVERWRITE_SOURCE_HOST) {
    const replacement = new URL(OVERWRITE_SOURCE_HOST)
    sourceUrl.protocol = replacement.protocol
    sourceUrl.hostname = replacement.hostname
    sourceUrl.port = replacement.port
  }
  console.log("Source url: " + sourceUrl)

  // Create sink
  const sink = api.createKeboolaTableSink(source.id, mappings[TABLE_MAPPING])
  console.log("Sink ID: " + sink.id)

  const params = {
    headers: {
      "My-Custom-Header": "custom header value",
    }
  }

  return { sourceId: source.id, sourceUrl: sourceUrl.toString(), params };
}

export function teardown(data) {
  api.deleteSource(data.sourceId)
}

export default function(data) {
  // Single request
  if (PARALLEL_REQS_PER_USER <= 1) {
    checkResponse(http.post(data.sourceUrl, randomItem(payloads), data.params))
    return
  }

  // Parallel requests
  http.batch(Array.from({ length: PARALLEL_REQS_PER_USER }, () => {
    return {
      method: 'POST',
      url: data.sourceUrl,
      body: randomItem(payloads),
      params: data.params,
    }
  })).forEach((res) => checkResponse(res))
}


function checkResponse(res) {
  let passed = check(res, {
    "status is 200/202": (r) => r.status === 200 || r.status === 202,
  })
  if (!passed) {
    console.error(`Request to ${res.request.url} with status ${res.status} failed the checks!`, res);
    errors_metrics.add(1, { url: res.request.url });
  }
}
