import exec from 'k6/execution';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import http from "k6/http";
import { Request, Client, checkstatus } from "k6/x/fasthttp"
import { URL } from 'https://jslib.k6.io/url/1.0.0/index.js';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';
import { Api } from "./api.js";

const SCENARIO = __ENV.K6_SCENARIO || "constant"; // constant or ramping
const TABLE_MAPPING = __ENV.K6_TABLE_MAPPING || "static"; // static, path or template

// Common
const USE_FASTHTTP = (__ENV.K6_FASTHTTP || "false") === "true"; // fast http plugin is faster, but don't count send bytes

// Target
const API_TOKEN = __ENV.K6_API_TOKEN;
const API_HOST = __ENV.K6_API_HOST || "http://localhost:8001";
const OVERWRITE_SOURCE_HOST = __ENV.K6_OVERWRITE_SOURCE_HOST; // to call directly k8s service

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
const SYNC_WAIT = (__ENV.STREAM_SYNC_WAIT || "true") === "true";

// Payload configuration
const PAYLOAD_SIZE = __ENV.STREAM_PAYLOAD_SIZE || 1 // 1 = 54B, 1024 = ~1KB (1077B), 1048500 = ~1MB (1048576)

// Available scenarios, use K6_SCENARIO env to select one
const scenarios = {
  constant: {
    executor: "shared-iterations",
    vus: CONST_VIRTUAL_USERS,
    iterations: CONST_TOTAL_REQUESTS,
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
      { type: "uuid", name: "id" },
      { type: "datetime", name: "datetime" },
      { type: "ip", name: "ip" },
      { type: "body", name: "body" },
      { type: "headers", name: "headers" },
    ],
  },
  path: {
    columns: [
      { type: "uuid", name: "id" },
      { type: "path", name: "int", path: "a" },
      { type: "path", name: "object", path: "c" },
      { type: "path", name: "string", path: "c.f.g", rawString: true },
      { type: "path", name: "undefined", path: "x", defaultValue: "default" },
    ],
  },
  template: {
    columns: [
      { type: "uuid", name: "id" },
      {
        type: "template",
        name: "template",
        template: {
          language: "jsonnet",
          content: `Body('a')+":"+Body('c.f.g')`,
        },
      },
      {
        type: "template",
        name: "object",
        template: {
          language: "jsonnet",
          content: `Body('c')`,
        },
      },
      {
        type: "template",
        name: "undefined",
        template: {
          language: "jsonnet",
          content: `Body('x', 'x')`,
        },
      },
    ],
  }
}

// K6 options
export const options = {
  systemTags: ['status', 'group'],
  discardResponseBodies: true, // we are checking only status codes
  teardownTimeout: '120s', scenarios: {
    [SCENARIO]: scenarios[SCENARIO]
  },
  // Improve results summary
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

// Partially unique payloads
const payloadsCount = 100
const payloads = new SharedArray('payloads', function() {
  let out = []
  for (let i = 0; i < payloadsCount; i++) {
    out.push(JSON.stringify({ a: 1, c: { d: "e", f: { g: randomString(10), h: "a".repeat(PAYLOAD_SIZE) } } }))
  }
  return out
})

const api = new Api(API_HOST, API_TOKEN)

const fastHttpClient = new Client()

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
  const sink = api.createKeboolaTableSink(source.id, TABLE_MAPPING, mappings[TABLE_MAPPING])
  console.log("Sink ID: " + sink.id)

  const headers = {
    "My-Custom-Header": "custom header value", "Content-Type": "application/json"
  }

  const expectedStatus = SYNC_WAIT ? 200 : 20

  return { sourceId: source.id, sourceUrl: sourceUrl.toString(), headers, expectedStatus };
}

export function teardown(data) {
  api.deleteSource(data.sourceId)
}

export default function(data) {
  const body = payloads[exec.scenario.iterationInTest % payloadsCount]

  if (USE_FASTHTTP) {
    const response = fastHttpClient.post(new Request(data.sourceUrl, { body: body, headers: data.headers }))
    checkstatus(data.expectedStatus, response,)
  } else {
    const response = http.post(data.sourceUrl, body, { headers: data.headers })
    check(response, { "status": (r) => r.status === data.expectedStatus })
  }
}
