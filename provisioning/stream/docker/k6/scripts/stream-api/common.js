import http from "k6/http";
import { URL } from 'https://jslib.k6.io/url/1.0.0/index.js';
import { sleep, check } from 'k6';
import { Counter } from 'k6/metrics';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

const TOKEN = __ENV.API_TOKEN;
const HOST = __ENV.API_HOST || "http://localhost:8001";
const ITERATIONS = __ENV.K6_ITERATIONS || 100000;
const PARALLELISM = __ENV.K6_PARALLELISM || 1000;
const TIMEOUT = __ENV.K6_TIMEOUT || "60s";

const commonHeaders = {
  "Content-Type": "application/json",
  "X-StorageApi-Token": TOKEN,
};

const errors_metrics = new Counter("failed_imports");

export const options = {
  scenarios: {
    default: {
      executor: "shared-iterations",
      vus: PARALLELISM,
      iterations: ITERATIONS,
      maxDuration: TIMEOUT,
    },
  },
  // Workaround: https://k6.io/docs/using-k6/workaround-to-calculate-iteration_duration/
  thresholds: {
    'http_req_duration{scenario:default}': [`max>=0`],
    'http_req_duration{group:::setup}': [`max>=0`],
    'http_req_duration{group:::teardown}': [`max>=0`],
    'iteration_duration{scenario:default}': [`max>=0`],
    'iteration_duration{group:::setup}': [`max>=0`],
    'iteration_duration{group:::teardown}': [`max>=0`],
  },
};

export function awaitTask(url) {
  const createSourceTimeoutSec = 60
  const taskUrl = stripUrlHost(url)
  for (let retries = createSourceTimeoutSec; retries > 0; retries--) {
    let res = get(taskUrl)
    if (res.status !== 200) {
      console.error(res);
      throw new Error("failed to get task");
    }
    if (res.json().status !== "processing") {
      if (res.json().error) {
        throw new Error("task failed: " + res.json().error);
      }
      break
    }
    sleep(1)
  }
}

export function setupSource() {
  if (!TOKEN) throw new Error("Please set the `API_TOKEN` env var.");

  const sourceId = "stream-" + randomString(8)
  let res = post("v1/branches/default/sources", {
    sourceId: sourceId,
    name: "Stream API Static Benchmark",
    type: "http",
  });
  if (res.status !== 202) {
    console.error(res);
    throw new Error("failed to create source task");
  }

  awaitTask(res.json().url)

  res = get(`v1/branches/default/sources/${sourceId}`);
  if (res.status !== 200) {
    throw new Error("failed to get source");
  }

  const sourceUrl = stripUrlHost(res.json().http.url)
  if (!sourceUrl) {
    throw new Error("source url is not set");
  }

  console.log("Source url: " + sourceUrl)
  return { id: sourceId, url: sourceUrl }
}

export function setupSink(sourceId, body) {
  if (!TOKEN) throw new Error("Please set the `API_TOKEN` env var.");

  let res = post(`v1/branches/default/sources/${sourceId}/sinks`, body);
  if (res.status !== 202) {
    console.error(res);
    throw new Error("failed to create sink task");
  }

  awaitTask(res.json().url)

  res = get(`v1/branches/default/sources/${sourceId}/sinks/${body.sinkId}`);
  if (res.status !== 200) {
    throw new Error("failed to get sink");
  }

  const sinkId = res.json().sinkId
  if (!sinkId) {
    throw new Error("sink id is not set");
  }

  console.log("Sink id: " + sinkId)
  return { id: sinkId }
}

export function stripUrlHost(url) {
  return (new URL(url)).pathname
}

export function teardownSource(sourceId) {
  const res = del(`v1/branches/default/sources/${sourceId}`);
  if (res.status !== 200) {
    console.error(res);
    throw new Error("failed to delete source");
  }
}

export function get(url, headers = {}) {
  return http.get(normalizeUrl(url), {
    headers: Object.assign({}, commonHeaders, headers),
  });
}

export function post(url, data, headers = {}) {
  return http.post(normalizeUrl(url), JSON.stringify(data), {
    headers: Object.assign({}, commonHeaders, headers),
  });
}

export function del(url, headers = {}) {
  return http.del(normalizeUrl(url), null, {
    headers: Object.assign({}, commonHeaders, headers),
  });
}

export function normalizeUrl(url) {
  if (url.indexOf('http://') !== 0 && url.indexOf('https://') !== 0) {
    url = `${HOST}/${url.replace(/^\//, '')}`
  }
  return url
}

export function checkResponse(res) {
  let passed = check(res, {
    "status is 200": (r) => r.status === 200,
  })
  if (!passed) {
    console.error(`Request to ${res.request.url} with status ${res.status} failed the checks!`, res);
    errors_metrics.add(1, { url: res.request.url });
  }
}
