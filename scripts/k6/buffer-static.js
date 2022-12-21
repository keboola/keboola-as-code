import http from "k6/http";
import { sleep } from "k6";

const PROTOCOL = __ENV.API_USE_HTTPS && __ENV.API_USE_HTTPS !== "false" ? "https" : "http";
const HOST = __ENV.API_HOST || "localhost";
const PORT = __ENV.API_PORT ? `${__ENV.API_PORT}` : ":8001";
const TOKEN = __ENV.API_TOKEN;
const USERS = __ENV.K6_USERS || 6 * 2 * 512; // assume at least 6 CPU cores with hyperthreading, 1024 virtual users per thread
const DURATION = __ENV.K6_DURATION || "30s";

const commonHeaders = {
  "Content-Type": "application/json",
  "X-StorageApi-Token": TOKEN,
};

const post = (endpoint, data, headers = {}) =>
  http.post(`${PROTOCOL}://${HOST}${PORT}/${endpoint}`, JSON.stringify(data), {
    headers: Object.assign({}, commonHeaders, headers),
  });

const del = (endpoint, headers = {}) =>
  http.del(`${PROTOCOL}://${HOST}${PORT}/${endpoint}`, null, {
    headers: Object.assign({}, commonHeaders, headers),
  });

export const options = {
  vus: USERS,
  duration: DURATION,
};

export function setup() {
  if (!TOKEN) throw new Error("Please set the `API_TOKEN` env var.");

  const res = post("v1/receivers", {
    id: "buffer-api-benchmark",
    name: "Buffer API Benchmark",
    exports: [
      {
        exportId: "static",
        name: "static",
        mapping: {
          tableId: "in.c-buffer-api-benchmark.static",
          columns: [
            { type: "id", name: "id" },
            { type: "datetime", name: "datetime" },
            { type: "ip", name: "ip" },
            { type: "body", name: "body" },
            { type: "headers", name: "headers" },
          ],
        },
      },
    ],
  });

  if (res.status !== 200) {
    console.error(res);
    throw new Error("failed to create receiver");
  }

  const { id: receiverId, url } = res.json();
  const endpoint = url.slice(url.indexOf("v1"));

  const data = { a: "b", c: { d: "e", f: { g: "h" } } };
  const headers = {
    "My-Custom-Header": "custom header value abcd",
  };

  return { receiverId, endpoint, data, headers };
}

export default function (input) {
  post(input.endpoint, input.data, input.headers);
  sleep(1);
}

export function teardown(input) {
  del(`v1/receivers/${input.receiverId}`);
}

