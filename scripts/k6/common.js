import http from "k6/http";

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

export function setup() {
  if (!TOKEN) throw new Error("Please set the `API_TOKEN` env var.");
}

export function post(endpoint, data, headers = {}) {
  return http.post(`${PROTOCOL}://${HOST}${PORT}/${endpoint}`, JSON.stringify(data), {
    headers: Object.assign({}, commonHeaders, headers),
  });
}

export function del(endpoint, headers = {}) {
  return http.del(`${PROTOCOL}://${HOST}${PORT}/${endpoint}`, null, {
    headers: Object.assign({}, commonHeaders, headers),
  });
}

export const options = {
  vus: USERS,
  duration: DURATION,
};

