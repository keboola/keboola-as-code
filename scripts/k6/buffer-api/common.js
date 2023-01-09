import http from "k6/http";
import {randomString} from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

const TOKEN = __ENV.API_TOKEN;
const HOST = __ENV.API_HOST || "http://localhost:8001";
const ITERATIONS = __ENV.K6_ITERATIONS || 100000;
const PARALLELISM = __ENV.K6_PARALLELISM || 1000;
const TIMEOUT = __ENV.K6_TIMEOUT || "60s";

const commonHeaders = {
    "Content-Type": "application/json",
    "X-StorageApi-Token": TOKEN,
};

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

export function setupReceiver(exports) {
    if (!TOKEN) throw new Error("Please set the `API_TOKEN` env var.");

    let id = "buffer-" + randomString(8)
    let res = post("v1/receivers", {
        id: id,
        name: "Buffer API Static Benchmark",
        exports: exports,
    });
    if (res.status !== 200) {
        console.error(res);
        throw new Error("failed to create receiver");
    }

    let url = res.json().url;
    url = url.slice(url.indexOf("v1"));

    return {id, url}
}

export function teardownReceiver(receiverId) {
    const res = del(`v1/receivers/${receiverId}`);
    if (res.status !== 200) {
        console.error(res);
        throw new Error("failed to delete receiver");
    }
}

export function post(endpoint, data, headers = {}) {
    return http.post(`${HOST}/${endpoint}`, JSON.stringify(data), {
        headers: Object.assign({}, commonHeaders, headers),
    });
}

export function del(endpoint, headers = {}) {
    return http.del(`${HOST}/${endpoint}`, null, {
        headers: Object.assign({}, commonHeaders, headers),
    });
}

