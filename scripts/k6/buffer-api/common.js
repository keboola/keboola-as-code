import http from "k6/http";
import {sleep, check} from 'k6';
import { Counter } from 'k6/metrics';
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

export function setupReceiver(exports) {
    if (!TOKEN) throw new Error("Please set the `API_TOKEN` env var.");

    const receiverId = "buffer-" + randomString(8)
    let res = post("v1/receivers", {
        id: receiverId,
        name: "Buffer API Static Benchmark",
        exports: exports,
    });
    if (res.status !== 202) {
        console.error(res);
        throw new Error("failed to create receiver task");
    }

    const createReceiverTimeoutSec = 60
    const taskUrl = res.json().url
    for (let retries = createReceiverTimeoutSec; retries > 0; retries--) {
        res = get(taskUrl)
        if (res.status !== 200) {
            throw new Error("failed to get receiver task");
        }
        if (res.status !== "processing") {
            if (res.error) {
                throw new Error("failed to create receiver: " + res.error);
            }
            break
        }
        sleep(1000)
    }

    res = get(`v1/receivers/${receiverId}`);
    if (res.status !== 200) {
        throw new Error("failed to get receiver");
    }

    const receiverUrl = res.json().url
    if (!receiverUrl) {
        throw new Error("receiver url is not set");
    }

    console.log("Receiver url: " + receiverUrl)
    return {id: receiverId, url: receiverUrl}
}

export function teardownReceiver(receiverId) {
    const res = del(`v1/receivers/${receiverId}`);
    if (res.status !== 200) {
        console.error(res);
        throw new Error("failed to delete receiver");
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
        url = `${HOST}/${url}`
    }
    return url
}

export function checkResponse(res) {
    let passed = check(res, {
        "status is 200": (r) => r.status === 200,
    })
    if (!passed) {
        console.error(`Request to ${res.request.url} with status ${res.status} failed the checks!`, res);
        errors_metrics.add(1, {url: res.request.url});
    }
}
