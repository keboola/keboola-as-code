import http from "k6/http";
import { URL } from 'https://jslib.k6.io/url/1.0.0/index.js';
import {sleep, check} from 'k6';
import { Counter } from 'k6/metrics';
import {randomString} from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// MAX_VIRTUAL_USERS is maximum number of connections.
const MAX_VIRTUAL_USERS = __ENV.BENCHMARK_MAX_VIRTUAL_USERS || 100;

// PARALLEL_REQS_PER_USER is number of parallel requests per VU/connection.
const PARALLEL_REQS_PER_USER = __ENV.BENCHMARK_PARALLEL_REQS_PER_USER || 10;

// RAMPING_DURATION defines the duration of initial increasing and final decreasing of the rate.
const RAMPING_DURATION = __ENV.BENCHMARK_RAMPING_DURATION || "2m";

// STABLE_RATE_DURATION defines the duration of the maximum rate.
const STABLE_RATE_DURATION = __ENV.BENCHMARK_STABLE_RATE_DURATION || "2m";

// HOST - Storage API host
const HOST = __ENV.BENCHMARK_API_HOST;

// TOKEN - Storage API token
const TOKEN = __ENV.BENCHMARK_API_TOKEN;

if (!TOKEN) {
    throw new Error("please specify BENCHMARK_API_TOKEN env")
}
if (!HOST) {
    throw new Error("please specify BENCHMARK_API_HOST env")
}


const commonHeaders = {
    "Content-Type": "application/json",
    "X-StorageApi-Token": TOKEN,
};

const errors_metrics = new Counter("failed_imports");

export const options = {
    teardownTimeout: '120s',
    batch: PARALLEL_REQS_PER_USER,
    batchPerHost: PARALLEL_REQS_PER_USER,
    scenarios: {
        default: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { target: MAX_VIRTUAL_USERS, duration: RAMPING_DURATION },
                { target: MAX_VIRTUAL_USERS, duration: STABLE_RATE_DURATION },
                { target: 0, duration: RAMPING_DURATION },
                { target: 0, duration: '10s' },
            ],
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

    // Create receiver
    const receiverId = "buffer-" + randomString(8)
    console.info(`Creating receiver ${receiverId}...`)
    let res = post("v1/receivers", {
        id: receiverId,
        name: "Buffer API Static Benchmark",
        exports: exports,
    });
    if (res.status !== 202) {
        console.error(res);
        throw new Error("Failed to create receiver task");
    }

    // Wait for create receiver task
    const createReceiverTimeoutSec = 60
    const taskUrl = stripUrlHost(res.json().url)
    for (let retries = createReceiverTimeoutSec; retries > 0; retries--) {
        res = get(taskUrl)
        if (res.status !== 200) {
            console.error(res);
            throw new Error("Failed to get receiver task");
        }
        if (res.json().status === "processing") {
            console.info("Waiting for create receiver task ...")
        } else {
            console.info("Create receiver task done")
            if (res.error) {
                throw new Error("Failed to create receiver: " + res.error);
            }
            break
        }
        sleep(1)
    }

    // Get receiver URL
    console.info("Loading receiver detail ...")
    res = get(`v1/receivers/${receiverId}`);
    if (res.status !== 200) {
        console.error(res)
        throw new Error("Failed to get receiver");
    }

    const receiverUrl = stripUrlHost(res.json().url)
    if (!receiverUrl) {
        throw new Error("Receiver url is not set");
    }

    console.log("Receiver url: " + receiverUrl)
    return {id: receiverId, url: receiverUrl}
}

export function stripUrlHost(url) {
    return (new URL(url)).pathname
}

export function teardownReceiver(receiverId) {
    const res = del(`v1/receivers/${receiverId}`);
    if (res.status !== 200) {
        console.error(res);
        throw new Error("Failed to delete receiver");
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

export function batchWithCheckResponse(req) {
    req.url = normalizeUrl(req.url)
    const requests = Array.from({length: PARALLEL_REQS_PER_USER}, () => req)
    const responses = http.batch(requests);
    responses.forEach((res) => checkResponse(res))
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
        console.error(`Request to ${res.request.url} with status ${res.status} failed the checks!`);
        errors_metrics.add(1, {url: res.request.url});
    }
}
