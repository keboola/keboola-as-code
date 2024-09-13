import { sleep } from 'k6';
import http from "k6/http";
import { SharedArray } from 'k6/data';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

export class Api {
  constructor(apiHost, apiToken) {
    if (!apiHost) throw new Error("Please set the `K6_API_HOST` env var.");
    if (!apiToken) throw new Error("Please set the `K6_API_TOKEN` env var.");
    this.apiHost = apiHost
    this.headers = { "X-StorageApi-Token": apiToken }
  }

  createHTTPSource() {
    const sourceId = "stream-" + randomString(8)
    const body = {
      sourceId: sourceId,
      name: "Stream API Static Benchmark",
      type: "http",
    }

    // Create source
    let res = http.post(`${this.apiHost}/v1/branches/default/sources`, JSON.stringify(body), { headers: this.headers, responseType: "text" });
    if (res.status !== 202) {
      console.error(res);
      throw new Error("failed to create source task");
    }
    this.awaitTask(res.json().url)

    // Modify settings
    // res = http.patch(`${this.apiHost}/v1/branches/default/sources/${sourceId}/settings`, {
    //   settings: [
    //     {
    //       key: "storage.level.local.encoding.sync.mode",
    //       value: SYNC_MODE,
    //     },
    //     {
    //       key: "storage.level.local.encoding.sync.wait",
    //       value: SYNC_WAIT === "1",
    //     },
    //   ],
    // }, {headers: this.headers, responseType:"text"});
    // awaitTask(res.json().url)

    // Get source
    res = http.get(`${this.apiHost}/v1/branches/default/sources/${sourceId}`, { headers: this.headers, responseType: "text" });
    if (res.status !== 200) {
      throw new Error("failed to get source");
    }

    // Get source URL
    let sourceUrl = new URL(res.json().http.url)
    if (!sourceUrl) {
      throw new Error("missing source URL");
    }

    return { id: sourceId, url: sourceUrl }
  }

  createKeboolaTableSink(sourceId, mapping_type, mapping) {
    const body = {
      sinkId: "test-sink",
      name: "test-sink" + mapping_type,
      type: "table",
      table: {
        type: "keboola",
        tableId: "in.c-bucket.test-sink" + mapping_type,
        mapping: mapping,
      },
    }

    // Create sink
    let res = http.post(`${this.apiHost}/v1/branches/default/sources/${sourceId}/sinks`, JSON.stringify(body), { headers: this.headers, responseType: "text" });
    if (res.status !== 202) {
      console.error(res);
      throw new Error("failed to create sink task");
    }
    this.awaitTask(res.json().url)

    // Get sink
    res = http.get(`${this.apiHost}/v1/branches/default/sources/${sourceId}/sinks/${body.sinkId}`, { headers: this.headers, responseType: "text" });
    if (res.status !== 200) {
      throw new Error("failed to get sink");
    }

    // Get sink ID
    const sinkId = res.json().sinkId
    if (!sinkId) {
      throw new Error("missing sink ID");
    }

    return { id: sinkId }
  }

  deleteSource(sourceId) {
    console.info("waiting 100s before source deletion")
    sleep(100)
    const res = http.del(`${this.apiHost}/v1/branches/default/sources/${sourceId}`, null, { headers: this.headers, responseType: "text" });
    if (res.status !== 202) {
      console.error(res);
      throw new Error("failed to delete source");
    }
  }

  awaitTask(taskUrl) {
    const taskTimeout = 60
    for (let retries = taskTimeout; retries > 0; retries--) {
      let res = http.get(taskUrl, { headers: this.headers, responseType: "text" })
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
}

export function randomPayloads(payloadLength) {
  return new SharedArray('payloads', function() {
    let payloads = []
    for (let i = 0; i < 100; i++) {
      payloads.push(JSON.stringify({ a: 1, c: { d: "e", f: { g: randomString(10), h: "a".repeat(payloadLength) } } }))
    }
    return payloads
  })
}
