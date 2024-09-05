import * as common from "./common.js";

const payloads = common.randomPayloads()

export const options = common.options;

export function setup() {
  let source = common.setupSource();

  let sink = common.setupSink(
    source.id,
    {
      sinkId: "test-sink",
      name: "test-sink",
      type: "table",
      table: {
        type: "keboola",
        tableId: "in.c-bucket.test-sink",
        mapping: {
          columns: [
            { type: "uuid", name: "id", primaryKey: true },
            { type: "datetime", name: "datetime" },
            { type: "ip", name: "ip" },
            { type: "body", name: "body" },
            { type: "headers", name: "headers" },
          ],
        },
      },
    },
  )

  const headers = {
    "My-Custom-Header": "custom header value abcd",
  };

  return { source, sink, headers };
}

export function teardown(data) {
  common.teardownSource(data.source.id)
}

export default function(data) {
  common.batchWithCheckResponse({
    method: 'POST',
    url: data.source.url,
    body: common.randomElement(payloads),
    params: {headers: data.headers,},
  });
}

