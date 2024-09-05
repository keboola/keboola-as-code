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
        tableId: "in.c-stream-template.data",
        mapping: {
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
  })
}
