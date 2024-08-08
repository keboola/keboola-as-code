import * as common from "./common.js";
import { checkResponse, post } from "./common.js";

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

  const payload = { a: "b", c: { d: "e", f: { g: "h" } } };
  const headers = {
    "My-Custom-Header": "custom header value abcd",
  };

  return { source, sink, payload, headers };
}

export function teardown(data) {
  common.teardownSource(data.source.id)
}

export default function(data) {
  checkResponse(post(data.source.url, data.payload, data.headers));
}
