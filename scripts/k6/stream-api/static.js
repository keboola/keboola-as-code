import * as common from "./common.js";
import { checkResponse, post } from "./common.js";

export const options = common.options;

export function setup() {
  let receiver = common.setupReceiver([
    {
      exportId: "test-export",
      name: "test-export",
      mapping: {
        tableId: "in.c-stream-static.data",
        columns: [
          { type: "id", name: "id" },
          { type: "datetime", name: "datetime" },
          { type: "ip", name: "ip" },
          { type: "body", name: "body" },
          { type: "headers", name: "headers" },
        ],
      },
    },
  ]);

  const payload = { a: "b", c: { d: "e", f: { g: "h" } } };
  const headers = {
    "My-Custom-Header": "custom header value abcd",
  };

  return { receiver, payload, headers };
}

export function teardown(data) {
  common.teardownReceiver(data.receiver.id)
}

export default function(data) {
  checkResponse(post(data.receiver.url, data.payload, data.headers));
}

