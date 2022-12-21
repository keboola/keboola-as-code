import { sleep } from "k6";
import * as common from "./common.js";
import { post, del } from "./common.js";

export const options = common.options;

export function setup() {
  common.setup();

  const res = post("v1/receivers", {
    id: "buffer-template",
    name: "Buffer API Template Benchmark",
    exports: [
      {
        exportId: "test-export",
        name: "test-export",
        mapping: {
          tableId: "in.c-buffer-template.data",
          columns: [
            { type: "id", name: "id" },
            {
              type: "template",
              name: "template",
              template: {
                language: "jsonnet",
                undefinedValueStrategy: "null",
                content: `Body.a.b+":"+Body.c.f.g`,
              },
            },
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

