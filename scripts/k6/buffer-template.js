import { sleep } from "k6";
import * as common from "./common.js";
import { post, del } from "./common.js";

export const options = common.options;

const RECEIVER_ID = "buffer-template";

export function setup() {
  common.setup();

  let res = del(`v1/receivers/${RECEIVER_ID}`);
  if (res.status !== 200 && res.status !== 404) {
    console.error(res);
    throw new Error("failed to delete receiver");
  }

  res = post("v1/receivers", {
    id: RECEIVER_ID,
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
                content: `Body().a+":"+Body().c.f.g`,
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

  const { url } = res.json();
  const endpoint = url.slice(url.indexOf("v1"));

  const data = { a: "b", c: { d: "e", f: { g: "h" } } };
  const headers = {
    "My-Custom-Header": "custom header value abcd",
  };

  res = post(endpoint, data, headers);
  if (res.status !== 200) {
    console.error(res);
    throw new Error("failed to import data");
  }

  return { endpoint, data, headers };
}

export default function (input) {
  post(input.endpoint, input.data, input.headers);
  sleep(1);
}

export function teardown() {
  const res = del(`v1/receivers/${RECEIVER_ID}`);
  if (res.status !== 200 && res.status !== 404) {
    console.error(res);
    throw new Error("failed to delete receiver");
  }
}

