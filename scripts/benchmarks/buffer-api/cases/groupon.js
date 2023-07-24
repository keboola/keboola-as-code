import * as common from "./common.js";
import {batchWithCheckResponse, checkResponse, post} from "./common.js";

export const options = common.options;

export function setup() {
    let receiver = common.setupReceiver([
        {
            exportId: "test-export-groupon",
            name: "test-export-groupon",
            mapping: {
                tableId: "in.c-test-bucket.test-export-groupon",
                columns: [
                    {type: "id", name: "id"},
                    {type: "datetime", name: "datetime"},
                    {type: "body", name: "body"},
                ],
            },
        },
    ]);

    const headers = {
        "Content-Type": "application/json",
    }

    const payload = {
        "events": [
            {
                "name": "view_item",
                "params": {
                    "visitor_id": "1234c93d6dacabb191573f2a2e837d541b1afe600764e0c6123453d2fff6a943",
                    "country": "US",
                    "region": "NA",
                    "deal_permalink": "foobar-2023-x-i-x-x-o-x-r-n-b-a-l-l-w-x-i-t-e-d-i-n-n-e-r-c-r-u-i-s-e-x-x-i-l-a-d-e-l-x-x-i-a",
                    "deal_uuid": "3d522c52-1234-42fa-1234-538d9865f400",
                    "item_id": "foobar-2023-x-i-x-x-o-x-r-n-b-a-l-l-w-x-i-t-e-d-i-n-n-e-r-c-r-u-i-s-e-x-x-i-l-a-d-e-l-x-x-i-a",
                    "channel": "main",
                    "clientPlatform": "Touch"
                }
            }
        ],
        "non_personalized_ads": false,
        "timestamp_micros": "1691133639153000",
        "user_id": "ec6bcd9cfce99df6ca0226add75c78bccd8akkk0bac490bb9fe1afa20b97770dc.e4b8c93d6dacaaseefefeyafee837d541b1afe600764e0c6589233d2fff6a943"
    };

    return {receiver, payload, headers};
}

export function teardown(data) {
    common.teardownReceiver(data.receiver.id)
}

export default function (data) {
    batchWithCheckResponse({
        method: 'POST',
        url: data.receiver.url,
        body: data.payload,
        params: {headers: data.headers,},
    });
}

