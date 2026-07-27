FROM grafana/xk6:latest AS build

RUN xk6 build \
    --with github.com/LeonAdato/xk6-output-statsd@latest \
    --with github.com/domsolutions/xk6-fasthttp@latest

FROM golang:1.24-alpine

RUN apk add --no-cache bash

COPY --from=build /xk6/k6 /usr/bin/k6

COPY scripts /scripts

COPY provisioning/stream/docker/k6/entrypoint.sh /entrypoint.sh

ENV K6_CLOUD_API_TOKEN=""

ENTRYPOINT ["/entrypoint.sh"]
