# Build xk6 from scratch using Go 1.25+ to ensure compatibility
FROM golang:1.25-alpine AS build

# Install necessary packages for building
RUN apk add --no-cache git

# Install xk6
RUN go install go.k6.io/xk6/cmd/xk6@latest

# Build k6 with the required extensions
RUN xk6 build \
    --with github.com/LeonAdato/xk6-output-statsd@latest \
    --with github.com/domsolutions/xk6-fasthttp@latest \
    --output /k6

FROM golang:1.25-alpine

RUN apk add --no-cache bash

COPY --from=build /k6 /usr/bin/k6

COPY scripts /scripts

COPY provisioning/stream/docker/k6/entrypoint.sh /entrypoint.sh

ENV K6_CLOUD_API_TOKEN=""

ENTRYPOINT ["/entrypoint.sh"]
