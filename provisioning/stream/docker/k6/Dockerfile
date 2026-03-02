# Build xk6 from scratch using Go 1.26+ to ensure compatibility
FROM golang:1.26-alpine AS build

# Install necessary packages for building
RUN apk add --no-cache git

# Prevent Go from attempting toolchain auto-switching (GOTOOLCHAIN=auto default in 1.21+)
# and skip GOSUM database lookups which can fail in Docker network environments
ENV GOTOOLCHAIN=local
ENV GONOSUMDB=go.k6.io,github.com/LeonAdato,github.com/domsolutions

# Install xk6
RUN go install go.k6.io/xk6/cmd/xk6@latest

# Build k6 with the required extensions
RUN xk6 build \
    --with github.com/LeonAdato/xk6-output-statsd@latest \
    --with github.com/domsolutions/xk6-fasthttp@latest \
    --output /k6

FROM golang:1.26-alpine

RUN apk add --no-cache bash

COPY --from=build /k6 /usr/bin/k6

COPY scripts /scripts

COPY provisioning/stream/docker/k6/entrypoint.sh /entrypoint.sh

ENV K6_CLOUD_API_TOKEN=""

ENTRYPOINT ["/entrypoint.sh"]
