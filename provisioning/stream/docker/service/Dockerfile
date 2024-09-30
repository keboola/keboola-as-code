# Build container
FROM golang:1.23.1-alpine3.19 AS buildContainer
RUN apk add -U --no-cache bash make curl
WORKDIR /app

COPY Makefile Makefile
COPY scripts scripts
RUN make tools

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY . .
RUN make build-stream-service

# Production container
FROM alpine:3.19
RUN apk add -U --no-cache ca-certificates git

COPY --from=buildContainer /app/target/stream/service /app/service
WORKDIR /app

# Storage writer ingress - UDP port
EXPOSE 6000

# HTTP source
EXPOSE 7000

# API
EXPOSE 8000

# Prometheus metrics
EXPOSE 9000
