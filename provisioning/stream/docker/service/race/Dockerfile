# Build container
FROM golang:1.24.0-alpine3.21 AS buildContainer
# gcc and g++ are needed for CGO_ENABLED racer
RUN apk add --update --no-cache bash make curl gcc g++ binutils-gold
WORKDIR /app

COPY Makefile Makefile
COPY scripts scripts

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY . .
RUN make build-stream-service-with-race

# Production container
FROM alpine:3.20
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
