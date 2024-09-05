# Build container
FROM golang:1.23.0-alpine3.19 AS buildContainer
RUN apk add -U --no-cache bash make curl
WORKDIR /app

COPY Makefile Makefile
COPY scripts scripts
RUN make tools

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY . .
RUN make build-apps-proxy

# Production container
FROM alpine:3.19
RUN apk add -U --no-cache ca-certificates git

COPY --from=buildContainer /app/target/apps-proxy/proxy /app/server
WORKDIR /app

# API
EXPOSE 8000

# Prometheus metrics
EXPOSE 9000

CMD ["/app/server", "--api-listen=0.0.0.0:8000", "--metrics-listen=0.0.0.0:9000"]
