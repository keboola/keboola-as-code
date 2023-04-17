# Build container
FROM golang:1.20.3-alpine3.17 AS buildContainer
RUN apk add -U --no-cache bash make curl
WORKDIR /app

COPY Makefile Makefile
COPY scripts scripts
RUN make tools

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY . .
RUN make build-buffer-worker

# Production container
FROM alpine:3.17
RUN apk add -U --no-cache ca-certificates git

COPY --from=buildContainer /app/target/buffer/worker /app/worker
WORKDIR /app

ENV HOST 0.0.0.0
ENV PORT 8000
EXPOSE 8000

CMD ["/app/worker"]
