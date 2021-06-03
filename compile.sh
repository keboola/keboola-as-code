#!/bin/bash

OUTPUT=${1:-./target/bin/local/kbc} # output, first argument
TARGET_VERSION="${TARGET_VERSION:-dev}"
GIT_COMMIT=$(git rev-list -1 HEAD)
BUILD_DATE=$(date '+%FT%T%z')
CGO_ENABLED=0

go build \
  -ldflags="-s -w \
    -X keboola-as-code/src/cli.Version=$TARGET_VERSION \
    -X keboola-as-code/src/cli.GitCommit=$GIT_COMMIT \
    -X keboola-as-code/src/cli.BuildDate=$BUILD_DATE" \
  -o "$OUTPUT" \
  ./src/main.go
