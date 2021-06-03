#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Cd to root dir
cd `dirname "$0"`/..

OUTPUT=${1:-./target/bin/local/kbc} # output, first argument
TARGET_VERSION="${TARGET_VERSION:-dev}"
BUILD_DATE=$(date '+%FT%T%z')
GIT_COMMIT=$(git rev-list -1 HEAD)
CGO_ENABLED=0

# Build: optimize for size + set version variables
go build \
  -ldflags="-s -w \
    -X keboola-as-code/src/version.BuildVersion=$TARGET_VERSION \
    -X keboola-as-code/src/version.BuildDate=$BUILD_DATE \
    -X keboola-as-code/src/version.GitCommit=$GIT_COMMIT" \
  -o "$OUTPUT" \
  ./src/main.go
