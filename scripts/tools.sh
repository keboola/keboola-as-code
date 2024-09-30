#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# GOBIN defaults to GOPATH/bin, if it is not set
GOBIN="$(go env GOBIN)"
GOBIN="${GOBIN:=$(go env GOPATH)/bin}"

# go-licenses
if ! command -v go-licenses &> /dev/null
then
  go install github.com/google/go-licenses@latest
fi

# gotestsum
if ! command -v gotestsum &> /dev/null
then
  # Disabled due to issues with windows curl command not working
  # Related to https://github.com/keboola/keboola-as-code/pull/1818/commits/df23a4e2855557a01460593f7d6ba59b5dc8825c
  # ./install-gotestsum.sh -b "$GOBIN"
  go install gotest.tools/gotestsum@latest
fi

# goreleaser
if ! command -v goreleaser &> /dev/null
then
  # Disabled due to issues with windows curl command not working
  # Related to https://github.com/keboola/keboola-as-code/pull/1818/commits/df23a4e2855557a01460593f7d6ba59b5dc8825c
  # ./install-goreleaser.sh -b "$GOBIN" v1.22.1
  go install github.com/goreleaser/goreleaser@latest
fi

# golangci-lint
if ! command -v golangci-lint &> /dev/null
then
  # Disabled due to issues with windows curl command not working
  # Related to https://github.com/keboola/keboola-as-code/pull/1818/commits/df23a4e2855557a01460593f7d6ba59b5dc8825c
  # curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$GOBIN" v1.58.0
  go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0
fi

# gci
if ! command -v gci &> /dev/null
then
  go install github.com/daixiang0/gci@latest
fi

# gofumpt
if ! command -v gofumpt &> /dev/null
then
  go install mvdan.cc/gofumpt@latest
fi

# air - code changes watcher
if ! command -v air &> /dev/null
then
  go install github.com/air-verse/air@latest
fi

# godoc
if ! command -v godoc &> /dev/null
then
  go install golang.org/x/tools/cmd/godoc@latest
fi

# benchstat
if ! command -v benchstat &> /dev/null
then
  go install golang.org/x/perf/cmd/benchstat@latest
fi

# go-mod-upgrade
if ! command -v go-mod-upgrade &> /dev/null
then
  go install github.com/oligot/go-mod-upgrade@latest
fi

# goa - api by design library
if ! command -v goa &> /dev/null
then
  go install goa.design/goa/v3/cmd/goa@v3.19.1
fi

if ! command -v protoc-gen-go &> /dev/null
then
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
fi

if ! command -v protoc-gen-go-grpc &> /dev/null
then
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
fi

if ! command -v jl &> /dev/null
then
  go install github.com/mightyguava/jl/cmd/jl@latest
fi
