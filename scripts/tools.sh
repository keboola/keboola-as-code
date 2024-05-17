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
  ./install-gotestsum.sh -b "$GOBIN"
fi

# goreleaser
if ! command -v goreleaser &> /dev/null
then
  ./install-goreleaser.sh -b "$GOBIN" v1.22.1
fi

# golangci-lint
if ! command -v golangci-lint &> /dev/null
then
  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$GOBIN" v1.58.0
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

# go-mod-upgrade
if ! command -v go-mod-upgrade &> /dev/null
then
  go install github.com/oligot/go-mod-upgrade@latest
fi

# goa - api by design library
if ! command -v goa &> /dev/null
then
  go install goa.design/goa/v3/cmd/goa@v3.16.0
fi
