#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# go-licenses
go install github.com/google/go-licenses@latest

# gotestsum
if ! command -v gotestsum &> /dev/null
then
  ./install-gotestsum.sh -b $(go env GOPATH)/bin
fi

# goreleaser
if ! command -v goreleaser &> /dev/null
then
  ./install-goreleaser.sh -b $(go env GOPATH)/bin v0.182.1
fi

# golangci-lint
if ! command -v golangci-lint &> /dev/null
then
  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.50.1
fi

# air - code changes watcher
if ! command -v air &> /dev/null
then
  curl -sSfL https://raw.githubusercontent.com/cosmtrek/air/master/install.sh | sh -s -- -b $(go env GOPATH)/bin
fi

# godoc
if ! command -v godoc &> /dev/null
then
  go install golang.org/x/tools/cmd/godoc@latest
fi

# goa - api by design library
if ! command -v goa &> /dev/null
then
  go install goa.design/goa/v3/cmd/goa@v3.11.1
fi
