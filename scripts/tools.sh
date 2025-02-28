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
