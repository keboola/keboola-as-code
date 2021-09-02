#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

cd "$(dirname "$0")"/..

# Check Go files format
echo "Downloading modules"
go mod download
echo "Ok."
echo

# Check modules
echo "Running go mod tidy/verify ..."
go mod tidy
git diff --exit-code -- go.mod go.sum
go mod verify
echo "Ok. Tidy: go.mod and go.sum are valid."
echo

# Run linters
cd ./src
echo "Running golangci-lint ..."
if golangci-lint run -c "../.golangci.yml"; then
    echo "Ok. The code looks good."
    echo
else
    echo "Please fix ^^^ errors. You can try run \"make fix\"."
    echo
    exit 1
fi
