#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Check Go files format
echo "Downloading modules"
go mod download
echo "Ok."
echo

# Check Go files format
echo "Running gofmt ..."
gofmtOut=`gofmt -s -l ./src`
if [[ "$gofmtOut" ]]; then
  echo "Go files are not properly formatted, please run \"make fix\" to fix."
  echo "Fix needed:"
  echo "$gofmtOut"
  exit 1
fi
echo "Ok. Code is properly formatted."
echo

# Check for suspicious constructs
echo "Running go vet ..."
go vet ./src/...
echo "Ok. No suspicious constructs found."
echo

# Check modules
echo "Running go mod tidy/verify ..."
go mod tidy
git diff --exit-code -- go.mod go.sum
go mod verify
echo "Ok. Tidy: go.mod and go.sum are valid."
echo

# Run staticcheck
echo "Running staticcheck ..."
staticcheck ./src/...
echo "Ok. The code looks good."
echo

# Run tests, sequentially because the API is shared resource
echo "Running tests ..."
richgo clean -testcache
RICHGO_FORCE_COLOR=1 richgo test -p 1 -timeout 360s -v -race -coverprofile=./target/profile.out ./src/... $@
echo "Ok. All tests passed."
echo
