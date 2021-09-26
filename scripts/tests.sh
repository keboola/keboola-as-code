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
go mod vendor
echo "Ok."
echo

# Run tests, sequentially because the API is shared resource
echo "Running tests ..."
export KBC_VERSION_CHECK=false # do not check the latest version in the tests
richgo clean -testcache
RICHGO_FORCE_COLOR=1 richgo test -p 1 -timeout 600s -v -race -coverprofile=/tmp/profile.out ./... $@
echo "Ok. All tests passed."
echo
