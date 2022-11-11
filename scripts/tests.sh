#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Change directory to the project root
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR/.."
pwd

TEST_VERBOSE="${TEST_VERBOSE:=false}"
TEST_PARALLELISM="${TEST_PARALLELISM:=8}"      # number of parallel tests inside package
TEST_PARALLELISM_PKG="${TEST_PARALLELISM_PKG:=8}"  # number of packages tested in parallel
TEST_LOG_FORMAT="${TEST_LOG_FORMAT:=testname}"
TEST_DETECT_RACE="${TEST_DETECT_RACE:=true}"
TEST_COVERAGE="${TEST_COVERAGE:=true}"
TEST_PACKAGE="${TEST_PACKAGE:=./...}"
TEST_ARGS="${TEST_ARGS:=}"
if [[ $TEST_DETECT_RACE == "true" ]]; then
  TEST_ARGS="$TEST_ARGS -race"
fi
if [[ $TEST_COVERAGE == "true" ]]; then
  TEST_ARGS="$TEST_ARGS -coverprofile=/tmp/profile.out"
fi

# Run tests, sequentially because the API is shared resource
echo "Running tests ..."
export KBC_VERSION_CHECK=false # do not check the latest version in the tests
cmd="gotestsum --no-color=false --format \"$TEST_LOG_FORMAT\" -- -timeout 600s -p $TEST_PARALLELISM_PKG -parallel $TEST_PARALLELISM -v $TEST_ARGS  "$TEST_PACKAGE" $@"
echo $cmd
eval $cmd
echo "Ok. All tests passed."
echo
