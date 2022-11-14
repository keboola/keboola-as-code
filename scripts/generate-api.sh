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

API_PKG=internal/pkg/api/server/$API_NAME

generate() {
  # Generate code by goa.design lib
  out=`goa gen github.com/keboola/keboola-as-code/api/${API_NAME} --output ./$API_PKG 2>&1`
  status="$?"

  # Always run finalize
  finalize

  # Check exit code
  if [ "$status" -eq 0 ]; then
    # Move files, if the code has been generated
    mv -f ./$API_PKG/gen/http/openapi* ./$API_PKG/openapi
  else
    # Print stdout/stderr only if an error occurred
    echo $out
  fi

  return $status
}

finalize() {
  rm -rf ./gen
  rm -rf ./goa*
  rm -rf ./$API_PKG/gen/http/cli
  rm -rf ./$API_PKG/gen/http/$API_NAME/client
}

echo "Generating ${API_NAME} API ..."
if generate; then
  echo "Ok."
  echo
else
  echo "Design \"api/${API_NAME}/design.go\" is not valid. Please fix ^^^ errors."
  echo
  exit 1
fi
