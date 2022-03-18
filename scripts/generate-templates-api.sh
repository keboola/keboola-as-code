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

generate() {
  # Generate code by goa.design lib
  out=`goa gen github.com/keboola/keboola-as-code/api/templates --output ./internal/pkg/template/api 2>&1`
  status="$?"

  # Run finalize always
  finalize

  # Print stdout/stderr only if an error occurred
  if [ "$status" -gt 0 ]; then
    echo $out
  fi

  return $status
}

finalize() {
    rm -rf ./internal/pkg/template/api/gen/http/cli
    rm -rf ./internal/pkg/template/api/gen/http/templates/client
    mv ./internal/pkg/template/api/gen/http/openapi* ./api/templates/gen 2>/dev/null || true
}

echo "Generating templates API ..."
if generate; then
    echo "Ok."
    echo
else
    echo "Design \"api/templates/design.go\" is not valid. Please fix ^^^ errors."
    echo
    exit 1
fi
