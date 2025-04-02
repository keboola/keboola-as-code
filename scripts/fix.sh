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

# Check the most important problems first
#echo "Running go vet ..."
#if ! go vet ./...; then
#    echo "Please fix ^^^ errors."
#    echo
#    exit 1
#fi

# Fix modules
echo "Running go mod tidy ..."
go mod tidy
echo "Running go mod vendor ..."
go mod vendor

# Format code, gofumpt and gci partially overlap, it is needed to run them separately
# https://github.com/golangci/golangci-lint/issues/1490
echo "Running gofumpt ..."
go tool gofumpt -w ./cmd ./internal ./pkg ./test
echo "Running gci ..."
go tool gci write --skip-generated -s standard -s default -s "prefix(github.com/keboola/keboola-as-code)" ./cmd ./internal ./pkg ./test


# Run all analyzers with -fix
# https://github.com/golang/tools/blob/master/gopls/doc/analyzers.md#modernize-simplify-code-by-using-modern-constructs
echo "Running modernize..."
go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix -test ./...

# Fix linters
echo "Running golangci-lint ..."
if golangci-lint run --fix -c "./build/ci/golangci.yml"; then
    echo "Ok. The code looks good."
    echo
else
    echo "Some errors ^^^ cannot be fixed. Please fix them manually."
    echo
    exit 1
fi
