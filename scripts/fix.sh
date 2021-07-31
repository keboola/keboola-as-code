#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

SRC_DIR=./src

# Fix Go files format
gofmt -s -l -w $SRC_DIR

# Fix go imports
goimports -w $SRC_DIR

# Fix modules
go mod tidy
