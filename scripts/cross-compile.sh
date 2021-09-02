#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

# Cd to root dir
cd `dirname "$0"`/..

binary_name="kbc"
archive_name="kbc"
platforms=(
  "linux/amd64"
  "linux/arm"
  "linux/arm64"
  "darwin/amd64"
  "darwin/arm64"
  "windows/amd64"
  "windows/arm"
)

# Get TARGET_VERSION from env
TARGET_VERSION="${TARGET_VERSION:-dev}"

# Create and clear dirs
mkdir -p ./target/bin
mkdir -p ./target/archive
rm -rf ./target/bin/*
rm -rf ./target/archive/*

# Compile for each platform
for platform in "${platforms[@]}"
do
    # Prepare variables
    platform_split=(${platform//\// })
    GOOS="${platform_split[0]}"
    GOARCH="${platform_split[1]}"
    binary_path="./target/bin/${GOOS}/${GOARCH}/${binary_name}"
    archive_path="./target/archive/${archive_name}-${TARGET_VERSION}-${GOOS}_${GOARCH}.zip"
    if [ $GOOS = "windows" ]; then
        binary_path+='.exe'
    fi

    # Compile
    echo -ne "Compiling for ${platform} ... "
    env GOOS=$GOOS GOARCH=$GOARCH ./scripts/compile.sh $binary_path
    if [ $? -ne 0 ]; then
        echo 'An error has occurred!'
        exit 1
    fi

    # Zip
    zip -j -q "${archive_path}" "${binary_path}"
    if [ $? -ne 0 ]; then
        echo 'An error has occurred!'
        exit 1
    fi

    echo "OK"
done

echo "All OK"
echo

echo "Created archives:"
stat -c "%s %n" ./target/archive/*  | numfmt --to=iec
echo
