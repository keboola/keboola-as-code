#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

if [ $# -lt 5 ]; then
  echo 1>&2 "$0: not enough arguments"
  exit 2
elif [ $# -gt 5 ]; then
  echo 1>&2 "$0: too many arguments"
  exit 2
fi

S3_MOUNTPOINT="/s3bucket"
ARTIFACT_PATH=$1
ARTIFACT_NAME=$2
PACKAGE_NAME=$3
VERSION=$4
ARCH=$5

if ! [[ "$ARTIFACT_NAME" =~ (\.zip|\.apk|\.deb|\.rpm)$ ]]; then
  echo "skipped '$ARTIFACT_PATH'"
  exit 0
fi

# Copy
DST_DIR="${S3_MOUNTPOINT}/releases/${VERSION}";
DST="${DST_DIR}/${ARTIFACT_NAME}";
mkdir -p "$DST_DIR";
cp -vf "$ARTIFACT_PATH" "$DST";

# APK Alpine repository - needs separated directory
if [[ "$ARTIFACT_NAME" =~ \.apk$ ]]; then
  case $ARCH in
    amd64)
      ALPINE_ARCH="x86_64"
      ;;
    arm64)
      ALPINE_ARCH="aarch64"
      ;;
    armv6)
      ALPINE_ARCH="armhf"
      ;;
    *)
      echo "Unexpected ARCH='$ARCH'"
      exit 2
      ;;
  esac

  # Copy
  export DST_DIR="${S3_MOUNTPOINT}/alpine/${ALPINE_ARCH}";
  export DST="${DST_DIR}/${PACKAGE_NAME}_${VERSION}.apk";
  mkdir -p "$DST_DIR";
  cp -vf "$ARTIFACT_PATH" "$DST";
fi
