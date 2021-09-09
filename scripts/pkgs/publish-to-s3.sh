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
OS=$5
ARCH=$6

# ZIP repository
if [[ "$ARTIFACT_NAME" =~ \.zip$ ]]; then
  DST_DIR="${S3_MOUNTPOINT}/zip";
  DST="${DST_DIR}/${PACKAGE_NAME}_${VERSION}_${OS}_${ARCH}.zip";
  mkdir -p "$DST_DIR";
  cp -v --remove-destination "$ARTIFACT_PATH" "$DST";
fi

# DEB repository
if [[ "$ARTIFACT_NAME" =~ \.deb$ ]]; then
  DST_DIR="${S3_MOUNTPOINT}/deb";
  DST="${DST_DIR}/${PACKAGE_NAME}_${VERSION}_${ARCH}.deb";
  mkdir -p "$DST_DIR";
  cp -v --remove-destination "$ARTIFACT_PATH" "$DST";
fi

# RPM repository
if [[ "$ARTIFACT_NAME" =~ \.rpm$ ]]; then
  DST_DIR="${S3_MOUNTPOINT}/rpm";
  DST="${DST_DIR}/${PACKAGE_NAME}_${VERSION}_${ARCH}.rpm";
  mkdir -p "$DST_DIR";
  cp -v --remove-destination "$ARTIFACT_PATH" "$DST";
fi

# APK Alpine repository
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

  export DST_DIR="${S3_MOUNTPOINT}/apk/${ALPINE_ARCH}";
  export DST="${DST_DIR}/${PACKAGE_NAME}_${VERSION}.apk";
  mkdir -p "$DST_DIR";
  cp -v --remove-destination "$ARTIFACT_PATH" "$DST";
fi
