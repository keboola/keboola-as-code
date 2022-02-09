#!/bin/bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

if [ $# -lt 6 ]; then
  echo 1>&2 "$0: not enough arguments"
  exit 2
elif [ $# -gt 6 ]; then
  echo 1>&2 "$0: too many arguments"
  exit 2
fi

ARTIFACT_PATH=$1
ARTIFACT_NAME=$2
PACKAGE_NAME=$3
VERSION=$4
OS=$5
ARCH=$6

GITHUB_RELEASE_DIR="./target/gh-release"
mkdir -p "$GITHUB_RELEASE_DIR"

# TXT file
if [[ "$ARTIFACT_NAME" =~ \.txt ]]; then
  DST_S3="s3://${AWS_BUCKET_NAME}/test/${ARTIFACT_NAME}";
  aws s3 cp "$ARTIFACT_PATH" "$DST_S3";
  cp "${ARTIFACT_PATH}" "${GITHUB_RELEASE_DIR}/${ARTIFACT_NAME}"
fi

# ZIP archive
if [[ "$ARTIFACT_NAME" =~ \.zip$ ]]; then
  DST_S3="s3://${AWS_BUCKET_NAME}/test/${ARTIFACT_NAME}";
  aws s3 cp "$ARTIFACT_PATH" "$DST_S3";
  cp "${ARTIFACT_PATH}" "${GITHUB_RELEASE_DIR}/${ARTIFACT_NAME}"
fi
#
## DEB repository
#if [[ "$ARTIFACT_NAME" =~ \.deb$ ]]; then
#  DST_S3="s3://${AWS_BUCKET_NAME}/deb/pool/${PACKAGE_NAME}_${VERSION}_${ARCH}.deb";
#  aws s3 cp "$ARTIFACT_PATH" "$DST_S3";
#  cp "${ARTIFACT_PATH}" "${GITHUB_RELEASE_DIR}/${ARTIFACT_NAME}"
#fi
#
## RPM repository
#if [[ "$ARTIFACT_NAME" =~ \.rpm$ ]]; then
#  DST_S3="s3://${AWS_BUCKET_NAME}/rpm/${PACKAGE_NAME}_${VERSION}_${ARCH}.rpm";
#  aws s3 cp "$ARTIFACT_PATH" "$DST_S3";
#  cp "${ARTIFACT_PATH}" "${GITHUB_RELEASE_DIR}/${ARTIFACT_NAME}"
#fi
#
## APK Alpine repository
#if [[ "$ARTIFACT_NAME" =~ \.apk$ ]]; then
#  case $ARCH in
#    amd64)
#      ALPINE_ARCH="x86_64"
#      ;;
#    arm64)
#      ALPINE_ARCH="aarch64"
#      ;;
#    armv6)
#      ALPINE_ARCH="armhf"
#      ;;
#    *)
#      echo "Unexpected ARCH='$ARCH'"
#      exit 2
#      ;;
#  esac

  export DST_S3="s3://${AWS_BUCKET_NAME}/apk/${ALPINE_ARCH}/${PACKAGE_NAME}-${VERSION}.apk";
  aws s3 cp "$ARTIFACT_PATH" "$DST_S3";
  cp "${ARTIFACT_PATH}" "${GITHUB_RELEASE_DIR}/${ARTIFACT_NAME}"
fi
