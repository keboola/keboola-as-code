#!/bin/bash

# Script to remove CLI release artifacts from S3 bucket
# This script removes all files for a given version from the S3 distribution bucket

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

if [ $# -lt 1 ]; then
  echo 1>&2 "$0: not enough arguments"
  echo "Usage: $0 <VERSION> [DRY_RUN]"
  echo "  VERSION: Version to remove (e.g., 1.2.3)"
  echo "  DRY_RUN: Set to 'true' to only show what would be deleted (optional)"
  exit 2
fi

VERSION=$1
DRY_RUN=${2:-false}

# Validate version format (basic semantic versioning check)
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-z]+\.[0-9]+)?$ ]]; then
  echo 1>&2 "Error: Invalid version format: $VERSION"
  echo "Expected format: X.Y.Z or X.Y.Z-prerelease.1"
  exit 2
fi

PACKAGE_NAME="keboola-cli"

echo "Removing CLI release version $VERSION from S3 bucket: ${AWS_BUCKET_NAME}"
if [ "$DRY_RUN" = "true" ]; then
  echo "DRY RUN MODE - No files will be deleted"
fi
echo

# Function to remove file from S3
remove_from_s3() {
  local s3_path=$1
  local description=$2
  
  if [ "$DRY_RUN" = "true" ]; then
    echo "[DRY RUN] Would remove: $s3_path ($description)"
  else
    if aws s3 ls "$s3_path" >/dev/null 2>&1; then
      echo "Removing: $s3_path ($description)"
      aws s3 rm "$s3_path"
    else
      echo "Not found (skipping): $s3_path ($description)"
    fi
  fi
}

# Function to remove directory from S3
remove_dir_from_s3() {
  local s3_path=$1
  local description=$2
  
  if [ "$DRY_RUN" = "true" ]; then
    echo "[DRY RUN] Would remove directory: $s3_path ($description)"
  else
    if aws s3 ls "$s3_path" >/dev/null 2>&1; then
      echo "Removing directory: $s3_path ($description)"
      aws s3 rm "$s3_path" --recursive
    else
      echo "Not found (skipping): $s3_path ($description)"
    fi
  fi
}

# Remove ZIP files
echo "Removing ZIP archives..."
remove_from_s3 "s3://${AWS_BUCKET_NAME}/zip/keboola-cli_${VERSION}_checksums.txt" "Checksums file"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/zip/keboola-cli_${VERSION}_linux_amd64.zip" "Linux AMD64 ZIP"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/zip/keboola-cli_${VERSION}_linux_arm64.zip" "Linux ARM64 ZIP"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/zip/keboola-cli_${VERSION}_linux_armv6.zip" "Linux ARMv6 ZIP"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/zip/keboola-cli_${VERSION}_darwin_amd64.zip" "macOS AMD64 ZIP"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/zip/keboola-cli_${VERSION}_darwin_arm64.zip" "macOS ARM64 ZIP"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/zip/keboola-cli_${VERSION}_windows_amd64.zip" "Windows AMD64 ZIP"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/zip/keboola-cli_${VERSION}_windows_arm64.zip" "Windows ARM64 ZIP"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/zip/keboola-cli_${VERSION}_windows_armv6.zip" "Windows ARMv6 ZIP"
echo

# Remove DEB packages
echo "Removing DEB packages..."
remove_from_s3 "s3://${AWS_BUCKET_NAME}/deb/pool/${PACKAGE_NAME}_${VERSION}_amd64.deb" "DEB AMD64"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/deb/pool/${PACKAGE_NAME}_${VERSION}_arm64.deb" "DEB ARM64"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/deb/pool/${PACKAGE_NAME}_${VERSION}_armv6.deb" "DEB ARMv6"
echo

# Remove RPM packages
echo "Removing RPM packages..."
remove_from_s3 "s3://${AWS_BUCKET_NAME}/rpm/${PACKAGE_NAME}_${VERSION}_amd64.rpm" "RPM AMD64"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/rpm/${PACKAGE_NAME}_${VERSION}_arm64.rpm" "RPM ARM64"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/rpm/${PACKAGE_NAME}_${VERSION}_armv6.rpm" "RPM ARMv6"
echo

# Remove APK packages
echo "Removing APK packages..."
remove_from_s3 "s3://${AWS_BUCKET_NAME}/apk/x86_64/${PACKAGE_NAME}-${VERSION}.apk" "APK x86_64"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/apk/aarch64/${PACKAGE_NAME}-${VERSION}.apk" "APK aarch64"
remove_from_s3 "s3://${AWS_BUCKET_NAME}/apk/armhf/${PACKAGE_NAME}-${VERSION}.apk" "APK armhf"
echo

# Remove MSI installer
echo "Removing MSI installer..."
remove_from_s3 "s3://${AWS_BUCKET_NAME}/msi/keboola-cli_${VERSION}.msi" "MSI installer"
echo

# Remove version directory if it exists (legacy structure)
echo "Removing version directory (if exists)..."
remove_dir_from_s3 "s3://${AWS_BUCKET_NAME}/keboola-cli/${VERSION}/" "Version directory"
echo

if [ "$DRY_RUN" = "true" ]; then
  echo "DRY RUN completed - no files were deleted"
  echo "Run without DRY_RUN=true to actually remove files"
else
  echo "✅ All files for version $VERSION have been removed from S3"
  echo "⚠️  Note: Linux package repositories need to be re-indexed after removing packages"
fi

