# CLI Release Revert Process

This document describes how to revert a CLI release that has been published to all distribution channels.

## ⚠️ Important Considerations

**Reverting a release is complex and may not be fully feasible** once packages are published to external package managers. The feasibility depends on:

1. **Timing**: How quickly you catch the issue
2. **Package Manager Policies**: Some package managers don't allow package removal
3. **User Impact**: Users who already installed the version will still have it

**Best Practice**: If possible, release a hotfix version instead of reverting, as it's cleaner and more reliable.

## Distribution Channels Overview

When a release is published, it's distributed through:

1. **GitHub Releases** - Artifacts uploaded as prerelease
2. **S3 Distribution** (`cli-dist.keboola.com`) - Direct downloads and Linux package repositories
3. **Homebrew** - macOS/Linux package manager (keboola/homebrew-keboola-cli)
4. **Scoop** - Windows package manager (keboola/scoop-keboola-cli)
5. **Chocolatey** - Windows package manager (community.chocolatey.org)
6. **WinGet** - Windows package manager (microsoft/winget-pkgs via PR)
7. **Linux Package Repositories** - DEB, RPM, APK packages indexed in S3

## Revert Steps

### Automated Revert (Recommended)

The automated workflow handles most of the revert process. See the [Automated Revert Workflow](#automated-revert-workflow) section below for detailed instructions.

**What's automated:**
- ✅ S3 file removal
- ✅ Linux repository re-indexing
- ✅ GitHub release unpublishing

**What still requires manual action:**
- ⚠️ Homebrew PR revert in [keboola/homebrew-keboola-cli](https://github.com/keboola/homebrew-keboola-cli)
- ⚠️ Scoop PR revert in [keboola/scoop-keboola-cli](https://github.com/keboola/scoop-keboola-cli)
- ⚠️ WinGet PR closure (if not merged)
- ⚠️ Chocolatey package unlisting (if possible)

### Manual Steps (Only for Non-Automated Channels)

These steps cannot be automated and must be done manually:

#### 1. Homebrew

**Action**: Revert the PR that added the version

**Repository**: `https://github.com/keboola/homebrew-keboola-cli`

**Process**:
1. Go to the repository
2. Find the PR that added the version (search for the version number)
3. Revert that PR or close it if not merged
4. If the PR is already merged, create a new PR to revert the formula to the previous version

#### 2. Scoop

**Action**: Revert the PR that added the version

**Repository**: `https://github.com/keboola/scoop-keboola-cli`

**Process**:
1. Go to the repository
2. Find the PR that added the version (search for the version number)
3. Revert that PR or close it if not merged
4. If the PR is already merged, create a new PR to revert the manifest to the previous version

#### 3. WinGet

**Action**: Close the PR (if not merged)

**Repository**: `https://github.com/microsoft/winget-pkgs`

**Process**:
1. Go to the repository
2. Find the PR for the version (search for "Keboola" and the version number)
3. If the PR is **not merged**: Close it with a comment explaining why
4. If the PR is **already merged**: 
   - You cannot easily revert it
   - Contact WinGet maintainers or create a new PR to remove/update the version
   - This is a manual process and may take time

**Note**: WinGet PRs can take days to merge, so if caught early, you may be able to close it.

#### 4. Chocolatey

**Action**: Unlist the package (if possible)

**Repository**: `https://community.chocolatey.org/packages/keboola-cli`

**Limitations**: 
- Chocolatey typically **does not allow package deletion** once published
- You can request **unlisting** through their moderation process
- Contact Chocolatey support or use their moderation interface

**Process**:
1. Log in to Chocolatey with the API key (stored in `CHOCOLATEY_KEY` secret)
2. Navigate to package page
3. Request unlisting through moderation interface
4. This may take time and may not be approved

**Alternative**: Release a hotfix version that marks the bad version as deprecated.

## Automated Revert Workflow

An automated GitHub Actions workflow handles the complete revert process for most distribution channels.

### How to Trigger the Workflow

1. Go to the [Actions tab](https://github.com/keboola/keboola-as-code/actions) in GitHub
2. Select the **"Revert: CLI Release"** workflow from the left sidebar
3. Click **"Run workflow"** button (top right)
4. Fill in the inputs:
   - **Version**: Enter the version to revert (e.g., `1.2.3`)
   - **Dry run**: Optionally enable to preview changes without making them
5. Click **"Run workflow"** to start

### What the Workflow Automates

The automated workflow handles:

1. **Version Validation**: Validates the semantic version format
2. **S3 Cleanup**: Removes all files for the version from S3 bucket
   - ZIP archives (all platforms and architectures)
   - DEB packages (all architectures)
   - RPM packages (all architectures)
   - APK packages (all architectures)
   - MSI installer
3. **Linux Repository Re-indexing**: Automatically re-indexes DEB, RPM, and APK repositories after package removal
4. **GitHub Release**: Unpublishes and deletes the GitHub release and tag

### After Running the Workflow

After the workflow completes, you need to manually revert PRs in the package manager repositories:

1. **Homebrew**: Revert the PR in [keboola/homebrew-keboola-cli](https://github.com/keboola/homebrew-keboola-cli) that added the version
2. **Scoop**: Revert the PR in [keboola/scoop-keboola-cli](https://github.com/keboola/scoop-keboola-cli) that added the version
3. **WinGet**: Close PR in [microsoft/winget-pkgs](https://github.com/microsoft/winget-pkgs) if not merged
4. **Chocolatey**: Request unlisting at [community.chocolatey.org](https://community.chocolatey.org/packages/keboola-cli) if possible

### Workflow Files

- **Workflow**: `.github/workflows/revert-cli-release.yml`
- **S3 Unpublish Script**: `build/package/s3/unpublish.sh`

### Dry Run Mode

The workflow supports dry-run mode to preview changes:
- Enable "Dry run" when triggering the workflow
- The workflow will show what would be done without actually making changes
- Useful for testing or verifying the revert process before execution

## Verification Steps

After reverting, verify:

1. **GitHub**: Release is deleted/unpublished
2. **S3**: Files are removed and repositories re-indexed
3. **Homebrew**: Formula doesn't reference the version
4. **Scoop**: Manifest doesn't reference the version
5. **Chocolatey**: Package is unlisted (if possible)
6. **WinGet**: PR is closed (if not merged)

Test installation from each channel to ensure the version is no longer available.

## Prevention

To prevent the need for reverts:

1. **Use prerelease tags** for testing: `v1.2.3-rc.1`
2. **Test thoroughly** before creating a production tag
3. **Monitor CI/CD** during release process
4. **Staged rollouts**: Release to one channel first, verify, then others
5. **Version validation**: Ensure version numbers follow semantic versioning

## When Revert is Not Feasible

If a revert is not feasible (e.g., Chocolatey package already published, WinGet PR merged), consider:

1. **Release a hotfix**: Quickly release a new version that fixes the issue
2. **Deprecate the version**: Mark it as deprecated in package managers that support it
3. **Communicate with users**: Announce the issue and provide upgrade instructions
4. **Document the issue**: Add known issues documentation

## Related Documentation

- [CLI Release Process](./cli-release.md) - How releases are created
- [S3 Distribution Setup](./cli-release.md#s3-distribution) - S3 bucket configuration
- [Package Manager Details](./cli-release.md) - Details about each package manager

