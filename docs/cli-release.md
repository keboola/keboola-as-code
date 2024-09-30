# CLI Release Process

The release process is handled by [.github/workflows](../.github/workflows).

## GoReleaser Automation

- [GoReleaser](https://goreleaser.com/) automation defined in [build/ci/goreleaser.yml](../build/ci/goreleaser.yml).
- The `cross-compile` step of the [GitHub Workflow](#workflow-steps) runs `goreleaser build` through `make build` to compile the binaries
- The `release` step of the [GitHub Workflow](#workflow-steps) runs `goreleaser release` through `make release` but the release functionality itself is disabled. The tool just:
  - signs the [macOS binary](#macos) for [Homebrew distribution](#homebrew)
  - creates mainfest for [Scoop](#scoop)
  - uses [GoReleaser publisher](https://goreleaser.com/customization/publishers/) functionality to upload the generated binaries to [distribution S3](#s3-distribution) 

## MSI Installer

- Exe files for Windows distributed through Winget and Chocolatey have to be bundled in installers
- [MSBuild](https://docs.microsoft.com/en-us/visualstudio/msbuild/msbuild) with [WiX Toolset](https://wixtoolset.org/) is used
- The MSI file is created in the `release-msi-windows` step of the [GitHub Workflow](#workflow-steps)
- The MSI file is uploaded to S3 in the same step

## WinGet

- [WinGet](https://learn.microsoft.com/en-us/windows/package-manager/winget/) is a package manager for Windows by Microsoft
- The manifest is created using [wingetcreate](https://github.com/microsoft/winget-create) tool and pushed to the repository using `update-repositories-windows` step of the [GitHub Workflow](#workflow-steps)
- The manifest is published to the official repository
- [Initial PR](https://github.com/microsoft/winget-pkgs/pull/47486)
- Our release workflow sends a new PR to [microsoft/winget-pkgs](https://github.com/microsoft/winget-pkgs/pulls?q=is%3Apr+sort%3Aupdated-desc+Keboola)
- We need to wait for one of the maintainers to merge the PR, this can take a few days
- To install Keboola CLI with winget, use `winget install --id=Keboola.KeboolaCLI  -e`

## macOS

- macOS binaries are signed with an Apple Developer code signing certificate
- The certificate is issued by Tomas Netrval's Apple Developer account for the time being
- macOS binaries are distributed using [Homebrew distribution](#homebrew)

## Homebrew

- [Homebrew](https://brew.sh/) is a package manager for macOS and Linux
- The manifest is created in the `release` step of the [GitHub Workflow](#workflow-steps)
- The manifest is pushed to the repository in the `update-repositories` step of the [GitHub Workflow](#workflow-steps) 
- We use our own manifest repository located on url https://github.com/keboola/homebrew-keboola-cli


## Chocolatey

- [Chocolatey](https://chocolatey.org/) is a package manager for Windows
- The package is created and pushed to the community repository using `update-repositories-windows` step of the [GitHub Workflow](#workflow-steps)
- The package is published to the community repository: https://community.chocolatey.org/packages/keboola-cli
- Updates to the package are authenticated by API key stored in `CHOCOLATEY_KEY` secret 
- Our release workflow sends the new version to Chocolatey which then runs 3 [steps](https://community.chocolatey.org/packages/keboola-cli#testingResults) called "Validation", "Verification" and "Scan"
- Check the current status on top of the page for the new version which is linked from the [version history](https://community.chocolatey.org/packages/keboola-cli#versionhistory)
- Sometimes Chocolatey checks may decide that a manual review is necessary which can take longer

## Scoop

- [Scoop](https://scoop.sh/) is a package manager for Windows
- The manifest is created using [build/ci/goreleaser.yml](../build/ci/goreleaser.yml) in the `release` step of the [GitHub Workflow](#workflow-steps)
- The manifest is pushed to the repository in the `update-repositories-windows` step of the [GitHub Workflow](#workflow-steps)
- We use our own manifest repository (bucket) located on url https://github.com/keboola/scoop-keboola-cli


## Linux Repositories

- The `update-repositories` step of the [GitHub Workflow](#workflow-steps) is handling updates to Linux repositories
- Supported packages:
  - [Alpine Linux packages](https://pkgs.alpinelinux.org/packages)
  - [Debian packages](https://packages.debian.org/)
  - [RPM Package Manager](https://rpm.org/)

## S3 Distribution

- The S3 bucket is publicly available on url https://cli-dist.keboola.com
- The bucket is provisioned in dedicated AWS account using Terraform.


## CLI Distribution Terraform Setup

### Terraform backend init

Testing:

```shell
export AWS_PROFILE="Test-Keboola-As-Code-Assets"
export AWS_DEFAULT_REGION="eu-central-1"
export TERRAFORM_BACKEND_STACK_PREFIX="keboola-ci-kac-assets"
./provisioning/cli-dist/scripts/create-backend.sh
```

Production:

```shell
export AWS_PROFILE="Prod-Keboola-As-Code-Assets"
export AWS_DEFAULT_REGION="eu-central-1"
export TERRAFORM_BACKEND_STACK_PREFIX="keboola-prod-kac-assets"
./provisioning/cli-dist/scripts/create-backend.sh
```

### OIDC authorization for GitHub Actions

See the [documentation](https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services) for the OIDC background between AWS and GitHub Actions.

#### 1. Create a GitHub OIDC Provider

- got to the [IAM console -> Identity providers](https://console.aws.amazon.com/iamv2/home?#/identity_providers)
- click Add new provider
- select OpenID Connect
- fill provider url: `https://token.actions.githubusercontent.com` **(Don't forget to click Get Thumbprint)**
- fill audience: `sts.amazonaws.com`
- click add provider

#### 2. Create AWS role for GitHub Actions to testing environment

Fill ARN from the previous step in env `GITHUB_OIDC_PROVIDER_ARN`

```shell
export AWS_PROFILE="Test-Keboola-As-Code-Assets"
export AWS_DEFAULT_REGION="eu-central-1"
export GITHUB_ORGANIZATION="keboola"
export GITHUB_REPOSITORY_NAME="keboola-as-code"
export GITHUB_OIDC_PROVIDER_ARN=arn:aws:iam::813746015128:oidc-provider/token.actions.githubusercontent.com
./provisioning/aws/scripts/create-github-testing-role.sh
```

The script will return the ARN **full admin access** role you will use in [aws-actions/configure-aws-credential](https://github.com/aws-actions/configure-aws-credentials) as a parameter `role-to-assume` to testing workflow.

#### 3. Create AWS roles for GitHub Actions to production environment

- fill ARN from the step one in env `GITHUB_OIDC_PROVIDER_ARN`
- fill terraform backend prefix CF stack  in env `TERRAFORM_BACKEND_STACK_PREFIX`

```shell
export AWS_PROFILE="Prod-Keboola-As-Code-Assets"
export AWS_DEFAULT_REGION="eu-central-1"
export GITHUB_ORGANIZATION="keboola"
export GITHUB_REPOSITORY_NAME="keboola-as-code"
export GITHUB_OIDC_PROVIDER_ARN=arn:aws:iam::455460941449:oidc-provider/token.actions.githubusercontent.com
export TERRAFORM_BACKEND_STACK_PREFIX=keboola-prod-kac-assets
./provisioning/aws/scripts/create-github-production-role.sh
```
The script will return the ARN roles:

- **full admin access** role that can be called in GitHub Actions only over the `main` branch
- read only role for the whole account and attached policy which allows you to run terraform provisioning plan, you can use this role over any branch

### AWS ACM Certificate configuration
ACM Certificate for Cloudfront distribution is prepared and validated manually:

#### Test-Keboola-As-Code-Assets

1. Login into `Test-Keboola-As-Code-Assets` AWS account as Administrator
2. Go to [AWS Certificate manager](https://us-east-1.console.aws.amazon.com/acm/home?region=us-east-1#/welcome) in us-east-1 region
3. Request Public certificate
    - Fully qualified domain name: `*.keboola.dev`
    - Validation methond - DNS validation
4. Copy the `CNAME name` and `CNAME value` of requested certificate
5. Switch to `Prod-KBC-multi-tenant-legacy` and create CNAME DNS record from previous step in Route 53 `keboola.dev` Hosted Zone
6. Switch back

#### Prod-Keboola-As-Code-Assets
1. Login into `Prod-Keboola-As-Code-Assets` AWS account as Administrator
2. Go to [AWS Certificate manager](https://us-east-1.console.aws.amazon.com/acm/home?region=us-east-1#/welcome) in us-east-1 region
3. Request Public certificate
   - Fully qualified domain name: `*.keboola.com`
   - Validation methond - DNS validation
4. Copy the `CNAME name` and `CNAME value` of requested certificate
5. Switch to `Prod-KBC-multi-tenant-legacy` and create CNAME DNS record from previous step in Route 53 `keboola.com` Hosted Zone
6. Switch back to `Prod-Keboola-As-Code-Assets` AWS Account and wait until the certificate is validated 
