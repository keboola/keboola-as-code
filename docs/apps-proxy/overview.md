# Apps proxy Architecture Overview

- Serves for data apps authentication and authorization.
- Typical usage is to perform OIDC login through some OIDC provider (e.g. Microsoft login, Google login etc.)
- Has possibility to add basic authorization which consists of password prompt on a web page.


## Entrypoint

[cmd/apps-proxy/main.go](../../cmd/apps-proxy/main.go)

## Apps Proxy Options

## Operations

**Note**: Apps Proxy provisioning has been migrated to GitOps (PAT-868). The legacy provisioning files in `provisioning/apps-proxy/` have been removed. For deployment and local development instructions, please refer to the platform GitOps repository.
