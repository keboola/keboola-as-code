# Apps proxy Architecture Overview

- Serves for data apps authentication and authorization.
- Typical usage is to perform OIDC login through some OIDC provider (e.g. Microsoft login, Google login etc.)
- Has possibility to add basic authorization which consists of password prompt on a web page.


## Entrypoint

[cmd/apps-proxy/main.go](../../cmd/apps-proxy/main.go)

## Apps Proxy Options

## Operations

**Note**: Apps Proxy deployment provisioning has been migrated to GitOps (PAT-868). The legacy deployment scripts and Kubernetes manifests have been removed from `provisioning/apps-proxy/`. Local development directories (`provisioning/apps-proxy/dev` and `provisioning/apps-proxy/docker`) are retained for local testing. For production deployment instructions, please refer to the platform GitOps repository.
