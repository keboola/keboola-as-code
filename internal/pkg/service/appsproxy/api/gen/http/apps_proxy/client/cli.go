// Code generated by goa v3.19.1, DO NOT EDIT.
//
// apps-proxy HTTP client CLI support package
//
// Command:
// $ goa gen github.com/keboola/keboola-as-code/api/appsproxy --output
// ./internal/pkg/service/appsproxy/api

package client

import (
	appsproxy "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/gen/apps_proxy"
)

// BuildValidatePayload builds the payload for the apps-proxy Validate endpoint
// from CLI flags.
func BuildValidatePayload(appsProxyValidateStorageAPIToken string) (*appsproxy.ValidatePayload, error) {
	var storageAPIToken string
	{
		storageAPIToken = appsProxyValidateStorageAPIToken
	}
	v := &appsproxy.ValidatePayload{}
	v.StorageAPIToken = storageAPIToken

	return v, nil
}
