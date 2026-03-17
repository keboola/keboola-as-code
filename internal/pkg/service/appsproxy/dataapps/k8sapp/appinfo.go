// Package k8sapp provides watching and patching of App CRDs in Kubernetes.
package k8sapp

import (
	"net/url"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group    = "apps.keboola.com"
	Version  = "v2"
	Resource = "apps"

	BackendTypeE2BSandbox = "e2bSandbox"
)

// AppGVR returns the GroupVersionResource for the App CRD.
func AppGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: Group, Version: Version, Resource: Resource}
}

// SecretGVR returns the GroupVersionResource for core/v1 Secrets.
func SecretGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}
}

// AppActualState is the observed state of the app, read from .status.currentState.
type AppActualState string

const (
	AppActualStateStopped  AppActualState = "Stopped"
	AppActualStateRunning  AppActualState = "Running"
	AppActualStateStarting AppActualState = "Starting"
	AppActualStateStopping AppActualState = "Stopping"
)

// appObject is a minimal struct for unmarshalling App CRD objects — only the fields we need.
type appObject struct {
	Spec   appSpec   `json:"spec"`
	Status appStatus `json:"status"`
}

type appSpec struct {
	AppID              string     `json:"appId"`
	AutoRestartEnabled *bool      `json:"autoRestartEnabled,omitempty"`
	Runtime            appRuntime `json:"runtime"`
}

type appRuntime struct {
	Backend appBackend `json:"backend"`
}

type appBackend struct {
	Type string `json:"type,omitempty"`
}

// AppInfo is the cached state for an app, read from the K8s watcher.
type AppInfo struct {
	ActualState        AppActualState
	AutoRestartEnabled bool
	// UpstreamTarget is the pre-parsed URL from .status.appsProxy.upstreamUrl.
	// Nil when the field is absent or unparseable.
	UpstreamTarget *url.URL
	// E2BAccessToken is the access token loaded from the K8s Secret
	// referenced by .status.e2bSandbox.accessTokenSecretName.
	// Empty when the app is not an E2B sandbox or the secret is unavailable.
	E2BAccessToken string
}

type appStatus struct {
	CurrentState AppActualState `json:"currentState"`
	AppsProxy    appsProxy      `json:"appsProxy"`
	E2BSandbox   e2bSandbox     `json:"e2bSandbox"`
}

type e2bSandbox struct {
	AccessTokenSecretName string `json:"accessTokenSecretName,omitempty"`
}

type appsProxy struct {
	UpstreamURL string `json:"upstreamUrl,omitempty"`
}
