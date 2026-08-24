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
	AppID              string          `json:"appId"`
	AutoRestartEnabled *bool           `json:"autoRestartEnabled,omitempty"`
	DevMode            *appDevModeSpec `json:"devMode,omitempty"`
	Runtime            appRuntime      `json:"runtime"`
	// ContainerSpec is decoded only to tell whether the field is present — the App
	// plays the workload role — or absent, which marks the product role. It is a map
	// rather than one of this struct's value types precisely because the marker is
	// absence, and a value type cannot tell an absent field from an empty one.
	ContainerSpec map[string]any `json:"containerSpec,omitempty"`
}

// appDevModeSpec mirrors the App CRD's spec.devMode block. The proxy only
// cares about the Enabled toggle; the remaining knobs (gitPollInterval,
// autoRunSetupOnDepChange) are interpreted by the operator and the in-pod
// runtime, not by apps-proxy.
type appDevModeSpec struct {
	Enabled bool `json:"enabled"`
}

type appRuntime struct {
	Backend appBackend `json:"backend"`
}

type appBackend struct {
	Type string `json:"type,omitempty"`
}

// isProduct reports whether the App plays the product (version-coordinator) role rather
// than the workload role. It mirrors the operator's App.IsProduct(): a product carries no
// containerSpec, because the things that run are its member Sandboxes.
func (o *appObject) isProduct() bool {
	return o.Spec.ContainerSpec == nil
}

// e2bAccessTokenSecretName returns the name of the Secret holding the app's E2B traffic
// access token, or "" when the app needs no such token.
//
// The two roles read different fields. On a workload App spec.runtime.backend.type is
// authoritative, and it also has to be consulted: status.e2bSandbox survives a switch
// away from the E2B backend — nothing clears it — while the Secret it names is owned by
// the deleted E2bSandbox and garbage-collected with it.
//
// A product App has no backend of its own. The backend belongs to the member Sandbox it
// routes to, and the operator mirrors that member's status.e2bSandbox here because the
// proxy routes by the App and does not watch Sandboxes. Its spec.runtime is either absent
// (sandboxes-service builds a product App without one) or /v1 residue left behind when a
// /v1 App was adopted into the product role by stripping containerSpec, so in neither
// case does it describe what serves the app's traffic.
func (o *appObject) e2bAccessTokenSecretName() string {
	name := o.Status.E2BSandbox.AccessTokenSecretName
	if name == "" {
		return ""
	}
	if !o.isProduct() && o.Spec.Runtime.Backend.Type != BackendTypeE2BSandbox {
		return ""
	}
	return name
}

// AppInfo is the cached state for an app, read from the K8s watcher.
type AppInfo struct {
	ActualState        AppActualState
	AutoRestartEnabled bool
	// DevMode mirrors spec.devMode.enabled from the App CRD. When true the
	// proxy enables the kai-preview iframe-auth path for the app.
	DevMode bool
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
