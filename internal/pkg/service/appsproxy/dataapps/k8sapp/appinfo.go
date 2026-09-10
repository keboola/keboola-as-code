// Package k8sapp provides watching and patching of App CRDs in Kubernetes.
package k8sapp

import (
	"net"
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

const (
	Group    = "apps.keboola.com"
	Version  = "v2"
	Resource = "apps"

	SandboxVersion  = "v1"
	SandboxResource = "sandboxes"

	BackendTypeE2BSandbox = "e2bSandbox"
)

// AppGVR returns the GroupVersionResource for the App CRD.
func AppGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: Group, Version: Version, Resource: Resource}
}

// SandboxGVR returns the GroupVersionResource for the Sandbox CRD.
func SandboxGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: Group, Version: SandboxVersion, Resource: SandboxResource}
}

// NormalizeHost strips any port and lowercases the hostname, so index keys and
// request hostnames are compared the same way.
func NormalizeHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.ToLower(host)
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

// WorkloadRef identifies the workload that serves a route.
// An empty SandboxName means the App CR itself owns the route.
type WorkloadRef struct {
	AppID       api.AppID
	SandboxName string
}

func (r WorkloadRef) String() string {
	if r.SandboxName == "" {
		return r.AppID.String()
	}
	var b strings.Builder
	b.WriteString(r.AppID.String())
	b.WriteString("/sandbox/")
	b.WriteString(r.SandboxName)
	return b.String()
}

// appObject is a minimal struct for unmarshalling App and Sandbox CRD objects.
// Both kinds are read through this one struct: every field the proxy needs has
// the same JSON name and meaning on both, because the Sandbox CRD reuses the
// App's RuntimeSpec/DevModeSpec and inlines the same WorkloadStatus.
type appObject struct {
	Spec   appSpec   `json:"spec"`
	Status appStatus `json:"status"`
}

type appSpec struct {
	AppID              string          `json:"appId"`
	Features           *appFeatures    `json:"features,omitempty"`
	AutoRestartEnabled *bool           `json:"autoRestartEnabled,omitempty"`
	DevMode            *appDevModeSpec `json:"devMode,omitempty"`
	Runtime            appRuntime      `json:"runtime"`
}

// appDevModeSpec mirrors the App CRD's spec.devMode block. The proxy only
// cares about the Enabled toggle; the remaining knobs (gitPollInterval,
// autoRunSetupOnDepChange) are interpreted by the operator and the in-pod
// runtime, not by apps-proxy.
type appDevModeSpec struct {
	Enabled bool `json:"enabled"`
}

// appFeatures carries only appsProxyIngress, and only its presence is read:
// the operator publishes status.appsProxy.publicUrl exactly when this block is
// set, so its absence means the workload will never own a hostname.
type appFeatures struct {
	AppsProxyIngress *appsProxyIngress `json:"appsProxyIngress,omitempty"`
}

type appsProxyIngress struct {
	Slug string `json:"slug,omitempty"`
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
	PublicURL   string `json:"publicUrl,omitempty"`
}
