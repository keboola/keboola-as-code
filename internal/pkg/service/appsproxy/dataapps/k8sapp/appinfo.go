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
)

// AppGVR returns the GroupVersionResource for the App CRD.
func AppGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: Group, Version: Version, Resource: Resource}
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
	AppID              string `json:"appId"`
	AutoRestartEnabled *bool  `json:"autoRestartEnabled,omitempty"`
}

// AppInfo is the cached state for an app, read from the K8s watcher.
type AppInfo struct {
	ActualState        AppActualState
	AutoRestartEnabled bool
	// UpstreamTarget is the pre-parsed URL from .status.appsProxy.upstreamUrl.
	// Nil when the field is absent or unparseable.
	UpstreamTarget *url.URL
}

type appStatus struct {
	CurrentState AppActualState `json:"currentState"`
	AppsProxy    appsProxy      `json:"appsProxy"`
}

type appsProxy struct {
	UpstreamURL string `json:"upstreamUrl,omitempty"`
}
