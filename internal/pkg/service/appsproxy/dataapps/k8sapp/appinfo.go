// Package k8sapp provides watching and patching of App CRDs in Kubernetes.
package k8sapp

import (
	"net"
	"net/url"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

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

// gvrFor returns the CRD that owns the workload's route, which is the one a
// state patch has to go to.
func gvrFor(ref WorkloadRef) schema.GroupVersionResource {
	if ref.IsSandbox() {
		return SandboxGVR()
	}
	return AppGVR()
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

// IsSandbox reports whether a Sandbox CR, rather than the App CR, owns the route.
func (r WorkloadRef) IsSandbox() bool {
	return r.SandboxName != ""
}

func (r WorkloadRef) String() string {
	if !r.IsSandbox() {
		return r.AppID.String()
	}
	var b strings.Builder
	b.WriteString(r.AppID.String())
	b.WriteString("/sandbox/")
	b.WriteString(r.SandboxName)
	return b.String()
}

// appObject unmarshals both App and Sandbox CRDs: every field the proxy needs
// has the same JSON name and meaning on each.
type appObject struct {
	Spec   appSpec   `json:"spec"`
	Status appStatus `json:"status"`
}

type appSpec struct {
	AppID              string          `json:"appId"`
	AutoRestartEnabled *bool           `json:"autoRestartEnabled,omitempty"`
	DevMode            *appDevModeSpec `json:"devMode,omitempty"`
	Runtime            appRuntime      `json:"runtime"`
	// AutoSuspendAfterSeconds is set on Sandbox CRs only. Absent means the
	// workload is never auto-suspended, which is why it is a pointer: zero is
	// not the same answer as unset.
	AutoSuspendAfterSeconds *int64 `json:"autoSuspendAfterSeconds,omitempty"`
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
	// PublicHost is the exact hostname the workload published at
	// .status.appsProxy.publicUrl. Empty when it published none.
	PublicHost string
	// E2BAccessToken is the access token loaded from the K8s Secret
	// referenced by .status.e2bSandbox.accessTokenSecretName.
	// Empty when the app is not an E2B sandbox or the secret is unavailable.
	E2BAccessToken string
}

type appStatus struct {
	CurrentState AppActualState `json:"currentState"`
	AppsProxy    appsProxy      `json:"appsProxy"`
	E2BSandbox   e2bSandbox     `json:"e2bSandbox"`
	// ProductionSandbox is set on App CRs only: the member Sandbox that runs
	// the workload the App routes to.
	ProductionSandbox string `json:"productionSandbox,omitempty"`
	// LastStartedTime is set on Sandbox CRs only.
	LastStartedTime *metav1.Time `json:"lastStartedTime,omitempty"`
}

type e2bSandbox struct {
	AccessTokenSecretName string `json:"accessTokenSecretName,omitempty"`
}

type appsProxy struct {
	UpstreamURL string `json:"upstreamUrl,omitempty"`
	PublicURL   string `json:"publicUrl,omitempty"`
}

// SuspendCandidate is a Running workload the idle-suspend loop may act on.
//
// Ref keys the activity the proxy records; SandboxName is the workload itself,
// which for an App route is the member the App names rather than the App. The
// two differ for every production workload, and the record belongs to the
// Sandbox, because that is what a restart and a deletion happen to.
type SuspendCandidate struct {
	Ref         WorkloadRef
	SandboxName string
	// SandboxUID is the owner the IdleTimer points at. Garbage collection
	// matches on it, so a record created with the wrong one is collected at once.
	SandboxUID types.UID
	// Threshold is zero when the Sandbox carries none, which means never
	// auto-suspend. The caller counts those; it is not a default.
	Threshold   time.Duration
	LastStarted time.Time
}

// WorkloadSnapshot is one round's view of what the idle-suspend loop can act on.
//
// Unresolved counts Running Apps left out because status.productionSandbox
// named no cached Sandbox. They never suspend, so they belong with workloads
// that carry no threshold: both are reasons the loop is doing nothing, and a
// gate that watches only one of them reads clear while the other is happening.
type WorkloadSnapshot struct {
	Candidates []SuspendCandidate
	Unresolved int
}
