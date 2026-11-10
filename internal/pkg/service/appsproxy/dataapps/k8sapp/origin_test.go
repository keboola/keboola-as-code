package k8sapp_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

func TestPublicOrigin(t *testing.T) {
	t.Parallel()

	slug := "my-app"
	app := api.AppConfig{ID: "123", AppSlug: &slug}
	https := &url.URL{Scheme: "https", Host: "hub.keboola.com"}
	withPort := &url.URL{Scheme: "http", Host: "hub.keboola.local:8000"}
	sandbox := k8sapp.WorkloadRef{AppID: "123", SandboxName: "app-123-sbx"}
	sandboxInfo := k8sapp.AppInfo{PublicHost: "my-app-123-dev.hub.keboola.com"}

	cases := []struct {
		name      string
		publicURL *url.URL
		workload  k8sapp.WorkloadRef
		info      k8sapp.AppInfo
		known     bool
		want      string
		wantOK    bool
	}{
		{name: "app", publicURL: https, workload: k8sapp.WorkloadRef{AppID: "123"}, want: "https://my-app-123.hub.keboola.com", wantOK: true},
		{name: "app ignores the sandbox host", publicURL: https, workload: k8sapp.WorkloadRef{AppID: "123"}, info: sandboxInfo, known: true, want: "https://my-app-123.hub.keboola.com", wantOK: true},
		{name: "app keeps the port", publicURL: withPort, workload: k8sapp.WorkloadRef{AppID: "123"}, want: "http://my-app-123.hub.keboola.local:8000", wantOK: true},
		{name: "sandbox", publicURL: https, workload: sandbox, info: sandboxInfo, known: true, want: "https://my-app-123-dev.hub.keboola.com", wantOK: true},
		{name: "sandbox keeps the port", publicURL: withPort, workload: sandbox, info: k8sapp.AppInfo{PublicHost: "my-app-123-dev.hub.keboola.local"}, known: true, want: "http://my-app-123-dev.hub.keboola.local:8000", wantOK: true},
		{name: "sandbox state unknown", publicURL: https, workload: sandbox},
		{name: "sandbox without host", publicURL: https, workload: sandbox, known: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := k8sapp.PublicOrigin(tc.publicURL, app, tc.workload, tc.info, tc.known)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}
