package dependencies

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/request"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/dynamic/fake"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestNewServiceScope_AuthProviderHTTPClientRefusesPrivateAddresses(t *testing.T) {
	t.Parallel()

	var called atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Store(true)
	}))
	t.Cleanup(srv.Close)

	cfg := config.New()
	cfg.SandboxesAPI.URL = "http://sandboxes-service-api.default.svc.cluster.local"
	cfg.K8s.AppsNamespace = "keboola"
	fakeClient := k8sfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		k8sapp.AppGVR():       "AppList",
		k8sapp.SandboxGVR():   "SandboxList",
		k8sapp.IdleTimerGVR(): "IdleTimerList",
	})
	mock := &mocked{Mocked: dependencies.NewMocked(t, t.Context()), config: cfg, fakeK8sClient: fakeClient}

	d, err := newServiceScope(t.Context(), mock, cfg)
	require.NoError(t, err)

	_, _, err = request.NewHTTPRequest(d.AuthProviderHTTPClient()).WithGet(srv.URL).Send(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not allowed")
	assert.False(t, called.Load())
}
