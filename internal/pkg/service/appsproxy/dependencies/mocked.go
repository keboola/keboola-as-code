package dependencies

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	k8sfake "k8s.io/client-go/dynamic/fake"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

// mocked implements Mocked interface.
type mocked struct {
	dependencies.Mocked
	config        config.Config
	fakeK8sClient *k8sfake.FakeDynamicClient
}

func (v *mocked) TestConfig() config.Config {
	return v.config
}

// TestFakeK8sClient returns the fake Kubernetes dynamic client used by this mock.
// Tests can use it to pre-populate App CRD objects and inspect PATCH actions.
func (v *mocked) TestFakeK8sClient() *k8sfake.FakeDynamicClient {
	return v.fakeK8sClient
}

// K8sDynamicClient implements k8sClientProvider, supplying the fake client to newServiceScope.
func (v *mocked) K8sDynamicClient() dynamic.Interface {
	return v.fakeK8sClient
}

func NewMockedServiceScope(tb testing.TB, ctx context.Context, cfg config.Config, opts ...dependencies.MockedOption) (ServiceScope, Mocked) {
	tb.Helper()

	commonMock := dependencies.NewMocked(tb, ctx, opts...)

	// Fill in missing fields
	if cfg.API.PublicURL == nil {
		var err error
		cfg.API.PublicURL, err = url.Parse("https://hub.keboola.local")
		require.NoError(tb, err)
	}
	if cfg.CookieSecretSalt == "" {
		cfg.CookieSecretSalt = "foo"
	}
	if cfg.CsrfTokenSalt == "" {
		cfg.CsrfTokenSalt = "bar"
	}
	if cfg.SandboxesAPI.URL == "" {
		cfg.SandboxesAPI.URL = "http://sandboxes-service-api.default.svc.cluster.local"
	}
	if cfg.SandboxesAPI.Token == "" {
		cfg.SandboxesAPI.Token = "my-token"
	}
	if cfg.K8s.AppsNamespace == "" {
		cfg.K8s.AppsNamespace = "keboola"
	}

	// Create fake K8s dynamic client. The App list kind is registered so the informer can list CRDs.
	scheme := runtime.NewScheme()
	fakeClient := k8sfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		k8sapp.AppGVR: "AppList",
	})

	// Validate config
	require.NoError(tb, configmap.ValidateAndNormalize(&cfg))

	mock := &mocked{Mocked: commonMock, config: cfg, fakeK8sClient: fakeClient}

	scope, err := newServiceScope(ctx, mock, cfg)
	require.NoError(tb, err)

	mock.DebugLogger().Truncate()
	if !commonMock.UseRealAPIs() {
		mock.MockedHTTPTransport().Reset()
	}
	return scope, mock
}
