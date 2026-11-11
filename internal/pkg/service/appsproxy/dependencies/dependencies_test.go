package dependencies

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/dynamic/fake"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestNewServiceScope_InvalidAppUserTokenKeysStopStartup(t *testing.T) {
	t.Parallel()

	key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keys, err := json.Marshal([]map[string]string{{"kid": "k1", "pem": string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))}})
	require.NoError(t, err)

	cfg := config.New()
	cfg.AppUserToken.SigningKeys = string(keys)
	cfg.AppUserToken.ActiveKid = "k1"
	cfg.SandboxesAPI.URL = "http://sandboxes-service-api.default.svc.cluster.local"
	cfg.K8s.AppsNamespace = "keboola"

	fakeClient := k8sfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		k8sapp.AppGVR():       "AppList",
		k8sapp.SandboxGVR():   "SandboxList",
		k8sapp.SecretGVR():    "SecretList",
		k8sapp.IdleTimerGVR(): "IdleTimerList",
	})
	mock := &mocked{Mocked: dependencies.NewMocked(t, t.Context()), config: cfg, fakeK8sClient: fakeClient}

	_, err = newServiceScope(t.Context(), mock, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `signingKeys[0] (kid "k1"): not an EC P-256 key`)
	assert.NotContains(t, err.Error(), "BEGIN")
}
