package authproxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

func TestManager_AttributeClaims(t *testing.T) {
	t.Parallel()

	app := api.AppConfig{ID: "my-app"}
	oidc := func(claims ...string) provider.OIDC {
		return provider.OIDC{Base: provider.Base{Info: provider.Info{ID: "my-provider", Type: provider.TypeOIDC}}, AttributeClaims: claims}
	}

	t.Run("a valid allowlist is used as it is", func(t *testing.T) {
		t.Parallel()
		logger := log.NewDebugLogger()
		m := &Manager{logger: logger}
		assert.Equal(t, []string{"department", "country"}, m.attributeClaims(app, oidc("department", "country")))
		assert.Empty(t, logger.AllMessages())
	})

	t.Run("no allowlist", func(t *testing.T) {
		t.Parallel()
		logger := log.NewDebugLogger()
		m := &Manager{logger: logger}
		assert.Nil(t, m.attributeClaims(app, oidc()))
		assert.Empty(t, logger.AllMessages())
	})

	t.Run("a bad allowlist is logged without a claim, and the app gets no attributes", func(t *testing.T) {
		t.Parallel()
		logger := log.NewDebugLogger()
		m := &Manager{logger: logger}
		assert.Nil(t, m.attributeClaims(app, oidc("department", "email")))
		logs := logger.AllMessages()
		assert.Contains(t, logs, `data app \"my-app\" provider \"my-provider\": the attributeClaims are not used`)
		assert.NotContains(t, logs, "department")
		assert.NotContains(t, logs, "email")
	})

	t.Run("a provider that has no allowlist", func(t *testing.T) {
		t.Parallel()
		m := &Manager{logger: log.NewDebugLogger()}
		assert.Nil(t, m.attributeClaims(app, provider.GitHub{}))
	})
}
