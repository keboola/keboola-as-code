package dependencies

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func TestNewPublicDeps_LazyLoadComponents(t *testing.T) {
	t.Parallel()
	tel := telemetry.NewNopTelemetry()
	httpClient := httpclient.New(tel)
	baseDeps := newBaseDeps(env.Empty(), log.NewNopLogger(), tel, clock.New(), httpClient)

	// Create public deps without loading components.
	deps, err := newPublicDeps(context.Background(), baseDeps, "https://connection.keboola.com")
	assert.NoError(t, err)
	// Check the components are loaded lazily.
	c, found := deps.Components().Get("keboola.ex-currency")
	assert.True(t, found)
	assert.Equal(t, "keboola.ex-currency", c.ID.String())
}
