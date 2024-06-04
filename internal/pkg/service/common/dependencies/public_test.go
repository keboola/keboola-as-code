package dependencies

import (
	"context"
	"os"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func TestNewPublicDeps_LazyLoadComponents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	httpClient := httpclient.New()
	baseDeps := newBaseScope(ctx, log.NewNopLogger(), telemetry.NewNop(), os.Stdout, os.Stderr, clock.New(), servicectx.NewForTest(t), httpClient)

	// Create public deps without loading components.
	deps, err := newPublicScope(context.Background(), baseDeps, "https://connection.keboola.com")
	require.NoError(t, err)

	// Check the components are loaded lazily.
	c, found := deps.Components().Get("keboola.ex-currency")
	assert.True(t, found)
	assert.Equal(t, "keboola.ex-currency", c.ID.String())
}
