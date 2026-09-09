package sessions

import (
	"net/url"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// stubDeps is the smallest thing satisfying the package's dependencies
// interface. The mocked service scope substitutes a cookie secret salt when one
// is missing, so it cannot be used to test what happens without it.
type stubDeps struct {
	logger log.DebugLogger
	cfg    config.Config
	proc   *servicectx.Process
}

func (d stubDeps) Clock() clockwork.Clock         { return clockwork.NewFakeClock() }
func (d stubDeps) Logger() log.Logger             { return d.logger }
func (d stubDeps) Config() config.Config          { return d.cfg }
func (d stubDeps) Process() *servicectx.Process   { return d.proc }
func (d stubDeps) Telemetry() telemetry.Telemetry { return telemetry.NewNop() }

func TestNewManager_EmptySaltDisablesTracking(t *testing.T) {
	t.Parallel()

	// With no salt the per-app signing key is SHA256 of a constant prefix and
	// the app id, both of which anyone can compute, so every session cookie
	// would be forgeable. Config validation requires the salt and should never
	// let this happen — but tracking that silently accepts cookies it cannot
	// verify is worse than tracking being off.
	logger := log.NewDebugLogger()
	cfg := config.New()
	cfg.Sessions.StreamURL = "https://stream.example.invalid/stream/1/s/secret"
	cfg.CookieSecretSalt = ""
	publicURL, err := url.Parse("https://hub.keboola.local")
	require.NoError(t, err)
	cfg.API.PublicURL = publicURL

	proc := servicectx.New(servicectx.WithoutSignals())
	t.Cleanup(func() { proc.Shutdown(t.Context(), nil); proc.WaitForShutdown() })

	m := NewManager(t.Context(), stubDeps{logger: logger, cfg: cfg, proc: proc})

	require.False(t, m.enabled, "tracking must not run with a forgeable key")
	require.Nil(t, m.writer, "no writer, so nothing can be sent either")
	logger.AssertJSONMessages(t, `{"level":"error","message":"session tracking is disabled, cookie secret salt is empty"}`)
}
