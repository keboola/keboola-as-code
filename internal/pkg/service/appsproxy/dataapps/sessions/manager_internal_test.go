package sessions

import (
	"net/url"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
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

func TestManager_ActivityAttributesARecreatedEntry(t *testing.T) {
	t.Parallel()

	// A websocket close removes the entry. The next frame of an overlapping
	// connection on the same session id recreates it through activity() alone,
	// without begin() running again — the realistic case being a Streamlit
	// reconnect whose new socket is up before the old one's close fires.
	//
	// An entry with no session cannot be turned into an event at all, so a
	// deploy would discard the frames it had collected, and silently.
	m := &Manager{
		enabled: true,
		clock:   clockwork.NewFakeClock(),
		cfg:     config.Sessions{HeartbeatInterval: time.Hour},
		store:   newStore(),
		metrics: testMetrics(),
		writer: &writer{
			logger:  log.NewNopLogger(),
			metrics: testMetrics(),
			queue:   make(chan Event, 8),
			done:    make(chan struct{}),
		},
	}

	s := &Session{ID: "sess-1", StartedAt: m.clock.Now()}
	ctx := contextWith(t.Context(), s)

	// begin() ran once for connection A, then A closed and took the entry.
	first, _ := m.store.getOrInit(s.ID, m.clock.Now())
	first.session = s
	_, taken := m.store.take(s.ID)
	require.True(t, taken)

	// Connection B's frames. The first one is past its (zero) heartbeat
	// deadline so it flushes and arms the throttle; the second only counts.
	m.ActivityWS(ctx)
	m.ActivityWS(ctx)

	item, created := m.store.getOrInit(s.ID, m.clock.Now())
	require.False(t, created, "activity should have recreated the entry")
	require.NotNil(t, item.session, "and attributed it, or nothing can drain it")

	event, ok := m.drain(t.Context(), item)
	require.True(t, ok, "a shutdown must still flush what connection B collected")
	assert.Equal(t, 1, event.WSFrames)
	assert.Equal(t, s.ID, event.SessionID)
}
