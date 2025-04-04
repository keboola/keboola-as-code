package dependencies

import (
	"crypto/rand"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

// mocked implements Mocked interface.
type mocked struct {
	dependencies.Mocked
	config              config.Config
	dummySinkController *dummy.SinkController
}

func (v *mocked) TestConfig() config.Config {
	return v.config
}

func (v *mocked) TestDummySinkController() *dummy.SinkController {
	return v.dummySinkController
}

func testConfig(tb testing.TB, d dependencies.Mocked) config.Config {
	tb.Helper()
	cfg := config.New()

	// Create empty volumes dir
	volumesPath := tb.TempDir()

	// Complete configuration
	cfg.NodeID = "test-node"
	cfg.Hostname = "localhost"

	cfg.StorageAPIHost = strings.TrimPrefix(d.StorageAPIHost(), "https://")
	cfg.Storage.VolumesPath = volumesPath
	cfg.API.PublicURL, _ = url.Parse("https://stream.keboola.local")
	cfg.Source.HTTP.PublicURL, _ = url.Parse("https://stream-in.keboola.local")
	cfg.Etcd = d.TestEtcdConfig()
	cfg.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(tb))

	// There are some timers with a few seconds interval.
	// It causes problems when mocked clock is used.
	// For example clock.Add(time.Hour) invokes the timer 3600 times, if the interval is 1s.
	if _, ok := d.Clock().(*clockwork.FakeClock); ok {
		cfg.Distribution.EventsGroupInterval = 0 // disable timer
		cfg.Storage.Statistics.Collector.Enabled = false
		cfg.Storage.Statistics.Cache.L2.Enabled = false
	}

	// Disable task cleanup in unrelated tests (it disturbs, for example, etcd ops logs)
	cfg.API.Task.CleanupEnabled = false

	// Use native encryption for tests
	secretKey := make([]byte, 32)
	_, err := rand.Read(secretKey)
	require.NoError(tb, err)
	cfg.Encryption.Provider = encryption.ProviderAES
	cfg.Encryption.AES.SecretKey = secretKey

	// Validate configuration
	require.NoError(tb, configmap.ValidateAndNormalize(&cfg))

	return cfg
}
