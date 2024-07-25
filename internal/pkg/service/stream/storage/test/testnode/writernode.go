package testnode

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

func StartDiskWriterNode(tb testing.TB, logger log.DebugLogger, etcdCfg etcdclient.Config, volumesCount int, modifyConfig func(cfg *config.Config), opts ...commonDeps.MockedOption) (dependencies.StorageWriterScope, dependencies.Mocked) {
	tb.Helper()

	// Create volumes directories
	volumesPath := tb.TempDir()
	for i := range volumesCount {
		volumeID := volume.ID(fmt.Sprintf("my-volume-%03d", i+1))
		volumePath := filepath.Join(volumesPath, "hdd", fmt.Sprintf("%03d", i+1))
		require.NoError(tb, os.MkdirAll(volumePath, 0o700))
		require.NoError(tb, os.WriteFile(filepath.Join(volumePath, volume.IDFile), []byte(volumeID), 0o600))
	}

	opts = append(opts, commonDeps.WithDebugLogger(logger), commonDeps.WithEtcdConfig(etcdCfg))
	d, mock := dependencies.NewMockedStorageWriterScopeWithConfig(
		tb,
		func(cfg *config.Config) {
			if modifyConfig != nil {
				modifyConfig(cfg)
			}
			cfg.NodeID = "disk-writer"
			cfg.Hostname = "localhost"
			cfg.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(tb))
			cfg.Storage.VolumesPath = volumesPath
		},
		opts...,
	)

	require.NoError(tb, writernode.Start(mock.TestContext(), d, mock.TestConfig()))

	return d, mock
}
