package volume_test

import (
	"context"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume"
)

type volumesTestCase struct {
	Logger      log.DebugLogger
	NodeID      string
	VolumesPath string
	Opener      volume.Opener[*test.Volume]
}

func newVolumesTestCase(t *testing.T) *volumesTestCase {
	t.Helper()
	return &volumesTestCase{
		Logger:      log.NewDebugLogger(),
		VolumesPath: t.TempDir(),
		Opener: func(info storage.VolumeSpec) (*test.Volume, error) {
			return test.NewTestVolume("my-volume", "my-node", info), nil
		},
	}
}

func (tc *volumesTestCase) OpenVolumes() (*volume.Collection[*test.Volume], error) {
	return volume.OpenVolumes[*test.Volume](context.Background(), tc.Logger, tc.NodeID, tc.VolumesPath, tc.Opener)
}
