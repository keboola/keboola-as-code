package volume

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume"
)

type collection = volume.Collection[*Volume]

type Volumes struct {
	*collection
}

// OpenVolumes function detects and opens all volumes in the path.
func OpenVolumes(ctx context.Context, logger log.Logger, clock clock.Clock, nodeID, volumesPath string, opts ...Option) (*Volumes, error) {
	v, err := volume.OpenVolumes(ctx, logger, nodeID, volumesPath, func(spec storage.VolumeSpec) (*Volume, error) {
		return Open(ctx, logger, clock, spec, opts...)
	})
	if err != nil {
		return nil, err
	}

	return &Volumes{collection: v}, nil
}
