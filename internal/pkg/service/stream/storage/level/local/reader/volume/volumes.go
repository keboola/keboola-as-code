package volume

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/opener"
)

type volumes = volume.Collection[*Volume]

type Volumes struct {
	*volumes
}

// OpenVolumes function detects and opens all volumes in the path.
func OpenVolumes(ctx context.Context, logger log.Logger, clock clock.Clock, nodeID, volumesPath string, opts ...Option) (*Volumes, error) {
	collection, err := opener.OpenVolumes(ctx, logger, nodeID, volumesPath, func(spec volume.Spec) (*Volume, error) {
		return Open(ctx, logger, clock, spec, opts...)
	})
	if err != nil {
		return nil, err
	}

	return &Volumes{volumes: collection}, nil
}
