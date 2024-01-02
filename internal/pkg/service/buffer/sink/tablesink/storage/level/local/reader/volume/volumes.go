package volume

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/volume"
)

type baseVolumes = volume.Volumes[*Volume]

type Volumes struct {
	*baseVolumes
}

// DetectVolumes function detects and opens all volumes in the path.
func DetectVolumes(ctx context.Context, logger log.Logger, clock clock.Clock, path string, opts ...Option) (*Volumes, error) {
	v, err := volume.DetectVolumes(ctx, logger, path, func(info volumeInfo) (*Volume, error) {
		return Open(ctx, logger, clock, info, opts...)
	})
	if err != nil {
		return nil, err
	}

	return &Volumes{baseVolumes: v}, nil
}
