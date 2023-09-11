package reader

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/volume"
)

type baseVolumes = volume.Volumes[*Volume]

type Volumes struct {
	*baseVolumes
}

func OpenVolumes(ctx context.Context, logger log.Logger, clock clock.Clock, path string, opts ...Option) (*Volumes, error) {
	v, err := volume.OpenVolumes(logger, path, func(info volumeInfo) (*Volume, error) {
		return OpenVolume(ctx, logger, clock, info, opts...)
	})
	if err != nil {
		return nil, err
	}

	return &Volumes{baseVolumes: v}, nil
}
