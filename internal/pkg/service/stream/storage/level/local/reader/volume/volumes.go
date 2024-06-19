package volume

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/opener"
)

type collection = volume.Collection[*Volume]

type Volumes struct {
	*collection
	logger log.Logger
}

// OpenVolumes function detects and opens all volumes in the path.
func OpenVolumes(ctx context.Context, logger log.Logger, clock clock.Clock, nodeID, volumesPath string, opts ...Option) (out *Volumes, err error) {
	out = &Volumes{logger: logger}
	out.collection, err = opener.OpenVolumes(ctx, logger, nodeID, volumesPath, func(spec volume.Spec) (*Volume, error) {
		return Open(ctx, logger, clock, spec, opts...)
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (v *Volumes) Close(ctx context.Context) error {
	v.logger.Info(ctx, "closing volumes")
	return v.collection.Close(ctx)
}
