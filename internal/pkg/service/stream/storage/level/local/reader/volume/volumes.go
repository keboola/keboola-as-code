package volume

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/opener"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Volumes struct {
	logger     log.Logger
	collection *volume.Collection[*Volume]
}

// OpenVolumes function detects and opens all volumes in the path.
func OpenVolumes(ctx context.Context, logger log.Logger, clock clock.Clock, process *servicectx.Process, nodeID, volumesPath string, opts ...Option) (out *Volumes, err error) {
	out = &Volumes{logger: logger}
	out.collection, err = opener.OpenVolumes(ctx, logger, nodeID, volumesPath, func(spec volume.Spec) (*Volume, error) {
		return Open(ctx, logger, clock, spec, opts...)
	})
	if err != nil {
		return nil, err
	}

	// Graceful shutdown
	process.OnShutdown(func(ctx context.Context) {
		logger.Info(ctx, "closing volumes")
		if err := out.collection.Close(ctx); err != nil {
			err := errors.PrefixError(err, "cannot close volumes")
			logger.Error(ctx, err.Error())
		}
		logger.Info(ctx, "closed volumes")
	})

	return out, nil
}

func (v *Volumes) Collection() *volume.Collection[*Volume] {
	return v.collection
}
