package volume

import (
	"context"

	"github.com/benbjohnson/clock"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"

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

type dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
}

// OpenVolumes function detects and opens all volumes in the path.
func OpenVolumes(ctx context.Context, d dependencies, nodeID, volumesPath string, opts ...Option) (v *Volumes, err error) {
	v = &Volumes{
		logger: d.Logger().WithComponent("storage.node.reader.volumes"),
	}

	v.collection, err = opener.OpenVolumes(ctx, v.logger, nodeID, volumesPath, func(spec volume.Spec) (*Volume, error) {
		return Open(ctx, v.logger, d.Clock(), spec, opts...)
	})
	if err != nil {
		return nil, err
	}

	// Graceful shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		v.logger.Info(ctx, "closing volumes")
		if err := v.collection.Close(ctx); err != nil {
			err := errors.PrefixError(err, "cannot close volumes")
			logger.Error(ctx, err.Error())
		}
		v.logger.Info(ctx, "closed volumes")
	})

	return v, nil
}

func (v *Volumes) Collection() *volume.Collection[*Volume] {
	return v.collection
}
