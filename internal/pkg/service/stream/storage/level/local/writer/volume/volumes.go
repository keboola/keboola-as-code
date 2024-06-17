package volume

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/opener"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
)

type collection = model.Collection[*Volume]

type Volumes struct {
	*collection
	logger log.Logger
	events *writer.Events
}

// OpenVolumes function detects and opens all volumes in the path.
func OpenVolumes(ctx context.Context, logger log.Logger, clock clock.Clock, nodeID, volumesPath string, wrCfg writer.Config, opts ...Option) (out *Volumes, err error) {
	out = &Volumes{logger: logger, events: writer.NewEvents()}
	out.collection, err = opener.OpenVolumes(ctx, logger, nodeID, volumesPath, func(spec model.Spec) (*Volume, error) {
		return Open(ctx, logger, clock, out.events.Clone(), wrCfg, spec, opts...)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (v *Volumes) Collection() *model.Collection[*Volume] {
	return v.collection
}

func (v *Volumes) Events() *writer.Events {
	return v.events
}

func (v *Volumes) Close(ctx context.Context) error {
	v.logger.Info(ctx, "closing volumes")
	return v.collection.Close(ctx)
}
