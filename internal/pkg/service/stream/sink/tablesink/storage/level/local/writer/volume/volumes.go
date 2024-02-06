package volume

import (
	"context"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume"
)

type collection = volume.Collection[*Volume]

type Volumes struct {
	*collection
	events *writer.Events
}

// OpenVolumes function detects and opens all volumes in the path.
func OpenVolumes(ctx context.Context, logger log.Logger, clock clock.Clock, nodeID, volumesPath string, opts ...Option) (out *Volumes, err error) {
	events := writer.NewEvents()
	out = &Volumes{events: events}
	out.collection, err = volume.OpenVolumes(ctx, logger, nodeID, volumesPath, func(spec volume.Spec) (*Volume, error) {
		return Open(ctx, logger, clock, events.Clone(), spec, opts...)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (v *Volumes) Events() *writer.Events {
	return v.events
}
