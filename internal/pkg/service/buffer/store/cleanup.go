package store

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// Cleanup deletes all unneeded tasks, files and slices data from the store.
func (s *Store) Cleanup(ctx context.Context, receiver model.Receiver) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.Cleanup")
	defer telemetry.EndSpan(span, &err)

	now := time.Now()

	// Delete all old tasks.
	tasks, err := s.schema.Tasks().InReceiver(receiver.ReceiverKey).GetAll().Do(ctx, s.client).All()
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if t.Value.FinishedAt == nil {
			continue
		}

		taskAge := now.Sub(t.Value.FinishedAt.Time())
		if t.Value.Error == "" {
			if taskAge < 1*time.Hour {
				continue
			}
			// Delete finished tasks older than 1 hour.
		} else {
			if taskAge < 24*time.Hour {
				continue
			}
			// Delete failed tasks older than 24 hours.
		}

		_, err = s.schema.Tasks().ByKey(t.Value.TaskKey).Delete().Do(ctx, s.client)
		if err != nil {
			s.logger.Error(err)
		}
		s.logger.Infof(`deleting task "%s"`, t.Value.TaskKey.String())
	}

	// Delete all old files.
	boundaryTime := now.AddDate(0, 0, -14)
	for _, e := range receiver.Exports {
		// TODO: iterate for all file states??
		fileBoundPrefix := s.schema.Files().InState(filestate.Opened).InExport(e.ExportKey).Prefix()
		fileBoundKey := fileBoundPrefix + key.UTCTime(boundaryTime).String()
		_, err = s.client.Delete(ctx, fileBoundPrefix, clientv3.WithRange(fileBoundKey))
		if err != nil {
			s.logger.Error(err)
		}

		// TODO: iterate for all slice states??
		sliceBoundPrefix := s.schema.Slices().InState(slicestate.Imported).InExport(e.ExportKey).Prefix()
		sliceBoundKey := sliceBoundPrefix + key.UTCTime(boundaryTime).String()
		_, err = s.client.Delete(ctx, sliceBoundPrefix, clientv3.WithRange(sliceBoundKey))
		if err != nil {
			s.logger.Error(err)
		}
	}

	return nil
}
