package store

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const BoundaryInDays = 14

// Cleanup deletes all unneeded tasks, files and slices data from the store.
func (s *Store) Cleanup(ctx context.Context, receiver model.Receiver) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.Cleanup")
	defer telemetry.EndSpan(span, &err)

	s.cleanupTasks(ctx, receiver.ReceiverKey)

	for _, e := range receiver.Exports {
		s.cleanupFiles(ctx, e.ExportKey)
	}

	return nil
}

func (s *Store) cleanupTasks(ctx context.Context, receiverKey key.ReceiverKey) {
	tasks, err := s.schema.Tasks().InReceiver(receiverKey).GetAll().Do(ctx, s.client).All()
	if err != nil {
		s.logger.Error(err)
		return
	}
	for _, t := range tasks {
		if !t.Value.IsForCleanup() {
			continue
		}

		_, err = s.schema.Tasks().ByKey(t.Value.TaskKey).Delete().Do(ctx, s.client)
		if err != nil {
			s.logger.Error(err)
		}
		s.logger.Infof(`deleting task "%s"`, t.Value.TaskKey.String())
	}
}

func (s *Store) cleanupFiles(ctx context.Context, exportKey key.ExportKey) {
	now := time.Now()
	boundaryTime := now.AddDate(0, 0, -BoundaryInDays)
	for _, fileState := range filestate.All() {
		fileBoundKey := key.UTCTime(boundaryTime).String()
		files, err := s.ListFilesInState(ctx, fileState, exportKey, iterator.WithEnd(fileBoundKey))
		if err != nil {
			s.logger.Error(err)
		}

		for _, file := range files {
			// Delete slices
			for _, sliceState := range slicestate.All() {
				slices, err := s.schema.Slices().InState(sliceState).InFile(file.FileKey).GetAll().Do(ctx, s.client).All()
				if err != nil {
					s.logger.Error(err)
					continue
				}
				for _, slice := range slices {
					// Delete records
					_, err := s.schema.Records().InSlice(slice.Value.SliceKey).DeleteAll().Do(ctx, s.client)
					if err != nil {
						s.logger.Error(err)
					}

					// Delete the slice
					_, err = s.schema.Slices().InState(sliceState).ByKey(slice.Value.SliceKey).Delete().Do(ctx, s.client)
					if err != nil {
						s.logger.Error(err)
					}
				}
			}

			// Delete the file
			_, err := s.schema.Files().InState(fileState).ByKey(file.FileKey).Delete().Do(ctx, s.client)
			if err != nil {
				s.logger.Error(err)
			}
		}
	}
}
