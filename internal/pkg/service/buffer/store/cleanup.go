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

	s.cleanupTasks(ctx, receiver.ReceiverKey)

	for _, e := range receiver.Exports {
		s.cleanupFiles(ctx, e.ExportKey)
		s.cleanupSlices(ctx, e.ExportKey)
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
	boundaryTime := now.AddDate(0, 0, -14)
	for _, state := range filestate.All() {
		fileBoundPrefix := s.schema.Files().InState(state).InExport(exportKey).Prefix()
		fileBoundKey := fileBoundPrefix + key.UTCTime(boundaryTime).String()
		_, err := s.client.Delete(ctx, fileBoundPrefix, clientv3.WithRange(fileBoundKey))
		if err != nil {
			s.logger.Error(err)
		}
	}
}

func (s *Store) cleanupSlices(ctx context.Context, exportKey key.ExportKey) {
	now := time.Now()
	boundaryTime := now.AddDate(0, 0, -14)
	for _, state := range slicestate.All() {
		sliceBoundPrefix := s.schema.Slices().InState(state).InExport(exportKey).Prefix()
		sliceBoundKey := sliceBoundPrefix + key.UTCTime(boundaryTime).String()
		_, err := s.client.Delete(ctx, sliceBoundPrefix, clientv3.WithRange(sliceBoundKey))
		if err != nil {
			s.logger.Error(err)
		}
	}
}
