package store

import (
	"context"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// CreateExport puts an export into the store.
// Logic errors:
// - CountLimitReachedError
// - ResourceAlreadyExistsError.
func (s *Store) CreateExport(ctx context.Context, projectID int, receiverID string, export model.Export) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateExport")
	defer telemetry.EndSpan(span, &err)

	exports := s.schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)

	count, err := exports.Count().Do(ctx, s.client)
	if err != nil {
		return err
	} else if count >= MaxExportsPerReceiver {
		return serviceError.NewCountLimitReachedError("export", MaxExportsPerReceiver, "receiver")
	}

	_, err = s.createExportOp(ctx, projectID, receiverID, export).Do(ctx, s.client)
	return err
}

func (s *Store) createExportOp(_ context.Context, projectID int, receiverID string, export model.Export) op.BoolOp {
	exports := s.schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)
	return exports.
		ID(export.ID).
		PutIfNotExists(export).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("export", export.ID, "receiver")
			}
			return ok, err
		})
}

// GetExport fetches an export from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) GetExport(ctx context.Context, projectID int, receiverID, exportID string) (r model.Export, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetExport")
	defer telemetry.EndSpan(span, &err)

	kv, err := s.getExportOp(ctx, projectID, receiverID, exportID).Do(ctx, s.client)
	if err != nil {
		return model.Export{}, err
	}
	return kv.Value, nil
}

func (s *Store) getExportOp(_ context.Context, projectID int, receiverID, exportID string) op.ForType[*op.KeyValueT[model.Export]] {
	exports := s.schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)
	return exports.
		ID(exportID).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Export], err error) (*op.KeyValueT[model.Export], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("export", fmt.Sprintf("%s/%s", receiverID, exportID))
			}
			return kv, err
		})
}

// ListExports from the store.
func (s *Store) ListExports(ctx context.Context, projectID int, receiverID string) (out []model.Export, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.ListExports")
	defer telemetry.EndSpan(span, &err)

	kvs, err := s.listExportsOp(ctx, projectID, receiverID).Do(ctx, s.client)
	if err != nil {
		return nil, err
	}

	return kvs.Values(), nil
}

func (s *Store) listExportsOp(_ context.Context, projectID int, receiverID string) op.ForType[op.KeyValuesT[model.Export]] {
	exports := s.schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)
	return exports.GetAll()
}

// DeleteExport deletes an export from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) DeleteExport(ctx context.Context, projectID int, receiverID, exportID string) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	_, err = s.deleteExportOp(ctx, projectID, receiverID, exportID).Do(ctx, s.client)
	return err
}

func (s *Store) deleteExportOp(_ context.Context, projectID int, receiverID string, exportID string) op.BoolOp {
	receivers := s.schema.Configs().Receivers().InProject(projectID).ID(receiverID)
	return receivers.
		Delete().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceNotFoundError("export", fmt.Sprintf("%s/%s", receiverID, exportID))
			}
			return ok, err
		})
}
