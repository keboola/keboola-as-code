package configstore

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
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

	if ok, err := exports.ID(export.ID).PutIfNotExists(export).Do(ctx, s.client); err != nil {
		return err
	} else if !ok {
		return serviceError.NewResourceAlreadyExistsError("export", export.ID, "receiver")
	}
	return nil
}

// GetExport fetches an export from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) GetExport(ctx context.Context, projectID int, receiverID, exportID string) (r model.Export, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetExport")
	defer telemetry.EndSpan(span, &err)

	exports := s.schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)
	kv, err := exports.ID(exportID).Get().Do(ctx, s.client)
	if err != nil {
		return model.Export{}, err
	} else if kv == nil {
		return model.Export{}, serviceError.NewResourceNotFoundError("export", fmt.Sprintf("%s/%s", receiverID, exportID))
	}

	return kv.Value, nil
}

// ListExports from the store.
func (s *Store) ListExports(ctx context.Context, projectID int, receiverID string) (out []model.Export, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.ListExports")
	defer telemetry.EndSpan(span, &err)

	exports := s.schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)
	kvs, err := exports.GetAll().Do(ctx, s.client)
	if err != nil {
		return nil, err
	}

	return kvs.Values(), nil
}

// DeleteExport deletes an export from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) DeleteExport(ctx context.Context, projectID int, receiverID, exportID string) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	exports := s.schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)
	deleted, err := exports.ID(exportID).Delete().Do(ctx, s.client)
	if err != nil {
		return err
	} else if !deleted {
		return serviceError.NewResourceNotFoundError("export", fmt.Sprintf("%s/%s", receiverID, exportID))
	}

	return nil
}
