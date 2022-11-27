package configstore

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// CreateReceiver puts a receiver into the store.
// Logic errors:
// - CountLimitReachedError
// - ResourceAlreadyExistsError.
func (s *Store) CreateReceiver(ctx context.Context, receiver model.Receiver) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateReceiver")
	defer telemetry.EndSpan(span, &err)

	receivers := s.schema.Configs().Receivers().InProject(receiver.ProjectID)
	count, err := receivers.Count().Do(ctx, s.client)
	if err != nil {
		return err
	} else if count >= MaxExportsPerReceiver {
		return serviceError.NewCountLimitReachedError("receiver", MaxExportsPerReceiver, "project")
	}

	if ok, err := receivers.ID(receiver.ID).PutIfNotExists(receiver).Do(ctx, s.client); err != nil {
		return err
	} else if !ok {
		return serviceError.NewResourceAlreadyExistsError("receiver", receiver.ID, "project")
	}

	return nil
}

// GetReceiver fetches a receiver from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) GetReceiver(ctx context.Context, projectID int, receiverID string) (r model.Receiver, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetReceiver")
	defer telemetry.EndSpan(span, &err)

	receivers := s.schema.Configs().Receivers().InProject(projectID)
	kv, err := receivers.ID(receiverID).Get().Do(ctx, s.client)
	if err != nil {
		return model.Receiver{}, err
	} else if kv == nil {
		return model.Receiver{}, serviceError.NewResourceNotFoundError("receiver", receiverID)
	}

	return kv.Value, nil
}

// ListReceivers from the store.
func (s *Store) ListReceivers(ctx context.Context, projectID int) (r []model.Receiver, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.ListReceivers")
	defer telemetry.EndSpan(span, &err)

	receivers := s.schema.Configs().Receivers().InProject(projectID)
	kvs, err := receivers.GetAll().Do(ctx, s.client)
	if err != nil {
		return nil, err
	}

	return kvs.Values(), nil
}

// DeleteReceiver deletes a receiver from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) DeleteReceiver(ctx context.Context, projectID int, receiverID string) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	receivers := s.schema.Configs().Receivers().InProject(projectID).ID(receiverID)
	deleted, err := receivers.Delete().Do(ctx, s.client)
	if err != nil {
		return err
	} else if !deleted {
		return serviceError.NewResourceNotFoundError("receiver", receiverID)
	}

	return nil
}
