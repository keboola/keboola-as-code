package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// CreateReceiver puts a receiver into the store.
// Logic errors:
// - CountLimitReachedError
// - ResourceAlreadyExistsError.
func (s *Store) CreateReceiver(ctx context.Context, receiver model.ReceiverBase) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateReceiver")
	defer telemetry.EndSpan(span, &err)

	receivers := s.schema.Configs().Receivers().InProject(receiver.ProjectID)
	count, err := receivers.Count().Do(ctx, s.client)
	if err != nil {
		return err
	} else if count >= MaxReceiversPerProject {
		return serviceError.NewCountLimitReachedError("receiver", MaxReceiversPerProject, "project")
	}

	_, err = op.MergeToTxn(s.createReceiverOp(ctx, receiver)).Do(ctx, s.client)
	return err
}

func (s *Store) createReceiverOp(_ context.Context, receiver model.ReceiverBase) op.BoolOp {
	return s.schema.
		Configs().
		Receivers().
		ByKey(receiver.ReceiverKey).
		PutIfNotExists(receiver).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("receiver", receiver.ReceiverKey.String(), "project")
			}
			return ok, err
		})
}

// GetReceiver fetches a receiver from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) GetReceiver(ctx context.Context, receiverKey key.ReceiverKey) (r model.ReceiverBase, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetReceiver")
	defer telemetry.EndSpan(span, &err)

	kv, err := s.getReceiverOp(ctx, receiverKey).Do(ctx, s.client)
	if err != nil {
		return model.ReceiverBase{}, err
	}

	return kv.Value, nil
}

func (s *Store) getReceiverOp(_ context.Context, receiverKey key.ReceiverKey) op.ForType[*op.KeyValueT[model.ReceiverBase]] {
	return s.schema.
		Configs().
		Receivers().
		ByKey(receiverKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.ReceiverBase], err error) (*op.KeyValueT[model.ReceiverBase], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("receiver", receiverKey.String())
			}
			return kv, err
		})
}

// ListReceivers from the store.
func (s *Store) ListReceivers(ctx context.Context, projectID int) (r []model.ReceiverBase, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.ListReceivers")
	defer telemetry.EndSpan(span, &err)

	kvs, err := s.listReceiversOp(ctx, projectID).Do(ctx, s.client)
	if err != nil {
		return nil, err
	}

	return kvs.Values(), nil
}

func (s *Store) listReceiversOp(_ context.Context, projectID int) op.ForType[op.KeyValuesT[model.ReceiverBase]] {
	receivers := s.schema.Configs().Receivers().InProject(projectID)
	return receivers.GetAll(etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
}

// DeleteReceiver deletes a receiver from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) DeleteReceiver(ctx context.Context, receiverKey key.ReceiverKey) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	_, err = s.deleteReceiverOp(ctx, receiverKey).Do(ctx, s.client)
	return err
}

func (s *Store) deleteReceiverOp(_ context.Context, receiverKey key.ReceiverKey) op.BoolOp {
	return s.schema.
		Configs().
		Receivers().
		ByKey(receiverKey).
		Delete().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceNotFoundError("receiver", receiverKey.String())
			}
			return ok, err
		})
}
