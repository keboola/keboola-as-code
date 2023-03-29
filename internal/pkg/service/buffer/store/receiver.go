package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// CreateReceiver puts a receiver into the store.
// Logic errors:
// - CountLimitReachedError
// - ResourceAlreadyExistsError.
func (s *Store) CreateReceiver(ctx context.Context, receiver model.Receiver) (err error) {
	ctx, span := s.tracer.Start(ctx, "keboola.go.buffer.store.CreateReceiver")
	defer telemetry.EndSpan(span, &err)

	// Check exports count
	if len(receiver.Exports) >= MaxExportsPerReceiver {
		return serviceError.NewCountLimitReachedError("export", MaxExportsPerReceiver, "receiver")
	}

	return op.Atomic().
		// Check receivers count
		Read(func() op.Op {
			return s.checkReceiversCountOp(receiver.ProjectID)
		}).
		// Create receiver and exports
		Write(func() op.Op {
			return s.createReceiverOp(ctx, receiver)
		}).
		Do(ctx, s.client)
}

func (s *Store) checkReceiversCountOp(projectID key.ProjectID) op.Op {
	return s.schema.
		Configs().
		Receivers().
		InProject(projectID).
		Count().
		WithOnResultOrErr(func(count int64) error {
			if count >= MaxReceiversPerProject {
				return serviceError.NewCountLimitReachedError("receiver", MaxReceiversPerProject, "project")
			}
			return nil
		})
}

func (s *Store) createReceiverOp(ctx context.Context, receiver model.Receiver) op.Op {
	// Create receiver
	txn := op.NewTxnOp()
	txn.Then(s.createReceiverBaseOp(ctx, receiver.ReceiverBase))

	// Create exports
	for _, export := range receiver.Exports {
		txn.Then(s.createExportOp(ctx, export))
	}

	return txn
}

func (s *Store) createReceiverBaseOp(_ context.Context, receiver model.ReceiverBase) op.BoolOp {
	return s.schema.
		Configs().
		Receivers().
		ByKey(receiver.ReceiverKey).
		PutIfNotExists(receiver).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("receiver", receiver.ReceiverID.String(), "project")
			}
			return ok, err
		})
}

// CheckCreateReceiver checks if a receiver can be created.
// Logic errors:
// - CountLimitReachedError.
// - ResourceAlreadyExistsError.
func (s *Store) CheckCreateReceiver(ctx context.Context, receiverKey key.ReceiverKey) (err error) {
	ctx, span := s.tracer.Start(ctx, "keboola.go.buffer.store.CheckCreateReceiver")
	defer telemetry.EndSpan(span, &err)

	err = s.getReceiverBaseOp(ctx, receiverKey).DoOrErr(ctx, s.client)
	if err == nil {
		return serviceError.NewResourceAlreadyExistsError("receiver", receiverKey.ReceiverID.String(), "project")
	}
	if !errors.Is(err, serviceError.NewResourceNotFoundError("receiver", receiverKey.ReceiverID.String(), "project")) {
		return err
	}

	return s.checkReceiversCountOp(receiverKey.ProjectID).DoOrErr(ctx, s.client)
}

// GetReceiver fetches a receiver from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) GetReceiver(ctx context.Context, receiverKey key.ReceiverKey) (r model.Receiver, err error) {
	ctx, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetReceiver")
	defer telemetry.EndSpan(span, &err)

	receiverBase, err := s.getReceiverBaseOp(ctx, receiverKey).Do(ctx, s.client)
	if err != nil {
		return model.Receiver{}, err
	}
	exports, err := s.ListExports(ctx, receiverKey)
	if err != nil {
		return model.Receiver{}, err
	}

	return model.Receiver{
		ReceiverBase: receiverBase.Value,
		Exports:      exports,
	}, nil
}

func (s *Store) getReceiverBaseOp(_ context.Context, receiverKey key.ReceiverKey) op.ForType[*op.KeyValueT[model.ReceiverBase]] {
	return s.schema.
		Configs().
		Receivers().
		ByKey(receiverKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.ReceiverBase], err error) (*op.KeyValueT[model.ReceiverBase], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("receiver", receiverKey.ReceiverID.String(), "project")
			}
			return kv, err
		})
}

func (s *Store) UpdateReceiver(ctx context.Context, k key.ReceiverKey, fn func(base model.ReceiverBase) (model.ReceiverBase, error)) (err error) {
	ctx, span := s.tracer.Start(ctx, "keboola.go.buffer.store.UpdateReceiver")
	defer telemetry.EndSpan(span, &err)

	var receiver *model.ReceiverBase
	return op.Atomic().
		Read(func() op.Op {
			return s.getReceiverBaseOp(ctx, k).WithOnResult(func(v *op.KeyValueT[model.ReceiverBase]) {
				receiver = &v.Value
			})
		}).
		WriteOrErr(func() (op.Op, error) {
			oldValue := *receiver
			newValue, err := fn(oldValue)
			if err != nil {
				return nil, err
			}
			return s.updateReceiverBaseOp(ctx, newValue), nil
		}).
		Do(ctx, s.client)
}

func (s *Store) updateReceiverBaseOp(_ context.Context, receiver model.ReceiverBase) op.NoResultOp {
	return s.schema.
		Configs().
		Receivers().
		ByKey(receiver.ReceiverKey).
		Put(receiver)
}

// ListReceivers from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) ListReceivers(ctx context.Context, projectID key.ProjectID) (receivers []model.Receiver, err error) {
	ctx, span := s.tracer.Start(ctx, "keboola.go.buffer.store.ListReceivers")
	defer telemetry.EndSpan(span, &err)

	err = s.
		receiversIterator(ctx, projectID).Do(ctx, s.client).
		ForEachValue(func(receiverBase model.ReceiverBase, header *iterator.Header) error {
			exports, err := s.ListExports(ctx, receiverBase.ReceiverKey, iterator.WithRev(header.Revision))
			if err != nil {
				return err
			}
			receivers = append(receivers, model.Receiver{
				ReceiverBase: receiverBase,
				Exports:      exports,
			})
			return nil
		})

	if err != nil {
		return nil, err
	}

	return receivers, nil
}

func (s *Store) receiversIterator(_ context.Context, projectID key.ProjectID) iterator.DefinitionT[model.ReceiverBase] {
	return s.schema.Configs().Receivers().InProject(projectID).GetAll()
}

// DeleteReceiver deletes a receiver from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) DeleteReceiver(ctx context.Context, receiverKey key.ReceiverKey) (err error) {
	ctx, span := s.tracer.Start(ctx, "keboola.go.buffer.store.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	_, err = op.MergeToTxn(
		s.deleteReceiverBaseOp(ctx, receiverKey),
		s.deleteReceiverMappingsOp(ctx, receiverKey),
		s.deleteReceiverTokensOp(ctx, receiverKey),
		s.schema.Configs().Exports().InReceiver(receiverKey).DeleteAll(),
		s.schema.ReceivedStats().InReceiver(receiverKey).DeleteAll(),
		s.schema.Files().Opened().InReceiver(receiverKey).DeleteAll(),
		s.schema.Files().Closing().InReceiver(receiverKey).DeleteAll(),
		s.schema.Files().Importing().InReceiver(receiverKey).DeleteAll(),
		s.schema.Files().Imported().InReceiver(receiverKey).DeleteAll(),
		s.schema.Files().Failed().InReceiver(receiverKey).DeleteAll(),
		s.schema.Slices().Writing().InReceiver(receiverKey).DeleteAll(),
		s.schema.Slices().Closing().InReceiver(receiverKey).DeleteAll(),
		s.schema.Slices().Uploading().InReceiver(receiverKey).DeleteAll(),
		s.schema.Slices().Uploaded().InReceiver(receiverKey).DeleteAll(),
		s.schema.Slices().Failed().InReceiver(receiverKey).DeleteAll(),
		s.schema.Slices().Imported().InReceiver(receiverKey).DeleteAll(),
		s.schema.Records().InReceiver(receiverKey).DeleteAll(),
		s.schema.Tasks().InReceiver(receiverKey).DeleteAll(),
		s.schema.Runtime().LastRecordID().InReceiver(receiverKey).DeleteAll(),
	).Do(ctx, s.client)
	return err
}

func (s *Store) deleteReceiverBaseOp(_ context.Context, receiverKey key.ReceiverKey) op.BoolOp {
	return s.schema.
		Configs().
		Receivers().
		ByKey(receiverKey).
		Delete().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceNotFoundError("receiver", receiverKey.ReceiverID.String(), "project")
			}
			return ok, err
		})
}
