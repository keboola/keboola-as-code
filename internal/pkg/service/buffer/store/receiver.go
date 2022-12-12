package store

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/storageapi"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// CreateReceiver puts a receiver into the store.
// Logic errors:
// - CountLimitReachedError
// - ResourceAlreadyExistsError.
func (s *Store) CreateReceiver(ctx context.Context, receiver model.Receiver, files map[key.ExportKey]*storageapi.File) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateReceiver")
	defer telemetry.EndSpan(span, &err)

	receivers := s.schema.Configs().Receivers().InProject(receiver.ProjectID)
	count, err := receivers.Count().Do(ctx, s.client)
	if err != nil {
		return err
	} else if count >= MaxReceiversPerProject {
		return serviceError.NewCountLimitReachedError("receiver", MaxReceiversPerProject, "project")
	}

	now := s.clock.Now()
	ops := []op.Op{s.createReceiverBaseOp(ctx, receiver.ReceiverBase)}
	for _, export := range receiver.Exports {
		fileKey := key.FileKey{FileID: now, ExportKey: export.ExportKey}
		slice := model.Slice{
			SliceKey:    key.SliceKey{SliceID: now, FileKey: fileKey},
			SliceNumber: 1,
		}
		fileRes, found := files[export.ExportKey]
		if !found {
			panic(fmt.Sprintf("file resource for export %s not found", export.ExportKey.String()))
		}
		file := model.File{
			FileKey:         fileKey,
			Mapping:         export.Mapping,
			StorageResource: fileRes,
		}
		ops = append(ops,
			s.createExportBaseOp(ctx, export.ExportBase),
			s.createMappingOp(ctx, export.Mapping),
			s.createTokenOp(ctx, model.TokenForExport{ExportKey: export.ExportKey, Token: export.Token}),
			s.createFileOp(ctx, file),
			s.createSliceOp(ctx, slice),
		)
	}

	_, err = op.MergeToTxn(ops...).Do(ctx, s.client)
	return err
}

func (s *Store) createReceiverBaseOp(_ context.Context, receiver model.ReceiverBase) op.BoolOp {
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
func (s *Store) GetReceiver(ctx context.Context, receiverKey key.ReceiverKey) (r model.Receiver, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetReceiver")
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
				return nil, serviceError.NewResourceNotFoundError("receiver", receiverKey.String())
			}
			return kv, err
		})
}

func (s *Store) UpdateReceiver(ctx context.Context, receiver model.Receiver) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.UpdateReceiver")
	defer telemetry.EndSpan(span, &err)

	_, err = op.MergeToTxn(s.updateReceiverBaseOp(ctx, receiver.ReceiverBase)).Do(ctx, s.client)

	return err
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
func (s *Store) ListReceivers(ctx context.Context, projectID int) (receivers []model.Receiver, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.ListReceivers")
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

func (s *Store) receiversIterator(_ context.Context, projectID int) iterator.DefinitionT[model.ReceiverBase] {
	return s.schema.Configs().Receivers().InProject(projectID).GetAll()
}

// DeleteReceiver deletes a receiver from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) DeleteReceiver(ctx context.Context, receiverKey key.ReceiverKey) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	_, err = op.MergeToTxn(
		s.deleteReceiverBaseOp(ctx, receiverKey),
		s.deleteReceiverExportsOp(ctx, receiverKey),
		s.deleteReceiverMappingsOp(ctx, receiverKey),
		s.deleteReceiverTokensOp(ctx, receiverKey),
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
				return false, serviceError.NewResourceNotFoundError("receiver", receiverKey.String())
			}
			return ok, err
		})
}
