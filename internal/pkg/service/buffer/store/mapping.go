package store

import (
	"context"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// CreateMapping puts a mapping revision into the store.
// Logic errors:
// - CountLimitReachedError.
func (s *Store) CreateMapping(ctx context.Context, mapping model.Mapping) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateMapping")
	defer telemetry.EndSpan(span, &err)

	if mapping.RevisionID != 0 {
		return errors.New("unexpected state: mapping revision ID should be 0, it is generated on create")
	}

	mappings := s.schema.Configs().Mappings().InExport(mapping.ExportKey)
	for {
		// Count
		count, err := mappings.Count().Do(ctx, s.client)
		if err != nil {
			return errors.Errorf("failed to generate RevisionID for mapping: %w", err)
		}

		// Checkout
		if count >= MaxMappingRevisionsPerExport {
			return serviceError.NewCountLimitReachedError("mapping revision", MaxMappingRevisionsPerExport, "export")
		}

		// Next RevisionID
		mapping.RevisionID = key.RevisionID(int(count) + 1)

		// Put
		_, err = s.createMappingOp(ctx, mapping).Do(ctx, s.client)
		if err != nil {
			if errors.As(err, &serviceError.ResourceAlreadyExistsError{}) {
				// Race condition, some other request has been faster, try again.
				continue
			}
			return err
		}

		break
	}

	return nil
}

func (s *Store) createMappingOp(_ context.Context, mapping model.Mapping) op.BoolOp {
	if mapping.RevisionID == 0 {
		panic(errors.New("unexpected state: mapping revision ID is 0, it should be set by code one level higher"))
	}

	return s.schema.
		Configs().
		Mappings().
		ByKey(mapping.MappingKey).
		PutIfNotExists(mapping).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("mapping", mapping.MappingKey.String(), "export")
			}
			return ok, err
		})
}

func (s *Store) updateMappingOp(_ context.Context, mapping model.Mapping) op.NoResultOp {
	if mapping.RevisionID == 0 {
		panic(errors.New("unexpected state: mapping revision ID is 0, it should be set by code one level higher"))
	}

	return s.schema.
		Configs().
		Mappings().
		ByKey(mapping.MappingKey).
		Put(mapping)
}

// GetLatestMapping fetches the current mapping from the store.
func (s *Store) GetLatestMapping(ctx context.Context, exportKey key.ExportKey) (r model.Mapping, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetLatestMapping")
	defer telemetry.EndSpan(span, &err)

	kv, err := s.getLatestMappingOp(ctx, exportKey).Do(ctx, s.client)
	if err != nil {
		return model.Mapping{}, err
	}
	return kv.Value, nil
}

func (s *Store) getLatestMappingOp(_ context.Context, exportKey key.ExportKey, opts ...etcd.OpOption) op.ForType[*op.KeyValueT[model.Mapping]] {
	opts = append(opts, etcd.WithSort(etcd.SortByKey, etcd.SortDescend))
	return s.schema.
		Configs().
		Mappings().
		InExport(exportKey).
		GetOne(opts...).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Mapping], err error) (*op.KeyValueT[model.Mapping], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("mapping", fmt.Sprintf("%s/mapping:latest", exportKey.String()))
			}
			return kv, err
		})
}

// GetMapping fetches a mapping from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) GetMapping(ctx context.Context, mappingKey key.MappingKey) (r model.Mapping, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetMapping")
	defer telemetry.EndSpan(span, &err)

	kv, err := s.getMappingOp(ctx, mappingKey).Do(ctx, s.client)
	if err != nil {
		return model.Mapping{}, err
	}
	return kv.Value, nil
}

func (s *Store) getMappingOp(_ context.Context, mappingKey key.MappingKey) op.ForType[*op.KeyValueT[model.Mapping]] {
	return s.schema.
		Configs().
		Mappings().
		ByKey(mappingKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Mapping], err error) (*op.KeyValueT[model.Mapping], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("mapping", mappingKey.String())
			}
			return kv, err
		})
}

func (s *Store) deleteExportMappingsOp(_ context.Context, exportKey key.ExportKey) op.CountOp {
	return s.schema.
		Configs().
		Mappings().
		InExport(exportKey).
		DeleteAll()
}

func (s *Store) deleteReceiverMappingsOp(_ context.Context, receiverKey key.ReceiverKey) op.CountOp {
	return s.schema.
		Configs().
		Mappings().
		InReceiver(receiverKey).
		DeleteAll()
}
