package store

import (
	"context"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// CreateMapping puts a mapping revision into the store.
// Logic errors:
// - CountLimitReachedError.
func (s *Store) CreateMapping(ctx context.Context, projectID int, receiverID string, exportID string, mapping model.Mapping) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateMapping")
	defer telemetry.EndSpan(span, &err)

	if mapping.RevisionID != 0 {
		return errors.New("unexpected state: mapping revision ID should be 0, it is generated on create")
	}

	mappings := s.schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID)

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
		mapping.RevisionID = int(count) + 1

		// Put
		_, err = s.createMappingOp(ctx, projectID, receiverID, exportID, mapping).Do(ctx, s.client)
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

func (s *Store) createMappingOp(_ context.Context, projectID int, receiverID string, exportID string, mapping model.Mapping) op.BoolOp {
	if mapping.RevisionID == 0 {
		panic(errors.New("unexpected state: mapping revision ID should be set by code one level higher"))
	}

	mappings := s.schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID)
	return mappings.
		Revision(mapping.RevisionID).
		PutIfNotExists(mapping).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("mapping", fmt.Sprintf("%s/%s/%08d", receiverID, exportID, mapping.RevisionID), "export")
			}
			return ok, err
		})
}

// GetMapping fetches the current mapping from the store.
func (s *Store) GetMapping(ctx context.Context, projectID int, receiverID, exportID string) (r model.Mapping, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetMapping")
	defer telemetry.EndSpan(span, &err)

	kv, err := s.getMappingOp(ctx, projectID, receiverID, exportID).Do(ctx, s.client)
	if err != nil {
		return model.Mapping{}, err
	}
	return kv.Value, nil
}

func (s *Store) getMappingOp(_ context.Context, projectID int, receiverID, exportID string) op.ForType[*op.KeyValueT[model.Mapping]] {
	mappings := s.schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID)
	return mappings.
		GetOne(etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Mapping], err error) (*op.KeyValueT[model.Mapping], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("mapping", fmt.Sprintf("%s/%s/latest", receiverID, exportID))
			}
			return kv, err
		})
}

// GetMappingByRevisionID fetches a mapping from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) GetMappingByRevisionID(ctx context.Context, projectID int, receiverID, exportID string, revisionID int) (r model.Mapping, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetMappingByRevisionID")
	defer telemetry.EndSpan(span, &err)

	kv, err := s.getMappingByRevisionIDOp(ctx, projectID, receiverID, exportID, revisionID).Do(ctx, s.client)
	if err != nil {
		return model.Mapping{}, err
	}
	return kv.Value, nil
}

func (s *Store) getMappingByRevisionIDOp(_ context.Context, projectID int, receiverID, exportID string, revisionID int) op.ForType[*op.KeyValueT[model.Mapping]] {
	mappings := s.schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID)
	return mappings.
		Revision(revisionID).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Mapping], err error) (*op.KeyValueT[model.Mapping], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("mapping", fmt.Sprintf("%s/%s/%08d", receiverID, exportID, revisionID))
			}
			return kv, err
		})
}
