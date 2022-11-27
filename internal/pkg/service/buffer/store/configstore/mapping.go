package configstore

import (
	"context"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// CreateMapping puts a mapping revision into the store.
// Logic errors:
// - CountLimitReachedError.
func (s *Store) CreateMapping(ctx context.Context, projectID int, receiverID string, exportID string, mapping model.Mapping) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateMapping")
	defer telemetry.EndSpan(span, &err)

	// nolint:godox
	// TODO
	//if mapping.RevisionID != 0 {
	//	return errors.New("unexpected state: mapping revision ID should be 0, it is generated on save")
	//}

	mappings := s.schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID)
	key := mappings.Revision(mapping.RevisionID)

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

		// nolint:godox
		// TODO
		//// Next RevisionID
		//mapping.RevisionID = int(count) + 1

		// Put
		ok, err := key.PutIfNotExists(mapping).Do(ctx, s.client)
		if err != nil {
			return err
		} else if !ok {
			// Race condition, some other request has been faster, try again.
			continue
		}

		break
	}

	return nil
}

// GetMapping fetches the current mapping from the store.
func (s *Store) GetMapping(ctx context.Context, projectID int, receiverID, exportID string) (r model.Mapping, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetMapping")
	defer telemetry.EndSpan(span, &err)

	mappings := s.schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID)
	kvs, err := mappings.GetAll(etcd.WithLimit(1), etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).Do(ctx, s.client)
	if err != nil {
		return model.Mapping{}, err
	}

	l := len(kvs)
	if l > 1 {
		return model.Mapping{}, errors.Errorf("unexpected state: exactly one result expected, but found %d", l)
	} else if l == 0 {
		return model.Mapping{}, errors.Errorf("unexpected state: exactly one result expected, but found nothing")
	}

	return kvs[0].Value, nil
}

// GetMappingByRevisionID fetches a mapping from the store.
// Logic errors:
// - ResourceNotFoundError.
func (s *Store) GetMappingByRevisionID(ctx context.Context, projectID int, receiverID, exportID string, revisionID int) (r model.Mapping, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetMappingByRevisionID")
	defer telemetry.EndSpan(span, &err)

	mappings := s.schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID)
	kv, err := mappings.Revision(revisionID).Get().Do(ctx, s.client)
	if err != nil {
		return model.Mapping{}, err
	} else if kv == nil {
		return model.Mapping{}, serviceError.NewResourceNotFoundError("mapping", fmt.Sprintf("%s/%s/%08d", receiverID, exportID, revisionID))
	}

	return kv.Value, nil
}
