package configstore

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// CreateExport puts an export into the store.
//
// This method guarantees that no two receivers in the store will have the same (projectID, receiverID, exportID) tuple.
//
// May fail if
// - limit is reached (`LimitReachedError`)
// - already exists (`AlreadyExistsError`)
// - validation of the model fails
// - JSON marshalling fails
// - any of the underlying ETCD calls fail.
func (c *Store) CreateExport(ctx context.Context, projectID int, receiverID string, export model.Export) (err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.CreateExport")
	defer telemetry.EndSpan(span, &err)

	if err := c.validator.Validate(ctx, export); err != nil {
		return err
	}

	prefix := schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)
	receiverExports, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if receiverExports.Count >= MaxExportsPerReceiver {
		return serviceError.NewCountLimitReachedError("export", MaxExportsPerReceiver, "receiver")
	}

	key := prefix.ID(export.ID)

	exports, err := client.KV.Get(ctx, key.Key(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if exports.Count > 0 {
		return serviceError.NewResourceAlreadyExistsError("export", export.ID, "receiver")
	}

	value, err := json.EncodeString(export, false)
	if err != nil {
		return err
	}

	_, err = client.KV.Put(ctx, key.Key(), value)
	if err != nil {
		return err
	}

	return nil
}

func (c *Store) ListExports(ctx context.Context, projectID int, receiverID string) (r []*model.Export, err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.ListExports")
	defer telemetry.EndSpan(span, &err)

	prefix := schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)

	resp, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return nil, err
	}

	exports := make([]*model.Export, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		export := &model.Export{}
		if err = json.DecodeString(string(kv.Value), export); err != nil {
			return nil, err
		}
		exports = append(exports, export)
	}

	return exports, nil
}
