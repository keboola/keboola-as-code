package configstore

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/schema"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func (c *Store) CreateMapping(ctx context.Context, projectID int, receiverID string, exportID string, mapping model.Mapping) (err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.GetCurrentMapping")
	defer telemetry.EndSpan(span, &err)

	if err := c.validator.Validate(ctx, mapping); err != nil {
		return err
	}

	key := schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID).Revision(mapping.RevisionID)

	value, err := json.EncodeString(mapping, false)
	if err != nil {
		return err
	}

	_, err = client.KV.Put(ctx, key.Key(), value)
	if err != nil {
		return err
	}

	return nil
}

func (c *Store) GetCurrentMapping(ctx context.Context, projectID int, receiverID string, exportID string) (r *model.Mapping, err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.GetCurrentMapping")
	defer telemetry.EndSpan(span, &err)

	prefix := schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID)

	// Get only the last mapping added (i.e. the one with the biggest timestamp)
	resp, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortDescend), etcd.WithLimit(1))
	if err != nil {
		return nil, err
	}

	// No mapping found
	if len(resp.Kvs) == 0 {
		return nil, serviceError.NewResourceNotFoundError("mapping", exportID)
	}

	mapping := &model.Mapping{}
	if err = json.DecodeString(string(resp.Kvs[0].Value), mapping); err != nil {
		return nil, err
	}
	return mapping, nil
}
