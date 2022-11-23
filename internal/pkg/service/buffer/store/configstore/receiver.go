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

// CreateReceiver puts a receiver into the store.
//
// This method guarantees that no two receivers in the store will have the same (projectID, receiverID) pair.
//
// May fail if
// - limit is reached (`LimitReachedError`)
// - already exists (`AlreadyExistsError`)
// - validation of the model fails
// - JSON marshalling fails
// - any of the underlying ETCD calls fail.
func (c *Store) CreateReceiver(ctx context.Context, receiver model.Receiver) (err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.CreateReceiver")
	defer telemetry.EndSpan(span, &err)

	if err := c.validator.Validate(ctx, receiver); err != nil {
		return err
	}

	prefix := schema.Configs().Receivers().InProject(receiver.ProjectID)
	allReceivers, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if allReceivers.Count >= MaxReceiversPerProject {
		return serviceError.NewCountLimitReachedError("receiver", MaxExportsPerReceiver, "project")
	}

	key := prefix.ID(receiver.ID)

	receivers, err := client.KV.Get(ctx, key.Key(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if receivers.Count > 0 {
		return serviceError.NewResourceAlreadyExistsError("receiver", receiver.ID, "project")
	}

	value, err := json.EncodeString(receiver, false)
	if err != nil {
		return err
	}

	_, err = client.KV.Put(ctx, key.Key(), value)
	if err != nil {
		return err
	}

	return nil
}

// GetReceiver fetches a receiver from the store.
//
// May fail if the receiver was not found (`NotFoundError`).
func (c *Store) GetReceiver(ctx context.Context, projectID int, receiverID string) (r *model.Receiver, err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.GetReceiver")
	defer telemetry.EndSpan(span, &err)

	key := schema.Configs().Receivers().InProject(projectID).ID(receiverID)

	resp, err := client.KV.Get(ctx, key.Key())
	if err != nil {
		return nil, err
	}

	// No receiver found
	if len(resp.Kvs) == 0 {
		return nil, serviceError.NewResourceNotFoundError("receiver", receiverID)
	}

	receiver := &model.Receiver{}
	if err = json.DecodeString(string(resp.Kvs[0].Value), receiver); err != nil {
		return nil, err
	}

	return receiver, nil
}

func (c *Store) ListReceivers(ctx context.Context, projectID int) (r []*model.Receiver, err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.ListReceivers")
	defer telemetry.EndSpan(span, &err)

	prefix := schema.Configs().Receivers().InProject(projectID)

	resp, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix())
	if err != nil {
		return nil, err
	}

	receivers := make([]*model.Receiver, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		receiver := &model.Receiver{}
		if err = json.DecodeString(string(kv.Value), receiver); err != nil {
			return nil, err
		}
		receivers = append(receivers, receiver)
	}

	return receivers, nil
}

// DeleteReceiver deletes a receiver from the store.
//
// May fail if the receiver is not found (`NotFoundError`), or if any of the underlying ETCD calls fail.
func (c *Store) DeleteReceiver(ctx context.Context, projectID int, receiverID string) (err error) {
	tracer, client := c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	key := schema.Configs().Receivers().InProject(projectID).ID(receiverID)

	r, err := client.KV.Delete(ctx, key.Key())
	if err != nil {
		return err
	}

	if r.Deleted == 0 {
		return serviceError.NewResourceNotFoundError("receiver", receiverID)
	}

	return nil
}
