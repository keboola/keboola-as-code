package dependencies

import (
	"context"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	telemetryUtils "github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type ConfigStore struct {
	logger     log.Logger
	etcdClient *etcd.Client
	validator  validator.Validator
	tracer     trace.Tracer
}

func NewConfigStore(logger log.Logger, etcdClient *etcd.Client, validator validator.Validator, tracer trace.Tracer) *ConfigStore {
	return &ConfigStore{logger, etcdClient, validator, tracer}
}

func ReceiverKey(projectId int, receiverId string) string {
	return fmt.Sprintf("config/%d/receiver/%s", projectId, receiverId)
}

func ProjectKey(projectId int) string {
	return fmt.Sprintf("config/%d", projectId)
}

func (c *ConfigStore) CountReceivers(ctx context.Context, projectId int) (count uint64, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.CreateReceiver")
	defer telemetryUtils.EndSpan(span, &err)

	key := ProjectKey(projectId)

	logger.Debugf(`Reading "%s" count`, key)
	r, err := client.KV.Get(ctx, key, etcd.WithPrefix(), etcd.WithCountOnly())
	if err != nil {
		return 0, err
	}

	return uint64(r.Count), nil
}

// CreateReceiver puts a receiver into the store.
//
// This method guarantees that no two receivers in the store will have the same (projectId, receiverId) pair.
func (c *ConfigStore) CreateReceiver(ctx context.Context, receiver model.Receiver) (err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.CreateReceiver")
	defer telemetryUtils.EndSpan(span, &err)

	if err := c.validator.Validate(ctx, receiver); err != nil {
		return err
	}

	key := ReceiverKey(receiver.ProjectID, receiver.ID)

	logger.Debugf(`Reading "%s" count`, key)
	r, err := client.KV.Get(ctx, key, etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if r.Count > 0 {
		return errors.Errorf(`receiver "%s" already exists`, receiver.ID)
	}

	logger.Debugf(`Encoding "%s"`, key)
	value, err := json.EncodeString(receiver, false)
	if err != nil {
		return err
	}

	logger.Debugf(`PUT "%s" "%s"`, key, value)
	_, err = client.KV.Put(ctx, key, value)
	if err != nil {
		return err
	}

	return nil
}

// GetReceiver fetches a receiver from the store.
//
// This method returns nil if no receiver was found.
func (c *ConfigStore) GetReceiver(ctx context.Context, projectId int, receiverId string) (r *model.Receiver, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.GetReceiver")
	defer telemetryUtils.EndSpan(span, &err)

	key := ReceiverKey(projectId, receiverId)

	logger.Debugf(`GET "%s"`, key)
	resp, err := client.KV.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// No receiver found
	if len(resp.Kvs) == 0 {
		logger.Debugf(`No receiver "%s" found`, key)
		return nil, nil
	}

	logger.Debugf(`Decoding "%s"`, key)
	receiver := &model.Receiver{}
	if err = json.DecodeString(string(resp.Kvs[0].Value), receiver); err != nil {
		return nil, err
	}

	return receiver, nil
}

func (c *ConfigStore) ListReceivers(ctx context.Context, projectId int) (r []*model.Receiver, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.ListReceivers")
	defer telemetryUtils.EndSpan(span, &err)

	key := ProjectKey(projectId)

	logger.Debugf(`GET "%s"`, key)
	resp, err := client.KV.Get(ctx, key, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Decoding list "%s"`, key)
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
