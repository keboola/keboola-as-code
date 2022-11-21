package dependencies

import (
	"context"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	telemetryUtils "github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const MaxReceiversPerProject = 100

type ConfigStore struct {
	logger     log.Logger
	etcdClient *etcd.Client
	validator  validator.Validator
	tracer     trace.Tracer
}

func NewConfigStore(logger log.Logger, etcdClient *etcd.Client, validator validator.Validator, tracer trace.Tracer) *ConfigStore {
	return &ConfigStore{logger, etcdClient, validator, tracer}
}

func ReceiverKey(projectID int, receiverID string) string {
	return fmt.Sprintf("config/receiver/%d/%s", projectID, receiverID)
}

func ReceiverPrefix(projectID int) string {
	return fmt.Sprintf("config/receiver/%d", projectID)
}

func ExportsPrefix(projectID int, receiverID string) string {
	return fmt.Sprintf("config/export/%d/%s/", projectID, receiverID)
}

func MappingsPrefix(projectID int, receiverID string, exportID string) string {
	return fmt.Sprintf("config/mapping/revision/%d/%s/%s/", projectID, receiverID, exportID)
}

func MappingKey(projectID int, receiverID string, exportID string, revisionID int) string {
	return fmt.Sprintf("config/mapping/revision/%d/%s/%s/%08d", projectID, receiverID, exportID, revisionID)
}

type ReceiverLimitReachedError struct{}

func (ReceiverLimitReachedError) Error() string {
	return fmt.Sprintf("receiver limit reached, the maximum is %d", MaxReceiversPerProject)
}

type ReceiverNotFoundError struct {
	ID string
}

func (r ReceiverNotFoundError) Error() string {
	return fmt.Sprintf("receiver \"%s\" not found", r.ID)
}

// CreateReceiver puts a receiver into the store.
//
// This method guarantees that no two receivers in the store will have the same (projectID, receiverID) pair.
//
// May fail if receiver limit is reached (`ReceiverLimitReachedError`), or if any of the underlying ETCD calls fail.
func (c *ConfigStore) CreateReceiver(ctx context.Context, receiver model.Receiver) (err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.CreateReceiver")
	defer telemetryUtils.EndSpan(span, &err)

	if err := c.validator.Validate(ctx, receiver); err != nil {
		return err
	}

	prefix := ReceiverPrefix(receiver.ProjectID)
	logger.Debugf(`Reading "%s" count`, prefix)
	allReceivers, err := client.KV.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if allReceivers.Count >= MaxReceiversPerProject {
		return ReceiverLimitReachedError{}
	}

	key := ReceiverKey(receiver.ProjectID, receiver.ID)

	logger.Debugf(`Reading "%s" count`, key)
	receivers, err := client.KV.Get(ctx, key, etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if receivers.Count > 0 {
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
func (c *ConfigStore) GetReceiver(ctx context.Context, projectID int, receiverID string) (r *model.Receiver, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.GetReceiver")
	defer telemetryUtils.EndSpan(span, &err)

	key := ReceiverKey(projectID, receiverID)

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

func (c *ConfigStore) ListReceivers(ctx context.Context, projectID int) (r []*model.Receiver, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.ListReceivers")
	defer telemetryUtils.EndSpan(span, &err)

	prefix := ReceiverPrefix(projectID)

	logger.Debugf(`GET "%s"`, prefix)
	resp, err := client.KV.Get(ctx, prefix, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Decoding list "%s"`, prefix)
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
// May fail if the receiver is not found (`ReceiverNotFoundError`), or if any of the underlying ETCD calls fail.
func (c *ConfigStore) DeleteReceiver(ctx context.Context, projectID int, receiverID string) (err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.DeleteReceiver")
	defer telemetryUtils.EndSpan(span, &err)

	key := ReceiverKey(projectID, receiverID)

	logger.Debugf(`DELETE "%s"`, key)
	r, err := client.KV.Delete(ctx, key)
	if err != nil {
		return err
	}

	if r.Deleted == 0 {
		return ReceiverNotFoundError{ID: receiverID}
	}

	return nil
}

func (c *ConfigStore) ListExports(ctx context.Context, projectID int, receiverID string) (r []*model.Export, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.ListExports")
	defer telemetryUtils.EndSpan(span, &err)

	key := ExportsPrefix(projectID, receiverID)

	logger.Debugf(`GET "%s"`, key)
	resp, err := client.KV.Get(ctx, key, etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Decoding list "%s"`, key)
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

func (c *ConfigStore) GetCurrentMapping(ctx context.Context, projectID int, receiverID string, exportID string) (r *model.Mapping, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "kac.api.server.buffer.dependencies.store.getCurrentMapping")
	defer telemetryUtils.EndSpan(span, &err)

	key := MappingsPrefix(projectID, receiverID, exportID)

	logger.Debugf(`GET "%s"`, key)
	// Get only the last mapping added (i.e. the one with the biggest timestamp)
	resp, err := client.KV.Get(ctx, key, etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortDescend), etcd.WithLimit(1))
	if err != nil {
		return nil, err
	}

	// No mapping found
	if len(resp.Kvs) == 0 {
		logger.Debugf(`No mapping "%s" found`, key)
		return nil, nil
	}

	logger.Debugf(`Decoding "%s"`, key)
	mapping := &model.Mapping{}
	if err = json.DecodeString(string(resp.Kvs[0].Value), mapping); err != nil {
		return nil, err
	}
	return mapping, nil
}
