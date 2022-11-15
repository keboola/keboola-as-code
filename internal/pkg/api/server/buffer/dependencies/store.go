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

func ReceiverKey(projectId int, receiverId string) string {
	return fmt.Sprintf("config/%d/receiver/%s", projectId, receiverId)
}

func NewConfigStore(logger log.Logger, etcdClient *etcd.Client, validator validator.Validator, tracer trace.Tracer) *ConfigStore {
	return &ConfigStore{logger, etcdClient, validator, tracer}
}

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
