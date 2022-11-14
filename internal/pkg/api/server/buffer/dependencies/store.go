package dependencies

import (
	"context"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type ConfigStore struct {
	logger     log.Logger
	etcdClient *etcd.Client
	validator  validator.Validator
}

func NewConfigStore(logger log.Logger, etcdClient *etcd.Client, validator validator.Validator) *ConfigStore {
	return &ConfigStore{logger, etcdClient, validator}
}

func (c *ConfigStore) CreateReceiver(ctx context.Context, receiver model.Receiver) error {
	logger, client := c.logger, c.etcdClient

	if err := c.validator.Validate(ctx, receiver); err != nil {
		return err
	}

	key := fmt.Sprintf("config/%d/receiver/%s", receiver.ProjectID, receiver.ID)

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
