package dependencies

import (
	"context"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ConfigStore struct {
	logger     log.Logger
	etcdClient *etcd.Client
}

func NewConfigStore(logger log.Logger, etcdClient *etcd.Client) *ConfigStore {
	return &ConfigStore{logger, etcdClient}
}

func (c *ConfigStore) CreateReceiver(ctx context.Context, config model.Receiver) error {
	logger := c.logger

	client := c.etcdClient

	key := fmt.Sprintf("config/%s/receiver/%s", config.ProjectID, config.ID)

	logger.Debugf(`Reading "%s" count`, key)
	r, err := client.KV.Get(ctx, key, etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if r.Count > 0 {
		return errors.Errorf(`receiver "%s" already exists`, config.ID)
	}

	logger.Debugf(`Encoding "%s"`, key)
	value, err := json.EncodeString(config, false)
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
