package dependencies

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	etcd "go.etcd.io/etcd/client/v3"
)

type storeDeps interface {
	Logger() log.Logger
	EtcdClient(ctx context.Context) (*etcd.Client, error)
}

type ConfigStore struct {
	d storeDeps
}

func NewConfigStore(d storeDeps) *ConfigStore {
	return &ConfigStore{d: d}
}

func (c *ConfigStore) CreateReceiver(ctx context.Context, config model.Receiver) error {
	logger := c.d.Logger()

	client, err := c.d.EtcdClient(ctx)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("config/%s/receiver/%s", config.ProjectID, config.ID)

	logger.Infof("Reading %s count", key)
	r, err := client.KV.Get(ctx, key, etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if r.Count > 0 {
		return errors.Errorf(`receiver "%s" already exists`, config.ID)
	}

	logger.Infof("Encoding %s", key)
	value, err := json.EncodeString(config, false)
	if err != nil {
		return err
	}

	logger.Infof("PUT %s %s", key, value)
	_, err = client.KV.Put(ctx, key, value)
	if err != nil {
		return err
	}

	return nil
}
