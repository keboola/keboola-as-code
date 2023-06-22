package etcdclient

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Credentials struct {
	Endpoint  string `mapstructure:"etcd-endpoint" usage:"etcd endpoint."`
	Namespace string `mapstructure:"etcd-namespace" usage:"etcd namespace."`
	Username  string `mapstructure:"etcd-username" usage:"etcd username."`
	Password  string `mapstructure:"etcd-password" usage:"etcd password." sensitive:"true"`
}

func (c *Credentials) Normalize() {
	c.Endpoint = strings.Trim(c.Endpoint, " /")
	c.Namespace = strings.Trim(c.Namespace, " /") + "/"
}

func (c *Credentials) Validate() error {
	if c.Endpoint == "" {
		return errors.New("etcd endpoint is not set")
	}
	if c.Namespace == "/" {
		return errors.New("etcd namespace is not set")
	}
	return nil
}
