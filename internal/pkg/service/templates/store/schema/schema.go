// Package schema defines etcd keys for the Templates service.
package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

type Schema struct {
	serde *serde.Serde
}

type dependencies interface {
	EtcdSerde() *serde.Serde
}

func New(d dependencies) *Schema {
	return &Schema{serde: d.EtcdSerde()}
}
