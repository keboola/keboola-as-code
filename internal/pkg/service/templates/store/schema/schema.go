// Package schema defines etcd keys for the Templates service.
package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

type Schema struct {
	serde *serde.Serde
}

type prefix = etcdop.Prefix

func New(validate serde.ValidateFn) *Schema {
	return &Schema{
		serde: serde.NewJSON(validate),
	}
}
