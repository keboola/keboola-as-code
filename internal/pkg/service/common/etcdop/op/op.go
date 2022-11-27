// Package op wraps low-level etcd operations to more easily usable high-level operation.
package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"
)

// Op is a generic operation.
type Op[P any] struct {
	opFactory Factory
	processor P
}

// Factory creates an etcd operation.
type Factory func(ctx context.Context) (etcd.Op, error)

// Op returns raw etcd.Op.
func (v Factory) Op(ctx context.Context) (etcd.Op, error) {
	return v(ctx)
}
