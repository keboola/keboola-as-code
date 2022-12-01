package iterator

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Definition[R any] struct {
	config
}

type Iterator[R any] struct {
	config
	ctx    context.Context
	client *etcd.Client
	page   int
	index  int
	err    error
}

func New[R any](start string, serde serde.Serde, opts ...Option) Definition[R] {
	// Apply options
	c := config{limit: DefaultLimit, start: start, serde: serde}
	for _, o := range opts {
		o(&c)
	}
	return Definition[R]{config: c}
}

func (v Definition[T]) Do(ctx context.Context, client *etcd.Client) *Iterator[T] {
	return &Iterator[T]{ctx: ctx, client: client, config: v.config, page: -1, index: -1}
}

func (v *Iterator[T]) Next() bool {
	return false
}

func (v *Iterator[T]) Value() op.KeyValueT[T] {
	if v.index == -1 {
		panic(errors.New("unexpected Value() call: Next() must be called first"))
	}
	if v.err != nil {
		panic(errors.Errorf("unexpected Value() call: %w", v.err))
	}
	return op.KeyValueT[T]{}
}

func (v *Iterator[T]) Err() error {
	return v.err
}
