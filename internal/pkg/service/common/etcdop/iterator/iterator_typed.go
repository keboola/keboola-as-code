package iterator

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type DefinitionT[T any] struct {
	config
}

type IteratorT[T any] struct {
	*Iterator                    // raw iterator, without T
	currentValue op.KeyValueT[T] // currentValue in the page, match currentIndex
}

func NewTyped[R any](start string, serde *serde.Serde, opts ...Option) DefinitionT[R] {
	return DefinitionT[R]{config: newConfig(start, serde, opts)}
}

// Do converts iterator definition to the iterator.
func (v DefinitionT[T]) Do(ctx context.Context, client etcd.KV, opts ...op.Option) *IteratorT[T] {
	out := &IteratorT[T]{Iterator: newIterator(v.config).Do(ctx, client, opts...)}
	out.serde = v.serde
	return out
}

// Next returns true if there is a next value.
// False is returned if there is no next value or an error occurred.
func (v *IteratorT[T]) Next() bool {
	if !v.Iterator.Next() {
		return false
	}

	// Decode item
	v.currentValue = op.KeyValueT[T]{Kv: v.values[v.currentIndex]}
	if err := v.serde.Decode(v.ctx, v.currentValue.Kv, &v.currentValue.Value); err != nil {
		v.err = errors.Errorf(`etcd iterator failed: cannot decode key "%s", page=%d, index=%d: %w`, v.currentValue.Kv.Key, v.page, v.currentIndex, err)
	}
	return v.err == nil
}

// Value returns the current value.
// It must be called after Next method.
func (v *IteratorT[T]) Value() op.KeyValueT[T] {
	if v.page == 0 {
		panic(errors.New("unexpected Value() call: Next() must be called first"))
	}
	if v.err != nil {
		panic(errors.Errorf("unexpected Value() call: %w", v.err))
	}
	return v.currentValue
}

// All returns all values as a slice.
//
// The values are sorted by key in ascending order.
func (v *IteratorT[T]) All() (out op.KeyValuesT[T], err error) {
	if err = v.AllTo(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// AllTo resets the slice and add all values to the slice.
//
// The values are sorted by key in ascending order.
func (v *IteratorT[T]) AllTo(out *op.KeyValuesT[T]) (err error) {
	*out = (*out)[:0]
	for v.Next() {
		*out = append(*out, v.Value())
	}
	if err = v.Err(); err != nil {
		return err
	}
	return nil
}

// ForEachKV iterates the KVs using a callback.
func (v *IteratorT[T]) ForEachKV(fn func(value op.KeyValueT[T], header *Header) error) (err error) {
	for v.Next() {
		if err = fn(v.Value(), v.Header()); err != nil {
			return err
		}
	}
	if err = v.Err(); err != nil {
		return err
	}
	return nil
}

// ForEachValue iterates the typed values using a callback.
func (v *IteratorT[T]) ForEachValue(fn func(value T, header *Header) error) (err error) {
	for v.Next() {
		if err = fn(v.Value().Value, v.Header()); err != nil {
			return err
		}
	}
	if err = v.Err(); err != nil {
		return err
	}
	return nil
}
