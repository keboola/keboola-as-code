package iterator

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type DefinitionT[T any] struct {
	Definition
	// serde is serialization/deserialization implementation, it is empty for not-typed iterator
	serde *serde.Serde
	// filters - true means accepting the value
	filters []func(*op.KeyValueT[T]) bool
}

type IteratorT[T any] struct {
	// Iterator is underlying iterator, without T
	*Iterator
	// serde is serialization/deserialization implementation, it is empty for not-typed iterator
	serde *serde.Serde
	// filters - true means accepting the value
	filters []func(*op.KeyValueT[T]) bool
	// currentValue in the page, match indexOnPage
	currentValue *op.KeyValueT[T]
}

// ForEachT definition, it can be part of a transaction.
type ForEachT[T any] struct {
	def     DefinitionT[T]
	onPage  []onPageFn
	onValue func(value T, header *Header) error
	onKV    func(value *op.KeyValueT[T], header *Header) error
}

func NewTyped[R any](client etcd.KV, serde *serde.Serde, prefix string, opts ...Option) DefinitionT[R] {
	return DefinitionT[R]{Definition: New(client, prefix, opts...), serde: serde}
}

// Do converts iterator definition to the iterator.
func (v DefinitionT[T]) Do(ctx context.Context, opts ...op.Option) *IteratorT[T] {
	out := &IteratorT[T]{Iterator: newIterator(v.config).Do(ctx, opts...), serde: v.serde, filters: v.filters}
	return out
}

// ForEach method converts iterator to for each operation definition, so it can be part of a transaction.
func (v DefinitionT[T]) ForEach(fn func(value T, header *Header) error) *ForEachT[T] {
	return &ForEachT[T]{def: v, onValue: fn}
}

// ForEachKV method converts iterator to for each operation definition, so it can be part of a transaction.
func (v DefinitionT[T]) ForEachKV(fn func(value *op.KeyValueT[T], header *Header) error) *ForEachT[T] {
	return &ForEachT[T]{def: v, onKV: fn}
}

// WithKVFilter adds KV filters. All filters must return true for the value to be accepted.
func (v DefinitionT[T]) WithKVFilter(fns ...func(kv *op.KeyValueT[T]) bool) DefinitionT[T] {
	clone := v
	clone.filters = nil
	clone.filters = append(clone.filters, v.filters...)
	clone.filters = append(clone.filters, fns...)
	return clone
}

// WithFilter adds value filters. All filters must return true for the value to be accepted.
func (v DefinitionT[T]) WithFilter(fns ...func(v T) bool) DefinitionT[T] {
	clone := v
	clone.filters = nil
	clone.filters = append(clone.filters, v.filters...)
	for _, fn := range fns {
		clone.filters = append(clone.filters, func(kv *op.KeyValueT[T]) bool {
			return fn(kv.Value)
		})
	}
	return clone
}

// WithAllTo method converts iterator to for each operation definition, so it can be part of a transaction.
func (v DefinitionT[T]) WithAllTo(slice *[]T) *ForEachT[T] {
	return v.
		ForEach(func(value T, header *Header) error {
			*slice = append(*slice, value)
			return nil
		}).
		AndOnFirstPage(func(response *etcd.GetResponse) error {
			*slice = nil
			return nil
		})
}

// WithAllKVsTo method converts iterator to for each operation definition, so it can be part of a transaction.
func (v DefinitionT[T]) WithAllKVsTo(slice *op.KeyValuesT[T]) *ForEachT[T] {
	return v.
		ForEachKV(func(kv *op.KeyValueT[T], header *Header) error {
			*slice = append(*slice, kv)
			return nil
		}).
		AndOnFirstPage(func(response *etcd.GetResponse) error {
			*slice = nil
			return nil
		})
}

// AndOnFirstPage registers a callback that is executed after the first page is successfully loaded.
func (v *ForEachT[T]) AndOnFirstPage(fn func(response *etcd.GetResponse) error) *ForEachT[T] {
	return v.AndOnPage(func(pageIndex int, response *etcd.GetResponse) error {
		if pageIndex == 0 {
			return fn(response)
		}
		return nil
	})
}

// AndOnPage registers a callback that is executed after each page is successfully loaded.
func (v *ForEachT[T]) AndOnPage(fn onPageFn) *ForEachT[T] {
	clone := *v
	clone.onPage = append(clone.onPage, fn)
	return &clone
}

func (v *ForEachT[T]) Op(ctx context.Context) (op.LowLevelOp, error) {
	// If ForEachT is combined with other operations into a transaction,
	// then the first page is loaded within the transaction.
	// Other pages are loaded within MapResponse method, see below.
	// Iterator always load next pages WithRevision,
	// so all results, from all pages, are from the same revision.
	firstPageOp, err := newFirstPageOp(v.def.config).Op(ctx)
	if err != nil {
		return op.LowLevelOp{}, err
	}

	return op.LowLevelOp{
		Op: firstPageOp.Op,
		MapResponse: func(ctx context.Context, response *op.RawResponse) (result any, err error) {
			// Create iterator, see comment above.
			itr := v.def.Do(ctx, response.Options()...).OnPage(v.onPage...)
			itr.client = response.Client()

			// Inject the first page, from the response
			itr.moveToPage(response.Get())

			// Process all records from the first page and load next pages, if any.
			return op.NoResult{}, v.forEach(itr)
		},
	}, nil
}

func (v *ForEachT[T]) Do(ctx context.Context, opts ...op.Option) (out Result) {
	// See comment in the Op method.
	itr := v.def.Do(ctx, opts...).OnPage(v.onPage...)

	if err := v.forEach(itr); err != nil {
		out.error = err
		return out
	}

	out.header = itr.header
	return out
}

func (v *ForEachT[T]) forEach(itr *IteratorT[T]) error {
	if v.onValue != nil {
		return itr.ForEachValue(v.onValue)
	} else if v.onKV != nil {
		return itr.ForEachKV(v.onKV)
	}
	return nil
}

// OnFirstPage registers a callback that is executed after the first page is successfully loaded.
func (v *IteratorT[T]) OnFirstPage(fns ...func(response *etcd.GetResponse) error) *IteratorT[T] {
	v.Iterator.OnFirstPage(fns...)
	return v
}

// OnPage registers a callback that is executed after each page is successfully loaded.
func (v *IteratorT[T]) OnPage(fns ...onPageFn) *IteratorT[T] {
	v.Iterator.OnPage(fns...)
	return v
}

// Next returns true if there is a next value.
// False is returned if there is no next value or an error occurred.
func (v *IteratorT[T]) Next() bool {
Loop:
	for {
		// Is there next item?
		if !v.Iterator.Next() {
			return false
		}

		// Decode item
		v.currentValue = &op.KeyValueT[T]{Kv: v.values[v.indexOnPage]}
		if err := v.serde.Decode(v.ctx, v.currentValue.Kv, &v.currentValue.Value); err != nil {
			v.err = errors.Errorf(`etcd iterator failed: cannot decode the value of key "%s", page=%d, index=%d: %w`, v.currentValue.Kv.Key, v.page, v.indexOnPage, err)
			return false
		}

		// Apply filters
		for _, filter := range v.filters {
			if !filter(v.currentValue) {
				continue Loop
			}
		}

		return true
	}
}

// Value returns the current value.
// It must be called after Next method.
func (v *IteratorT[T]) Value() *op.KeyValueT[T] {
	if v.page == 0 {
		panic(errors.New("unexpected Value() call: Next() must be called first"))
	}
	if v.err != nil {
		panic(errors.Errorf("unexpected Value() call: %w", v.err))
	}
	return v.currentValue
}

// All returns all values as a slice.
func (v *IteratorT[T]) All() (out []T, err error) {
	if err = v.AllTo(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// AllKVs returns all values as a slice.
func (v *IteratorT[T]) AllKVs() (out op.KeyValuesT[T], err error) {
	if err = v.AllKVsTo(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// AllTo resets the slice and add all values to the slice.
func (v *IteratorT[T]) AllTo(out *[]T) (err error) {
	*out = nil
	for v.Next() {
		*out = append(*out, v.Value().Value)
	}
	if err = v.Err(); err != nil {
		return err
	}
	return nil
}

// AllKVsTo resets the slice and add all values to the slice.
func (v *IteratorT[T]) AllKVsTo(out *op.KeyValuesT[T]) (err error) {
	*out = nil
	for v.Next() {
		*out = append(*out, v.Value())
	}
	if err = v.Err(); err != nil {
		return err
	}
	return nil
}

// ForEachKV iterates the KVs using a callback.
func (v *IteratorT[T]) ForEachKV(fn func(value *op.KeyValueT[T], header *Header) error) (err error) {
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
