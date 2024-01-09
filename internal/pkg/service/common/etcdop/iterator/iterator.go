// Package iterator provides iterator for etcd prefix.
// Iterator is for raw values and IteratorT for typed values, with serialization support.
package iterator

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	end = "\000"
)

type Header = etcdserverpb.ResponseHeader

type Definition struct {
	config
}

type Iterator struct {
	config       config
	ctx          context.Context
	opts         []op.Option
	err          error
	start        string         // page start prefix
	end          string         // page start prefix
	page         int            // page number, start from 1
	lastIndex    int            // lastIndex in the page, 0 means empty
	currentIndex int            // currentIndex in the page, start from 0
	values       []*op.KeyValue // values in the page
	header       *Header        // page response header
	currentValue *op.KeyValue   // currentValue in the page, match currentIndex
	onPage       []onPageFn
}

// ForEachOp definition, it can be part of a transaction.
type ForEachOp struct {
	def    Definition
	onPage []onPageFn
	fn     func(value *op.KeyValue, header *Header) error
}

// Result of the ForEachOp and ForEachOpT operations.
type Result struct {
	header *Header
	error  error
}

type onPageFn func(pageIndex int, response *etcd.GetResponse) error

func New(client etcd.KV, start string, opts ...Option) Definition {
	return newIterator(newConfig(client, nil, start, opts))
}

func newIterator(config config) Definition {
	return Definition{config: config}
}

// Do converts iterator definition to the iterator.
func (v Definition) Do(ctx context.Context, opts ...op.Option) *Iterator {
	return &Iterator{ctx: ctx, opts: opts, config: v.config, start: v.config.prefix, end: v.config.end, page: 0, currentIndex: 0}
}

// ForEach method converts iterator to for each operation definition, so it can be part of a transaction.
func (v Definition) ForEach(fn func(value *op.KeyValue, header *Header) error) *ForEach {
	return &ForEach{def: v, fn: fn}
}

func (v *ForEach) Op(ctx context.Context) (op.LowLevelOp, error) {
	// If ForEach is combined with other operations into a transaction,
	// then the first page is loaded within the transaction.
	// Other pages are loaded within MapResponse function, see below.
	// Iterator always load next pages WithRevision,
	// so all results, from all pages, are from the same revision.
	firstPageOp, err := newFirstPageOp(v.def.client, v.def.prefix, v.def.end, v.def.pageSize, v.def.revision).Op(ctx)
	if err != nil {
		return op.LowLevelOp{}, err
	}

	return op.LowLevelOp{
		Op: firstPageOp.Op,
		MapResponse: func(ctx context.Context, response op.RawResponse) (result any, err error) {
			// Create iterator, see comment above.
			itr := v.def.Do(ctx, response.Options...).OnPage(v.onPage...)
			itr.config.client = response.Client

			// Inject the first page, from the response
			itr.moveToPage(response.Get())
			itr.currentIndex--

			// Process all records from the first page and load next pages, if any.
			return op.NoResult{}, itr.ForEach(v.fn)
		},
	}, nil
}

func (v *ForEach) Do(ctx context.Context, opts ...op.Option) (out Result) {
	// See comment in the Op method.
	itr := v.def.Do(ctx, opts...).OnPage(v.onPage...)
	if err := itr.ForEach(v.fn); err != nil {
		out.error = err
		return out
	}
	out.header = itr.header
	return out
}

// OnFirstPage registers a callback that is executed after the first page is successfully loaded.
func (v *Iterator) OnFirstPage(fns ...func(response *etcd.GetResponse) error) *Iterator {
	for _, fn := range fns {
		v.onPage = append(v.onPage, func(pageIndex int, response *etcd.GetResponse) error {
			if pageIndex == 0 {
				return fn(response)
			}
			return nil
		})
	}
	return v
}

// OnPage registers a callback that is executed after each page is successfully loaded.
func (v *Iterator) OnPage(fns ...onPageFn) *Iterator {
	v.onPage = append(v.onPage, fns...)
	return v
}

// Next returns true if there is a next value.
// False is returned if there is no next value or an error occurred.
func (v *Iterator) Next() bool {
	select {
	case <-v.ctx.Done():
		// Stop iteration if the context is done
		v.err = v.ctx.Err()
		return false
	default:
		// Is there one more item?
		if !v.nextItem() && !v.nextPage() {
			return false
		}

		v.currentValue = v.values[v.currentIndex]
		return true
	}
}

// Value returns the current value.
// It must be called after Next method.
func (v *Iterator) Value() *op.KeyValue {
	if v.page == 0 {
		panic(errors.New("unexpected Value() call: Next() must be called first"))
	}
	if v.err != nil {
		panic(errors.Errorf("unexpected Value() call: %w", v.err))
	}
	return v.currentValue
}

// Header returns header of the page etcd response.
func (v *Iterator) Header() *Header {
	return v.header
}

// Err returns error. It must be checked after iterations (Next() == false).
func (v *Iterator) Err() error {
	return v.err
}

// All returns all values as a slice.
//
// The values are sorted by key in ascending order.
func (v *Iterator) All() (out []*op.KeyValue, err error) {
	if err = v.AllTo(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// AllTo resets the slice and add all values to the slice.
//
// The values are sorted by key in ascending order.
func (v *Iterator) AllTo(out *[]*op.KeyValue) (err error) {
	*out = (*out)[:0]
	for v.Next() {
		*out = append(*out, v.Value())
	}
	if err = v.Err(); err != nil {
		return err
	}
	return nil
}

// ForEach iterates the KVs using a callback.
func (v *Iterator) ForEach(fn func(value *op.KeyValue, header *Header) error) (err error) {
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

func (v *Iterator) nextItem() bool {
	if v.lastIndex > v.currentIndex {
		v.currentIndex++
		return true
	}
	return false
}

func (v *Iterator) nextPage() bool {
	// Is there one more page?
	if v.start == end {
		return false
	}

	// If these keys can change, we will ensure that all pages are from the same revision.
	// Enabled by default, see WithFromSameRev.
	revision := int64(0)
	if v.header != nil && v.config.fromSameRev {
		revision = v.header.Revision
	} else if v.config.revision > 0 {
		revision = v.config.revision
	}

	// Do with retry
	r := nextPageOp(v.config.client, v.start, v.end, v.config.pageSize, revision).Do(v.ctx, v.opts...)
	if err := r.Err(); err != nil {
		v.err = errors.Errorf(`etcd iterator failed: cannot get page "%s", page=%d, revision=%d: %w`, v.start, v.page, revision, err)
		return false
	}

	return v.moveToPage(r.Response().Get())
}

func (v *Iterator) moveToPage(resp *etcd.GetResponse) bool {
	kvs := resp.Kvs
	header := resp.Header
	more := resp.More

	// Invoke callbacks
	for _, cb := range v.onPage {
		if err := cb(v.page, resp); err != nil {
			v.err = err
			return false
		}
	}

	// Handle empty result
	v.values = kvs
	v.header = header
	v.lastIndex = len(v.values) - 1
	if v.lastIndex == -1 {
		return false
	}

	// Prepare next page
	if more {
		// Start of the next page is one key after the last key
		lastKey := string(v.values[v.lastIndex].Key)
		v.start = etcd.GetPrefixRangeEnd(lastKey)
	} else {
		v.start = end
	}

	v.currentIndex = 0
	v.page++
	return true
}

func (v Result) Header() *Header {
	return v.header
}

func (v Result) Err() error {
	return v.error
}

func newFirstPageOp(client etcd.KV, prefix, end string, pageSize int, revision int64) op.GetManyOp {
	return nextPageOp(client, prefix, end, pageSize, revision)
}

func nextPageOp(client etcd.KV, start, end string, pageSize int, revision int64) op.GetManyOp {
	// Range options
	opts := []etcd.OpOption{
		etcd.WithFromKey(),
		etcd.WithRange(end), // iterate to the end of the prefix
		etcd.WithLimit(int64(pageSize)),
		etcd.WithSort(etcd.SortByKey, etcd.SortAscend),
	}

	// Ensure atomicity
	if revision > 0 {
		opts = append(opts, etcd.WithRev(revision), etcd.WithSerializable())
	}

	return op.NewGetManyOp(
		client,
		func(ctx context.Context) (etcd.Op, error) {
			return etcd.OpGet(start, opts...), nil
		},
		func(_ context.Context, raw op.RawResponse) ([]*op.KeyValue, error) {
			return raw.Get().Kvs, nil
		},
	)
}
