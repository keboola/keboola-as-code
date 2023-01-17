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
	end = ""
)

type Header = etcdserverpb.ResponseHeader

type Definition struct {
	config
}

type Iterator struct {
	config
	ctx          context.Context
	opts         []op.Option
	client       etcd.KV
	err          error
	start        string         // page start prefix
	page         int            // page number, start from 1
	lastIndex    int            // lastIndex in the page, 0 means empty
	currentIndex int            // currentIndex in the page, start from 0
	values       []*op.KeyValue // values in the page
	header       *Header        // page response header
	currentValue *op.KeyValue   // currentValue in the page, match currentIndex
}

func New(start string, opts ...Option) Definition {
	return newIterator(newConfig(start, nil, opts))
}

func newIterator(config config) Definition {
	return Definition{config: config}
}

// Do converts iterator definition to the iterator.
func (v Definition) Do(ctx context.Context, client etcd.KV, opts ...op.Option) *Iterator {
	return &Iterator{ctx: ctx, client: client, opts: opts, config: v.config, start: v.config.prefix, page: 0, currentIndex: 0}
}
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

	// Do with retry
	_, raw, err := nextPageOp(v.prefix, v.start, v.pageSize, v.revision).DoWithRaw(v.ctx, v.client, v.opts...)
	if err != nil {
		v.err = errors.Errorf(`etcd iterator failed: cannot get page "%s", page=%d: %w`, v.start, v.page, err)
		return false
	}

	return v.moveToPage(raw.Get())
}

func (v *Iterator) moveToPage(resp *etcd.GetResponse) bool {
	kvs := resp.Kvs
	header := resp.Header
	more := resp.More

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
	v.revision = header.Revision
	v.page++
	return true
}

func nextPageOp(prefix, start string, pageSize int, revision int64) op.GetManyOp {
	// Range options
	opts := []etcd.OpOption{
		etcd.WithFromKey(),
		etcd.WithRange(etcd.GetPrefixRangeEnd(prefix)), // iterate to the end of the prefix
		etcd.WithLimit(int64(pageSize)),
		etcd.WithSort(etcd.SortByKey, etcd.SortAscend),
	}

	// Ensure atomicity
	if revision > 0 {
		opts = append(opts, etcd.WithRev(revision))
	}

	return op.NewGetManyOp(
		func(ctx context.Context) (etcd.Op, error) {
			return etcd.OpGet(start, opts...), nil
		},
		func(_ context.Context, r etcd.OpResponse) ([]*op.KeyValue, error) {
			return r.Get().Kvs, nil
		},
	)
}
