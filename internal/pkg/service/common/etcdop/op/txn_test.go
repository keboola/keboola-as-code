package op

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestNewTxnOp_ToRawOp_Empty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Convert high-level transaction to the low-level raw etcd operation
	rawOp, err := NewTxnOp().Op(ctx)
	assert.NoError(t, err)

	// Check mapping of the high-level transaction -> low-level transaction
	cmps, thenOps, elseOps := rawOp.Txn()
	assert.Empty(t, cmps)
	assert.Empty(t, thenOps)
	assert.Empty(t, elseOps)
}

func TestNewTxnOp_ToRawOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Convert high-level transaction to the low-level raw etcd operation
	rawOp, err := txnForTest(true, false).Op(ctx)
	assert.NoError(t, err)

	// Check mapping of the high-level transaction -> low-level transaction
	cmps, thenOps, elseOps := rawOp.Txn()
	assert.Equal(t, []etcd.Cmp{
		etcd.Compare(etcd.Version("missingKey"), "=", 0),
	}, cmps)
	assert.Equal(t, []etcd.Op{
		etcd.OpPut("foo1", "bar1"),
		etcd.OpPut("foo2", "bar2"),
		etcd.OpTxn(
			[]etcd.Cmp{etcd.Compare(etcd.Version("foo3"), "=", 0)},
			[]etcd.Op{etcd.OpPut("foo3", "value3"), etcd.OpGet("foo3")},
			[]etcd.Op{etcd.OpGet("foo3")},
		),
		etcd.OpTxn(
			[]etcd.Cmp{etcd.Compare(etcd.Version("foo4"), "=", 0)},
			[]etcd.Op{etcd.OpPut("foo4", "value4"), etcd.OpGet("foo", etcd.WithPrefix())},
			[]etcd.Op{etcd.OpGet("foo", etcd.WithPrefix())},
		),
		etcd.OpGet("foo", etcd.WithPrefix(), etcd.WithCountOnly()),
	}, thenOps)
	assert.Equal(t, []etcd.Op{
		etcd.OpGet("foo", etcd.WithPrefix(), etcd.WithCountOnly()),
	}, elseOps)
}

func TestNewTxnOp_If_True_Then(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)
	op := txnForTest(true, false)

	// "If" should be true and pass to "Then" ops
	r, err := op.Do(ctx, client)
	assert.NoError(t, err)

	r = clearRawValues(r)
	assert.Equal(t, []any{
		NoResult{},
		NoResult{},
		&KeyValue{Key: []byte("foo3"), Value: []byte("value3")},
		[]*KeyValue{
			{Key: []byte("foo1"), Value: []byte("bar1")},
			{Key: []byte("foo2"), Value: []byte("bar2")},
			{Key: []byte("foo3"), Value: []byte("value3")},
			{Key: []byte("foo4"), Value: []byte("value4")},
		},
		int64(400), // modified by the processor
	}, r.Responses)
}

func TestNewTxnOp_If_False_Else(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)
	op := txnForTest(false, false)

	// "If" should be true and pass to "Then" ops
	r, err := op.Do(ctx, client)
	assert.NoError(t, err)

	r = clearRawValues(r)
	assert.Equal(t, []any{
		int64(-100), // modified by the processor
	}, r.Responses)
}

func TestNewTxnOp_If_True_Then_MapperError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)
	op := txnForTest(true, true)

	// "If" should be true and pass to "Then" ops
	// But there is an error from a mapper
	r, err := op.Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, "cannot process etcd response from the transaction step [then][1]: error from the mapper", err.Error())
	assert.Empty(t, r)
}

func TestMergeToTxn_ToRaw_Complex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	txn1 := MergeToTxn(opsForTest(false)...)
	txn1.If(etcd.Compare(etcd.Version("missingKey"), "=", 0))

	// Convert high-level transaction to the low-level raw etcd operation
	rawOp, err := txn1.Op(ctx)
	assert.NoError(t, err)

	// Check mapping of the high-level transaction -> low-level transaction
	cmps, thenOps, elseOps := rawOp.Txn()
	assert.Equal(t, []etcd.Cmp{
		etcd.Compare(etcd.Version("missingKey"), "=", 0),
	}, cmps)
	assert.Equal(t, []etcd.Op{
		etcd.OpPut("foo1", "bar1"),
		etcd.OpPut("foo2", "bar2"),
		etcd.OpTxn(
			[]etcd.Cmp{etcd.Compare(etcd.Version("foo3"), "=", 0)},
			[]etcd.Op{etcd.OpPut("foo3", "value3"), etcd.OpGet("foo3")},
			[]etcd.Op{etcd.OpGet("foo3")},
		),
		etcd.OpTxn(
			[]etcd.Cmp{etcd.Compare(etcd.Version("foo4"), "=", 0)},
			[]etcd.Op{etcd.OpPut("foo4", "value4"), etcd.OpGet("foo", etcd.WithPrefix())},
			[]etcd.Op{etcd.OpGet("foo", etcd.WithPrefix())},
		),
		etcd.OpGet("foo", etcd.WithPrefix(), etcd.WithCountOnly()),
	}, thenOps)
	assert.Equal(t, []etcd.Op{}, elseOps)
}

func TestMergeToTxn_ToRaw_Nested_Txn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)

	// Merge 3 operations
	txn := MergeToTxn(
		NewTxnOp().
			If(etcd.Compare(etcd.Version("missingKey1"), "=", 0)).
			Then(
				NewNoResultOp(
					func(ctx context.Context) (etcd.Op, error) {
						return etcd.OpPut("foo1", "bar1"), nil
					},
					func(ctx context.Context, r etcd.OpResponse) error {
						return nil
					},
				),
			).Else(
			NewGetOneOp(
				func(ctx context.Context) (etcd.Op, error) {
					return etcd.OpGet("missingKey1"), nil
				},
				func(ctx context.Context, r etcd.OpResponse) (*KeyValue, error) {
					get := r.Get()
					if get.Count > 0 {
						return get.Kvs[0], nil
					}
					return nil, nil
				},
			),
		),
		NewTxnOp().
			If(etcd.Compare(etcd.Version("missingKey2"), "=", 0)).
			Then(
				NewNoResultOp(
					func(ctx context.Context) (etcd.Op, error) {
						return etcd.OpPut("foo2", "bar2"), nil
					},
					func(ctx context.Context, r etcd.OpResponse) error {
						return nil
					},
				),
			).Else(
			NewCountOp(
				func(ctx context.Context) (etcd.Op, error) {
					return etcd.OpGet("missingKey", etcd.WithPrefix(), etcd.WithCountOnly()), nil
				},
				func(ctx context.Context, r etcd.OpResponse) (int64, error) {
					return r.Get().Count, nil
				},
			).WithProcessor(func(_ context.Context, _ etcd.OpResponse, result int64, err error) (int64, error) {
				// Modify value from the etcd
				return result + 1000, nil
			}),
		),
		NewNoResultOp(
			func(ctx context.Context) (etcd.Op, error) {
				return etcd.OpPut("foo3", "bar3"), nil
			},
			func(ctx context.Context, r etcd.OpResponse) error {
				return nil
			},
		),
	)

	// Convert high-level transaction to the low-level raw etcd operation
	rawOp, err := txn.Op(ctx)
	assert.NoError(t, err)

	// Check mapping of the high-level transaction -> low-level transaction
	cmps, thenOps, elseOps := rawOp.Txn()
	assert.Equal(t, []etcd.Cmp{
		etcd.Compare(etcd.Version("missingKey1"), "=", 0),
		etcd.Compare(etcd.Version("missingKey2"), "=", 0),
	}, cmps)
	assert.Equal(t, []etcd.Op{
		etcd.OpPut("foo1", "bar1"),
		etcd.OpPut("foo2", "bar2"),
		etcd.OpPut("foo3", "bar3"),
	}, thenOps)
	assert.Equal(t, []etcd.Op{
		etcd.OpGet("missingKey1"),
		etcd.OpGet("missingKey", etcd.WithPrefix(), etcd.WithCountOnly()),
	}, elseOps)

	// Try txn, IF false, key exists
	_, err = client.Put(ctx, "missingKey1", "foo")
	assert.NoError(t, err)
	r, err := txn.Do(ctx, client)
	assert.NoError(t, err)
	assert.False(t, r.Succeeded)
	r = clearRawValues(r)
	assert.Equal(t, []any{
		&KeyValue{Key: []byte("missingKey1"), Value: []byte("foo")},
		int64(1001), // modified by the processor
	}, r.Responses)
	dump, err := etcdhelper.DumpAll(ctx, client)
	assert.NoError(t, err)
	expected := `
<<<<<
missingKey1
-----
foo
>>>>>
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), dump)

	// Try txn, IF true, key not exists
	_, err = client.Delete(ctx, "missingKey1")
	assert.NoError(t, err)
	r, err = txn.Do(ctx, client)
	assert.NoError(t, err)
	assert.True(t, r.Succeeded)
	r = clearRawValues(r)
	assert.Equal(t, []any{NoResult{}, NoResult{}, NoResult{}}, r.Responses) // 3x PUT
	dump, err = etcdhelper.DumpAll(ctx, client)
	assert.NoError(t, err)
	expected = `
<<<<<
foo1
-----
bar1
>>>>>

<<<<<
foo2
-----
bar2
>>>>>

<<<<<
foo3
-----
bar3
>>>>>
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), dump)
}

func txnForTest(success bool, withMapperError bool) *TxnOp {
	txn := NewTxnOp()

	if success {
		txn.If(etcd.Compare(etcd.Version("missingKey"), "=", 0))
	} else {
		txn.If(etcd.Compare(etcd.Version("missingKey"), "!=", 0))
	}

	// Then
	for _, op := range opsForTest(withMapperError) {
		txn.Then(op)
	}

	// Else
	txn.Else(
		NewCountOp(
			func(ctx context.Context) (etcd.Op, error) {
				// Count
				return etcd.OpGet("foo", etcd.WithPrefix(), etcd.WithCountOnly()), nil
			},
			func(ctx context.Context, r etcd.OpResponse) (int64, error) {
				return r.Get().Count, nil
			},
		).WithProcessor(func(_ context.Context, _ etcd.OpResponse, result int64, err error) (int64, error) {
			return result - 100, nil
		}),
	)

	return txn
}

func opsForTest(withMapperError bool) []Op {
	return []Op{
		NewNoResultOp(
			func(ctx context.Context) (etcd.Op, error) {
				return etcd.OpPut("foo1", "bar1"), nil
			},
			func(ctx context.Context, r etcd.OpResponse) error {
				return nil
			},
		),
		NewNoResultOp(
			func(ctx context.Context) (etcd.Op, error) {
				return etcd.OpPut("foo2", "bar2"), nil
			},
			func(ctx context.Context, r etcd.OpResponse) error {
				if withMapperError {
					return errors.New("error from the mapper")
				}
				return nil
			},
		),
		NewGetOneOp(
			func(ctx context.Context) (etcd.Op, error) {
				// Put if not exists + get value always
				return etcd.OpTxn(
					[]etcd.Cmp{etcd.Compare(etcd.Version("foo3"), "=", 0)},
					[]etcd.Op{etcd.OpPut("foo3", "value3"), etcd.OpGet("foo3")},
					[]etcd.Op{etcd.OpGet("foo3")},
				), nil
			},
			func(ctx context.Context, r etcd.OpResponse) (*KeyValue, error) {
				if r.Txn().Succeeded {
					// Then: get foo3 key
					get := r.Txn().Responses[1].GetResponseRange()
					if get.Count > 0 {
						return get.Kvs[0], nil
					}
					return nil, nil
				}
				// Else: get foo3 key
				get := r.Txn().Responses[0].GetResponseRange()
				if get.Count > 0 {
					return get.Kvs[0], nil
				}
				return nil, nil
			},
		),
		NewGetManyOp(
			func(ctx context.Context) (etcd.Op, error) {
				// Put if not exists + get value always
				return etcd.OpTxn(
					[]etcd.Cmp{etcd.Compare(etcd.Version("foo4"), "=", 0)},
					[]etcd.Op{etcd.OpPut("foo4", "value4"), etcd.OpGet("foo", etcd.WithPrefix())},
					[]etcd.Op{etcd.OpGet("foo", etcd.WithPrefix())},
				), nil
			},
			func(ctx context.Context, r etcd.OpResponse) ([]*KeyValue, error) {
				if r.Txn().Succeeded {
					// Then: get foo prefix
					return r.Txn().Responses[1].GetResponseRange().Kvs, nil
				}
				// Else: get foo prefix
				return r.Txn().Responses[0].GetResponseRange().Kvs, nil
			},
		),
		NewCountOp(
			func(ctx context.Context) (etcd.Op, error) {
				// Count
				return etcd.OpGet("foo", etcd.WithPrefix(), etcd.WithCountOnly()), nil
			},
			func(ctx context.Context, r etcd.OpResponse) (int64, error) {
				return r.Get().Count, nil
			},
		).WithProcessor(func(_ context.Context, _ etcd.OpResponse, result int64, err error) (int64, error) {
			// Modify value from the etcd
			return result * 100, nil
		}),
	}
}

// clearRawValues from the TxnResponse, for easier comparison.
func clearRawValues(v TxnResponse) TxnResponse {
	for i, r := range v.Responses {
		switch r := r.(type) {
		case *KeyValue:
			if r != nil {
				r = &KeyValue{Key: r.Key, Value: r.Value}
				v.Responses[i] = r
			}
		case []*KeyValue:
			for j, item := range r {
				item = &KeyValue{Key: item.Key, Value: item.Value}
				r[j] = item
			}
			v.Responses[i] = r
		}
	}
	return v
}
