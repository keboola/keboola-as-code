package op_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
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

	// Test processor
	var processorInvoked bool
	op = op.WithProcessor(func(_ context.Context, _ *etcd.TxnResponse, _ TxnResult, _ error) error {
		processorInvoked = true
		return nil
	})

	// Test nested processor
	var newstedProcessorInvoked bool
	op.Then(
		NewTxnOp().WithProcessor(func(_ context.Context, _ *etcd.TxnResponse, _ TxnResult, _ error) error {
			newstedProcessorInvoked = true
			return nil
		}),
	)

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
		TxnResult{
			Succeeded: true,
			Results:   nil,
		},
	}, r.Results)
	assert.True(t, processorInvoked)
	assert.True(t, newstedProcessorInvoked)
}

func TestNewTxnOp_If_False_Else(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)
	op := txnForTest(false, false)

	// Test processor
	var processorInvoked bool
	op = op.WithProcessor(func(_ context.Context, _ *etcd.TxnResponse, _ TxnResult, _ error) error {
		processorInvoked = true
		return nil
	})

	// "If" should be true and pass to "Then" ops
	r, err := op.Do(ctx, client)
	assert.NoError(t, err)

	r = clearRawValues(r)
	assert.Equal(t, []any{
		int64(-100), // modified by the processor
	}, r.Results)
	assert.True(t, processorInvoked)
}

func TestNewTxnOp_If_True_Then_MapperError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)
	op := txnForTest(true, true)

	// Test processor
	op = op.WithProcessor(func(_ context.Context, _ *etcd.TxnResponse, _ TxnResult, err error) error {
		if err != nil {
			err = errors.Errorf("%w, wrapped by the processor", err)
		}
		return err
	})

	// "If" should be true and pass to "Then" ops
	// But there is an error from a mapper
	r, err := op.Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, "error from the mapper foo2, wrapped by the processor", err.Error())
	assert.Empty(t, r)
}

func TestMergeToTxn_ToRaw_Complex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	txn1 := MergeToTxn(opsForTest(false)...).Txn(ctx)

	// Add another IF
	txn1.If(etcd.Compare(etcd.Version("missingKey"), "=", 0))

	// Check mapping of the high-level transaction -> low-level transaction
	rawOp, err := txn1.Op(ctx)
	assert.NoError(t, err)
	cmps, thenOps, elseOps := rawOp.Txn()
	assert.Equal(t, []etcd.Cmp{
		etcd.Compare(etcd.Version("foo3"), "=", 0),
		etcd.Compare(etcd.Version("foo4"), "=", 0),
		etcd.Compare(etcd.Version("missingKey"), "=", 0),
	}, cmps)
	assert.Equal(t, []etcd.Op{
		etcd.OpPut("foo1", "bar1"),
		etcd.OpPut("foo2", "bar2"),
		etcd.OpTxn(
			[]etcd.Cmp{},
			[]etcd.Op{etcd.OpPut("foo3", "value3"), etcd.OpGet("foo3")},
			[]etcd.Op{},
		),
		etcd.OpTxn(
			[]etcd.Cmp{},
			[]etcd.Op{etcd.OpPut("foo4", "value4"), etcd.OpGet("foo", etcd.WithPrefix())},
			[]etcd.Op{},
		),
		etcd.OpGet("foo", etcd.WithPrefix(), etcd.WithCountOnly()),
	}, thenOps)
	assert.Equal(t, []etcd.Op{
		etcd.OpTxn(
			[]etcd.Cmp{etcd.Compare(etcd.Version("foo3"), "=", 0)},
			[]etcd.Op{},
			[]etcd.Op{etcd.OpGet("foo3")},
		),
		etcd.OpTxn(
			[]etcd.Cmp{etcd.Compare(etcd.Version("foo4"), "=", 0)},
			[]etcd.Op{},
			[]etcd.Op{etcd.OpGet("foo", etcd.WithPrefix())},
		),
	}, elseOps)
}

func TestMergeToTxn_ToRaw_Nested_Txn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)

	// Merge 3 operations
	txn := MergeToTxn(
		NewTxnOp().
			If(etcd.Compare(etcd.Version("shouldBeMissing1"), "=", 0)).
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
					return etcd.OpGet("shouldBeMissing1"), nil
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
			If(etcd.Compare(etcd.Version("shouldBeMissing2"), "=", 0)).
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
					return etcd.OpGet("shouldBeMissing", etcd.WithPrefix(), etcd.WithCountOnly()), nil
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

	// Check mapping of the high-level transaction -> low-level transaction
	rawOp, err := txn.Op(ctx)
	assert.NoError(t, err)
	cmps, thenOps, elseOps := rawOp.Txn()
	assert.Equal(t, []etcd.Cmp{
		etcd.Compare(etcd.Version("shouldBeMissing1"), "=", 0),
		etcd.Compare(etcd.Version("shouldBeMissing2"), "=", 0),
	}, cmps)
	assert.Equal(t, []etcd.Op{
		etcd.OpTxn(
			[]etcd.Cmp{},
			[]etcd.Op{etcd.OpPut("foo1", "bar1")},
			[]etcd.Op{},
		),
		etcd.OpTxn(
			[]etcd.Cmp{},
			[]etcd.Op{etcd.OpPut("foo2", "bar2")},
			[]etcd.Op{},
		),
		etcd.OpPut("foo3", "bar3"),
	}, thenOps)
	assert.Equal(t, []etcd.Op{
		etcd.OpTxn(
			[]etcd.Cmp{etcd.Compare(etcd.Version("shouldBeMissing1"), "=", 0)},
			[]etcd.Op{},
			[]etcd.Op{etcd.OpGet("shouldBeMissing1")},
		),
		etcd.OpTxn(
			[]etcd.Cmp{etcd.Compare(etcd.Version("shouldBeMissing2"), "=", 0)},
			[]etcd.Op{},
			[]etcd.Op{etcd.OpGet("shouldBeMissing", etcd.WithPrefix(), etcd.WithCountOnly())},
		),
	}, elseOps)

	// Check failed transaction, keys exists
	_, err = client.Put(ctx, "shouldBeMissing1", "foo")
	assert.NoError(t, err)
	_, err = client.Put(ctx, "shouldBeMissing2", "bar")
	assert.NoError(t, err)
	r, err := txn.Do(ctx, client)
	assert.NoError(t, err)
	assert.False(t, r.Succeeded)
	r = clearRawValues(r)
	assert.Equal(t, TxnResult{
		Succeeded: false,
		// Response from the "Else" branch
		Results: []any{
			TxnResult{
				Succeeded: false,
				Results: []any{
					&KeyValue{Key: []byte("shouldBeMissing1"), Value: []byte("foo")},
				},
			},
			TxnResult{
				Succeeded: false,
				Results:   []any{int64(1002)}, // modified by the processor: 1000 + 2 (number of shouldBeMissing* keys)
			},
		},
	}, clearRawValues(r))

	// Check etcd: no change
	expected := `
<<<<<
shouldBeMissing1
-----
foo
>>>>>

<<<<<
shouldBeMissing2
-----
bar
>>>>>
`
	dump, err := etcdhelper.DumpAllToString(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), dump)

	// Check successful transaction, keys exist
	_, err = client.Delete(ctx, "shouldBeMissing1")
	assert.NoError(t, err)
	_, err = client.Delete(ctx, "shouldBeMissing2")
	assert.NoError(t, err)
	r, err = txn.Do(ctx, client)
	assert.NoError(t, err)
	assert.True(t, r.Succeeded)
	r = clearRawValues(r)
	assert.Equal(t, []any{
		// Response from the "Then" branch
		TxnResult{
			Succeeded: true,
			Results:   []any{NoResult{}},
		},
		TxnResult{
			Succeeded: true,
			Results:   []any{NoResult{}},
		},
		NoResult{},
	}, clearRawValues(r).Results)

	// Check etcd: 3x PUT
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
	dump, err = etcdhelper.DumpAllToString(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), dump)
}

func TestMergeToTxn_Processors(t *testing.T) {
	t.Parallel()

	// Setup
	client := etcdhelper.ClientForTest(t)
	ctx := context.Background()
	pfx := etcdop.Prefix("my/prefix")

	// There is a nested txn with some processors.
	createProcessor := func(key string) func(context.Context, etcd.OpResponse, bool, error) (bool, error) {
		return func(_ context.Context, _ etcd.OpResponse, result bool, err error) (bool, error) {
			if !result && err == nil {
				return false, errors.Errorf(`key "%s" already exists`, key)
			}
			return result, err
		}
	}
	txn := MergeToTxn(
		pfx.Key("key1").PutIfNotExists("value").WithProcessor(createProcessor("key1")),
		pfx.Key("key2").PutIfNotExists("value").WithProcessor(createProcessor("key2")),
		MergeToTxn(
			pfx.Key("key3").PutIfNotExists("value").WithProcessor(createProcessor("key3")),
			pfx.Key("key4").PutIfNotExists("value").WithProcessor(createProcessor("key4")),
		),
	)

	testCases := []struct{ existingKeys, expectedErrs []string }{
		{
			existingKeys: nil,
			expectedErrs: nil,
		},
		{
			existingKeys: []string{"key1"},
			expectedErrs: []string{`key "key1" already exists`},
		},
		{
			existingKeys: []string{"key2"},
			expectedErrs: []string{`key "key2" already exists`},
		},
		{
			existingKeys: []string{"key3"},
			expectedErrs: []string{`key "key3" already exists`},
		},
		{
			existingKeys: []string{"key4"},
			expectedErrs: []string{`key "key4" already exists`},
		},
		{
			existingKeys: []string{"key1", "key2"},
			expectedErrs: []string{`key "key1" already exists`, `key "key2" already exists`},
		},
		{
			existingKeys: []string{"key3", "key4"},
			expectedErrs: []string{`key "key3" already exists`, `key "key4" already exists`},
		},
		{
			existingKeys: []string{"key1", "key3"},
			expectedErrs: []string{`key "key1" already exists`, `key "key3" already exists`},
		},
		{
			existingKeys: []string{"key2", "key4"},
			expectedErrs: []string{`key "key2" already exists`, `key "key4" already exists`},
		},
		{
			existingKeys: []string{"key1", "key2", "key3", "key4"},
			expectedErrs: []string{
				`key "key1" already exists`,
				`key "key2" already exists`,
				`key "key3" already exists`,
				`key "key4" already exists`,
			},
		},
	}

	// Run test cases
	for i, tc := range testCases {
		desc := fmt.Sprintf("test case %d", i)

		// Clean the prefix
		_, err := pfx.DeleteAll().Do(ctx, client)
		assert.NoError(t, err, desc)

		// Create keys
		for _, key := range tc.existingKeys {
			assert.NoError(t, pfx.Key(key).Put("value").Do(ctx, client), desc)
		}

		// Do and compare errors
		var actualErrs []string
		if _, err := txn.Do(ctx, client); err != nil {
			for _, item := range err.(errors.MultiError).WrappedErrors() {
				actualErrs = append(actualErrs, item.Error())
			}
		}
		assert.Equal(t, tc.expectedErrs, actualErrs, desc)
	}
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
					return errors.New("error from the mapper foo2")
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
func clearRawValues(v TxnResult) TxnResult {
	v.Header = nil
	for i, r := range v.Results {
		switch r := r.(type) {
		case *KeyValue:
			if r != nil {
				r = &KeyValue{Key: r.Key, Value: r.Value}
				v.Results[i] = r
			}
		case []*KeyValue:
			for j, item := range r {
				item = &KeyValue{Key: item.Key, Value: item.Value}
				r[j] = item
			}
			v.Results[i] = r
		case TxnResult:
			v.Results[i] = clearRawValues(r)
		}
	}
	return v
}
