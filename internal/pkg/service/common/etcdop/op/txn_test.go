package op_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type txnTestCase struct {
	Name              string
	InitialEtcdState  string
	ExpectedLogs      string
	ExpectedEtcdState string
	ExpectedTxnResult txnResult
}

func (tc txnTestCase) Run(t *testing.T, ctx context.Context, client *etcd.Client, log *strings.Builder, txn *TxnOp[NoResult]) {
	t.Helper()
	t.Logf(`test case: %s`, tc.Name)

	log.Reset()
	require.NoError(t, etcdop.Prefix("").DeleteAll(client).Do(ctx).Err(), tc.Name)
	require.NoError(t, etcdhelper.PutAllFromSnapshot(ctx, client, tc.InitialEtcdState), tc.Name)

	assert.Equal(t, tc.ExpectedTxnResult, simplifyTxnResult(txn.Do(ctx)), tc.Name)
	etcdhelper.AssertKVsString(t, client, tc.ExpectedEtcdState)
	assert.Equal(t, strings.TrimSpace(tc.ExpectedLogs), strings.TrimSpace(log.String()), tc.Name)
}

func TestTxnOp_Empty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	txn := Txn(client)

	assert.True(t, txn.Empty())

	// Validate low-level representation of the transaction
	if lowLevel, err := txn.Op(ctx); assert.NoError(t, err) {
		assert.Equal(t, etcd.OpTxn(
			// If
			[]etcd.Cmp{},
			// Then
			[]etcd.Op{},
			// Else
			[]etcd.Op{},
		), lowLevel.Op)
	}

	// Execute
	assert.Equal(t, txnResult{Succeeded: true, Error: "", Results: nil}, simplifyTxnResult(txn.Do(ctx)))
}

func TestTxnOp_OpError_Create(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Create operation failing on create
	op := testOp{Error: errors.New("some error")}

	txn := Txn(client).
		Then(op).
		Then(op).
		Else(op).
		Else(op).
		Merge(op).
		Merge(op).
		Merge(Txn(client).Then(op))

	assert.False(t, txn.Empty())

	if err := txn.Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
- cannot create operation [then][0]: some error
- cannot create operation [then][1]: some error
- cannot create operation [else][0]: some error
- cannot create operation [else][1]: some error
- cannot create operation [merge][0]: some error
- cannot create operation [merge][1]: some error
- cannot create operation [merge][2]:
  - cannot create operation [then][0]: some error
`), err.Error())
	}
}

func TestTxnOp_OpError_MapResult_IfBranch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Create operation failing on MapResponse
	opFactory := func(i int) Op {
		return testOp{Operation: LowLevelOp{
			Op: etcd.OpPut(fmt.Sprintf("key/foo%d", i), "bar"), // duplicate key in a transaction is not allowed
			MapResponse: func(_ context.Context, _ RawResponse) (result any, err error) {
				return nil, errors.Errorf("some error (%d)", i)
			},
		}}
	}

	txn := Txn(client).
		Then(opFactory(1)).
		Then(opFactory(2)).
		Else(opFactory(3)).
		Else(opFactory(4)).
		Merge(opFactory(5)).
		Merge(opFactory(6)).
		Merge(Txn(client).Then(opFactory(7)))

	if err := txn.Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
- some error (1)
- some error (2)
- some error (5)
- some error (6)
- some error (7)
`), err.Error())
	}
}

func TestTxnOp_OpError_MapResult_ElseBranch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Create operation failing on MapResponse
	opFactory := func(i int) Op {
		return testOp{Operation: LowLevelOp{
			Op: etcd.OpPut(fmt.Sprintf("key/foo%d", i), "bar"), // duplicate key in a transaction is not allowed
			MapResponse: func(_ context.Context, _ RawResponse) (result any, err error) {
				return nil, errors.Errorf("some error (%d)", i)
			},
		}}
	}

	txn := Txn(client).
		If(etcd.Compare(etcd.Value("key/foo"), "=", "bar")).
		Then(opFactory(1)).
		Then(opFactory(2)).
		Else(opFactory(3)).
		Else(opFactory(4)).
		Merge(opFactory(5)).
		Merge(opFactory(6)).
		Merge(Txn(client).Then(opFactory(7)))

	if err := txn.Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
- some error (3)
- some error (4)
`), err.Error())
	}
}

func TestTxnOp_ServerError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	txn := Txn(client).
		Then(etcdop.Key("foo").Put(client, "bar")).
		Then(etcdop.Key("foo").Put(client, "bar"))

	if err := txn.Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `etcdserver: duplicate key given in txn request`, err.Error())
	}
}

func TestTxnOp_IfThenElse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Helpers to test processor callbacks
	var log strings.Builder
	onNoResult, onGetResult := newLogHelpers(&log)

	// Define transaction
	txn := Txn(client)
	txn.If(etcd.Compare(etcd.Value("key/foo"), "=", "foo"))
	txn.If(etcd.Compare(etcd.Value("key/bar"), "=", "bar"))
	txn.Then(etcdop.Key("key/1").Put(client, "a").WithOnResult(onNoResult("put 1/1")).WithOnResult(onNoResult("put 1/2")))
	txn.Then(etcdop.Key("key/2").Put(client, "b").WithOnResult(onNoResult("put 2")))
	txn.Then(etcdop.Key("key/foo").Get(client).WithOnResult(onGetResult("get foo")))
	txn.Then(etcdop.Key("key/bar").Get(client).WithOnResult(onGetResult("get bar")))
	txn.Else(etcdop.Key("key/3").Put(client, "c").WithOnResult(onNoResult("put 3")))
	txn.Else(etcdop.Key("key/4").Put(client, "d").WithOnResult(onNoResult("put 4")))
	txn.OnSucceeded(func(r *TxnResult[NoResult]) {
		fmt.Fprintf(&log, "txn OnSucceeded\n")
	})
	txn.OnFailed(func(r *TxnResult[NoResult]) {
		fmt.Fprintf(&log, "txn OnFailed\n")
	})
	txn.OnResult(func(r *TxnResult[NoResult]) {
		if r.Succeeded() {
			fmt.Fprintln(&log, "txn OnResult: succeeded")
		} else {
			fmt.Fprintln(&log, "txn OnResult: failed")
		}
	})

	// Validate low-level representation of the transaction
	if lowLevel, err := txn.Op(ctx); assert.NoError(t, err) {
		assert.Equal(t, etcd.OpTxn(
			// If
			[]etcd.Cmp{
				etcd.Compare(etcd.Value("key/foo"), "=", "foo"),
				etcd.Compare(etcd.Value("key/bar"), "=", "bar"),
			},
			// Then
			[]etcd.Op{
				etcd.OpPut("key/1", "a"),
				etcd.OpPut("key/2", "b"),
				etcd.OpGet("key/foo"),
				etcd.OpGet("key/bar"),
			},
			// Else
			[]etcd.Op{
				etcd.OpPut("key/3", "c"),
				etcd.OpPut("key/4", "d"),
			},
		), lowLevel.Op)
	}

	// Test cases
	cases := []txnTestCase{
		// -------------------------------------------------------------------------------------------------------------
		{
			Name:             "Succeeded: false, Else branch",
			InitialEtcdState: ``,
			ExpectedEtcdState: `
<<<<<
key/3
-----
c
>>>>>

<<<<<
key/4
-----
d
>>>>>
`,
			ExpectedLogs: `
put 3
put 4
txn OnFailed
txn OnResult: failed
`,
			ExpectedTxnResult: txnResult{
				Succeeded: false,
				Results: []any{
					// Results from the Else branch
					NoResult{},
					NoResult{},
				},
			},
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			Name: "Succeeded: true, Then branch",
			InitialEtcdState: `
<<<<<
key/foo
-----
foo
>>>>>

<<<<<
key/bar
-----
bar
>>>>>
`,
			ExpectedEtcdState: `
<<<<<
key/foo
-----
foo
>>>>>

<<<<<
key/bar
-----
bar
>>>>>

<<<<<
key/1
-----
a
>>>>>

<<<<<
key/2
-----
b
>>>>>
`,
			ExpectedLogs: `
put 1/1
put 1/2
put 2
get foo foo
get bar bar
txn OnSucceeded
txn OnResult: succeeded
`,
			ExpectedTxnResult: txnResult{
				Succeeded: true,
				Results: []any{
					// Results from the Then branch
					NoResult{},
					NoResult{},
					&KeyValue{
						Key:   []byte("key/foo"),
						Value: []byte("foo"),
					},
					&KeyValue{
						Key:   []byte("key/bar"),
						Value: []byte("bar"),
					},
				},
			},
		},
		// -------------------------------------------------------------------------------------------------------------
	}

	// Run test-cases
	for _, tc := range cases {
		tc.Run(t, ctx, client, &log, txn)
	}
}

func TestTxnOp_Then_CalledWithTxn_1(t *testing.T) {
	t.Parallel()

	client := etcd.KV(nil)
	assert.PanicsWithError(t, `invalid operation[0]: op is a transaction, use Merge or ThenTxn, not Then`, func() {
		Txn(client).Then(Txn(client)).Op(context.Background())
	})
}

func TestTxnOp_Then_CalledWithTxn_2(t *testing.T) {
	t.Parallel()

	client := etcd.KV(nil)

	// Low-level txn, but not *TxnOp
	txnOp := NewNoResultOp(
		client,
		// Factory
		func(ctx context.Context) (etcd.Op, error) {
			return etcd.OpTxn(nil, nil, nil), nil
		},
		// Mapper
		func(ctx context.Context, raw RawResponse) error {
			return nil
		},
	)

	_, err := Txn(client).Then(txnOp).Op(context.Background())
	if assert.Error(t, err) {
		assert.Equal(t, "cannot create operation [then][0]: operation is a transaction, use Merge or ThenTxn, not Then", err.Error())
	}
}

func TestTxnOp_ThenTxn_CalledWithoutTxn(t *testing.T) {
	t.Parallel()

	client := etcd.KV(nil)
	_, err := Txn(client).ThenTxn(etcdop.Key("foo").Put(client, "bar")).Op(context.Background())
	if assert.Error(t, err) {
		assert.Equal(t, "cannot create operation [thenTxn][0]: operation is not a transaction, use Then, not ThenTxn", err.Error())
	}
}

func TestTxnOp_Then_Simple(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Collect processors output
	var log strings.Builder

	// Define transaction
	txn := Txn(client).
		Then(etcdop.Key("key1").Put(client, "value1")).
		ThenTxn(
			Txn(client).
				Then(etcdop.Key("key2").Put(client, "value2").WithOnResult(func(NoResult) {
					log.WriteString("put key2 succeeded\n")
				})).
				ThenTxn(
					Txn(client).
						Then(etcdop.Key("key3").Put(client, "value3").WithOnResult(func(NoResult) {
							log.WriteString("put key3 succeeded\n")
						})).
						OnSucceeded(func(*TxnResult[NoResult]) {
							log.WriteString("nested transaction succeeded\n")
						}),
				),
		).
		Then(etcdop.Key("key4").Put(client, "value4").WithOnResult(func(NoResult) {
			log.WriteString("put key4 succeeded\n")
		})).
		OnSucceeded(func(*TxnResult[NoResult]) {
			log.WriteString("root transaction succeeded\n")
		})

	// Check low-level representation
	if lowLevel, err := txn.Op(ctx); assert.NoError(t, err) {
		// ----- Txn - Level 1 ------
		assert.Equal(t, etcd.OpTxn(
			// If
			[]etcd.Cmp{},
			// Then
			[]etcd.Op{
				etcd.OpPut("key1", "value1"),
				// ----- Txn - Level 2 ------
				etcd.OpTxn(
					// If
					[]etcd.Cmp{},
					// Then
					[]etcd.Op{
						etcd.OpPut("key2", "value2"),
						// ----- Txn - Level 3 ------
						etcd.OpTxn(
							// If
							[]etcd.Cmp{},
							// Then
							[]etcd.Op{
								etcd.OpPut("key3", "value3"),
							},
							// Else
							[]etcd.Op{},
						),
					},
					// Else
					[]etcd.Op{},
				),
				// -----
				etcd.OpPut("key4", "value4"),
			},
			// Else
			[]etcd.Op{},
		), lowLevel.Op)
	}

	// Run transaction
	result := txn.Do(ctx)
	require.NoError(t, result.Err())
	assert.True(t, result.Succeeded())

	// Check processors
	assert.Equal(t, strings.TrimSpace(`
put key2 succeeded
put key3 succeeded
nested transaction succeeded
put key4 succeeded
root transaction succeeded
`), strings.TrimSpace(log.String()))
}

func TestTxnOp_Merge_Simple(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Collect processors output
	var log strings.Builder

	// Define transaction
	txn := Txn(client).
		Merge(
			etcdop.Key("key1").Put(client, "value1"),
			Txn(client).
				Then(etcdop.Key("key2").Put(client, "value2").WithOnResult(func(NoResult) {
					log.WriteString("put key2 succeeded\n")
				})).
				Merge(
					Txn(client).
						Then(etcdop.Key("key3").Put(client, "value3").WithOnResult(func(NoResult) {
							log.WriteString("put key3 succeeded\n")
						})).
						OnSucceeded(func(*TxnResult[NoResult]) {
							log.WriteString("nested transaction succeeded\n")
						}),
				),
			etcdop.Key("key4").Put(client, "value4").WithOnResult(func(NoResult) {
				log.WriteString("put key4 succeeded\n")
			}),
		).
		OnSucceeded(func(*TxnResult[NoResult]) {
			log.WriteString("root transaction succeeded\n")
		})

	// Check low-level representation
	if lowLevel, err := txn.Op(ctx); assert.NoError(t, err) {
		assert.Equal(t, etcd.OpTxn(
			// If
			[]etcd.Cmp{},
			// Then
			[]etcd.Op{
				etcd.OpPut("key1", "value1"),
				etcd.OpPut("key2", "value2"),
				etcd.OpPut("key3", "value3"),
				etcd.OpPut("key4", "value4"),
			},
			// Else
			[]etcd.Op{},
		), lowLevel.Op)
	}

	// Run transaction
	result := txn.Do(ctx)
	require.NoError(t, result.Err())
	assert.True(t, result.Succeeded())

	// Check processors
	assert.Equal(t, strings.TrimSpace(`
put key2 succeeded
put key3 succeeded
nested transaction succeeded
put key4 succeeded
root transaction succeeded
`), strings.TrimSpace(log.String()))
}

func TestTxnOp_Then_Simple_Ifs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Collect processors output
	var log strings.Builder

	// Define transaction
	txn := Txn(client).
		If(etcd.Compare(etcd.Version("key1"), "=", 0)).
		Then(etcdop.Key("key1").Put(client, "value1")).
		ThenTxn(
			Txn(client).
				If(etcd.Compare(etcd.Version("key2"), "=", 0)).
				Then(etcdop.Key("key2").Put(client, "value2").WithOnResult(func(NoResult) {
					log.WriteString("put key2 succeeded\n")
				})).
				OnSucceeded(func(*TxnResult[NoResult]) {
					log.WriteString("nested transaction succeeded - 1\n")
				}).
				OnFailed(func(result *TxnResult[NoResult]) {
					log.WriteString("nested transaction failed - 1\n")
				}).
				ThenTxn(
					Txn(client).
						If(etcd.Compare(etcd.Version("key3"), "=", 0)).
						Then(etcdop.Key("key3").Put(client, "value3").WithOnResult(func(NoResult) {
							log.WriteString("put key3 succeeded\n")
						})).
						OnSucceeded(func(*TxnResult[NoResult]) {
							log.WriteString("nested transaction succeeded - 2\n")
						}).
						OnFailed(func(result *TxnResult[NoResult]) {
							log.WriteString("nested transaction failed - 2\n")
						}),
				),
		).
		If(etcd.Compare(etcd.Version("key4"), "=", 0)).
		Then(etcdop.Key("key4").Put(client, "value4").WithOnResult(func(NoResult) {
			log.WriteString("put key4 succeeded\n")
		})).
		OnSucceeded(func(*TxnResult[NoResult]) {
			log.WriteString("root transaction succeeded\n")
		}).
		OnFailed(func(*TxnResult[NoResult]) {
			log.WriteString("root transaction failed\n")
		})

	// Check low-level representation
	if lowLevel, err := txn.Op(ctx); assert.NoError(t, err) {
		// ----- Txn - Level 1 ------
		assert.Equal(t, etcd.OpTxn(
			// If
			[]etcd.Cmp{
				etcd.Compare(etcd.Version("key1"), "=", 0),
				etcd.Compare(etcd.Version("key4"), "=", 0),
			},
			// Then
			[]etcd.Op{
				etcd.OpPut("key1", "value1"),
				// ----- Txn - Level 2 ------
				etcd.OpTxn(
					// If
					[]etcd.Cmp{
						etcd.Compare(etcd.Version("key2"), "=", 0),
					},
					// Then
					[]etcd.Op{
						etcd.OpPut("key2", "value2"),
						// ----- Txn - Level 3 ------
						etcd.OpTxn(
							// If
							[]etcd.Cmp{
								etcd.Compare(etcd.Version("key3"), "=", 0),
							},
							// Then
							[]etcd.Op{
								etcd.OpPut("key3", "value3"),
							},
							// Else
							[]etcd.Op{},
						),
					},
					// Else
					[]etcd.Op{},
				),
				// -----
				etcd.OpPut("key4", "value4"),
			},
			// Else
			[]etcd.Op{},
		), lowLevel.Op)
	}

	// Run transaction - success
	result := txn.Do(ctx)
	require.NoError(t, result.Err())
	assert.True(t, result.Succeeded())
	assert.Equal(t, strings.TrimSpace(`
put key2 succeeded
put key3 succeeded
nested transaction succeeded - 2
nested transaction succeeded - 1
put key4 succeeded
root transaction succeeded
`), strings.TrimSpace(log.String()))

	// Run transaction - partial fail - keys [key2,key3] already exists
	log.Reset()
	require.NoError(t, etcdop.Key("key1").Delete(client).Do(ctx).Err())
	require.NoError(t, etcdop.Key("key4").Delete(client).Do(ctx).Err())
	result = txn.Do(ctx)
	require.NoError(t, result.Err())
	assert.True(t, result.Succeeded())
	assert.Equal(t, strings.TrimSpace(`
nested transaction failed - 1
put key4 succeeded
root transaction succeeded
`), strings.TrimSpace(log.String()))
}

func TestTxnOp_Merge_Simple_Ifs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Collect processors output
	var log strings.Builder

	// Define transaction
	txn := Txn(client).
		If(etcd.Compare(etcd.Version("key1"), "=", 0)).
		Then(etcdop.Key("key1").Put(client, "value1")).
		Merge(
			Txn(client).
				If(etcd.Compare(etcd.Version("key2"), "=", 0)).
				Then(etcdop.Key("key2").Put(client, "value2").WithOnResult(func(NoResult) {
					log.WriteString("put key2 succeeded\n")
				})).
				OnSucceeded(func(*TxnResult[NoResult]) {
					log.WriteString("nested transaction succeeded - 1\n")
				}).
				OnFailed(func(result *TxnResult[NoResult]) {
					log.WriteString("nested transaction failed - 1\n")
				}).
				Merge(
					Txn(client).
						If(etcd.Compare(etcd.Version("key3"), "=", 0)).
						Then(etcdop.Key("key3").Put(client, "value3").WithOnResult(func(NoResult) {
							log.WriteString("put key3 succeeded\n")
						})).
						OnSucceeded(func(*TxnResult[NoResult]) {
							log.WriteString("nested transaction succeeded - 2\n")
						}).
						OnFailed(func(result *TxnResult[NoResult]) {
							log.WriteString("nested transaction failed - 2\n")
						}),
				),
		).
		If(etcd.Compare(etcd.Version("key4"), "=", 0)).
		Then(etcdop.Key("key4").Put(client, "value4").WithOnResult(func(NoResult) {
			log.WriteString("put key4 succeeded\n")
		})).
		OnSucceeded(func(*TxnResult[NoResult]) {
			log.WriteString("root transaction succeeded\n")
		}).
		OnFailed(func(*TxnResult[NoResult]) {
			log.WriteString("root transaction failed\n")
		})

	// Check low-level representation
	if lowLevel, err := txn.Op(ctx); assert.NoError(t, err) {
		assert.Equal(t, etcd.OpTxn(
			// If
			[]etcd.Cmp{
				// All conditions, from all merged transactions, have to be fulfilled
				etcd.Compare(etcd.Version("key1"), "=", 0),
				etcd.Compare(etcd.Version("key2"), "=", 0),
				etcd.Compare(etcd.Version("key3"), "=", 0),
				etcd.Compare(etcd.Version("key4"), "=", 0),
			},
			// Then
			[]etcd.Op{
				// Then all operations are applied
				etcd.OpPut("key1", "value1"),
				etcd.OpPut("key2", "value2"),
				etcd.OpPut("key3", "value3"),
				etcd.OpPut("key4", "value4"),
			},
			// Else
			[]etcd.Op{
				etcd.OpTxn(
					// If
					[]etcd.Cmp{
						// Check conditions of the nested transaction - 1, for processors
						etcd.Compare(etcd.Version("key2"), "=", 0),
						etcd.Compare(etcd.Version("key3"), "=", 0),
					},
					// Then
					[]etcd.Op{},
					// Else
					[]etcd.Op{
						// Check conditions of the nested transaction - 2, for processors
						etcd.OpTxn([]etcd.Cmp{etcd.Compare(etcd.Version("key3"), "=", 0)}, []etcd.Op{}, []etcd.Op{}),
					},
				),
			},
		), lowLevel.Op)
	}

	// Run transaction - success
	result := txn.Do(ctx)
	require.NoError(t, result.Err())
	assert.True(t, result.Succeeded())
	assert.Equal(t, strings.TrimSpace(`
put key2 succeeded
put key3 succeeded
nested transaction succeeded - 2
nested transaction succeeded - 1
put key4 succeeded
root transaction succeeded
`), strings.TrimSpace(log.String()))

	// Run transaction - failed - keys [key2,key3] already exists
	log.Reset()
	require.NoError(t, etcdop.Key("key1").Delete(client).Do(ctx).Err())
	require.NoError(t, etcdop.Key("key4").Delete(client).Do(ctx).Err())
	result = txn.Do(ctx)
	require.NoError(t, result.Err())
	assert.False(t, result.Succeeded())
	assert.Equal(t, strings.TrimSpace(`
nested transaction failed - 2
nested transaction failed - 1
root transaction failed
`), strings.TrimSpace(log.String()))
}

func TestTxnOp_Merge_RealExample(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Collect processors output
	var log strings.Builder

	// Create 2 simple sub-transactions
	putOp := etcdop.Key("key/put").PutIfNotExists(client, "value").WithResultValidator(func(ok bool) error {
		_, _ = fmt.Fprintf(&log, "put succeeded: %t\n", ok)
		if !ok {
			return errors.New("key/put already exists")
		}
		return nil
	})
	deleteOp := etcdop.Key("key/delete").DeleteIfExists(client).WithResultValidator(func(ok bool) error {
		_, _ = fmt.Fprintf(&log, "delete succeeded: %t\n", ok)
		if !ok {
			return errors.New("key/delete not found")
		}
		return nil
	})

	// Compose transaction, "key/put" must not exist, "key/delete" must exist
	txn := Txn(client)
	txn.Merge(putOp)
	txn.Merge(deleteOp)
	txn.Then(etcdop.Key("key/txn/succeeded").Put(client, "true"))
	txn.Else(etcdop.Key("key/txn/succeeded").Put(client, "false"))
	txn.AddProcessor(func(ctx context.Context, r *TxnResult[NoResult]) {
		if err := r.Err(); err != nil {
			fmt.Fprintf(&log, "txn succeeded: error: %s", strings.ReplaceAll(err.Error(), "\n", ";"))
		} else {
			fmt.Fprintf(&log, "txn succeeded: %t\n", r.Succeeded())
		}
	})

	// Validate low-level representation of the transaction
	if lowLevel, err := txn.Op(ctx); assert.NoError(t, err) {
		assert.Equal(t, etcd.OpTxn(
			// If
			[]etcd.Cmp{
				etcd.Compare(etcd.Version("key/put"), "=", 0),
				etcd.Compare(etcd.Version("key/delete"), "!=", 0),
			},
			// Then
			[]etcd.Op{
				etcd.OpPut("key/put", "value"),
				etcd.OpDelete("key/delete"),
				etcd.OpPut("key/txn/succeeded", "true"),
			},
			// Else
			[]etcd.Op{
				etcd.OpTxn(
					[]etcd.Cmp{etcd.Compare(etcd.Version("key/put"), "=", 0)}, // If
					[]etcd.Op{}, // Then
					[]etcd.Op{}, // Else
				),
				etcd.OpTxn(
					[]etcd.Cmp{etcd.Compare(etcd.Version("key/delete"), "!=", 0)}, // If
					[]etcd.Op{}, // Then
					[]etcd.Op{}, // Else
				),
				etcd.OpPut("key/txn/succeeded", "false"),
			},
		), lowLevel.Op)
	}

	// Test cases
	cases := []txnTestCase{
		// -------------------------------------------------------------------------------------------------------------
		{
			Name: "PutIfNotExists=fail | DeleteIfExists=fail",
			InitialEtcdState: `
<<<<<
key/put
-----
foo
>>>>>
`,
			ExpectedEtcdState: `
<<<<<
key/put
-----
foo
>>>>>

<<<<<
key/txn/succeeded
-----
false
>>>>>
`,
			ExpectedLogs: `
put succeeded: false
delete succeeded: false
txn succeeded: error: - key/put already exists;- key/delete not found
`,
			ExpectedTxnResult: txnResult{
				Succeeded: false,
				Error:     "- key/put already exists\n- key/delete not found",
				Results: []any{
					"ERROR: key/put already exists", // putOp
					"ERROR: key/delete not found",   // deleteOp
					NoResult{},                      // else put
				},
			},
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			Name:             "PutIfNotExists=success | DeleteIfExists=fail",
			InitialEtcdState: ``,
			ExpectedEtcdState: `
<<<<<
key/txn/succeeded
-----
false
>>>>>
`,
			ExpectedLogs: `
delete succeeded: false
txn succeeded: error: key/delete not found
`,
			ExpectedTxnResult: txnResult{
				Succeeded: false,
				Error:     "key/delete not found",
				Results: []any{
					NoResult{},                    // put op
					"ERROR: key/delete not found", // deleteOp
					NoResult{},                    // else put
				},
			},
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			Name: "PutIfNotExists=fail | DeleteIfExists=success",
			InitialEtcdState: `
<<<<<
key/put
-----
foo
>>>>>

<<<<<
key/delete
-----
bar
>>>>>
`,
			ExpectedEtcdState: `
<<<<<
key/put
-----
foo
>>>>>

<<<<<
key/delete
-----
bar
>>>>>

<<<<<
key/txn/succeeded
-----
false
>>>>>
`,
			ExpectedLogs: `
put succeeded: false
txn succeeded: error: key/put already exists
`,
			ExpectedTxnResult: txnResult{
				Succeeded: false,
				Error:     "key/put already exists",
				Results: []any{
					"ERROR: key/put already exists", // put op
					NoResult{},                      // deleteOp
					NoResult{},                      // else put
				},
			},
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			Name: "PutIfNotExists=success | DeleteIfExists=success",
			InitialEtcdState: `
<<<<<
key/delete
-----
bar
>>>>>
`,
			ExpectedEtcdState: `
<<<<<
key/put
-----
value
>>>>>

<<<<<
key/txn/succeeded
-----
true
>>>>>
`,
			ExpectedLogs: `
put succeeded: true
delete succeeded: true
txn succeeded: true
`,
			ExpectedTxnResult: txnResult{
				Succeeded: true,
				Results: []any{
					true,       // put op
					true,       // delete op
					NoResult{}, // then put
				},
			},
		},
		// -----------------------------------------------------------------------------------------------------------------
	}

	// Run test-cases
	for _, tc := range cases {
		tc.Run(t, ctx, client, &log, txn)
	}
}

func TestTxnOp_Merge_Complex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Helpers to test processor callbacks
	var log strings.Builder
	onNoResult, onGetResult := newLogHelpers(&log)

	// Define transaction
	txn := Txn(client).
		If(etcd.Compare(etcd.Value("txn/if"), "=", "ok")).
		Then(etcdop.Key("txn/then/put").Put(client, "ok").WithOnResult(onNoResult("txn then put"))).
		Then(etcdop.Key("txn/then/get").Get(client).WithOnResult(onGetResult("txn then get"))).
		Else(etcdop.Key("txn/else/put").Put(client, "ok").WithOnResult(onNoResult("txn else put"))).
		Else(etcdop.Key("txn/else/get").Get(client).WithOnResult(onGetResult("txn else get"))).
		OnResult(func(r *TxnResult[NoResult]) {
			_, _ = fmt.Fprintf(&log, "txn succeeded: %t\n", r.Succeeded())
		}).
		Merge(
			Txn(client).
				If(etcd.Compare(etcd.Value("txn1/if"), "=", "ok")).
				Then(etcdop.Key("txn1/then/put").Put(client, "ok").WithOnResult(onNoResult("txn1 then put"))).
				Then(etcdop.Key("txn1/then/get").Get(client).WithOnResult(onGetResult("txn1 then get"))).
				Else(etcdop.Key("txn1/else/put").Put(client, "ok").WithOnResult(onNoResult("txn1 else put"))).
				Else(etcdop.Key("txn1/else/get").Get(client).WithOnResult(onGetResult("txn1 else get"))).
				OnResult(func(r *TxnResult[NoResult]) {
					_, _ = fmt.Fprintf(&log, "txn1 succeeded: %t %v\n", r.Succeeded(), simplifyTxnResult(r).Results)
				}),
		).
		Merge(
			Txn(client).
				If(etcd.Compare(etcd.Value("txn2/if"), "=", "ok")).
				Then(etcdop.Key("txn2/then/put").Put(client, "ok").WithOnResult(onNoResult("txn2 then put"))).
				Then(etcdop.Key("txn2/then/get").Get(client).WithOnResult(onGetResult("txn2 then get"))).
				Else(etcdop.Key("txn2/else/put").Put(client, "ok").WithOnResult(onNoResult("txn2 else put"))).
				Else(etcdop.Key("txn2/else/get").Get(client).WithOnResult(onGetResult("txn2 else get"))).
				OnResult(func(r *TxnResult[NoResult]) {
					_, _ = fmt.Fprintf(&log, "txn2 succeeded: %t %v\n", r.Succeeded(), simplifyTxnResult(r).Results)
				}),
		)

	// Validate low-level representation of the transaction
	if lowLevel, err := txn.Op(ctx); assert.NoError(t, err) {
		assert.Equal(t, etcd.OpTxn(
			// If
			[]etcd.Cmp{
				etcd.Compare(etcd.Value("txn/if"), "=", "ok"),
				etcd.Compare(etcd.Value("txn1/if"), "=", "ok"),
				etcd.Compare(etcd.Value("txn2/if"), "=", "ok"),
			},
			// Then
			[]etcd.Op{
				etcd.OpPut("txn/then/put", "ok"),
				etcd.OpGet("txn/then/get"),
				etcd.OpPut("txn1/then/put", "ok"),
				etcd.OpGet("txn1/then/get"),
				etcd.OpPut("txn2/then/put", "ok"),
				etcd.OpGet("txn2/then/get"),
			},
			// Else
			[]etcd.Op{
				etcd.OpPut("txn/else/put", "ok"),
				etcd.OpGet("txn/else/get"),
				etcd.OpTxn(
					// If
					[]etcd.Cmp{
						etcd.Compare(etcd.Value("txn1/if"), "=", "ok"),
					},
					// Then
					[]etcd.Op{},
					// Else
					[]etcd.Op{
						etcd.OpPut("txn1/else/put", "ok"),
						etcd.OpGet("txn1/else/get"),
					},
				),
				etcd.OpTxn(
					// If
					[]etcd.Cmp{
						etcd.Compare(etcd.Value("txn2/if"), "=", "ok"),
					},
					// Then
					[]etcd.Op{},
					// Else
					[]etcd.Op{
						etcd.OpPut("txn2/else/put", "ok"),
						etcd.OpGet("txn2/else/get"),
					},
				),
			},
		), lowLevel.Op)
	}

	// Test cases
	cases := []txnTestCase{
		// -------------------------------------------------------------------------------------------------------------
		{
			Name: "Succeeded: false, Else branch",
			InitialEtcdState: `
<<<<<
txn1/else/get
-----
value
>>>>>
`,
			ExpectedEtcdState: `
<<<<<
txn1/else/get
-----
value
>>>>>

<<<<<
txn/else/put
-----
ok
>>>>>

<<<<<
txn1/else/put
-----
ok
>>>>>

<<<<<
txn2/else/put
-----
ok
>>>>>
`,
			ExpectedLogs: `
txn else put
txn else get <nil>
txn1 else put
txn1 else get value
txn1 succeeded: false [{} key:"txn1/else/get" value:"value" ]
txn2 else put
txn2 else get <nil>
txn2 succeeded: false [{} <nil>]
txn succeeded: false
`,
			ExpectedTxnResult: txnResult{
				Succeeded: false,
				Results: []any{
					// Results from the Else branch
					NoResult{},       // txn: put
					(*KeyValue)(nil), // txn: get
					// txn1
					txnResult{
						Succeeded: false, // false -> Else branch
						// Else branch results:
						Results: []any{
							// txn1/else/put
							NoResult{},
							// txn1/else/get
							&KeyValue{
								Key:   []byte("txn1/else/get"),
								Value: []byte("value"),
							},
						},
					},
					// txn2
					txnResult{
						Succeeded: false, // false -> Else branch
						// Else branch results:
						Results: []any{
							// txn2/else/put
							NoResult{},
							// txn2/else/get
							(*KeyValue)(nil),
						},
					},
				},
			},
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			Name: "Succeeded: partial, Else branch",
			InitialEtcdState: `
<<<<<
txn1/if
-----
ok
>>>>>
`,
			ExpectedEtcdState: `
<<<<<
txn1/if
-----
ok
>>>>>

<<<<<
txn/else/put
-----
ok
>>>>>

<<<<<
txn2/else/put
-----
ok
>>>>>
`,
			ExpectedLogs: `
txn else put
txn else get <nil>
txn2 else put
txn2 else get <nil>
txn2 succeeded: false [{} <nil>]
txn succeeded: false
`,
			ExpectedTxnResult: txnResult{
				Succeeded: false,
				Results: []any{
					// Results from the Else branch
					NoResult{},       // txn: put
					(*KeyValue)(nil), // txn: get
					// txn1
					NoResult{}, // skipped, conditions for execution of txn1 are met, but txn2 has blocked the entire txn
					// txn2
					txnResult{
						Succeeded: false, // false -> Else branch
						Results: []any{
							// txn2/else/put
							NoResult{},
							// txn2/else/get
							(*KeyValue)(nil),
						},
					},
				},
			},
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			Name: "Succeeded: true, Then branch",
			InitialEtcdState: `
<<<<<
txn/if
-----
ok
>>>>>

<<<<<
txn1/if
-----
ok
>>>>>

<<<<<
txn2/if
-----
ok
>>>>>

<<<<<
txn2/then/get
-----
value
>>>>>
`,
			ExpectedEtcdState: `
<<<<<
txn/if
-----
ok
>>>>>

<<<<<
txn1/if
-----
ok
>>>>>

<<<<<
txn2/if
-----
ok
>>>>>

<<<<<
txn2/then/get
-----
value
>>>>>

<<<<<
txn/then/put
-----
ok
>>>>>

<<<<<
txn1/then/put
-----
ok
>>>>>

<<<<<
txn2/then/put
-----
ok
>>>>>
`,
			ExpectedLogs: `
txn then put
txn then get <nil>
txn1 then put
txn1 then get <nil>
txn1 succeeded: true [{} <nil>]
txn2 then put
txn2 then get value
txn2 succeeded: true [{} key:"txn2/then/get" value:"value" ]
txn succeeded: true
`,
			ExpectedTxnResult: txnResult{
				Succeeded: true,
				Results: []any{
					// Results from the Then branch
					NoResult{},       // txn: put
					(*KeyValue)(nil), // txn: get
					txnResult{
						Succeeded: true, // true -> If branch
						Results: []any{
							NoResult{},       // txn1: put
							(*KeyValue)(nil), // txn1: get
						},
					},
					txnResult{
						Succeeded: true, // true -> If branch
						Results: []any{
							NoResult{}, // txn2: put
							&KeyValue{Key: []byte("txn2/then/get"), Value: []byte("value")}, // txn2: get
						},
					},
				},
			},
		},
		// -------------------------------------------------------------------------------------------------------------
	}

	// Run test-cases
	for _, tc := range cases {
		tc.Run(t, ctx, client, &log, txn)
	}
}

// txnResult is simplified version of the TxnResult, without dynamic values, for easier comparison in tests.
type txnResult struct {
	Succeeded bool
	Error     string
	Results   []any
}

func newLogHelpers(log *strings.Builder) (func(msg string) func(r NoResult), func(msg string) func(r *KeyValue)) {
	onNoResult := func(msg string) func(r NoResult) {
		return func(r NoResult) {
			log.WriteString(msg)
			log.WriteString("\n")
		}
	}
	onGetResult := func(msg string) func(r *KeyValue) {
		return func(r *KeyValue) {
			log.WriteString(msg)
			log.WriteString(" ")
			if r == nil {
				log.WriteString("<nil>")
			} else {
				log.WriteString(string(r.Value))
			}
			log.WriteString("\n")
		}
	}
	return onNoResult, onGetResult
}

func simplifyTxnResult(v *TxnResult[NoResult]) (out txnResult) {
	out.Succeeded = v.Succeeded()

	if v.Err() != nil {
		out.Error = v.Err().Error()
	}

	for _, r := range v.SubResults() {
		switch r := r.(type) {
		case *KeyValue:
			if r != nil {
				out.Results = append(out.Results, &KeyValue{Key: r.Key, Value: r.Value})
			} else {
				out.Results = append(out.Results, (*KeyValue)(nil))
			}
		case []*KeyValue:
			var sub []any
			for _, item := range r {
				sub = append(sub, &KeyValue{Key: item.Key, Value: item.Value})
			}
			out.Results = append(out.Results, sub)
		case *TxnResult[NoResult]:
			out.Results = append(out.Results, simplifyTxnResult(r))
		case error:
			out.Results = append(out.Results, fmt.Sprintf("ERROR: %s", r.Error()))
		default:
			out.Results = append(out.Results, r)
		}
	}
	return out
}
