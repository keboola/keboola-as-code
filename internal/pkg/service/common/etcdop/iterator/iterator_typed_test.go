package iterator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

type obj struct {
	Value string `json:"val"`
}

type resultT struct {
	key   string
	value obj
}

type testCaseT struct {
	name             string
	inTxn            bool
	kvCount          int
	pageSize         int
	options          []iterator.Option
	expectedCountAll int
	expectedResults  []resultT
	expectedLogs     string
}

func TestIteratorT(t *testing.T) {
	t.Parallel()

	cases := []testCaseT{
		{
			name:             "txn: empty",
			inTxn:            true,
			kvCount:          0,
			pageSize:         3,
			expectedCountAll: 0,
			expectedResults:  []resultT{},
			expectedLogs: `
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET ["some/prefix/", "some/prefix0")
✔️  TXN | succeeded: true | rev: %d
`,
		},
		{
			name:             "txn: count 1, under page size",
			inTxn:            true,
			kvCount:          1,
			pageSize:         3,
			expectedCountAll: 1,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{Value: "bar001"}},
			},
			expectedLogs: `
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET ["some/prefix/", "some/prefix0")
✔️  TXN | succeeded: true | rev: %d
`,
		},
		{
			name:             "txn: two on the second page",
			inTxn:            true,
			kvCount:          5,
			pageSize:         3,
			expectedCountAll: 5,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{Value: "bar001"}},
				{key: "some/prefix/foo002", value: obj{Value: "bar002"}},
				{key: "some/prefix/foo003", value: obj{Value: "bar003"}},
				{key: "some/prefix/foo004", value: obj{Value: "bar004"}},
				{key: "some/prefix/foo005", value: obj{Value: "bar005"}},
			},
			expectedLogs: `
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET ["some/prefix/", "some/prefix0")
✔️  TXN | succeeded: true | rev: %d
➡️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 2
`,
		},
		{
			name:             "empty",
			kvCount:          0,
			pageSize:         3,
			expectedCountAll: 0,
			expectedResults:  nil, // empty slice
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 0
`,
		},
		{
			name:             "count 1, under page size",
			kvCount:          1,
			pageSize:         3,
			expectedCountAll: 1,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 1
`,
		},
		{
			name:             "count 1, equal to page size",
			kvCount:          1,
			pageSize:         1,
			expectedCountAll: 1,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 1
`,
		},
		{
			name:             "count 2, under page size",
			kvCount:          2,
			pageSize:         3,
			expectedCountAll: 2,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 2
`,
		},
		{
			name:             "count 3, equal to page size",
			kvCount:          3,
			pageSize:         3,
			expectedCountAll: 3,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 3
`,
		},
		{
			name:             "one on the second page",
			kvCount:          4,
			pageSize:         3,
			expectedCountAll: 4,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 4 | loaded: 3
➡️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 1
`,
		},
		{
			name:             "two on the second page",
			kvCount:          5,
			pageSize:         3,
			expectedCountAll: 5,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo005", value: obj{"bar005"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | loaded: 3
➡️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 2
`,
		},
		{
			name:             "WithFromSameRev = false",
			kvCount:          5,
			pageSize:         1,
			options:          []iterator.Option{iterator.WithFromSameRev(false)},
			expectedCountAll: 5,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo005", value: obj{"bar005"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | loaded: 1
➡️  GET ["some/prefix/foo002", "some/prefix0")
✔️  GET ["some/prefix/foo002", "some/prefix0") | rev: %d | count: 4 | loaded: 1
➡️  GET ["some/prefix/foo003", "some/prefix0")
✔️  GET ["some/prefix/foo003", "some/prefix0") | rev: %d | count: 3 | loaded: 1
➡️  GET ["some/prefix/foo004", "some/prefix0")
✔️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 2 | loaded: 1
➡️  GET ["some/prefix/foo005", "some/prefix0")
✔️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | count: 1
`,
		},
		{
			name:             "limit=3",
			kvCount:          5,
			pageSize:         3,
			options:          []iterator.Option{iterator.WithLimit(3)},
			expectedCountAll: 5,
			expectedResults: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | loaded: 3
`,
		},
		{
			name:             "sort=SortDescend",
			kvCount:          5,
			pageSize:         3,
			options:          []iterator.Option{iterator.WithSort(etcd.SortDescend)},
			expectedCountAll: 5,
			expectedResults: []resultT{
				{key: "some/prefix/foo005", value: obj{"bar005"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo001", value: obj{"bar001"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | loaded: 3
➡️  GET ["some/prefix/", "some/prefix/foo003") | rev: %d | serializable
✔️  GET ["some/prefix/", "some/prefix/foo003") | rev: %d | count: 2
`,
		},
		{
			name:             "sort=SortDescend + limit=3",
			kvCount:          5,
			pageSize:         2,
			options:          []iterator.Option{iterator.WithSort(etcd.SortDescend), iterator.WithLimit(3)},
			expectedCountAll: 5,
			expectedResults: []resultT{
				{key: "some/prefix/foo005", value: obj{"bar005"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | loaded: 2
➡️  GET ["some/prefix/", "some/prefix/foo004") | rev: %d | serializable
✔️  GET ["some/prefix/", "some/prefix/foo004") | rev: %d | count: 3 | loaded: 2
`,
		},
		{
			name:             "startOffset(excluded), pageSize=1",
			kvCount:          5,
			pageSize:         1,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", false)},
			expectedCountAll: 3,
			expectedResults: []resultT{
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo005", value: obj{"bar005"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/foo003", "some/prefix0")
✔️  GET ["some/prefix/foo003", "some/prefix0") | rev: %d | count: 3 | loaded: 1
➡️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 2 | loaded: 1
➡️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | count: 1
`,
		},
		{
			name:             "startOffset(excluded), pageSize=1, txn",
			kvCount:          5,
			pageSize:         1,
			inTxn:            true,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", false)},
			expectedCountAll: 3,
			expectedResults: []resultT{
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo005", value: obj{"bar005"}},
			},
			expectedLogs: `
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET ["some/prefix/foo003", "some/prefix0")
✔️  TXN | succeeded: true | rev: %d
➡️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 2 | loaded: 1
➡️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | count: 1
`,
		},
		{
			name:             "startOffset(excluded), endOffset, pageSize=1",
			kvCount:          5,
			pageSize:         1,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", false), iterator.WithEndOffset("foo005", false)},
			expectedCountAll: 2,
			expectedResults: []resultT{
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/foo003", "some/prefix/foo005")
✔️  GET ["some/prefix/foo003", "some/prefix/foo005") | rev: %d | count: 2 | loaded: 1
➡️  GET ["some/prefix/foo004", "some/prefix/foo005") | rev: %d | serializable
✔️  GET ["some/prefix/foo004", "some/prefix/foo005") | rev: %d | count: 1
`,
		},
		{
			name:             "startOffset(excluded), pageSize=2",
			kvCount:          5,
			pageSize:         2,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", false)},
			expectedCountAll: 3,
			expectedResults: []resultT{
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo005", value: obj{"bar005"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/foo003", "some/prefix0")
✔️  GET ["some/prefix/foo003", "some/prefix0") | rev: %d | count: 3 | loaded: 2
➡️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | count: 1
`,
		},
		{
			name:             "endOffset(excluded), sort=SortDescend, pageSize=1",
			kvCount:          5,
			pageSize:         1,
			options:          []iterator.Option{iterator.WithEndOffset("foo004", false), iterator.WithSort(etcd.SortDescend)},
			expectedCountAll: 3,
			expectedResults: []resultT{
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo001", value: obj{"bar001"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix/foo004")
✔️  GET ["some/prefix/", "some/prefix/foo004") | rev: %d | count: 3 | loaded: 1
➡️  GET ["some/prefix/", "some/prefix/foo003") | rev: %d | serializable
✔️  GET ["some/prefix/", "some/prefix/foo003") | rev: %d | count: 2 | loaded: 1
➡️  GET ["some/prefix/", "some/prefix/foo002") | rev: %d | serializable
✔️  GET ["some/prefix/", "some/prefix/foo002") | rev: %d | count: 1
`,
		},
		{
			name:             "startOffset(excluded), endOffset, sort=SortDescend, pageSize=1",
			kvCount:          5,
			pageSize:         1,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", false), iterator.WithEndOffset("foo005", false), iterator.WithSort(etcd.SortDescend)},
			expectedCountAll: 2,
			expectedResults: []resultT{
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/foo003", "some/prefix/foo005")
✔️  GET ["some/prefix/foo003", "some/prefix/foo005") | rev: %d | count: 2 | loaded: 1
➡️  GET ["some/prefix/foo003", "some/prefix/foo004") | rev: %d | serializable
✔️  GET ["some/prefix/foo003", "some/prefix/foo004") | rev: %d | count: 1
`,
		},
		{
			name:             "startOffset(excluded), sort=SortDescend, pageSize=2",
			kvCount:          5,
			pageSize:         2,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", false), iterator.WithSort(etcd.SortDescend)},
			expectedCountAll: 3,
			expectedResults: []resultT{
				{key: "some/prefix/foo005", value: obj{"bar005"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/foo003", "some/prefix0")
✔️  GET ["some/prefix/foo003", "some/prefix0") | rev: %d | count: 3 | loaded: 2
➡️  GET ["some/prefix/foo003", "some/prefix/foo004") | rev: %d | serializable
✔️  GET ["some/prefix/foo003", "some/prefix/foo004") | rev: %d | count: 1
`,
		},
		{
			name:             "startOffset(excluded), sort=SortDescend, pageSize=2, txn",
			kvCount:          5,
			pageSize:         2,
			inTxn:            true,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", false), iterator.WithSort(etcd.SortDescend)},
			expectedCountAll: 3,
			expectedResults: []resultT{
				{key: "some/prefix/foo005", value: obj{"bar005"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
			},
			expectedLogs: `
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET ["some/prefix/foo003", "some/prefix0")
✔️  TXN | succeeded: true | rev: %d
➡️  GET ["some/prefix/foo003", "some/prefix/foo004") | rev: %d | serializable
✔️  GET ["some/prefix/foo003", "some/prefix/foo004") | rev: %d | count: 1
`,
		},
		{
			name:             "startOffset(included), endOffset(included), sort=SortAscend, pageSize=1",
			kvCount:          5,
			pageSize:         1,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", true), iterator.WithEndOffset("foo004", true), iterator.WithSort(etcd.SortAscend)},
			expectedCountAll: 3,
			expectedResults: []resultT{
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/foo002", "some/prefix/foo005")
✔️  GET ["some/prefix/foo002", "some/prefix/foo005") | rev: %d | count: 3 | loaded: 1
➡️  GET ["some/prefix/foo003", "some/prefix/foo005") | rev: %d | serializable
✔️  GET ["some/prefix/foo003", "some/prefix/foo005") | rev: %d | count: 2 | loaded: 1
➡️  GET ["some/prefix/foo004", "some/prefix/foo005") | rev: %d | serializable
✔️  GET ["some/prefix/foo004", "some/prefix/foo005") | rev: %d | count: 1
`,
		},
		{
			name:             "startOffset(included), endOffset(included), sort=SortDescend, pageSize=1",
			kvCount:          5,
			pageSize:         1,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", true), iterator.WithEndOffset("foo004", true), iterator.WithSort(etcd.SortDescend)},
			expectedCountAll: 3,
			expectedResults: []resultT{
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
			},
			expectedLogs: `
➡️  GET ["some/prefix/foo002", "some/prefix/foo005")
✔️  GET ["some/prefix/foo002", "some/prefix/foo005") | rev: %d | count: 3 | loaded: 1
➡️  GET ["some/prefix/foo002", "some/prefix/foo004") | rev: %d | serializable
✔️  GET ["some/prefix/foo002", "some/prefix/foo004") | rev: %d | count: 2 | loaded: 1
➡️  GET ["some/prefix/foo002", "some/prefix/foo003") | rev: %d | serializable
✔️  GET ["some/prefix/foo002", "some/prefix/foo003") | rev: %d | count: 1
`,
		},
	}

	for _, tc := range cases {
		var logs strings.Builder
		ctx := t.Context()
		client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
		loggerOpts := []etcdlogger.Option{etcdlogger.WithoutRequestNumber(), etcdlogger.WithNewLineSeparator(false), etcdlogger.WithoutDuration()}
		client.KV = etcdlogger.KVLogWrapper(client.KV, &logs, loggerOpts...)
		prefix := generateKVsT(t, tc.kvCount, ctx, client)
		ops := append([]iterator.Option{iterator.WithPageSize(tc.pageSize)}, tc.options...)

		// Test transaction
		if tc.inTxn {
			// Loading of the first page is part of the transaction.
			// Next pages are loaded with the same revision.
			logs.Reset()
			actual := make([]resultT, 0)
			txn := op.Txn(client).Then(prefix.GetAll(client, ops...).ForEachKV(func(kv *op.KeyValueT[obj], header *iterator.Header) error {
				assert.NotNil(t, header)
				actual = append(actual, resultT{key: kv.Key(), value: kv.Value})
				return nil
			}))
			require.NoError(t, txn.Do(ctx).Err())
			assert.Equal(t, tc.expectedResults, actual, tc.name)
			wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)
			continue
		}

		// Test iteration methods
		logs.Reset()
		actual := iterateAllT(t, ctx, prefix.GetAll(client, ops...))
		assert.Equal(t, tc.expectedResults, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test All method
		logs.Reset()
		actualValues, err := prefix.GetAll(client, ops...).Do(ctx).All()
		require.NoError(t, err)
		var expectedValues []obj
		for _, v := range tc.expectedResults {
			expectedValues = append(expectedValues, v.value)
		}
		assert.Equal(t, expectedValues, actualValues, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test AllKVs method
		logs.Reset()
		actualKvs, err := prefix.GetAll(client, ops...).Do(ctx).AllKVs()
		require.NoError(t, err)
		actual = nil
		for _, kv := range actualKvs {
			actual = append(actual, resultT{key: string(kv.Kv.Key), value: kv.Value})
		}
		assert.Equal(t, tc.expectedResults, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test ForEachKV method
		logs.Reset()
		itr := prefix.GetAll(client, ops...).Do(ctx)
		actual = nil
		require.NoError(t, itr.ForEachKV(func(kv *op.KeyValueT[obj], header *iterator.Header) error {
			assert.NotNil(t, header)
			actual = append(actual, resultT{key: string(kv.Kv.Key), value: kv.Value})
			return nil
		}))
		assert.Equal(t, tc.expectedResults, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test ForEachValue method
		logs.Reset()
		itr = prefix.GetAll(client, ops...).Do(ctx)
		values := make([]obj, 0)
		require.NoError(t, itr.ForEachValue(func(value obj, header *iterator.Header) error {
			assert.NotNil(t, header)
			values = append(values, value)
			return nil
		}))
		assert.Len(t, values, len(tc.expectedResults))
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Next method is stable
		assert.False(t, itr.Next(), tc.name)
		assert.False(t, itr.Next(), tc.name)

		// Test CountAll method
		count, err := prefix.GetAll(client, ops...).CountAll().Do(ctx).ResultOrErr()
		assert.Equal(t, tc.expectedCountAll, int(count), tc.name)
		require.NoError(t, err, tc.name)
	}
}

func TestIteratorT_Revision(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	serialization := serde.NewJSON(serde.NoValidation)
	prefix := etcdop.NewTypedPrefix[obj]("some/prefix", serialization)

	// There are 3 keys
	require.NoError(t, prefix.Key("foo001").Put(client, obj{Value: "bar001"}).Do(ctx).Err())
	require.NoError(t, prefix.Key("foo002").Put(client, obj{Value: "bar002"}).Do(ctx).Err())
	require.NoError(t, prefix.Key("foo003").Put(client, obj{Value: "bar003"}).Do(ctx).Err())

	// Get current revision
	r, err := prefix.Key("foo003").GetKV(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	revision := r.Kv.ModRevision

	// Add more keys
	require.NoError(t, prefix.Key("foo004").Put(client, obj{Value: "bar004"}).Do(ctx).Err())
	require.NoError(t, prefix.Key("foo005").Put(client, obj{Value: "bar005"}).Do(ctx).Err())

	// Get all WithRev
	var actual []resultT
	require.NoError(
		t,
		prefix.
			GetAll(client, iterator.WithRev(revision)).Do(ctx).
			ForEachKV(func(kv *op.KeyValueT[obj], _ *iterator.Header) error {
				actual = append(actual, resultT{key: string(kv.Kv.Key), value: kv.Value})
				return nil
			}),
	)

	// The iterator only sees the values in the revision
	assert.Equal(t, []resultT{
		{key: "some/prefix/foo001", value: obj{"bar001"}},
		{key: "some/prefix/foo002", value: obj{"bar002"}},
		{key: "some/prefix/foo003", value: obj{"bar003"}},
	}, actual)
}

func TestIteratorT_Value_UsedIncorrectly(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	prefix := generateKVsT(t, 3, ctx, client)

	it := prefix.GetAll(client).Do(ctx)
	assert.PanicsWithError(t, "unexpected Value() call: Next() must be called first", func() {
		it.Value()
	})
}

func TestIteratorT_ForEach(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	out := ioutil.NewAtomicWriter()
	prefix := generateKVsT(t, 5, ctx, client)
	tracker := op.NewTracker(client)

	// Define op
	getAllOp := prefix.
		GetAll(tracker, iterator.WithPageSize(2)).
		ForEach(func(value obj, header *iterator.Header) error {
			_, _ = fmt.Fprintf(out, "value: %s\n", value.Value)
			return nil
		}).
		AndOnFirstPage(func(response *etcd.GetResponse) error {
			_, _ = out.WriteString("first page\n")
			return nil
		}).
		AndOnPage(func(pageIndex int, response *etcd.GetResponse) error {
			_, _ = fmt.Fprintf(out, "page index: %d\n", pageIndex)
			return nil
		})

	// Run op
	require.NoError(t, getAllOp.Do(ctx).Err())

	// Clear loaded KVs before assert
	operations := tracker.Operations()
	for i := range operations {
		operations[i].KVs = nil
	}

	// All requests can be tracked by the TrackerKV
	assert.Equal(t, []op.TrackedOp{
		{Type: op.GetOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 5},
		{Type: op.GetOp, Key: []byte("some/prefix/foo003"), RangeEnd: []byte("some/prefix0"), Count: 3},
		{Type: op.GetOp, Key: []byte("some/prefix/foo005"), RangeEnd: []byte("some/prefix0"), Count: 1},
	}, operations)

	// All values have been received
	assert.Equal(t, strings.TrimSpace(`
first page
page index: 0
value: bar001
value: bar002
page index: 1
value: bar003
value: bar004
page index: 2
value: bar005
`), strings.TrimSpace(out.String()))
}

func TestIteratorT_WithAllTo(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	prefix := generateKVsT(t, 5, ctx, client)

	var target []obj
	require.NoError(t, prefix.GetAll(client).WithAllTo(&target).Do(ctx).Err())
	assert.Equal(t, []obj{
		{Value: "bar001"},
		{Value: "bar002"},
		{Value: "bar003"},
		{Value: "bar004"},
		{Value: "bar005"},
	}, target)
}

func TestIteratorT_WithAllKVsTo(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	prefix := generateKVsT(t, 5, ctx, client)

	var target op.KeyValuesT[obj]
	require.NoError(t, prefix.GetAll(client).WithAllKVsTo(&target).Do(ctx).Err())
	assert.Len(t, target, 5)
}

func TestIteratorT_AllTo(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	prefix := generateKVsT(t, 5, ctx, client)

	var target []obj
	require.NoError(t, prefix.GetAll(client).Do(ctx).AllTo(&target))
	assert.Equal(t, []obj{
		{Value: "bar001"},
		{Value: "bar002"},
		{Value: "bar003"},
		{Value: "bar004"},
		{Value: "bar005"},
	}, target)
}

func TestIteratorT_AllKVsTo(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	prefix := generateKVsT(t, 5, ctx, client)

	var target op.KeyValuesT[obj]
	require.NoError(t, prefix.GetAll(client).Do(ctx).AllKVsTo(&target))
	assert.Len(t, target, 5)
}

func TestIteratorT_All(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	prefix := generateKVsT(t, 5, ctx, client)

	target, err := prefix.GetAll(client).Do(ctx).All()
	require.NoError(t, err)
	assert.Equal(t, []obj{
		{Value: "bar001"},
		{Value: "bar002"},
		{Value: "bar003"},
		{Value: "bar004"},
		{Value: "bar005"},
	}, target)
}

func TestIteratorT_AllKVs(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	prefix := generateKVsT(t, 5, ctx, client)

	target, err := prefix.GetAll(client).Do(ctx).AllKVs()
	require.NoError(t, err)
	assert.Len(t, target, 5)
}

func TestIteratorT_WithKVFilter(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	prefix := generateKVsT(t, 5, ctx, client)

	assert.Equal(
		t,
		[]resultT{
			{
				key: "some/prefix/foo003",
				value: obj{
					Value: "bar003",
				},
			},
		},
		iterateAllT(t, ctx, prefix.
			GetAll(client, iterator.WithPageSize(2)).
			WithKVFilter(
				func(kv *op.KeyValueT[obj]) bool {
					return strings.HasSuffix(kv.Value.Value, "003") // <<<<<<<<<<<<<
				},
			),
		),
	)
}

func TestIteratorT_WithFilter(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	prefix := generateKVsT(t, 5, ctx, client)

	assert.Equal(
		t,
		[]resultT{
			{
				key: "some/prefix/foo003",
				value: obj{
					Value: "bar003",
				},
			},
		},
		iterateAllT(t, ctx, prefix.
			GetAll(client, iterator.WithPageSize(2)).
			WithFilter(
				func(v obj) bool {
					return strings.HasSuffix(v.Value, "003") // <<<<<<<<<<<<<
				},
			),
		),
	)
}

func iterateAllT(t *testing.T, ctx context.Context, def iterator.DefinitionT[obj]) (actual []resultT) {
	t.Helper()
	it := def.Do(ctx)
	for it.Next() {
		kv := it.Value()
		actual = append(actual, resultT{key: string(kv.Kv.Key), value: kv.Value})
	}
	require.NoError(t, it.Err())
	return actual
}

func generateKVsT(t *testing.T, count int, ctx context.Context, client *etcd.Client) etcdop.PrefixT[obj] {
	t.Helper()

	// There are some keys before the prefix
	require.NoError(t, etcdop.Key("some/abc").Put(client, "foo").Do(ctx).Err())

	// Create keys in the iterated prefix
	serialization := serde.NewJSON(serde.NoValidation)
	prefix := etcdop.NewTypedPrefix[obj]("some/prefix", serialization)
	for i := 1; i <= count; i++ {
		key := prefix.Key(fmt.Sprintf("foo%03d", i))
		val := obj{fmt.Sprintf("bar%03d", i)}
		require.NoError(t, key.Put(client, val).Do(ctx).Err())
	}

	// There are some keys after the prefix
	require.NoError(t, etcdop.Key("some/xyz").Put(client, "foo").Do(ctx).Err())

	return prefix
}
