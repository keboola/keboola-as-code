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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

type testCase struct {
	name             string
	inTxn            bool
	kvCount          int
	pageSize         int
	options          []iterator.Option
	expectedCountAll int
	expectedResults  []result
	expectedLogs     string
}

type result struct {
	key   string
	value string
}

func TestIterator(t *testing.T) {
	t.Parallel()

	cases := []testCase{
		{
			name:             "txn: empty",
			inTxn:            true,
			kvCount:          0,
			pageSize:         3,
			expectedCountAll: 0,
			expectedResults:  []result{},
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
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
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
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo005", value: "bar005"},
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
			expectedResults:  []result{},
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
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
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
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
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
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
				{key: "some/prefix/foo002", value: "bar002"},
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
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo003", value: "bar003"},
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
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
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
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo005", value: "bar005"},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | loaded: 3
➡️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 2
`,
		},
		{
			name:             "pageSize=1",
			kvCount:          5,
			pageSize:         1,
			expectedCountAll: 5,
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo005", value: "bar005"},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | loaded: 1
➡️  GET ["some/prefix/foo002", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo002", "some/prefix0") | rev: %d | count: 4 | loaded: 1
➡️  GET ["some/prefix/foo003", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo003", "some/prefix0") | rev: %d | count: 3 | loaded: 1
➡️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 2 | loaded: 1
➡️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | serializable
✔️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | count: 1
`,
		},
		{
			name:             "WithFromSameRev = false",
			kvCount:          5,
			pageSize:         1,
			options:          []iterator.Option{iterator.WithFromSameRev(false)},
			expectedCountAll: 5,
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo005", value: "bar005"},
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
			expectedResults: []result{
				{key: "some/prefix/foo001", value: "bar001"},
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo003", value: "bar003"},
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
			expectedResults: []result{
				{key: "some/prefix/foo005", value: "bar005"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo001", value: "bar001"},
			},
			expectedLogs: `
➡️  GET ["some/prefix/", "some/prefix0")
✔️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | loaded: 3
➡️  GET ["some/prefix/", "some/prefix/foo003") | rev: %d | serializable
✔️  GET ["some/prefix/", "some/prefix/foo003") | rev: %d | count: 2
`,
		},
		{
			name:             "sort=SortDescend, limit=3",
			kvCount:          5,
			pageSize:         2,
			options:          []iterator.Option{iterator.WithSort(etcd.SortDescend), iterator.WithLimit(3)},
			expectedCountAll: 5,
			expectedResults: []result{
				{key: "some/prefix/foo005", value: "bar005"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo003", value: "bar003"},
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
			expectedResults: []result{
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo005", value: "bar005"},
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
			expectedResults: []result{
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo005", value: "bar005"},
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
			expectedResults: []result{
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
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
			expectedResults: []result{
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo005", value: "bar005"},
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
			expectedResults: []result{
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo001", value: "bar001"},
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
			name:             "startOffset(excluded), endOffset(excluded), sort=SortDescend, pageSize=1",
			kvCount:          5,
			pageSize:         1,
			options:          []iterator.Option{iterator.WithStartOffset("foo002", false), iterator.WithEndOffset("foo005", false), iterator.WithSort(etcd.SortDescend)},
			expectedCountAll: 2,
			expectedResults: []result{
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo003", value: "bar003"},
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
			expectedResults: []result{
				{key: "some/prefix/foo005", value: "bar005"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo003", value: "bar003"},
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
			expectedResults: []result{
				{key: "some/prefix/foo005", value: "bar005"},
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo003", value: "bar003"},
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
			expectedResults: []result{
				{key: "some/prefix/foo002", value: "bar002"},
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo004", value: "bar004"},
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
			expectedResults: []result{
				{key: "some/prefix/foo004", value: "bar004"},
				{key: "some/prefix/foo003", value: "bar003"},
				{key: "some/prefix/foo002", value: "bar002"},
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
		ctx := context.Background()
		client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
		loggerOpts := []etcdlogger.Option{etcdlogger.WithoutRequestNumber(), etcdlogger.WithNewLineSeparator(false), etcdlogger.WithoutDuration()}
		client.KV = etcdlogger.KVLogWrapper(client.KV, &logs, loggerOpts...)
		prefix := generateKVs(t, tc.kvCount, ctx, client)
		ops := append([]iterator.Option{iterator.WithPageSize(tc.pageSize)}, tc.options...)

		// Test transaction
		if tc.inTxn {
			// Loading of the first page is part of the transaction.
			// Next pages are loaded with the same revision.
			logs.Reset()
			actual := make([]result, 0)
			txn := op.Txn(client).Then(prefix.GetAll(client, ops...).ForEach(func(kv *op.KeyValue, header *iterator.Header) error {
				assert.NotNil(t, header)
				actual = append(actual, result{key: string(kv.Key), value: string(kv.Value)})
				return nil
			}))
			require.NoError(t, txn.Do(ctx).Err())
			assert.Equal(t, tc.expectedResults, actual, tc.name)
			wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)
			continue
		}

		// Test iteration methods
		logs.Reset()
		actual := iterateAll(t, prefix.GetAll(client, ops...), ctx)
		assert.Equal(t, tc.expectedResults, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test All method
		logs.Reset()
		actualKvs, err := prefix.GetAll(client, ops...).Do(ctx).All()
		assert.NoError(t, err)
		actual = make([]result, 0)
		for _, kv := range actualKvs {
			actual = append(actual, result{key: string(kv.Key), value: string(kv.Value)})
		}
		assert.Equal(t, tc.expectedResults, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test ForEach method
		logs.Reset()
		itr := prefix.GetAll(client, ops...).Do(ctx)
		actual = make([]result, 0)
		assert.NoError(t, itr.ForEach(func(kv *op.KeyValue, header *iterator.Header) error {
			assert.NotNil(t, header)
			actual = append(actual, result{key: string(kv.Key), value: string(kv.Value)})
			return nil
		}))
		assert.Equal(t, tc.expectedResults, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Next method is stable
		assert.False(t, itr.Next(), tc.name)
		assert.False(t, itr.Next(), tc.name)

		// Test CountAll method
		count, err := prefix.GetAll(client, ops...).CountAll().Do(ctx).ResultOrErr()
		assert.Equal(t, tc.expectedCountAll, int(count), tc.name)
		assert.NoError(t, err, tc.name)
	}
}

func TestIterator_AllKeys(t *testing.T) {
	t.Parallel()

	var logs strings.Builder
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	loggerOpts := []etcdlogger.Option{etcdlogger.WithoutRequestNumber(), etcdlogger.WithNewLineSeparator(false), etcdlogger.WithoutDuration()}
	client.KV = etcdlogger.KVLogWrapper(client.KV, &logs, loggerOpts...)
	ops := []iterator.Option{iterator.WithPageSize(3)}

	// Generate keys
	prefix := etcdop.Prefix("") // <<<<< all!
	for i := 1; i <= 5; i++ {
		key := prefix.Key(fmt.Sprintf("foo/bar%03d", i))
		val := fmt.Sprintf("bar%03d", i)
		assert.NoError(t, key.Put(client, val).Do(ctx).Err())
	}

	// Get all keys from the etcd
	logs.Reset()
	actualKvs, err := prefix.GetAll(client, ops...).Do(ctx).All()
	assert.NoError(t, err)
	actual := make([]result, 0)
	for _, kv := range actualKvs {
		actual = append(actual, result{key: string(kv.Key), value: string(kv.Value)})
	}
	assert.Equal(t, []result{
		{key: "foo/bar001", value: "bar001"},
		{key: "foo/bar002", value: "bar002"},
		{key: "foo/bar003", value: "bar003"},
		{key: "foo/bar004", value: "bar004"},
		{key: "foo/bar005", value: "bar005"},
	}, actual)
	wildcards.Assert(t, `
➡️  GET ["<NUL>", "<NUL>")
✔️  GET ["<NUL>", "<NUL>") | rev: %d | count: 5 | loaded: 3
➡️  GET ["foo/bar004", "<NUL>") | rev: %d | serializable
✔️  GET ["foo/bar004", "<NUL>") | rev: %d | count: 2
`, logs.String())
}

func TestIterator_Revision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	prefix := etcdop.NewPrefix("some/prefix")

	// There are 3 keys
	assert.NoError(t, prefix.Key("foo001").Put(client, "bar001").Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo002").Put(client, "bar002").Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo003").Put(client, "bar003").Do(ctx).Err())

	// Get current revision
	r, err := prefix.Key("foo003").Get(client).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	revision := r.ModRevision

	// Add more keys
	assert.NoError(t, prefix.Key("foo004").Put(client, "bar004").Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo005").Put(client, "bar005").Do(ctx).Err())

	// Get all WithRev
	var actual []result
	assert.NoError(
		t,
		prefix.
			GetAll(client, iterator.WithRev(revision)).Do(ctx).
			ForEach(func(kv *op.KeyValue, _ *iterator.Header) error {
				actual = append(actual, result{key: string(kv.Key), value: string(kv.Value)})
				return nil
			}),
	)

	// The iterator only sees the values in the revision
	assert.Equal(t, []result{
		{key: "some/prefix/foo001", value: "bar001"},
		{key: "some/prefix/foo002", value: "bar002"},
		{key: "some/prefix/foo003", value: "bar003"},
	}, actual)
}

func TestIterator_End(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	prefix := etcdop.NewPrefix("some/prefix")

	// There are 5 keys
	assert.NoError(t, prefix.Key("foo001").Put(client, "bar001").Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo002").Put(client, "bar002").Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo003").Put(client, "bar003").Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo004").Put(client, "bar004").Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo005").Put(client, "bar005").Do(ctx).Err())

	// Get all WithEndOffset, so only the first 3 keys are loaded
	var actual []result
	assert.NoError(
		t,
		prefix.
			GetAll(client, iterator.WithEndOffset("foo004", false)).Do(ctx).
			ForEach(func(kv *op.KeyValue, _ *iterator.Header) error {
				actual = append(actual, result{key: string(kv.Key), value: string(kv.Value)})
				return nil
			}),
	)

	// The iterator only sees the values in the revision
	assert.Equal(t, []result{
		{key: "some/prefix/foo001", value: "bar001"},
		{key: "some/prefix/foo002", value: "bar002"},
		{key: "some/prefix/foo003", value: "bar003"},
	}, actual)
}

func TestIterator_Value_UsedIncorrectly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	prefix := generateKVs(t, 3, ctx, client)

	it := prefix.GetAll(client).Do(ctx)
	assert.PanicsWithError(t, "unexpected Value() call: Next() must be called first", func() {
		it.Value()
	})
}

func TestIterator_ForEach(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	out := ioutil.NewAtomicWriter()
	prefix := generateKVs(t, 5, ctx, client)
	tracker := op.NewTracker(client)

	// Define op
	getAllOp := prefix.
		GetAll(tracker, iterator.WithPageSize(2)).
		ForEach(func(value *op.KeyValue, _ *iterator.Header) error {
			_, _ = out.WriteString(fmt.Sprintf("%s\n", string(value.Value)))
			return nil
		})

	// Run op
	assert.NoError(t, getAllOp.Do(ctx).Err())

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
bar001
bar002
bar003
bar004
bar005
`), strings.TrimSpace(out.String()))
}

func iterateAll(t *testing.T, def iterator.Definition, ctx context.Context) []result {
	t.Helper()
	it := def.Do(ctx)
	actual := make([]result, 0)
	for it.Next() {
		kv := it.Value()
		actual = append(actual, result{key: string(kv.Key), value: string(kv.Value)})
	}
	assert.NoError(t, it.Err())
	return actual
}

func generateKVs(t *testing.T, count int, ctx context.Context, client *etcd.Client) etcdop.Prefix {
	t.Helper()

	// There are some keys before the prefix
	assert.NoError(t, etcdop.Key("some/abc").Put(client, "foo").Do(ctx).Err())

	// Create keys in the iterated prefix
	prefix := etcdop.NewPrefix("some/prefix")
	for i := 1; i <= count; i++ {
		key := prefix.Key(fmt.Sprintf("foo%03d", i))
		val := fmt.Sprintf("bar%03d", i)
		assert.NoError(t, key.Put(client, val).Do(ctx).Err())
	}

	// There are some keys after the prefix
	assert.NoError(t, etcdop.Key("some/xyz").Put(client, "foo").Do(ctx).Err())

	return prefix
}
