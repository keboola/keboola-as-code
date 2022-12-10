package iterator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type obj struct {
	Value string `json:"val"`
}

type result struct {
	key   string
	value obj
}

type testCase struct {
	name         string
	kvCount      int
	pageSize     int
	expected     []result
	expectedLogs string
}

func TestIterator(t *testing.T) {
	t.Parallel()

	cases := []testCase{
		{
			name:     "empty",
			kvCount:  0,
			pageSize: 3,
			expected: []result{},
			expectedLogs: `
ETCD_REQUEST[%d] GET "some/prefix/" | start
ETCD_REQUEST[%d] GET "some/prefix/" | done | count: 0 | %s
`,
		},
		{
			name:     "count 1, under page size",
			kvCount:  1,
			pageSize: 3,
			expected: []result{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] GET "some/prefix/" | start
ETCD_REQUEST[%d] GET "some/prefix/" | done | count: 1 | %s
`,
		},
		{
			name:     "count 1, equal to page size",
			kvCount:  1,
			pageSize: 1,
			expected: []result{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] GET "some/prefix/" | start
ETCD_REQUEST[%d] GET "some/prefix/" | done | count: 1 | %s
`,
		},
		{
			name:     "count 2, under page size",
			kvCount:  2,
			pageSize: 3,
			expected: []result{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] GET "some/prefix/" | start
ETCD_REQUEST[%d] GET "some/prefix/" | done | count: 2 | %s
`,
		},
		{
			name:     "count 3, equal to page size",
			kvCount:  3,
			pageSize: 3,
			expected: []result{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] GET "some/prefix/" | start
ETCD_REQUEST[%d] GET "some/prefix/" | done | count: 3 | %s
`,
		},
		{
			name:     "one on the second page",
			kvCount:  4,
			pageSize: 3,
			expected: []result{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] GET "some/prefix/" | start
ETCD_REQUEST[%d] GET "some/prefix/" | done | count: 4 | %s
ETCD_REQUEST[%d] GET "some/prefix/foo004" | start
ETCD_REQUEST[%d] GET "some/prefix/foo004" | done | count: 1 | %s
`,
		},
		{
			name:     "two on the second page",
			kvCount:  5,
			pageSize: 3,
			expected: []result{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo005", value: obj{"bar005"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] GET "some/prefix/" | start
ETCD_REQUEST[%d] GET "some/prefix/" | done | count: 5 | %s
ETCD_REQUEST[%d] GET "some/prefix/foo004" | start
ETCD_REQUEST[%d] GET "some/prefix/foo004" | done | count: 2 | %s
`,
		},
		{
			name:     "page size = 1",
			kvCount:  5,
			pageSize: 1,
			expected: []result{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo005", value: obj{"bar005"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] GET "some/prefix/" | start
ETCD_REQUEST[%d] GET "some/prefix/" | done | count: 5 | %s
ETCD_REQUEST[%d] GET "some/prefix/foo002" | start
ETCD_REQUEST[%d] GET "some/prefix/foo002" | done | count: 4 | %s
ETCD_REQUEST[%d] GET "some/prefix/foo003" | start
ETCD_REQUEST[%d] GET "some/prefix/foo003" | done | count: 3 | %s
ETCD_REQUEST[%d] GET "some/prefix/foo004" | start
ETCD_REQUEST[%d] GET "some/prefix/foo004" | done | count: 2 | %s
ETCD_REQUEST[%d] GET "some/prefix/foo005" | start
ETCD_REQUEST[%d] GET "some/prefix/foo005" | done | count: 1 | %s
`,
		},
	}

	for _, tc := range cases {
		var logs strings.Builder
		ctx := context.Background()
		client := etcdhelper.ClientForTest(t)
		client.KV = etcdhelper.KVLogWrapper(client.KV, &logs)
		prefix := generateKVs(t, tc.kvCount, ctx, client)

		// Test iteration methods
		logs.Reset()
		actual := iterateAll(t, prefix.GetAll(iterator.WithPageSize(tc.pageSize)), ctx, client)
		assert.Equal(t, tc.expected, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test All method
		logs.Reset()
		actualKvs, err := prefix.GetAll(iterator.WithPageSize(tc.pageSize)).Do(ctx, client).All()
		assert.NoError(t, err)
		actual = make([]result, 0)
		for _, kv := range actualKvs {
			actual = append(actual, result{key: string(kv.KV.Key), value: kv.Value})
		}
		assert.Equal(t, tc.expected, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test ForEachKV method
		logs.Reset()
		itr := prefix.GetAll(iterator.WithPageSize(tc.pageSize)).Do(ctx, client)
		actual = make([]result, 0)
		assert.NoError(t, itr.ForEachKV(func(kv op.KeyValueT[obj], header *iterator.Header) error {
			assert.NotNil(t, header)
			actual = append(actual, result{key: string(kv.KV.Key), value: kv.Value})
			return nil
		}))
		assert.Equal(t, tc.expected, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test ForEachValue method
		logs.Reset()
		itr = prefix.GetAll(iterator.WithPageSize(tc.pageSize)).Do(ctx, client)
		values := make([]obj, 0)
		assert.NoError(t, itr.ForEachValue(func(value obj, header *iterator.Header) error {
			assert.NotNil(t, header)
			values = append(values, value)
			return nil
		}))
		assert.Len(t, values, len(tc.expected))
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)
	}
}

func TestIterator_Revision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)

	serialization := serde.NewJSON(serde.NoValidation)
	prefix := etcdop.NewTypedPrefix[obj]("some/prefix", serialization)

	// There are 3 keys
	assert.NoError(t, prefix.Key("foo001").Put(obj{Value: "bar001"}).Do(ctx, client))
	assert.NoError(t, prefix.Key("foo002").Put(obj{Value: "bar002"}).Do(ctx, client))
	assert.NoError(t, prefix.Key("foo003").Put(obj{Value: "bar003"}).Do(ctx, client))

	// Get current revision
	r, err := prefix.Key("foo003").Get().Do(ctx, client)
	assert.NoError(t, err)
	revision := r.KV.ModRevision

	// Add more keys
	assert.NoError(t, prefix.Key("foo004").Put(obj{Value: "bar004"}).Do(ctx, client))
	assert.NoError(t, prefix.Key("foo005").Put(obj{Value: "bar005"}).Do(ctx, client))

	// Get all WithRev
	var actual []result
	assert.NoError(
		t,
		prefix.
			GetAll(iterator.WithRev(revision)).Do(ctx, client).
			ForEachKV(func(kv op.KeyValueT[obj], _ *iterator.Header) error {
				actual = append(actual, result{key: string(kv.KV.Key), value: kv.Value})
				return nil
			}),
	)

	// The iterator only sees the values in the revision
	assert.Equal(t, []result{
		{key: "some/prefix/foo001", value: obj{"bar001"}},
		{key: "some/prefix/foo002", value: obj{"bar002"}},
		{key: "some/prefix/foo003", value: obj{"bar003"}},
	}, actual)
}

func TestIterator_Value_UsedIncorrectly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)
	prefix := generateKVs(t, 3, ctx, client)

	it := prefix.GetAll().Do(ctx, client)
	assert.PanicsWithError(t, "unexpected Value() call: Next() must be called first", func() {
		it.Value()
	})
}

func iterateAll(t *testing.T, def iterator.DefinitionT[obj], ctx context.Context, client *etcd.Client) []result {
	t.Helper()
	it := def.Do(ctx, client)
	actual := make([]result, 0)
	for it.Next() {
		kv := it.Value()
		actual = append(actual, result{key: string(kv.KV.Key), value: kv.Value})
	}
	assert.NoError(t, it.Err())
	return actual
}

func generateKVs(t *testing.T, count int, ctx context.Context, client *etcd.Client) etcdop.PrefixT[obj] {
	t.Helper()

	// There are some keys before the prefix
	assert.NoError(t, etcdop.Key("some/abc").Put("foo").Do(ctx, client))

	// Create keys in the iterated prefix
	serialization := serde.NewJSON(serde.NoValidation)
	prefix := etcdop.NewTypedPrefix[obj]("some/prefix", serialization)
	for i := 1; i <= count; i++ {
		key := prefix.Key(fmt.Sprintf("foo%03d", i))
		val := obj{fmt.Sprintf("bar%03d", i)}
		assert.NoError(t, key.Put(val).Do(ctx, client))
	}

	// There are some keys after the prefix
	assert.NoError(t, etcdop.Key("some/xyz").Put("foo").Do(ctx, client))

	return prefix
}
