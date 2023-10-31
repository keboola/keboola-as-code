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
	name         string
	kvCount      int
	pageSize     int
	options      []iterator.Option
	expected     []resultT
	expectedLogs string
}

func TestIteratorT(t *testing.T) {
	t.Parallel()

	cases := []testCaseT{
		{
			name:     "empty",
			kvCount:  0,
			pageSize: 3,
			expected: []resultT{},
			expectedLogs: `
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 0 | %s
`,
		},
		{
			name:     "count 1, under page size",
			kvCount:  1,
			pageSize: 3,
			expected: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 1 | %s
`,
		},
		{
			name:     "count 1, equal to page size",
			kvCount:  1,
			pageSize: 1,
			expected: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 1 | %s
`,
		},
		{
			name:     "count 2, under page size",
			kvCount:  2,
			pageSize: 3,
			expected: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 2 | %s
`,
		},
		{
			name:     "count 3, equal to page size",
			kvCount:  3,
			pageSize: 3,
			expected: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 3 | %s
`,
		},
		{
			name:     "one on the second page",
			kvCount:  4,
			pageSize: 3,
			expected: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 4 | %s
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 1 | %s
`,
		},
		{
			name:     "two on the second page",
			kvCount:  5,
			pageSize: 3,
			expected: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo005", value: obj{"bar005"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | %s
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 2 | %s
`,
		},
		{
			name:     "WithFromSameRev = false",
			kvCount:  5,
			pageSize: 1,
			options:  []iterator.Option{iterator.WithFromSameRev(false)},
			expected: []resultT{
				{key: "some/prefix/foo001", value: obj{"bar001"}},
				{key: "some/prefix/foo002", value: obj{"bar002"}},
				{key: "some/prefix/foo003", value: obj{"bar003"}},
				{key: "some/prefix/foo004", value: obj{"bar004"}},
				{key: "some/prefix/foo005", value: obj{"bar005"}},
			},
			expectedLogs: `
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/", "some/prefix0") | rev: %d | count: 5 | %s
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/foo002", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/foo002", "some/prefix0") | rev: %d | count: 4 | %s
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/foo003", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/foo003", "some/prefix0") | rev: %d | count: 3 | %s
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/foo004", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/foo004", "some/prefix0") | rev: %d | count: 2 | %s
ETCD_REQUEST[%d] ➡️  GET ["some/prefix/foo005", "some/prefix0")
ETCD_REQUEST[%d] ✔️️  GET ["some/prefix/foo005", "some/prefix0") | rev: %d | count: 1 | %s
`,
		},
	}

	for _, tc := range cases {
		var logs strings.Builder
		ctx := context.Background()
		client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
		client.KV = etcdlogger.KVLogWrapper(client.KV, &logs)
		prefix := generateKVsT(t, tc.kvCount, ctx, client)
		ops := append([]iterator.Option{iterator.WithPageSize(tc.pageSize)}, tc.options...)

		// Test iteration methods
		logs.Reset()
		actual := iterateAllT(t, prefix.GetAll(client, ops...), ctx)
		assert.Equal(t, tc.expected, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test All method
		logs.Reset()
		actualKvs, err := prefix.GetAll(client, ops...).Do(ctx).All()
		assert.NoError(t, err)
		actual = make([]resultT, 0)
		for _, kv := range actualKvs {
			actual = append(actual, resultT{key: string(kv.Kv.Key), value: kv.Value})
		}
		assert.Equal(t, tc.expected, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test ForEachKV method
		logs.Reset()
		itr := prefix.GetAll(client, ops...).Do(ctx)
		actual = make([]resultT, 0)
		assert.NoError(t, itr.ForEachKV(func(kv op.KeyValueT[obj], header *iterator.Header) error {
			assert.NotNil(t, header)
			actual = append(actual, resultT{key: string(kv.Kv.Key), value: kv.Value})
			return nil
		}))
		assert.Equal(t, tc.expected, actual, tc.name)
		wildcards.Assert(t, tc.expectedLogs, logs.String(), tc.name)

		// Test ForEachValue method
		logs.Reset()
		itr = prefix.GetAll(client, ops...).Do(ctx)
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

func TestIteratorT_Revision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	serialization := serde.NewJSON(serde.NoValidation)
	prefix := etcdop.NewTypedPrefix[obj]("some/prefix", serialization)

	// There are 3 keys
	assert.NoError(t, prefix.Key("foo001").Put(client, obj{Value: "bar001"}).Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo002").Put(client, obj{Value: "bar002"}).Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo003").Put(client, obj{Value: "bar003"}).Do(ctx).Err())

	// Get current revision
	r, err := prefix.Key("foo003").Get(client).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	revision := r.Kv.ModRevision

	// Add more keys
	assert.NoError(t, prefix.Key("foo004").Put(client, obj{Value: "bar004"}).Do(ctx).Err())
	assert.NoError(t, prefix.Key("foo005").Put(client, obj{Value: "bar005"}).Do(ctx).Err())

	// Get all WithRev
	var actual []resultT
	assert.NoError(
		t,
		prefix.
			GetAll(client, iterator.WithRev(revision)).Do(ctx).
			ForEachKV(func(kv op.KeyValueT[obj], _ *iterator.Header) error {
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
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	prefix := generateKVsT(t, 3, ctx, client)

	it := prefix.GetAll(client).Do(ctx)
	assert.PanicsWithError(t, "unexpected Value() call: Next() must be called first", func() {
		it.Value()
	})
}

func TestIteratorT_ForEachOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	out := ioutil.NewAtomicWriter()
	prefix := generateKVsT(t, 5, ctx, client)
	tracker := op.NewTracker(client)

	// Define op
	getAllOp := prefix.
		GetAll(tracker, iterator.WithPageSize(2)).
		ForEachOp(func(value obj, header *iterator.Header) error {
			_, _ = out.WriteString(fmt.Sprintf("%s\n", value.Value))
			return nil
		})

	// Run op
	assert.NoError(t, getAllOp.Do(ctx).Err())

	// All requests can be tracked by the TrackerKV
	assert.Equal(t, []op.TrackedOp{
		{Type: op.GetOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 5},
		{Type: op.GetOp, Key: []byte("some/prefix/foo003"), RangeEnd: []byte("some/prefix0"), Count: 3},
		{Type: op.GetOp, Key: []byte("some/prefix/foo005"), RangeEnd: []byte("some/prefix0"), Count: 1},
	}, tracker.Operations())

	// All values have been received
	assert.Equal(t, strings.TrimSpace(`
bar001
bar002
bar003
bar004
bar005
`), strings.TrimSpace(out.String()))
}

func iterateAllT(t *testing.T, def iterator.DefinitionT[obj], ctx context.Context) []resultT {
	t.Helper()
	it := def.Do(ctx)
	actual := make([]resultT, 0)
	for it.Next() {
		kv := it.Value()
		actual = append(actual, resultT{key: string(kv.Kv.Key), value: kv.Value})
	}
	assert.NoError(t, it.Err())
	return actual
}

func generateKVsT(t *testing.T, count int, ctx context.Context, client *etcd.Client) etcdop.PrefixT[obj] {
	t.Helper()

	// There are some keys before the prefix
	assert.NoError(t, etcdop.Key("some/abc").Put(client, "foo").Do(ctx).Err())

	// Create keys in the iterated prefix
	serialization := serde.NewJSON(serde.NoValidation)
	prefix := etcdop.NewTypedPrefix[obj]("some/prefix", serialization)
	for i := 1; i <= count; i++ {
		key := prefix.Key(fmt.Sprintf("foo%03d", i))
		val := obj{fmt.Sprintf("bar%03d", i)}
		assert.NoError(t, key.Put(client, val).Do(ctx).Err())
	}

	// There are some keys after the prefix
	assert.NoError(t, etcdop.Key("some/xyz").Put(client, "foo").Do(ctx).Err())

	return prefix
}
