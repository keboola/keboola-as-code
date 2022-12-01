package iterator_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type testStruct struct {
	Value string
}

func TestIterator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)

	// Create KVs
	serialization := serde.NewJSON(serde.NoValidation)
	prefix := etcdop.NewTypedPrefix[testStruct]("some/prefix", serialization)
	expected := make([]op.KeyValueT[testStruct], 0)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("foo%03d", i)
		val := testStruct{fmt.Sprintf("bar%03d", i)}
		expected = append(expected, op.KeyValueT[testStruct]{
			KV: &op.KeyValue{
				Key:   []byte(key),
				Value: []byte(val.Value),
			},
			Value: val,
		})
		assert.NoError(t, prefix.Key(key).Put(val).Do(ctx, client))
	}

	// Iterate KVs
	actual := make([]testStruct, 0)
	it := iterator.New[testStruct](prefix.Prefix(), serialization).Do(ctx, client)
	for it.Next() {
		actual = append(actual, it.Value().Value)
	}
	assert.NoError(t, it.Err())

	// Check results
	assert.Equal(t, expected, actual)
}

func TestIterator_Value_UsedIncorrectly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)
	serialization := serde.NewJSON(serde.NoValidation)
	prefix := etcdop.NewTypedPrefix[testStruct]("some/prefix", serialization)

	it := iterator.New[testStruct](prefix.Prefix(), serialization).Do(ctx, client)
	assert.PanicsWithError(t, "unexpected Value() call: Next() must be called first", func() {
		it.Value()
	})
}
