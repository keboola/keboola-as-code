package op_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type fooResult struct {
	field1 string
	field2 string
}

func TestJoin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)

	// Create keys with some values
	key1 := etcdop.Key("key1")
	key2 := etcdop.Key("key2")
	assert.NoError(t, key1.Put("value1").Do(ctx, client))
	assert.NoError(t, key2.Put("value2").Do(ctx, client))

	// Use Join
	result := fooResult{}
	joinTxn := Join(
		&result,
		key1.Get().WithOnResult(func(kv *KeyValue) {
			result.field1 = string(kv.Value)
		}),
		key2.Get().WithOnResult(func(kv *KeyValue) {
			result.field2 = string(kv.Value)
		}),
	)

	// Check results
	out, err := joinTxn.Do(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, "value1", out.field1)
	assert.Equal(t, "value2", out.field2)
}
