package etcdop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestValidationError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	pfxNoValidation := NewTypedPrefix[fooType]("my-prefix", serde.NewJSON(serde.NoValidation))
	pfxFailingValidation := NewTypedPrefix[fooType]("my-prefix", serde.NewJSON(
		func(ctx context.Context, value any) error {
			return errors.New("validation error")
		},
	))

	// Test Put
	err := pfxFailingValidation.Key("my-key").Put(client, "value").Do(ctx).Err()
	require.Error(t, err)
	assert.Equal(t, `etcd operation "put" failed: invalid value for "my-prefix/my-key": validation error`, err.Error())

	// Test PutIfNotExists
	err = pfxFailingValidation.Key("my-key").PutIfNotExists(client, "value").Do(ctx).Err()
	require.Error(t, err)
	assert.Equal(t, `etcd operation "put if not exists" failed: invalid value for "my-prefix/my-key": validation error`, err.Error())

	// Create key
	require.NoError(t, pfxNoValidation.Key("my-key").Put(client, `"foo"`).Do(ctx).Err())

	// Test GetKV
	err = pfxFailingValidation.Key("my-key").GetKV(client).Do(ctx).Err()
	require.Error(t, err)
	assert.Equal(t, `etcd operation "get" failed: invalid value for "my-prefix/my-key": validation error`, err.Error())

	// Test GetAll
	_, err = pfxFailingValidation.GetAll(client).Do(ctx).AllKVs()
	require.Error(t, err)
	assert.Equal(t, `etcd iterator failed: cannot decode the value of key "my-prefix/my-key", page=1, index=0: validation error`, err.Error())
}

func TestEncodeDecodeError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	pfxNoValidation := NewTypedPrefix[fooType]("my-prefix", serde.NewJSON(serde.NoValidation))
	pfxFailingEncode := NewTypedPrefix[fooType]("my-prefix", serde.New(
		func(_ context.Context, value any) (string, error) {
			return "", errors.New("encode error")
		},
		func(_ context.Context, data []byte, target any) error {
			return errors.New("decode error")
		},
		func(ctx context.Context, value any) error {
			return nil
		},
	))

	// Test Put
	err := pfxFailingEncode.Key("my-key").Put(client, "value").Do(ctx).Err()
	require.Error(t, err)
	assert.Equal(t, `etcd operation "put" failed: invalid value for "my-prefix/my-key": encode error`, err.Error())

	// Test PutIfNotExists
	err = pfxFailingEncode.Key("my-key").PutIfNotExists(client, "value").Do(ctx).Err()
	require.Error(t, err)
	assert.Equal(t, `etcd operation "put if not exists" failed: invalid value for "my-prefix/my-key": encode error`, err.Error())

	// Create key
	require.NoError(t, pfxNoValidation.Key("my-key").Put(client, `"foo"`).Do(ctx).Err())

	// Test GetKV
	err = pfxFailingEncode.Key("my-key").GetKV(client).Do(ctx).Err()
	require.Error(t, err)
	assert.Equal(t, `etcd operation "get" failed: invalid value for "my-prefix/my-key": decode error`, err.Error())

	// Test GetAll
	_, err = pfxFailingEncode.GetAll(client).Do(ctx).AllKVs()
	require.Error(t, err)
	assert.Equal(t, `etcd iterator failed: cannot decode the value of key "my-prefix/my-key", page=1, index=0: decode error`, err.Error())
}
