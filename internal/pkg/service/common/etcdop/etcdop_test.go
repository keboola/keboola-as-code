package etcdop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestValidationError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)

	pfxNoValidation := NewTypedPrefix[fooType]("my-prefix", serde.NewJSON(serde.NoValidation))
	pfxFailingValidation := NewTypedPrefix[fooType]("my-prefix", serde.NewJSON(
		func(ctx context.Context, value any) error {
			return errors.New("validation error")
		},
	))

	// Test Put
	err := pfxFailingValidation.Key("my-key").Put("value").Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, `etcd operation "put" failed: invalid value for "my-prefix/my-key": validation error`, err.Error())

	// Test PutIfNotExists
	_, err = pfxFailingValidation.Key("my-key").PutIfNotExists("value").Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, `etcd operation "put if not exists" failed: invalid value for "my-prefix/my-key": validation error`, err.Error())

	// Create key
	assert.NoError(t, pfxNoValidation.Key("my-key").Put(`"foo"`).Do(ctx, client))

	// Test Get
	_, err = pfxFailingValidation.Key("my-key").Get().Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, `etcd operation "get one" failed: invalid value for "my-prefix/my-key": validation error`, err.Error())

	// Test GetAll
	_, err = pfxFailingValidation.GetAll().Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, `etcd operation "get all" failed: invalid value for "my-prefix/my-key": validation error`, err.Error())
}

func TestEncodeDecodeError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)

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
	err := pfxFailingEncode.Key("my-key").Put("value").Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, `etcd operation "put" failed: invalid value for "my-prefix/my-key": encode error`, err.Error())

	// Test PutIfNotExists
	_, err = pfxFailingEncode.Key("my-key").PutIfNotExists("value").Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, `etcd operation "put if not exists" failed: invalid value for "my-prefix/my-key": encode error`, err.Error())

	// Create key
	assert.NoError(t, pfxNoValidation.Key("my-key").Put(`"foo"`).Do(ctx, client))

	// Test Get
	_, err = pfxFailingEncode.Key("my-key").Get().Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, `etcd operation "get one" failed: invalid value for "my-prefix/my-key": decode error`, err.Error())

	// Test GetAll
	_, err = pfxFailingEncode.GetAll().Do(ctx, client)
	assert.Error(t, err)
	assert.Equal(t, `etcd operation "get all" failed: invalid value for "my-prefix/my-key": decode error`, err.Error())
}
