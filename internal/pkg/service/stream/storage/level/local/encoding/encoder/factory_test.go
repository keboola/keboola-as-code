package encoder_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

type discardOutput struct{}

func (discardOutput) Write(ctx context.Context, aligned bool, b []byte) (n int, err error) {
	return 0, nil
}

func (discardOutput) IsReady() bool {
	return true
}

func (discardOutput) Sync(ctx context.Context) error {
	return nil
}

func (discardOutput) Close(context.Context) error {
	return nil
}

// TestDefaultFactory_FileTypeCSV tests that csv.WriterVolume is created for the storage.FileTypeCSV.
// Test for csv.WriterVolume itself are in the "csv" package.
func TestDefaultFactory_FileTypeCSV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	d, _ := dependencies.NewMockedSourceScope(t, ctx)

	slice := test.NewSlice()
	slice.Encoding.Encoder.Type = encoder.TypeCSV

	w, err := d.EncodingManager().OpenPipeline(ctx, slice.SliceKey, slice.Mapping, slice.Encoding, discardOutput{})
	require.NoError(t, err)
	assert.NotNil(t, w)
}

// TestDefaultFactory_FileTypeInvalid test handling of an invalid file type.
func TestDefaultFactory_FileTypeInvalid(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	d, _ := dependencies.NewMockedSourceScope(t, ctx)
	slice := test.NewSlice()
	slice.Encoding.Encoder.Type = "invalid"

	_, err := d.EncodingManager().OpenPipeline(ctx, slice.SliceKey, slice.Mapping, slice.Encoding, discardOutput{})
	if assert.Error(t, err) {
		assert.Equal(t, `unexpected encoder type "invalid"`, err.Error())
	}
}
