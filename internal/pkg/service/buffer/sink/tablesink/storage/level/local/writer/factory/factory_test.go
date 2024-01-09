package factory_test

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/factory"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/volume"
)

// TestDefaultFactory_FileTypeCSV tests that csv.WriterVolume is created for the storage.FileTypeCSV.
// Test for csv.WriterVolume itself are in the "csv" package.
func TestDefaultFactory_FileTypeCSV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()
	clk := clock.New()
	spec := storage.VolumeSpec{NodeID: "my-node", Path: t.TempDir(), Type: "hdd", Label: "001"}

	v, err := volume.Open(ctx, logger, clk, writer.NewEvents(), spec, volume.WithWriterFactory(factory.Default))
	assert.NoError(t, err)

	slice := test.NewSlice()
	slice.Type = storage.FileTypeCSV

	w, err := v.NewWriterFor(slice)
	assert.NoError(t, err)
	assert.NotNil(t, w)

	_, ok := w.Unwrap().(*csv.Writer)
	assert.True(t, ok)
}

// TestDefaultFactory_FileTypeInvalid test handling of an invalid file type.
func TestDefaultFactory_FileTypeInvalid(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()
	clk := clock.New()
	spec := storage.VolumeSpec{NodeID: "my-node", Path: t.TempDir(), Type: "hdd", Label: "001"}

	v, err := volume.Open(ctx, logger, clk, writer.NewEvents(), spec, volume.WithWriterFactory(factory.Default))
	assert.NoError(t, err)

	slice := test.NewSlice()
	slice.Type = "invalid"
	_, err = v.NewWriterFor(slice)
	if assert.Error(t, err) {
		assert.Equal(t, `unexpected file type "invalid"`, err.Error())
	}
}
