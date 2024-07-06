package factory_test

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	writerVolume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/format/factory"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

// TestDefaultFactory_FileTypeCSV tests that csv.WriterVolume is created for the storage.FileTypeCSV.
// Test for csv.WriterVolume itself are in the "csv" package.
func TestDefaultFactory_FileTypeCSV(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()
	clk := clock.New()
	spec := volume.Spec{NodeID: "my-node", Path: t.TempDir(), Type: "hdd", Label: "001"}

	v, err := writerVolume.Open(ctx, logger, clk, events.New[encoding.Writer](), local.NewConfig(), spec, writerVolume.WithFormatWriterFactory(factory.Default))
	require.NoError(t, err)

	slice := test.NewSlice()
	slice.Type = model.FileTypeCSV

	w, err := v.OpenWriter(slice)
	require.NoError(t, err)
	assert.NotNil(t, w)

	assert.NoError(t, v.Close(ctx))
}

// TestDefaultFactory_FileTypeInvalid test handling of an invalid file type.
func TestDefaultFactory_FileTypeInvalid(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()
	clk := clock.New()
	spec := volume.Spec{NodeID: "my-node", Path: t.TempDir(), Type: "hdd", Label: "001"}

	v, err := writerVolume.Open(ctx, logger, clk, events.New[encoding.Writer](), local.NewConfig(), spec, writerVolume.WithFormatWriterFactory(factory.Default))
	assert.NoError(t, err)

	slice := test.NewSlice()
	slice.Type = "invalid"
	_, err = v.OpenWriter(slice)
	if assert.Error(t, err) {
		assert.Equal(t, `unexpected file type "invalid"`, err.Error())
	}

	assert.NoError(t, v.Close(ctx))
}
