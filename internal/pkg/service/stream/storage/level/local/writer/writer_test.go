package writer_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := writer.NewConfig()
	logger := log.NewDebugLogger()
	clk := clock.New()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := newTestSlice(t)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	w, err := writer.New(ctx, logger, clk, cfg, slice, file, disksync.NewSyncer, test.DummyWriterFactory, writer.NewEvents())
	require.NoError(t, err)

	// Test getters
	assert.Equal(t, slice.SliceKey, w.SliceKey())

	// Test write methods
	assert.NoError(t, w.WriteRecord(clk.Now(), []any{"123", "456", "789"}))
	assert.NoError(t, w.WriteRecord(clk.Now(), []any{"abc", "def", "ghj"}))

	// Test Close method
	assert.NoError(t, w.Close(ctx))

	// Try Close again
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "writer is already closed", err.Error())
	}

	// Check file content
	content, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("123,456,789\nabc,def,ghj\n"), content)
}

func TestWriter_FlushError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := writer.NewConfig()
	logger := log.NewDebugLogger()
	clk := clock.NewMock()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := newTestSlice(t)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	writerFactory := func(cfg writer.Config, out io.Writer, slice *model.Slice) (writer.FormatWriter, error) {
		w := test.NewDummyWriter(cfg, out, slice)
		w.FlushError = errors.New("some error")
		return w, nil
	}

	w, err := writer.New(ctx, logger, clk, cfg, slice, file, disksync.NewSyncer, writerFactory, writer.NewEvents())
	require.NoError(t, err)

	// Test Close method
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "chain sync error:\n- chain flush error:\n  - cannot flush \"*test.DummyWriter\": some error", err.Error())
	}
}

func TestWriter_CloseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := writer.NewConfig()
	logger := log.NewDebugLogger()
	clk := clock.NewMock()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := newTestSlice(t)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	writerFactory := func(cfg writer.Config, out io.Writer, slice *model.Slice) (writer.FormatWriter, error) {
		w := test.NewDummyWriter(cfg, out, slice)
		w.CloseError = errors.New("some error")
		return w, nil
	}

	w, err := writer.New(ctx, logger, clk, cfg, slice, file, disksync.NewSyncer, writerFactory, writer.NewEvents())
	require.NoError(t, err)

	// Test Close method
	err = w.Close(ctx)
	if assert.Error(t, err) {
		assert.Equal(t, "chain close error:\n- cannot close \"*test.DummyWriter\": some error", err.Error())
	}
}

func newTestSlice(tb testing.TB) *model.Slice {
	tb.Helper()

	s := test.NewSlice()

	// Slice definition must be valid
	val := validator.New()
	require.NoError(tb, val.Validate(context.Background(), s))

	return s
}
