package writer

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/test"
	"os"
	"path/filepath"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestBaseWriter(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	clk := clock.New()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := newTestSlice(t)

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)
	w := NewBaseWriter(logger, clk, slice, dirPath, filePath, writechain.New(logger, file), NewEvents())

	// Test getters
	assert.Equal(t, logger, w.Logger())
	assert.Equal(t, slice.SliceKey, w.SliceKey())
	assert.Equal(t, slice.Columns, w.Columns())
	assert.NotSame(t, slice.Columns, w.Columns())
	assert.Equal(t, slice.Type, w.Type())
	assert.Equal(t, slice.LocalStorage.Compression, w.Compression())
	assert.Equal(t, dirPath, w.DirPath())
	assert.Equal(t, filePath, w.FilePath())

	// Test write methods
	n, notifier, err := w.WriteWithNotify([]byte("123"))
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	assert.NoError(t, notifier.Wait())
	n, err = w.Write([]byte("456"))
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	n, err = w.WriteString("789")
	w.AddWriteOp(1)
	assert.Equal(t, 3, n)
	assert.NoError(t, err)
	notifier, err = w.DoWithNotify(func() error {
		_, err := w.Write([]byte("abc"))
		w.AddWriteOp(1)
		return err
	})
	assert.NoError(t, err)
	assert.NoError(t, notifier.Wait())

	// Test Close method
	assert.NoError(t, w.Close())

	// Try Close again
	err = w.Close()
	if assert.Error(t, err) {
		assert.Equal(t, "syncer is already stopped: context canceled", err.Error())
	}

	// Check file content
	content, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("123456789abc"), content)
}

func TestBaseWriter_CloseError(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	clk := clock.NewMock()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := newTestSlice(t)

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)
	w := NewBaseWriter(logger, clk, slice, dirPath, filePath, writechain.New(logger, file), NewEvents())

	w.AppendCloseFn("my-closer", func() error {
		return errors.New("some error")
	})

	// Test Close method
	err = w.Close()
	if assert.Error(t, err) {
		assert.Equal(t, "chain close error: cannot close \"my-closer\": some error", err.Error())
	}
}

func newTestSlice(tb testing.TB) *storage.Slice {
	tb.Helper()

	s := test.NewSlice()

	// Slice definition must be valid
	val := validator.New()
	require.NoError(tb, val.Validate(context.Background(), s))
	return s
}
