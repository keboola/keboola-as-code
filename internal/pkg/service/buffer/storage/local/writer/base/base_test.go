package base_test

import (
	"context"
	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/base"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
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
	chain := writechain.New(logger, file)
	w := base.NewWriter(logger, clk, slice, dirPath, filePath, chain)

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
	chain := writechain.New(logger, file)
	w := base.NewWriter(logger, clk, slice, dirPath, filePath, chain)

	w.AppendCloseFn("my-closer", func() error {
		return errors.New("some error")
	})

	// Test Close method
	err = w.Close()
	if assert.Error(t, err) {
		assert.Equal(t, "chain close error: cannot close \"my-closer\": some error", err.Error())
	}
}

func newTestSlice(t testing.TB) *storage.Slice {
	s := &storage.Slice{
		SliceKey: storage.SliceKey{
			FileKey: storage.FileKey{
				ExportKey: key.ExportKey{
					ReceiverKey: key.ReceiverKey{
						ProjectID:  123,
						ReceiverID: "my-receiver",
					},
					ExportID: "my-export",
				},
				FileID: storage.FileID{
					OpenedAt: utctime.MustParse("2000-01-01T19:00:00.000Z"),
				},
			},
			SliceID: storage.SliceID{
				VolumeID: "my-volume",
				OpenedAt: utctime.MustParse("2000-01-01T19:00:00.000Z"),
			},
		},
		Type:  storage.FileTypeCSV,
		State: storage.SliceWriting,
		Columns: column.Columns{
			column.ID{},
			column.Headers{},
			column.Body{},
		},
		LocalStorage: local.Slice{
			Dir:           "my-dir",
			Filename:      "slice.csv",
			AllocateSpace: 10 * datasize.KB,
			Compression:   compression.DefaultNoneConfig(),
			Sync:          disksync.DefaultConfig(),
		},
		StagingStorage: staging.Slice{
			Path:        "slice.csv",
			Compression: compression.DefaultNoneConfig(),
		},
	}

	// Slice definition must be valid
	val := validator.New()
	require.NoError(t, val.Validate(context.Background(), s))
	return s
}
