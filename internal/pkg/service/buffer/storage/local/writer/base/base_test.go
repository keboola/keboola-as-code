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
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBaseWriter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	clk := clock.NewMock()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := newTestSlice(t)

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	var syncer *disksync.Syncer
	chain := writechain.New(logger, file)
	chain.PrependWriter(func(w writechain.Writer) io.Writer {
		syncer = disksync.NewSyncer(ctx, logger, clk, disksync.DefaultConfig(), w, chain)
		assert.NotNil(t, syncer)
		return syncer
	})

	w := base.NewWriter(logger, slice, dirPath, filePath, chain, syncer)

	// Test getters
	assert.Equal(t, logger, w.Logger())
	assert.Equal(t, slice.SliceKey, w.SliceKey())
	assert.Equal(t, slice.Type, w.Type())
	assert.Equal(t, slice.LocalStorage.Compression, w.Compression())
	assert.Equal(t, dirPath, w.DirPath())
	assert.Equal(t, filePath, w.FilePath())
	assert.Equal(t, chain, w.Chain())
	assert.Equal(t, syncer, w.Syncer())

	// Test Close method
	assert.NoError(t, w.Close())
}

func TestBaseWriter_CloseError(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, "file")
	slice := newTestSlice(t)

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	assert.NoError(t, err)

	w := base.NewWriter(logger, slice, dirPath, filePath, writechain.New(logger, file), nil)

	w.Chain().AppendCloseFn(func() error {
		return errors.New("some error")
	})

	// Test Close method
	err = w.Close()
	if assert.Error(t, err) {
		assert.Equal(t, "chain close error:\n- cannot close \"writechain.closeFn\": some error", err.Error())
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
			Compression: compression.Config{
				Type: compression.TypeNone,
			},
			Sync: disksync.Config{
				Mode:            disksync.ModeDisk,
				Wait:            true,
				BytesTrigger:    100 * datasize.KB,
				IntervalTrigger: 100 * time.Millisecond,
			},
		},
		StagingStorage: staging.Slice{
			Path: "slice.csv",
			Compression: compression.Config{
				Type: compression.TypeNone,
			},
		},
	}

	// Slice definition must be valid
	val := validator.New()
	require.NoError(t, val.Validate(context.Background(), s))
	return s
}
