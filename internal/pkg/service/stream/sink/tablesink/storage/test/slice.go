package test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// sliceRepository interface to prevent package import cycle.
type sliceRepository interface {
	Close(now time.Time, fileVolumeKey storage.FileVolumeKey) *op.AtomicOp[op.NoResult]
	Get(k storage.SliceKey) op.WithResult[storage.Slice]
	StateTransition(now time.Time, sliceKey storage.SliceKey, from, to storage.SliceState) *op.AtomicOp[storage.Slice]
}

func NewSliceKey() storage.SliceKey {
	return NewSliceKeyOpenedAt("2000-01-01T20:00:00.000Z")
}

func NewSliceKeyOpenedAt(openedAtStr string) storage.SliceKey {
	openedAt := utctime.MustParse(openedAtStr)
	return storage.SliceKey{
		FileVolumeKey: storage.FileVolumeKey{
			FileKey:  NewFileKeyOpenedAt("2000-01-01T19:00:00.000Z"),
			VolumeID: "my-volume",
		},
		SliceID: storage.SliceID{
			OpenedAt: openedAt,
		},
	}
}

func NewSlice() *storage.Slice {
	return NewSliceOpenedAt("2000-01-01T20:00:00.000Z")
}

func NewSliceOpenedAt(openedAt string) *storage.Slice {
	return &storage.Slice{
		SliceKey: NewSliceKeyOpenedAt(openedAt),
		Type:     storage.FileTypeCSV,
		State:    storage.SliceWriting,
		Columns: column.Columns{
			column.ID{},
			column.Headers{},
			column.Body{},
		},
		LocalStorage: local.Slice{
			Dir:                openedAt,
			Filename:           "slice.csv",
			AllocatedDiskSpace: 10 * datasize.KB,
			Compression:        compression.NewNoneConfig(),
			DiskSync: disksync.Config{
				Mode:            disksync.ModeDisk,
				Wait:            true,
				CheckInterval:   duration.From(1 * time.Millisecond),
				CountTrigger:    500,
				BytesTrigger:    1 * datasize.MB,
				IntervalTrigger: duration.From(50 * time.Millisecond),
			},
		},
		StagingStorage: staging.Slice{
			Path:        "slice.csv",
			Compression: compression.NewNoneConfig(),
		},
	}
}

func SwitchSliceStates(t *testing.T, ctx context.Context, clk *clock.Mock, sliceRepo sliceRepository, sliceKey storage.SliceKey, states []storage.SliceState) {
	t.Helper()
	from := states[0]
	for _, to := range states[1:] {
		clk.Add(time.Hour)

		// Slice must be closed by the Close method
		var slice storage.Slice
		var err error
		if to == storage.SliceClosing {
			require.NoError(t, sliceRepo.Close(clk.Now(), sliceKey.FileVolumeKey).Do(ctx).Err())
			slice, err = sliceRepo.Get(sliceKey).Do(ctx).ResultOrErr()
			require.NoError(t, err)
		} else {
			slice, err = sliceRepo.StateTransition(clk.Now(), sliceKey, from, to).Do(ctx).ResultOrErr()
			require.NoError(t, err)
		}

		// Slice state has been switched
		assert.Equal(t, to, slice.State)

		// Retry should be reset
		assert.Equal(t, 0, slice.RetryAttempt)
		assert.Nil(t, slice.LastFailedAt)

		// Check timestamp
		switch to {
		case storage.SliceClosing:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.ClosingAt.String())
		case storage.SliceUploading:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.UploadingAt.String())
		case storage.SliceUploaded:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.UploadedAt.String())
		case storage.SliceImported:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.ImportedAt.String())
		default:
			panic(errors.Errorf(`unexpected slice state "%s"`, to))
		}

		from = to
	}
}
