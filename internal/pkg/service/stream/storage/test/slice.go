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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// sliceRepository interface to prevent package import cycle.
type sliceRepository interface {
	Get(k model.SliceKey) op.WithResult[model.Slice]
	StateTransition(sliceKey model.SliceKey, now time.Time, from, to model.SliceState) *op.AtomicOp[model.Slice]
}

func NewSliceKey() model.SliceKey {
	return NewSliceKeyOpenedAt("2000-01-01T20:00:00.000Z")
}

func NewSliceKeyOpenedAt(openedAtStr string) model.SliceKey {
	openedAt := utctime.MustParse(openedAtStr)
	return model.SliceKey{
		FileVolumeKey: model.FileVolumeKey{
			FileKey:  NewFileKeyOpenedAt("2000-01-01T19:00:00.000Z"),
			VolumeID: "my-volume",
		},
		SliceID: model.SliceID{
			OpenedAt: openedAt,
		},
	}
}

func NewSlice() *model.Slice {
	return NewSliceOpenedAt("2000-01-01T20:00:00.000Z")
}

func NewSliceOpenedAt(openedAt string) *model.Slice {
	return &model.Slice{
		SliceKey: NewSliceKeyOpenedAt(openedAt),
		Type:     model.FileTypeCSV,
		State:    model.SliceWriting,
		Columns: column.Columns{
			column.ID{},
			column.Headers{},
			column.Body{},
		},
		LocalStorage: local.Slice{
			Dir:                local.NormalizeDirPath(openedAt),
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

func SwitchSliceStates(t *testing.T, ctx context.Context, clk *clock.Mock, sliceRepo sliceRepository, sliceKey model.SliceKey, interval time.Duration, states []model.SliceState) {
	t.Helper()
	from := states[0]
	for _, to := range states[1:] {
		clk.Add(interval)

		// Slice must be closed by the Close method
		var slice model.Slice
		var err error
		if to == model.SliceClosing {
			require.Fail(t, "to switch slice to the SliceClosing state use: File Rotate/Close operations, or Slice Rotate operation")
		} else {
			slice, err = sliceRepo.StateTransition(sliceKey, clk.Now(), from, to).Do(ctx).ResultOrErr()
			require.NoError(t, err)
		}

		// Slice state has been switched
		assert.Equal(t, to, slice.State)

		// Retry should be reset
		assert.Equal(t, 0, slice.RetryAttempt)
		assert.Nil(t, slice.LastFailedAt)

		// Check timestamp
		switch to {
		case model.SliceClosing:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.ClosingAt.String())
		case model.SliceUploading:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.UploadingAt.String())
		case model.SliceUploaded:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.UploadedAt.String())
		case model.SliceImported:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.ImportedAt.String())
		default:
			panic(errors.Errorf(`unexpected slice state "%s"`, to))
		}

		from = to
	}
}

func StatsPerSlice(k model.SliceKey) statistics.PerSlice {
	return statistics.PerSlice{
		SliceKey: k,
		Value: statistics.Value{
			SlicesCount:      1,
			FirstRecordAt:    k.OpenedAt(),
			LastRecordAt:     k.OpenedAt().Add(time.Minute),
			RecordsCount:     100,
			UncompressedSize: 100 * datasize.MB,
			CompressedSize:   100 * datasize.MB,
		},
	}
}
