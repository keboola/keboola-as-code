package csv_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/sourcenode/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/test/testcase"
	writerVolume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type fileCompression struct {
	Name        string
	Config      compression.Config
	FileDecoder func(t *testing.T, r io.Reader) io.Reader
}

func TestCSVWriter_InvalidNumberOfValues(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open volume
	clk := clock.New()
	spec := volume.Spec{NodeID: "my-node", Path: t.TempDir(), Type: "hdd", Label: "001"}
	vol, err := writerVolume.Open(ctx, log.NewNopLogger(), clk, writer.NewEvents(), writer.NewConfig(), spec)
	require.NoError(t, err)

	// Create slice
	slice := testcase.NewTestSlice(vol)
	slice.Type = model.FileTypeCSV
	slice.Columns = column.Columns{column.UUID{Name: "id"}, column.Body{Name: "body"}} // <<<<< two columns
	val := validator.New()
	assert.NoError(t, val.Validate(ctx, slice))

	// Create writer
	w, err := vol.OpenWriter(slice)
	require.NoError(t, err)

	// Write invalid number of values
	err = w.WriteRecord(clk.Now(), []any{"foo"})
	if assert.Error(t, err) {
		assert.Equal(t, `expected 2 columns in the row, given 1`, err.Error())
	}

	// Close volume
	assert.NoError(t, vol.Close(ctx))
}

func TestCSVWriter_CastToStringError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open volume
	clk := clock.New()
	spec := volume.Spec{NodeID: "my-node", Path: t.TempDir(), Type: "hdd", Label: "001"}
	vol, err := writerVolume.Open(ctx, log.NewNopLogger(), clock.New(), writer.NewEvents(), writer.NewConfig(), spec)
	require.NoError(t, err)

	// Create slice
	slice := testcase.NewTestSlice(vol)
	slice.Type = model.FileTypeCSV
	slice.Columns = column.Columns{column.UUID{Name: "id"}}
	val := validator.New()
	assert.NoError(t, val.Validate(ctx, slice))

	// Create writer
	w, err := vol.OpenWriter(slice)
	require.NoError(t, err)

	// Write invalid number of values
	err = w.WriteRecord(clk.Now(), []any{struct{}{}})
	if assert.Error(t, err) {
		assert.Equal(t, `cannot convert value of the column "id" to the string: unable to cast struct {}{} of type struct {} to string`, err.Error())
	}

	// Close volume
	assert.NoError(t, vol.Close(ctx))
}

// TestCSVWriter_Close_WaitForWrites tests that the Close method waits for writes in progress
// and rows counter backup file contains the right value after the Close - count of the written rows.
func TestCSVWriter_Close_WaitForWrites(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create lock to defer file sync
	syncLock := &sync.Mutex{}

	// Open volume
	volPath := t.TempDir()
	vol, err := writerVolume.Open(
		ctx,
		log.NewNopLogger(),
		clock.New(),
		writer.NewEvents(),
		writer.NewConfig(),
		volume.Spec{NodeID: "my-node", Path: volPath, Type: "hdd", Label: "001"},
		writerVolume.WithFileOpener(func(filePath string) (writerVolume.File, error) {
			file, err := writerVolume.DefaultFileOpener(filePath)
			if err != nil {
				return nil, err
			}
			return &testFile{File: file, SyncLock: syncLock}, nil
		}))
	require.NoError(t, err)

	// Create slice
	slice := testcase.NewTestSlice(vol)
	slice.Type = model.FileTypeCSV
	slice.Columns = column.Columns{column.UUID{Name: "id"}}
	slice.LocalStorage.DiskSync.Mode = writesync.ModeDisk
	slice.LocalStorage.DiskSync.Wait = true
	// prevent sync during the test
	slice.LocalStorage.DiskSync.CheckInterval = duration.From(2 * time.Second)
	slice.LocalStorage.DiskSync.IntervalTrigger = duration.From(2 * time.Second)
	val := validator.New()
	assert.NoError(t, val.Validate(ctx, slice))
	filePath := filepath.Join(volPath, slice.LocalStorage.Dir, slice.LocalStorage.Filename)

	// Create writer
	w, err := vol.OpenWriter(slice)
	require.NoError(t, err)

	// Block sync
	syncLock.Lock()

	// Start two parallel writes
	now := utctime.MustParse("2000-01-01T00:00:00.000Z").Time()
	go func() { assert.NoError(t, w.WriteRecord(now, []any{"value"})) }()
	go func() { assert.NoError(t, w.WriteRecord(now, []any{"value"})) }()
	assert.Eventually(t, func() bool {
		return w.AcceptedWrites() == 2
	}, time.Second, 5*time.Millisecond)

	// Close writer
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		assert.NoError(t, w.Close(ctx))
	}()

	// Unblock sync and wait for Close
	syncLock.Unlock()
	select {
	case <-closeDone:
		// ok
	case <-time.After(time.Second):
		assert.Fail(t, "timeout")
	}

	// Check file content
	content, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, "\"value\"\n\"value\"\n", string(content))

	// Check statistics

	assert.Equal(t, uint64(2), w.AcceptedWrites())
	assert.Equal(t, uint64(2), w.CompletedWrites())
	assert.Equal(t, "2000-01-01T00:00:00.000Z", w.FirstRecordAt().String())
	assert.Equal(t, "2000-01-01T00:00:00.000Z", w.LastRecordAt().String())

	// Close volume
	assert.NoError(t, vol.Close(ctx))
}

// nolint:tparallel // false positive
func TestCSVWriter(t *testing.T) {
	t.Parallel()

	compressions := []fileCompression{
		{
			Name:   "none",
			Config: compression.NewNoneConfig(),
		},
		{
			Name:   "gzip",
			Config: compression.NewGZIPConfig(),
			FileDecoder: func(t *testing.T, r io.Reader) io.Reader {
				t.Helper()
				r, err := gzip.NewReader(r)
				require.NoError(t, err)
				return r
			},
		},
		{
			Name:   "zstd",
			Config: compression.NewZSTDConfig(),
			FileDecoder: func(t *testing.T, r io.Reader) io.Reader {
				t.Helper()
				r, err := zstd.NewReader(r)
				require.NoError(t, err)
				return r
			},
		},
	}

	syncModes := []writesync.Mode{
		writesync.ModeDisabled,
		writesync.ModeDisk,
		writesync.ModeCache,
	}

	// Generate all possible combinations of the parameters
	// nolint:paralleltest // false positive
	for _, comp := range compressions {
		for _, syncMode := range syncModes {
			for _, syncWait := range []bool{false, true} {
				for _, parallelWrite := range []bool{false, true} {
					// Skip invalid combination
					if syncMode == writesync.ModeDisabled && syncWait {
						continue
					}

					// Run test case
					if tc := newTestCase(comp, syncMode, syncWait, parallelWrite); tc != nil {
						t.Run(tc.Name, tc.Run)
					}
				}
			}
		}
	}
}

func newTestCase(comp fileCompression, syncMode writesync.Mode, syncWait bool, parallelWrite bool) *testcase.WriterTestCase {
	// Input rows
	data := []testcase.RowBatch{
		{
			Parallel: parallelWrite,
			Rows:     [][]any{{"abc", 123}, {`"def"`, 456}},
		},
		{
			Parallel: parallelWrite,
			Rows:     [][]any{{"foo", "bar"}, {`xyz`, false}},
		},
	}

	// Expected file content
	var validateFn func(t *testing.T, fileContent string)
	if parallelWrite {
		validateFn = func(t *testing.T, fileContent string) {
			t.Helper()
			assert.Equal(t, 4, strings.Count(fileContent, "\n"))
			assert.Contains(t, fileContent, "\"abc\",\"123\"\n")
			assert.Contains(t, fileContent, "\"\"\"def\"\"\",\"456\"\n")
			assert.Contains(t, fileContent, "\"foo\",\"bar\"\n")
			assert.Contains(t, fileContent, "\"xyz\",\"false\"\n")
		}
	} else {
		validateFn = func(t *testing.T, fileContent string) {
			t.Helper()
			assert.Equal(t, "\"abc\",\"123\"\n\"\"\"def\"\"\",\"456\"\n\"foo\",\"bar\"\n\"xyz\",\"false\"\n", fileContent)
		}
	}

	// Set sync interval
	var intervalTrigger time.Duration
	if syncWait {
		// Trigger sync by the intervalTrigger if syncWait=true
		intervalTrigger = 10 * time.Millisecond
	} else {
		// Other writes are not blocking, sync is triggered by the writer Close.
		intervalTrigger = 1 * time.Second
	}

	// Sync config
	var syncConfig writesync.Config
	switch syncMode {
	case writesync.ModeDisabled:
		syncConfig = writesync.Config{Mode: writesync.ModeDisabled}
	case writesync.ModeDisk:
		syncConfig = writesync.Config{
			Mode:                     writesync.ModeDisk,
			Wait:                     syncWait,
			CheckInterval:            duration.From(5 * time.Millisecond),
			CountTrigger:             5000,
			UncompressedBytesTrigger: 10 * datasize.MB,
			CompressedBytesTrigger:   1 * datasize.MB,
			IntervalTrigger:          duration.From(intervalTrigger),
		}
	case writesync.ModeCache:
		syncConfig = writesync.Config{
			Mode:                     writesync.ModeCache,
			Wait:                     syncWait,
			CheckInterval:            duration.From(5 * time.Millisecond),
			CountTrigger:             5000,
			UncompressedBytesTrigger: 10 * datasize.MB,
			CompressedBytesTrigger:   1 * datasize.MB,
			IntervalTrigger:          duration.From(intervalTrigger),
		}
	default:
		panic(errors.Errorf(`unexpected mode "%v"`, syncMode))
	}

	return &testcase.WriterTestCase{
		Name:     fmt.Sprintf("compression-%s-sync-%s-wait-%t-parallel-%t", comp.Name, syncMode, syncWait, parallelWrite),
		FileType: model.FileTypeCSV,
		Columns: column.Columns{
			// 2 columns, only the count is important for CSV
			column.Body{},
			column.Template{},
		},
		Allocate:    1 * datasize.MB,
		Sync:        syncConfig,
		Compression: comp.Config,
		Data:        data,
		FileDecoder: comp.FileDecoder,
		Validator:   validateFn,
	}
}

type testFile struct {
	writerVolume.File
	SyncLock *sync.Mutex
}

func (f *testFile) Sync() error {
	f.SyncLock.Lock()
	defer f.SyncLock.Unlock()
	return f.File.Sync()
}
