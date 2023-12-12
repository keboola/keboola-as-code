package csv_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/test/testcase"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open volume
	clk := clock.New()
	vol, err := volume.Open(ctx, log.NewNopLogger(), clk, writer.NewEvents(), volume.NewInfo(t.TempDir(), "hdd", "1"))
	require.NoError(t, err)

	// Create slice
	slice := testcase.NewTestSlice(vol)
	slice.Type = storage.FileTypeCSV
	slice.Columns = column.Columns{column.ID{Name: "id"}, column.Body{Name: "body"}} // <<<<< two columns
	val := validator.New()
	assert.NoError(t, val.Validate(ctx, slice))

	// Create writer
	w, err := vol.NewWriterFor(slice)
	require.NoError(t, err)

	// Write invalid number of values
	err = w.WriteRow(clk.Now(), []any{"foo"})
	if assert.Error(t, err) {
		assert.Equal(t, `expected 2 columns in the row, given 1`, err.Error())
	}
}

func TestCSVWriter_CastToStringError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open volume
	clk := clock.New()
	vol, err := volume.Open(ctx, log.NewNopLogger(), clock.New(), writer.NewEvents(), volume.NewInfo(t.TempDir(), "hdd", "1"))
	require.NoError(t, err)

	// Create slice
	slice := testcase.NewTestSlice(vol)
	slice.Type = storage.FileTypeCSV
	slice.Columns = column.Columns{column.ID{Name: "id"}}
	val := validator.New()
	assert.NoError(t, val.Validate(ctx, slice))

	// Create writer
	w, err := vol.NewWriterFor(slice)
	require.NoError(t, err)

	// Write invalid number of values
	err = w.WriteRow(clk.Now(), []any{struct{}{}})
	if assert.Error(t, err) {
		assert.Equal(t, `cannot convert value of the column "id" to the string: unable to cast struct {}{} of type struct {} to string`, err.Error())
	}
}

// TestCSVWriter_Close_WaitForWrites tests that the Close method waits for writes in progress
// and rows counter backup file contains the right value after the Close - count of the written rows.
func TestCSVWriter_Close_WaitForWrites(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create lock to defer file sync
	syncLock := &sync.Mutex{}

	// Open volume
	vol, err := volume.Open(
		ctx,
		log.NewNopLogger(),
		clock.New(),
		writer.NewEvents(),
		volume.NewInfo(t.TempDir(), "hdd", "1"),
		volume.WithFileOpener(func(filePath string) (volume.File, error) {
			file, err := volume.DefaultFileOpener(filePath)
			if err != nil {
				return nil, err
			}
			return &testFile{File: file, SyncLock: syncLock}, nil
		}))
	require.NoError(t, err)

	// Create slice
	slice := testcase.NewTestSlice(vol)
	slice.Type = storage.FileTypeCSV
	slice.Columns = column.Columns{column.ID{Name: "id"}}
	slice.LocalStorage.DiskSync.Mode = disksync.ModeDisk
	slice.LocalStorage.DiskSync.Wait = true
	val := validator.New()
	assert.NoError(t, val.Validate(ctx, slice))

	// Create writer
	w, err := vol.NewWriterFor(slice)
	require.NoError(t, err)

	// Block sync
	syncLock.Lock()

	// Start two parallel writes
	now := utctime.MustParse("2000-01-01T00:00:00.000Z").Time()
	go func() { assert.NoError(t, w.WriteRow(now, []any{"value"})) }()
	go func() { assert.NoError(t, w.WriteRow(now, []any{"value"})) }()
	assert.Eventually(t, func() bool {
		return w.Unwrap().(*csv.Writer).WaitingWriteOps() == 2
	}, time.Second, 5*time.Millisecond)

	// Close writer
	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		assert.NoError(t, w.Close())
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
	content, err := os.ReadFile(w.FilePath())
	assert.NoError(t, err)
	assert.Equal(t, "value\nvalue\n", string(content))

	// Check rows count file
	content, err = os.ReadFile(filesystem.Join(w.DirPath(), csv.RowsCounterFile))
	assert.NoError(t, err)
	assert.Equal(t, "2,2000-01-01T00:00:00.000Z,2000-01-01T00:00:00.000Z", string(content))
}

// nolint:tparallel // false positive
func TestCSVWriter(t *testing.T) {
	t.Parallel()

	compressions := []fileCompression{
		{
			Name:   "none",
			Config: compression.DefaultNoneConfig(),
		},
		{
			Name:   "gzip",
			Config: compression.DefaultGZIPConfig(),
			FileDecoder: func(t *testing.T, r io.Reader) io.Reader {
				t.Helper()
				r, err := gzip.NewReader(r)
				require.NoError(t, err)
				return r
			},
		},
		{
			Name:   "zstd",
			Config: compression.DefaultZSTDConfig(),
			FileDecoder: func(t *testing.T, r io.Reader) io.Reader {
				t.Helper()
				r, err := zstd.NewReader(r)
				require.NoError(t, err)
				return r
			},
		},
	}

	syncModes := []disksync.Mode{
		disksync.ModeDisabled,
		disksync.ModeDisk,
		disksync.ModeCache,
	}

	// Generate all possible combinations of the parameters
	// nolint:paralleltest // false positive
	for _, comp := range compressions {
		for _, syncMode := range syncModes {
			for _, syncWait := range []bool{false, true} {
				for _, parallelWrite := range []bool{false, true} {
					// Skip invalid combination
					if syncMode == disksync.ModeDisabled && syncWait {
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

func newTestCase(comp fileCompression, syncMode disksync.Mode, syncWait bool, parallelWrite bool) *testcase.WriterTestCase {
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
			assert.Contains(t, fileContent, "abc,123\n")
			assert.Contains(t, fileContent, "\"\"\"def\"\"\",456\n")
			assert.Contains(t, fileContent, "foo,bar\n")
			assert.Contains(t, fileContent, "xyz,false\n")
		}
	} else {
		validateFn = func(t *testing.T, fileContent string) {
			t.Helper()
			assert.Equal(t, "abc,123\n\"\"\"def\"\"\",456\nfoo,bar\nxyz,false\n", fileContent)
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
	var syncConfig disksync.Config
	switch syncMode {
	case disksync.ModeDisabled:
		syncConfig = disksync.Config{Mode: disksync.ModeDisabled}
	case disksync.ModeDisk:
		syncConfig = disksync.Config{
			Mode:            disksync.ModeDisk,
			Wait:            syncWait,
			CheckInterval:   5 * time.Millisecond,
			CountTrigger:    5000,
			BytesTrigger:    1 * datasize.MB,
			IntervalTrigger: intervalTrigger,
		}
	case disksync.ModeCache:
		syncConfig = disksync.Config{
			Mode:            disksync.ModeCache,
			Wait:            syncWait,
			CheckInterval:   5 * time.Millisecond,
			CountTrigger:    5000,
			BytesTrigger:    1 * datasize.MB,
			IntervalTrigger: intervalTrigger,
		}
	default:
		panic(errors.Errorf(`unexpected mode "%v"`, syncMode))
	}

	return &testcase.WriterTestCase{
		Name:     fmt.Sprintf("compression=%s,syncMode=%s,wait=%t,parallelWrite=%t", comp.Name, syncMode, syncWait, parallelWrite),
		FileType: storage.FileTypeCSV,
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
	volume.File
	SyncLock *sync.Mutex
}

func (f *testFile) Sync() error {
	f.SyncLock.Lock()
	defer f.SyncLock.Unlock()
	return f.File.Sync()
}
