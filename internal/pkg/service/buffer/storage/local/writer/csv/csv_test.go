package csv_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/test/testcase"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
	"time"
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
	volume, err := writer.OpenVolume(ctx, log.NewNopLogger(), clock.New(), t.TempDir())
	require.NoError(t, err)

	// Create slice
	slice := testcase.NewTestSlice(volume)
	slice.Type = storage.FileTypeCSV
	slice.Columns = column.Columns{column.ID{Name: "id"}, column.Body{Name: "body"}} // <<<<< two columns
	val := validator.New()
	assert.NoError(t, val.Validate(ctx, slice))

	// Create writer
	w, err := volume.NewWriterFor(slice)
	require.NoError(t, err)

	// Write invalid number of values
	err = w.WriteRow([]any{"foo"})
	if assert.Error(t, err) {
		assert.Equal(t, `expected 2 columns in the row, given 1`, err.Error())
	}
}

func TestCSVWriter_CastToStringError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open volume
	volume, err := writer.OpenVolume(ctx, log.NewNopLogger(), clock.New(), t.TempDir())
	require.NoError(t, err)

	// Create slice
	slice := testcase.NewTestSlice(volume)
	slice.Type = storage.FileTypeCSV
	slice.Columns = column.Columns{column.ID{Name: "id"}}
	val := validator.New()
	assert.NoError(t, val.Validate(ctx, slice))

	// Create writer
	w, err := volume.NewWriterFor(slice)
	require.NoError(t, err)

	// Write invalid number of values
	err = w.WriteRow([]any{struct{}{}})
	if assert.Error(t, err) {
		assert.Equal(t, `cannot convert value of the column "id" to the string: unable to cast struct {}{} of type struct {} to string`, err.Error())
	}
}

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
				r, err := gzip.NewReader(r)
				require.NoError(t, err)
				return r
			},
		},
		{
			Name:   "zstd",
			Config: compression.DefaultZSTDConfig(),
			FileDecoder: func(t *testing.T, r io.Reader) io.Reader {
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
						t.Run(tc.Name, func(t *testing.T) {
							tc.Run(t)
						})
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
			assert.Equal(t, 4, strings.Count(fileContent, "\n"))
			assert.Contains(t, fileContent, "abc,123\n")
			assert.Contains(t, fileContent, "\"\"\"def\"\"\",456\n")
			assert.Contains(t, fileContent, "foo,bar\n")
			assert.Contains(t, fileContent, "xyz,false\n")
		}
	} else {
		validateFn = func(t *testing.T, fileContent string) {
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
