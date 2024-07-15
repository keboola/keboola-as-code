package csv_test

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testcase"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type fileCompression struct {
	Name              string
	Config            compression.Config
	FileDecoder       func(t *testing.T, r io.Reader) io.Reader
	DisableValidation bool
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
			DisableValidation: true,
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
	columns := column.Columns{
		column.Datetime{},
		column.Body{},
	}
	data := []testcase.RecordsBatch{
		{
			Parallel: parallelWrite,
			Records: []recordctx.Context{
				recordctx.FromHTTP(
					utctime.MustParse("2000-01-01T01:00:00.000Z").Time(),
					&http.Request{Body: io.NopCloser(strings.NewReader("abc"))},
				),
				recordctx.FromHTTP(
					utctime.MustParse("2000-01-01T02:00:00.000Z").Time(),
					&http.Request{Body: io.NopCloser(strings.NewReader(`"def"`))},
				),
			},
		},
		{
			Parallel: parallelWrite,
			Records: []recordctx.Context{
				recordctx.FromHTTP(
					utctime.MustParse("2000-01-01T03:00:00.000Z").Time(),
					&http.Request{Body: io.NopCloser(strings.NewReader("foo"))},
				),
				recordctx.FromHTTP(
					utctime.MustParse("2000-01-01T04:00:00.000Z").Time(),
					&http.Request{Body: io.NopCloser(strings.NewReader("bar"))},
				),
			},
		},
	}

	// Expected file content
	var validateFn func(t *testing.T, fileContent string)
	if parallelWrite {
		validateFn = func(t *testing.T, fileContent string) {
			t.Helper()
			assert.Equal(t, 4, strings.Count(fileContent, "\n"))
			assert.Contains(t, fileContent, "\"2000-01-01T01:00:00.000Z\",\"abc\"\n")
			assert.Contains(t, fileContent, "\"2000-01-01T02:00:00.000Z\",\"\"\"def\"\"\"\n")
			assert.Contains(t, fileContent, "\"2000-01-01T03:00:00.000Z\",\"foo\"\n")
			assert.Contains(t, fileContent, "\"2000-01-01T04:00:00.000Z\",\"bar\"\n")
		}
	} else {
		validateFn = func(t *testing.T, fileContent string) {
			t.Helper()
			assert.Equal(t, "\"2000-01-01T01:00:00.000Z\",\"abc\"\n\"2000-01-01T02:00:00.000Z\",\"\"\"def\"\"\"\n\"2000-01-01T03:00:00.000Z\",\"foo\"\n\"2000-01-01T04:00:00.000Z\",\"bar\"\n", fileContent)
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
		Name:              fmt.Sprintf("compression-%s-sync-%s-wait-%t-parallel-%t", comp.Name, syncMode, syncWait, parallelWrite),
		FileType:          model.FileTypeCSV,
		Columns:           columns,
		Allocate:          1 * datasize.MB,
		Sync:              syncConfig,
		Compression:       comp.Config,
		Data:              data,
		FileDecoder:       comp.FileDecoder,
		DisableValidation: comp.DisableValidation,
		Validator:         validateFn,
	}
}
