package csv_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testcase"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type fileCompression struct {
	Name              string
	Config            compression.Config
	FileDecoder       func(t *testing.T, r io.Reader) io.Reader
	DisableValidation bool
}

type unknown struct {
	Name string `json:"name" validate:"required"`
}

type staticNotifier struct {
	notifier *notify.Notifier
}

func newStaticNotifier() *staticNotifier {
	return &staticNotifier{}
}

func (v unknown) ColumnType() column.Type {
	return column.Type("unknown")
}

func (v unknown) ColumnName() string {
	return v.Name
}

func (v unknown) IsPrimaryKey() bool {
	return true
}

func (s *staticNotifier) createStaticNotifier(_ context.Context) *notify.Notifier {
	if s.notifier == nil {
		notifier := notify.New()
		s.notifier = notifier
		return notifier
	}

	return s.notifier
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
		// ZSTD compression is not fully supported, we cannot test it, config/entities validation fails.
		//{
		//	Name:   "zstd",
		//	Config: compression.NewZSTDConfig(),
		//	FileDecoder: func(t *testing.T, r io.Reader) io.Reader {
		//		t.Helper()
		//		r, err := zstd.NewReader(r)
		//		require.NoError(t, err)
		//		return r
		//	},
		//	DisableValidation: true,
		// },
	}

	syncModes := []writesync.Mode{
		writesync.ModeCache,
		writesync.ModeDisk,
	}

	// Generate all possible combinations of the parameters
	// nolint:paralleltest // false positive
	for _, comp := range compressions {
		for _, syncMode := range syncModes {
			for _, syncWait := range []bool{false, true} {
				for _, parallelWrite := range []bool{false, true} {
					// Run test case
					if tc := newTestCase(comp, syncMode, syncWait, parallelWrite); tc != nil {
						t.Run(tc.Name, tc.Run)
					}
				}
			}
		}
	}
}

func TestCSVWriterAboveLimit(t *testing.T) {
	t.Parallel()

	// Input rows
	columns := table.Mapping{
		Columns: column.Columns{
			column.Datetime{Name: "datetime"},
			column.Body{Name: "body"},
		},
	}
	newNotifier := func(ctx context.Context) *notify.Notifier {
		return notify.New()
	}
	csvEncoder, err := csv.NewEncoder(0, 40*datasize.B, columns, io.Discard, newNotifier)
	require.NoError(t, err)
	record := recordctx.FromHTTP(
		utctime.MustParse("2000-01-01T03:00:00.000Z").Time(),
		&http.Request{Body: io.NopCloser(strings.NewReader("foobar"))},
	)
	_, err = csvEncoder.WriteRecord(record)
	require.NoError(t, err)

	record = recordctx.FromHTTP(
		utctime.MustParse("2000-01-01T03:00:00.000Z").Time(),
		&http.Request{Body: io.NopCloser(strings.NewReader("foobartoomuch"))},
	)
	_, err = csvEncoder.WriteRecord(record)
	assert.Equal(t, "too big CSV row, column: \"body\", row limit: 40 B", err.Error())
}

// TestCSVWriterDoNotGetNotifierBeforeWrite guarantees that notifier is always obtained after successful writeRow
// not before writeRow, during mapping CSV columns.
func TestCSVWriterDoNotGetNotifierBeforeWrite(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// Input rows
	columns := table.Mapping{
		Columns: column.Columns{
			column.Datetime{Name: "datetime"},
			column.Body{Name: "body"},
			unknown{Name: "path"},
		},
	}
	staticNotifier := newStaticNotifier()
	csvEncoder, err := csv.NewEncoder(0, 40*datasize.B, columns, io.Discard, staticNotifier.createStaticNotifier)
	require.NoError(t, err)
	record := recordctx.FromHTTP(
		utctime.MustParse("2000-01-01T03:00:00.000Z").Time(),
		&http.Request{Body: io.NopCloser(strings.NewReader("foobar"))},
	)
	notifier := staticNotifier.createStaticNotifier(ctx)
	notifier.Done(nil)
	r, err := csvEncoder.WriteRecord(record)
	require.Error(t, err)

	// It is expected that no notifier is returned before writing actual record, but after writing record
	if !assert.Nil(t, r.Notifier) {
		err = r.Notifier.Wait(ctx)
		require.NoError(t, err)
		return
	}
}

func newTestCase(comp fileCompression, syncMode writesync.Mode, syncWait bool, parallelWrite bool) *testcase.WriterTestCase {
	// Input rows
	columns := column.Columns{
		column.Datetime{Name: "datetime"},
		column.Body{Name: "body"},
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
			if assert.Equal(t, 4, strings.Count(fileContent, "\n")) {
				assert.Contains(t, fileContent, "\"2000-01-01T01:00:00.000Z\",\"abc\"\n")
				assert.Contains(t, fileContent, "\"2000-01-01T02:00:00.000Z\",\"\"\"def\"\"\"\n")
				assert.Contains(t, fileContent, "\"2000-01-01T03:00:00.000Z\",\"foo\"\n")
				assert.Contains(t, fileContent, "\"2000-01-01T04:00:00.000Z\",\"bar\"\n")
			}
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
