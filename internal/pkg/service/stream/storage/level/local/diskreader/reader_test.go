package diskreader_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	compressionReader "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression/reader"
	compressionWriter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression/writer"
	volumeModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// TestVolume_NewReaderFor_Ok tests Volume.OpenReader method and Reader getters.
func TestVolume_NewReaderFor_Ok(t *testing.T) {
	t.Parallel()
	tc := newReaderTestCase(t)

	r, err := tc.NewReader(false)
	require.NoError(t, err)
	assert.Len(t, tc.Volume.Readers(), 1)

	assert.Equal(t, tc.Slice.SliceKey, r.SliceKey())

	// Wait for file open
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		tc.Logger.AssertJSONMessages(collect, `
{"level":"debug","message":"opened file","volume.id":"my-volume","file.path":"%s","project.id":"123","branch.id":"456","source.id":"my-source","sink.id":"my-sink","file.id":"2000-01-01T19:00:00.000Z","slice.id":"2000-01-01T20:00:00.000Z"}
`)
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, r.Close(t.Context()))
	assert.Empty(t, tc.Volume.Readers())

	// Check logs
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		tc.Logger.AssertJSONMessages(collect, `
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"chain closed"}
`)
	}, 5*time.Second, 10*time.Millisecond)
}

// TestVolume_NewReaderFor_Duplicate tests that only one reader for a slice can exist simultaneously.
func TestVolume_NewReaderFor_Duplicate(t *testing.T) {
	t.Parallel()
	tc := newReaderTestCase(t)

	// Create the writer first time - ok
	w, err := tc.NewReader(false)
	require.NoError(t, err)
	assert.Len(t, tc.Volume.Readers(), 1)

	// Create writer for the same slice again - error
	_, err = tc.NewReader(false)
	if assert.Error(t, err) {
		assert.Equal(t, `reader for slice "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z" already exists`, err.Error())
	}
	assert.Len(t, tc.Volume.Readers(), 1)
	require.NoError(t, w.Close(t.Context()))
	assert.Empty(t, tc.Volume.Readers())
}

// TestVolume_NewReaderFor_ClosedVolume tests that a new reader cannot be created on closed volume.
func TestVolume_NewReaderFor_ClosedVolume(t *testing.T) {
	t.Parallel()
	tc := newReaderTestCase(t)

	// Open volume
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Close the volume
	require.NoError(t, vol.Close(t.Context()))

	// Try crate a reader
	_, err = tc.NewReader(false)
	if assert.Error(t, err) {
		wildcards.Assert(t, "reader for slice \"%s\" cannot be created: volume is closed:\n- context canceled", err.Error())
	}
}

// TestVolume_NewReaderFor_MultipleFilesVolume tests that a new reader works on multiple files.
func TestVolume_NewReaderFor_MultipleFilesSingleVolume(t *testing.T) {
	t.Parallel()
	tc := newReaderTestCase(t)
	tc.Slice = test.NewSliceOpenedAt("2000-01-01T20:00:00.000Z")
	tc.Files = []string{"my-node1", "my-node2"}

	r, err := tc.NewReader(false)
	require.NoError(t, err)
	assert.Len(t, tc.Volume.Readers(), 1)

	assert.Equal(t, tc.Slice.SliceKey, r.SliceKey())

	require.NoError(t, r.Close(t.Context()))
	assert.Empty(t, tc.Volume.Readers())

	// Check logs
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		tc.Logger.AssertJSONMessages(collect, `
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file","volume.id":"my-volume","file.path":"%sslice-my-node%d.csv","project.id":"123","branch.id":"456","source.id":"my-source","sink.id":"my-sink","file.id":"2000-01-01T19:00:00.000Z","slice.id":"2000-01-01T20:00:00.000Z"}
{"level":"debug","message":"opened file","volume.id":"my-volume","file.path":"%sslice-my-node%d.csv","project.id":"123","branch.id":"456","source.id":"my-source","sink.id":"my-sink","file.id":"2000-01-01T19:00:00.000Z","slice.id":"2000-01-01T20:00:00.000Z"}
	`)
	}, 5*time.Second, 10*time.Millisecond)
}

// TestVolume_NewBackupReader_NoIssue tests that a new reader works with backup reader.
func TestVolume_NewBackupReader_NoIssue(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("hidden files work different on Windows")
	}

	// Prepare writer
	localData := bytes.NewBuffer(nil)
	var localWriter io.Writer = localData

	// Create GZIP writer
	gzipWriter := gzip.NewWriter(localWriter)
	_, err := gzipWriter.Write([]byte("foo bar"))
	require.NoError(t, err)
	require.NoError(t, gzipWriter.Close())

	tc := newReaderTestCase(t)
	tc.Slice.Encoding.Compression = compression.NewGZIPConfig()
	tc.SliceData = localData.Bytes()
	tc.WithBackup = true

	r, err := tc.NewReader(false)
	require.NoError(t, err)
	assert.Len(t, tc.Volume.Readers(), 1)

	assert.Equal(t, tc.Slice.SliceKey, r.SliceKey())

	// Check logs
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		tc.Logger.AssertJSONMessages(collect, `
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"moved hidden file \"%s.slice-my-node.csv\" to \"%s\"","volume.id":"my-volume","project.id":"123","branch.id":"456","source.id":"my-source","sink.id":"my-sink","file.id":"2000-01-01T19:00:00.000Z","slice.id":"2000-01-01T20:00:00.000Z"}
{"level":"debug","message":"opened file","volume.id":"my-volume","file.path":"%sslice-my-node.csv","project.id":"123","branch.id":"456","source.id":"my-source","sink.id":"my-sink","file.id":"2000-01-01T19:00:00.000Z","slice.id":"2000-01-01T20:00:00.000Z"}
	`)
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, r.Close(t.Context()))

	assert.Empty(t, tc.Volume.Readers())
}

// TestVolume_NewReader_CompressionIssue tests that a new reader with wrong compression works with backup reader.
func TestVolume_NewBackupReader_CompressionIssue(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("hidden files work different on Windows")
	}

	// Prepare writer
	localData := bytes.NewBuffer(nil)
	var localWriter io.Writer = localData

	// Write corrupted data
	// Add GZIP header: magic numbers + method + flags + mtime + xfl + os
	gzipHeader := []byte{0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	_, err := localWriter.Write(gzipHeader)
	require.NoError(t, err)
	_, err = localWriter.Write([]byte("foo bar"))
	require.NoError(t, err)

	tc := newReaderTestCase(t)
	tc.SliceData = localData.Bytes()
	tc.Slice.Encoding.Compression = compression.NewGZIPConfig()
	tc.Slice.StagingStorage.Compression = compression.NewNoneConfig()
	tc.WithBackup = true

	_, err = tc.NewReader(false)
	require.Error(t, err)

	// Check logs
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		tc.Logger.AssertJSONMessages(collect, `
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"error","message":"check of hidden file \"%s\" failed: cannot read hidden compressed file \"%s\": flate: corrupt input before offset 1"}
`)
	}, 5*time.Second, 10*time.Millisecond)
}

// TestVolume_NewReaderFor_Compression tests multiple local and staging compression combinations.
func TestVolume_NewReaderFor_Compression(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := []*compressionTestCase{
		{
			Name:               "None_To_None",
			LocalCompression:   compression.NewNoneConfig(),
			StagingCompression: compression.NewNoneConfig(),
		},
		{
			Name:               "None_To_GZIP",
			LocalCompression:   compression.NewNoneConfig(),
			StagingCompression: compression.NewGZIPConfig(),
		},
		{
			Name:               "None_To_ZSTD",
			LocalCompression:   compression.NewNoneConfig(),
			StagingCompression: compression.NewZSTDConfig(),
			DisableValidation:  true, // zstd is not currently considered valid
		},
		{
			Name:               "GZIP_To_None",
			LocalCompression:   compression.NewGZIPConfig(),
			StagingCompression: compression.NewNoneConfig(),
			UseBackupReader:    true,
		},
		{
			Name:               "ZSTD_To_None",
			LocalCompression:   compression.NewZSTDConfig(),
			StagingCompression: compression.NewNoneConfig(),
			DisableValidation:  true, // zstd is not currently considered valid
			UseBackupReader:    true,
		},
		{
			Name:               "ZSTD_To_GZIP",
			LocalCompression:   compression.NewZSTDConfig(),
			StagingCompression: compression.NewGZIPConfig(),
			DisableValidation:  true, // zstd is not currently considered valid
			UseBackupReader:    true,
		},
		{
			Name:               "GZIP_To_None_Backup",
			LocalCompression:   compression.NewGZIPConfig(),
			StagingCompression: compression.NewNoneConfig(),
			UseBackupReader:    true,
		},
	}

	// Run test cases for OK/ReadError/CloseError scenarios
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			t.Run("Ok", tc.TestOk)
			t.Run("ReadError", tc.TestReadError)
			t.Run("CloseError", tc.TestCloseError)
		})
	}
}

type compressionTestCase struct {
	Name               string
	UseBackupReader    bool
	LocalCompression   compression.Config
	StagingCompression compression.Config
	DisableValidation  bool
}

// TestOk tests successful read chain.
func (tc *compressionTestCase) TestOk(t *testing.T) {
	t.Parallel()

	// Prepare writer
	localData := bytes.NewBuffer(nil)
	var localWriter io.Writer = localData

	// Create encoder, if any
	var err error
	var encoder io.WriteCloser
	if tc.LocalCompression.Type != compression.TypeNone {
		encoder, err = compressionWriter.New(localData, tc.LocalCompression)
		require.NoError(t, err)
		localWriter = encoder
	}

	// Write data
	_, err = localWriter.Write([]byte("foo bar"))
	require.NoError(t, err)
	if encoder != nil {
		require.NoError(t, encoder.Close())
	}

	// Setup slice
	rtc := newReaderTestCase(t)
	rtc.WithBackup = tc.UseBackupReader
	rtc.SliceData = localData.Bytes()
	rtc.Slice.Encoding.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Create reader
	r, err := rtc.NewReader(tc.DisableValidation)
	require.NoError(t, err)

	// Read all
	var buf bytes.Buffer
	_, err = r.WriteTo(&buf)
	require.NoError(t, err)

	// Decode
	content := buf.Bytes()
	if tc.StagingCompression.Type != compression.TypeNone {
		decoder, err := compressionReader.New(&buf, tc.StagingCompression)
		require.NoError(t, err)
		content, err = io.ReadAll(decoder)
		require.NoError(t, err)
	}

	// Check content
	assert.Equal(t, []byte("foo bar"), content)

	// Close
	require.NoError(t, r.Close(t.Context()))
}

// TestReadError tests propagation of the file read error through read chain.
func (tc *compressionTestCase) TestReadError(t *testing.T) {
	t.Parallel()

	// Prepare writer
	localData := bytes.NewBuffer(nil)
	var localWriter io.Writer = localData

	// Create encoder, if any
	var err error
	var encoder io.WriteCloser
	if tc.LocalCompression.Type != compression.TypeNone {
		encoder, err = compressionWriter.New(localData, tc.LocalCompression)
		require.NoError(t, err)
		localWriter = encoder
	}

	// Write data
	_, err = localWriter.Write([]byte("foo bar"))
	require.NoError(t, err)
	if encoder != nil {
		require.NoError(t, encoder.Close())
	}

	// Setup slice
	rtc := newReaderTestCase(t)
	rtc.WithBackup = tc.UseBackupReader
	rtc.Slice.Encoding.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Replace file opener
	readError := errors.New("some read error")
	rtc.Config.OverrideFileOpener = diskreader.FileOpenerFn(func(filePath string) (diskreader.File, error) {
		f := newTestFile(localData)
		f.ReadError = readError
		return f, nil
	})

	// Create reader, which inspects the file when compressed
	// Make sure that the file is compressed correctly
	r, err := rtc.NewReader(tc.DisableValidation)
	if rtc.WithBackup && err != nil {
		require.Contains(t, err.Error(), readError.Error())
		return
	}

	require.NoError(t, err)

	// Read all
	var buf bytes.Buffer
	if _, err = r.WriteTo(&buf); assert.Error(t, err) {
		assert.Contains(t, err.Error(), readError.Error())
	}

	// Close
	require.NoError(t, r.Close(t.Context()))
}

// TestCloseError tests propagation of the file close error through read chain.
func (tc *compressionTestCase) TestCloseError(t *testing.T) {
	t.Parallel()

	// Prepare writer
	localData := bytes.NewBuffer(nil)
	var localWriter io.Writer = localData

	// Create encoder, if any
	var err error
	var encoder io.WriteCloser
	if tc.LocalCompression.Type != compression.TypeNone {
		encoder, err = compressionWriter.New(localData, tc.LocalCompression)
		require.NoError(t, err)
		localWriter = encoder
	}

	// Write data
	_, err = localWriter.Write([]byte("foo bar"))
	require.NoError(t, err)
	if encoder != nil {
		require.NoError(t, encoder.Close())
	}

	// Setup slice
	rtc := newReaderTestCase(t)
	rtc.WithBackup = tc.UseBackupReader
	rtc.Slice.Encoding.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Replace file opener
	closeError := errors.New("some close error")
	rtc.Config.OverrideFileOpener = diskreader.FileOpenerFn(func(filePath string) (diskreader.File, error) {
		f := newTestFile(localData)
		f.CloseError = closeError
		return f, nil
	})

	// Create reader, which inspects the file when compressed
	// Make sure that the file is compressed correctly
	r, err := rtc.NewReader(tc.DisableValidation)
	if rtc.WithBackup && err != nil {
		require.Contains(t, err.Error(), closeError.Error())
		return
	}

	require.NoError(t, err)

	// Read all
	var buf bytes.Buffer
	_, err = r.WriteTo(&buf)
	if assert.Error(t, err) {
		assert.Equal(t, closeError, err)
	}
}

// readerTestCase is a helper to open disk reader in tests.
type readerTestCase struct {
	*volumeTestCase
	Volume    *diskreader.Volume
	Slice     *model.Slice
	SliceData []byte
	Files     []string
}

func newReaderTestCase(tb testing.TB) *readerTestCase {
	tb.Helper()
	tc := &readerTestCase{}
	tc.volumeTestCase = newVolumeTestCase(tb)
	tc.Slice = test.NewSlice()
	tc.Files = []string{"my-node"}
	return tc
}

func (tc *readerTestCase) OpenVolume() (*diskreader.Volume, error) {
	// Write file with the ID
	require.NoError(tc.TB, os.WriteFile(filepath.Join(tc.VolumePath, volumeModel.IDFile), []byte("my-volume"), 0o640))

	vol, err := tc.volumeTestCase.OpenVolume()
	tc.Volume = vol

	return vol, err
}

func (tc *readerTestCase) NewReader(disableValidation bool) (diskreader.Reader, error) {
	if tc.Volume == nil {
		// Open volume
		vol, err := tc.OpenVolume()
		require.NoError(tc.TB, err)

		// Close volume after the test
		tc.TB.Cleanup(func() {
			require.NoError(tc.TB, vol.Close(context.Background()))
		})
	}

	if !disableValidation {
		// Slice definition must be valid
		val := validator.New()
		require.NoError(tc.TB, val.Validate(context.Background(), tc.Slice))
	}

	// Write slice data
	require.NoError(tc.TB, os.MkdirAll(tc.Slice.LocalStorage.DirName(tc.VolumePath), 0o750))
	for _, file := range tc.Files {
		if tc.WithBackup {
			require.NoError(tc.TB, os.WriteFile(tc.Slice.LocalStorage.FileNameWithBackup(tc.VolumePath, file), tc.SliceData, 0o640))
		} else {
			require.NoError(tc.TB, os.WriteFile(tc.Slice.LocalStorage.FileName(tc.VolumePath, file), tc.SliceData, 0o640))
		}
	}

	r, err := tc.Volume.OpenReader(tc.Slice.SliceKey, tc.Slice.LocalStorage, tc.Slice.Encoding.Compression, tc.Slice.StagingStorage.Compression)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// testFile provides implementation of the File interface for tests.
type testFile struct {
	reader     io.Reader
	SeekError  error
	ReadError  error
	CloseError error
}

func newTestFile(r io.Reader) *testFile {
	return &testFile{reader: r}
}

func (r *testFile) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if r.ReadError != nil && (n == 0 || errors.Is(err, io.EOF)) {
		return 0, r.ReadError
	}
	return n, err
}

func (r *testFile) Seek(offset int64, whence int) (int64, error) {
	if r.SeekError != nil {
		return 0, r.SeekError
	}

	return 0, nil
}

func (r *testFile) Close() error {
	return r.CloseError
}
