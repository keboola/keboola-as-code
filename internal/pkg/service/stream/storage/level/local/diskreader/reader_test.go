package diskreader_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
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

	assert.NoError(t, r.Close(context.Background()))
	assert.Len(t, tc.Volume.Readers(), 0)

	// Check logs
	tc.AssertLogs(`
{"level":"info","message":"opening volume"}
{"level":"info","message":"opened volume"}
{"level":"debug","message":"opened file","volume.id":"my-volume","file.path":"%s","projectId":"123","branchId":"456","sourceId":"my-source","sinkId":"my-sink","fileId":"2000-01-01T19:00:00.000Z","sliceId":"2000-01-01T20:00:00.000Z"}
{"level":"debug","message":"closing chain"}
{"level":"debug","message":"chain closed"}
`)
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

	assert.NoError(t, w.Close(context.Background()))
	assert.Len(t, tc.Volume.Readers(), 0)
}

// TestVolume_NewReaderFor_ClosedVolume tests that a new reader cannot be created on closed volume.
func TestVolume_NewReaderFor_ClosedVolume(t *testing.T) {
	t.Parallel()
	tc := newReaderTestCase(t)

	// Open volume
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Close the volume
	assert.NoError(t, vol.Close(context.Background()))

	// Try crate a reader
	_, err = tc.NewReader(false)
	if assert.Error(t, err) {
		wildcards.Assert(t, "reader for slice \"%s\" cannot be created: volume is closed:\n- context canceled", err.Error())
	}
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
		},
		{
			Name:               "ZSTD_To_None",
			LocalCompression:   compression.NewZSTDConfig(),
			StagingCompression: compression.NewNoneConfig(),
			DisableValidation:  true, // zstd is not currently considered valid
		},
		{
			Name:               "ZSTD_To_GZIP",
			LocalCompression:   compression.NewZSTDConfig(),
			StagingCompression: compression.NewGZIPConfig(),
			DisableValidation:  true, // zstd is not currently considered valid
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
	rtc.SliceData = localData.Bytes()
	rtc.Slice.LocalStorage.Encoding.Compression = tc.LocalCompression
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
	assert.NoError(t, r.Close(context.Background()))
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
	rtc.Slice.LocalStorage.Encoding.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Replace file opener
	readError := errors.New("some read error")
	rtc.Config.OverrideFileOpener = diskreader.FileOpenerFn(func(filePath string) (diskreader.File, error) {
		f := newTestFile(localData)
		f.ReadError = readError
		return f, nil
	})

	// Create reader
	r, err := rtc.NewReader(tc.DisableValidation)
	require.NoError(t, err)

	// Read all
	var buf bytes.Buffer
	if _, err = r.WriteTo(&buf); assert.Error(t, err) {
		assert.Equal(t, "some read error", err.Error())
	}

	// Close
	assert.NoError(t, r.Close(context.Background()))
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
	rtc.Slice.LocalStorage.Encoding.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Replace file opener
	closeError := errors.New("some close error")
	rtc.Config.OverrideFileOpener = diskreader.FileOpenerFn(func(filePath string) (diskreader.File, error) {
		f := newTestFile(localData)
		f.CloseError = closeError
		return f, nil
	})

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
	err = r.Close(context.Background())
	if assert.Error(t, err) {
		assert.Equal(t, "chain close error: cannot close file: some close error", err.Error())
	}
}

// readerTestCase is a helper to open disk reader in tests.
type readerTestCase struct {
	*volumeTestCase
	Volume    *diskreader.Volume
	Slice     *model.Slice
	SliceData []byte
}

func newReaderTestCase(tb testing.TB) *readerTestCase {
	tb.Helper()
	tc := &readerTestCase{}
	tc.volumeTestCase = newVolumeTestCase(tb)
	tc.Slice = test.NewSlice()
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
			assert.NoError(tc.TB, vol.Close(context.Background()))
		})
	}

	if !disableValidation {
		// Slice definition must be valid
		val := validator.New()
		require.NoError(tc.TB, val.Validate(context.Background(), tc.Slice))
	}

	// Write slice data
	path := filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir, tc.Slice.LocalStorage.Filename)
	assert.NoError(tc.TB, os.MkdirAll(filepath.Dir(path), 0o750))
	assert.NoError(tc.TB, os.WriteFile(path, tc.SliceData, 0o640))

	r, err := tc.Volume.OpenReader(tc.Slice)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// testFile provides implementation of the File interface for tests.
type testFile struct {
	reader     io.Reader
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

func (r *testFile) Close() error {
	return r.CloseError
}
