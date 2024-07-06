package reader_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	compressionReader "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression/reader"
	compressionWriter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/reader/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// TestVolume_NewReaderFor_Ok tests Volume.OpenReader method and Reader getters.
func TestVolume_NewReaderFor_Ok(t *testing.T) {
	t.Parallel()
	tc := test.NewReaderTestCase(t)

	r, err := tc.NewReader()
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
	tc := test.NewReaderTestCase(t)

	// Create the writer first time - ok
	w, err := tc.NewReader()
	require.NoError(t, err)
	assert.Len(t, tc.Volume.Readers(), 1)

	// Create writer for the same slice again - error
	_, err = tc.NewReader()
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
	tc := test.NewReaderTestCase(t)

	// Open volume
	vol, err := tc.OpenVolume()
	require.NoError(t, err)

	// Close the volume
	assert.NoError(t, vol.Close(context.Background()))

	// Try crate a reader
	_, err = tc.NewReader()
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
		},
		{
			Name:               "ZSTD_To_GZIP",
			LocalCompression:   compression.NewZSTDConfig(),
			StagingCompression: compression.NewGZIPConfig(),
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
	rtc := test.NewReaderTestCase(t)
	rtc.SliceData = localData.Bytes()
	rtc.Slice.LocalStorage.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Create reader
	r, err := rtc.NewReader()
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
	rtc := test.NewReaderTestCase(t)
	rtc.Slice.LocalStorage.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Create reader
	readError := errors.New("some read error")
	r, err := rtc.NewReader(volume.WithFileOpener(func(filePath string) (volume.File, error) {
		f := test.NewReaderTestFile(localData)
		f.ReadError = readError
		return f, nil
	}))
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
	rtc := test.NewReaderTestCase(t)
	rtc.Slice.LocalStorage.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Create reader
	closeError := errors.New("some close error")
	r, err := rtc.NewReader(volume.WithFileOpener(func(filePath string) (volume.File, error) {
		f := test.NewReaderTestFile(localData)
		f.CloseError = closeError
		return f, nil
	}))
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
