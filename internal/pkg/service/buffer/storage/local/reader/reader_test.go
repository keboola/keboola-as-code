package reader

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	compressionReader "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression/reader"
	compressionWriter "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// TestVolume_NewReaderFor_Ok tests Volume.NewReaderFor method and SliceReader getters.
func TestVolume_NewReaderFor_Ok(t *testing.T) {
	t.Parallel()
	tc := newReaderTestCase(t)

	w, err := tc.NewReader()
	assert.NoError(t, err)
	assert.Len(t, tc.Volume.readers, 1)

	assert.Equal(t, tc.Slice.SliceKey, w.SliceKey())
	assert.Equal(t, filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir), w.DirPath())
	assert.Equal(t, filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir, tc.Slice.LocalStorage.Filename), w.FilePath())

	assert.NoError(t, w.Close())
	assert.Len(t, tc.Volume.readers, 0)
}

// TestVolume_NewReaderFor_Duplicate tests that only one reader for a slice can exist simultaneously.
func TestVolume_NewReaderFor_Duplicate(t *testing.T) {
	t.Parallel()
	tc := newReaderTestCase(t)

	// Create the writer first time - ok
	w, err := tc.NewReader()
	assert.NoError(t, err)
	assert.Len(t, tc.Volume.readers, 1)

	// Create writer for the same slice again - error
	_, err = tc.NewReader()
	if assert.Error(t, err) {
		assert.Equal(t, `reader for slice "123/my-receiver/my-export/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z" already exists`, err.Error())
	}
	assert.Len(t, tc.Volume.readers, 1)

	assert.NoError(t, w.Close())
	assert.Len(t, tc.Volume.readers, 0)
}

// TestVolume_NewReaderFor_ClosedVolume tests that a new reader cannot be created on closed volume.
func TestVolume_NewReaderFor_ClosedVolume(t *testing.T) {
	t.Parallel()
	tc := newReaderTestCase(t)

	// Open volume
	volume, err := tc.OpenVolume()
	require.NoError(t, err)

	// Close the volume
	assert.NoError(t, volume.Close())

	// Try crate a reader
	_, err = tc.NewReader()
	if assert.Error(t, err) {
		assert.Equal(t, "volume is closed: context canceled", err.Error())
	}
}

// TestVolume_NewReaderFor_Compression tests multiple local and staging compression combinations.
func TestVolume_NewReaderFor_Compression(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := []*compressionTestCase{
		{
			Name:               "None_To_None",
			LocalCompression:   compression.DefaultNoneConfig(),
			StagingCompression: compression.DefaultNoneConfig(),
		},
		{
			Name:               "None_To_GZIP",
			LocalCompression:   compression.DefaultNoneConfig(),
			StagingCompression: compression.DefaultGZIPConfig(),
		},
		{
			Name:               "None_To_ZSTD",
			LocalCompression:   compression.DefaultNoneConfig(),
			StagingCompression: compression.DefaultZSTDConfig(),
		},
		{
			Name:               "GZIP_To_None",
			LocalCompression:   compression.DefaultGZIPConfig(),
			StagingCompression: compression.DefaultNoneConfig(),
		},
		{
			Name:               "ZSTD_To_None",
			LocalCompression:   compression.DefaultZSTDConfig(),
			StagingCompression: compression.DefaultNoneConfig(),
		},
		{
			Name:               "ZSTD_To_GZIP",
			LocalCompression:   compression.DefaultZSTDConfig(),
			StagingCompression: compression.DefaultGZIPConfig(),
		},
	}

	// Run test cases for OK/ReadError/CloseError scenarios
	for _, tc := range cases {
		tc := tc
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
	rtc := newReaderTestCase(t)
	rtc.SliceData = localData.Bytes()
	rtc.Slice.LocalStorage.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Create reader
	r, err := rtc.NewReader()
	require.NoError(t, err)

	// Create decoder, if any
	var toStagingReader io.ReadCloser = r
	var decoder io.ReadCloser
	if tc.StagingCompression.Type != compression.TypeNone {
		decoder, err = compressionReader.New(toStagingReader, tc.StagingCompression)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, decoder.Close())
		})
		toStagingReader = decoder
	}

	// Read all
	content, err := io.ReadAll(toStagingReader)
	assert.NoError(t, err)
	assert.Equal(t, []byte("foo bar"), content)

	// Close
	assert.NoError(t, r.Close())
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
	rtc.Slice.LocalStorage.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Create reader
	readError := errors.New("some read error")
	r, err := rtc.NewReader(WithFileOpener(func(filePath string) (File, error) {
		f := newTestFile(localData)
		f.ReadError = readError
		return f, nil
	}))
	require.NoError(t, err)

	// Create decoder, if any
	var toStagingReader io.ReadCloser = r
	var decoder io.ReadCloser
	if tc.StagingCompression.Type != compression.TypeNone {
		decoder, err = compressionReader.New(toStagingReader, tc.StagingCompression)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, decoder.Close())
		})
		toStagingReader = decoder
	}

	// Read all
	_, err = io.ReadAll(toStagingReader)
	if assert.Error(t, err) {
		assert.Equal(t, "some read error", err.Error())
	}

	// Close
	assert.NoError(t, r.Close())
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
	rtc.Slice.LocalStorage.Compression = tc.LocalCompression
	rtc.Slice.StagingStorage.Compression = tc.StagingCompression

	// Create reader
	closeError := errors.New("some close error")
	r, err := rtc.NewReader(WithFileOpener(func(filePath string) (File, error) {
		f := newTestFile(localData)
		f.CloseError = closeError
		return f, nil
	}))
	require.NoError(t, err)

	// Create decoder, if any
	var toStagingReader io.ReadCloser = r
	var decoder io.ReadCloser
	if tc.StagingCompression.Type != compression.TypeNone {
		decoder, err = compressionReader.New(toStagingReader, tc.StagingCompression)
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, decoder.Close())
		})
		toStagingReader = decoder
	}

	// Read all
	_, err = io.ReadAll(toStagingReader)
	assert.NoError(t, err)

	// Close
	err = r.Close()
	if assert.Error(t, err) {
		assert.Equal(t, "chain close error:\n- cannot close \"*reader.testFile\": some close error", err.Error())
	}
}

type readerTestCase struct {
	*volumeTestCase
	Volume    *Volume
	Slice     *storage.Slice
	SliceData []byte
}

func newReaderTestCase(tb testing.TB) *readerTestCase {
	tb.Helper()
	tc := &readerTestCase{}
	tc.volumeTestCase = newVolumeTestCase(tb)
	tc.Slice = newTestSlice()
	return tc
}

func (tc *readerTestCase) OpenVolume(opts ...Option) (*Volume, error) {
	// Write file with the VolumeID
	require.NoError(tc.TB, os.WriteFile(filepath.Join(tc.VolumePath, local.VolumeIDFile), []byte("my-volume"), 0o640))

	volume, err := tc.volumeTestCase.OpenVolume(opts...)
	tc.Volume = volume
	return volume, err
}

func (tc *readerTestCase) NewReader(opts ...Option) (SliceReader, error) {
	if tc.Volume == nil {
		// Open volume
		_, err := tc.OpenVolume(opts...)
		require.NoError(tc.TB, err)
	}

	// Slice definition must be valid
	val := validator.New()
	require.NoError(tc.TB, val.Validate(context.Background(), tc.Slice))

	// Write slice data
	path := filepath.Join(tc.VolumePath, tc.Slice.LocalStorage.Dir, tc.Slice.LocalStorage.Filename)
	assert.NoError(tc.TB, os.MkdirAll(filepath.Dir(path), 0o750))
	assert.NoError(tc.TB, os.WriteFile(path, tc.SliceData, 0o640))

	r, err := tc.Volume.NewReaderFor(tc.Slice)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func newTestSlice() *storage.Slice {
	return newTestSliceOpenedAt("2000-01-01T20:00:00.000Z")
}

func newTestSliceOpenedAt(openedAt string) *storage.Slice {
	return &storage.Slice{
		SliceKey: storage.SliceKey{
			FileKey: storage.FileKey{
				ExportKey: key.ExportKey{
					ReceiverKey: key.ReceiverKey{
						ProjectID:  123,
						ReceiverID: "my-receiver",
					},
					ExportID: "my-export",
				},
				FileID: storage.FileID{
					OpenedAt: utctime.MustParse("2000-01-01T19:00:00.000Z"),
				},
			},
			SliceID: storage.SliceID{
				VolumeID: "my-volume",
				OpenedAt: utctime.MustParse(openedAt),
			},
		},
		Type:  storage.FileTypeCSV,
		State: storage.SliceWriting,
		Columns: column.Columns{
			column.ID{},
			column.Headers{},
			column.Body{},
		},
		LocalStorage: local.Slice{
			Dir:           openedAt,
			Filename:      "slice.csv",
			AllocateSpace: 10 * datasize.KB,
			Compression: compression.Config{
				Type: compression.TypeNone,
			},
			Sync: disksync.Config{
				Mode:            disksync.ModeDisk,
				Wait:            true,
				CheckInterval:   1 * time.Millisecond,
				CountTrigger:    500,
				BytesTrigger:    1 * datasize.MB,
				IntervalTrigger: 50 * time.Millisecond,
			},
		},
		StagingStorage: staging.Slice{
			Path: "slice.csv",
			Compression: compression.Config{
				Type: compression.TypeNone,
			},
		},
	}
}
