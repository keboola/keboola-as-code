package reader

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
)

func TestReader(t *testing.T) {
	t.Parallel()

	// Encoders to compressed data before test
	noneDecoder := func(t *testing.T, w io.Writer) io.Writer {
		t.Helper()
		return w
	}
	gzipEncoder := func(t *testing.T, w io.Writer) io.Writer {
		t.Helper()
		// The standard gzip implementation is used to encode data.
		return gzip.NewWriter(w)
	}
	zstdEncoder := func(t *testing.T, w io.Writer) io.Writer {
		t.Helper()
		r, err := zstd.NewWriter(w)
		require.NoError(t, err)
		return r
	}

	// Test cases
	cases := []struct {
		Name    string
		Config  compression.Config
		Encoder func(t *testing.T, w io.Writer) io.Writer
	}{
		{
			Name:    "none",
			Encoder: noneDecoder,
			Config: compression.Config{
				Type: compression.TypeNone,
			},
		},
		{
			Name:    "gzip.standard",
			Encoder: gzipEncoder,
			Config: compression.Config{
				Type: compression.TypeGZIP,
				GZIP: &compression.GZIPConfig{
					Level:          compression.DefaultGZIPLevel,
					Implementation: compression.GZIPImplStandard, // <<<<<<<<
				},
			},
		},
		{
			Name:    "gzip.fast",
			Encoder: gzipEncoder,
			Config: compression.Config{
				Type: compression.TypeGZIP,
				GZIP: &compression.GZIPConfig{
					Level:          compression.DefaultGZIPLevel,
					Implementation: compression.GZIPImplFast, // <<<<<<<<
				},
			},
		},
		{
			Name:    "gzip.parallel",
			Encoder: gzipEncoder,
			Config: compression.Config{
				Type: compression.TypeGZIP,
				GZIP: &compression.GZIPConfig{
					Level:          compression.DefaultGZIPLevel,
					Implementation: compression.GZIPImplParallel, // <<<<<<<<
					Concurrency:    4,
				},
			},
		},
		{
			Name:    "zstd",
			Encoder: zstdEncoder,
			Config: compression.Config{
				Type: compression.TypeZSTD,
				ZSTD: &compression.ZSTDConfig{
					Level:       compression.DefaultZSTDLevel,
					Concurrency: 4,
				},
			},
		},
	}

	// Random data for compression
	dataLen := 4 * datasize.MB
	step := 100 * datasize.KB
	data := make([]byte, dataLen.Bytes())
	rnd := rand.New(rand.NewSource(time.Now().UnixMilli()))
	n, err := rnd.Read(data)
	assert.Equal(t, int(dataLen.Bytes()), n)
	assert.NoError(t, err)

	// The data is written 2x, in halves, to simulate reopen of the compressed file.
	// Thus, it is checked that it is possible to continue compression after restarting the pod.
	writePart := func(pos datasize.ByteSize, w io.Writer) {
		var part []byte
		if pos > dataLen-step {
			part = data[pos:]
		} else {
			part = data[pos : pos+step]
		}
		n, err := w.Write(part)
		assert.Equal(t, len(part), n)
		assert.NoError(t, err)
	}

	// Run test cases in parallel
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			var out bytes.Buffer
			w := tc.Encoder(t, &out)

			// Write the first half
			pos := datasize.ByteSize(0)
			for ; pos < dataLen/2; pos += step {
				writePart(pos, w)
			}

			// Close the writer - simulate file close - some outage
			if v, ok := w.(io.Closer); ok {
				assert.NoError(t, v.Close())
			}

			// Reopen writer - simulates recovery from the outage
			w = tc.Encoder(t, &out)

			// Write the second half
			for ; pos < dataLen; pos += step {
				writePart(pos, w)
			}

			// Close the writer
			if v, ok := w.(io.Closer); ok {
				assert.NoError(t, v.Close())
			}

			// Create reader
			r, err := New(&out, tc.Config)
			require.NoError(t, err)

			// Decode all
			decoded, err := io.ReadAll(r)
			assert.NoError(t, err)

			// Compare md5 checksum, because assert library cannot diff such big data.
			assert.NoError(t, err)
			assert.Equal(t, md5.Sum(data), md5.Sum(decoded))
		})
	}
}
