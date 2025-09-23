package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGzipMiddleware(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		acceptEncoding     string
		contentType        string
		content            string
		expectedCompressed bool
		expectedHeaders    map[string]string
	}{
		{
			name:               "should compress JSON response with gzip header",
			acceptEncoding:     "gzip",
			contentType:        "application/json",
			content:            `{"key": "value", "data": "test"}`,
			expectedCompressed: true,
			expectedHeaders: map[string]string{
				"Content-Encoding": "gzip",
			},
		},
		{
			name:               "should not compress when no gzip in Accept-Encoding",
			acceptEncoding:     "deflate",
			contentType:        "application/json",
			content:            `{"key": "value"}`,
			expectedCompressed: false,
			expectedHeaders:    map[string]string{},
		},
		{
			name:               "should not compress when Accept-Encoding is empty",
			acceptEncoding:     "",
			contentType:        "application/json",
			content:            `{"key": "value"}`,
			expectedCompressed: false,
			expectedHeaders:    map[string]string{},
		},
		{
			name:               "should compress text/plain",
			acceptEncoding:     "gzip",
			contentType:        "text/plain",
			content:            "This is a test response with some content",
			expectedCompressed: true,
			expectedHeaders: map[string]string{
				"Content-Encoding": "gzip",
			},
		},
		{
			name:               "should not compress image/png",
			acceptEncoding:     "gzip",
			contentType:        "image/png",
			content:            "fake image data",
			expectedCompressed: false,
			expectedHeaders:    map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create test handler.
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tt.contentType)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(tt.content))
			})

			// Create gzip middleware.
			middleware := Gzip()

			// Create test request.
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.Header.Set("Accept-Encoding", tt.acceptEncoding)

			// Create response recorder.
			rr := httptest.NewRecorder()

			// Execute request through middleware.
			middleware(handler).ServeHTTP(rr, req)

			// Check status code.
			assert.Equal(t, http.StatusOK, rr.Code)

			// Check headers.
			for key, expectedValue := range tt.expectedHeaders {
				assert.Equal(t, expectedValue, rr.Header().Get(key))
			}

			// Check if response was compressed.
			if tt.expectedCompressed {
				assert.Equal(t, "gzip", rr.Header().Get("Content-Encoding"))

				// Verify content is actually compressed.
				body := rr.Body.Bytes()
				assert.NotEqual(t, tt.content, string(body))

				// Try to decompress.
				reader, err := gzip.NewReader(strings.NewReader(string(body)))
				require.NoError(t, err)
				defer reader.Close()

				decompressed, err := io.ReadAll(reader)
				require.NoError(t, err)
				assert.Equal(t, tt.content, string(decompressed))
			} else {
				assert.Equal(t, tt.content, rr.Body.String())
			}
		})
	}
}

func TestGzipMiddleware_EmptyBodyPaths(t *testing.T) {
	t.Parallel()

	t.Run("compressible content-type, no body -> compressed {}", func(t *testing.T) {
		t.Parallel()
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			// no body written
		})
		mw := Gzip()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rr := httptest.NewRecorder()
		mw(handler).ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "gzip", rr.Header().Get("Content-Encoding"))
		// Content-Type should remain as set by handler
		assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

		// Body should be compressed {}
		body := rr.Body.Bytes()
		reader, err := gzip.NewReader(strings.NewReader(string(body)))
		require.NoError(t, err)
		defer reader.Close()
		decompressed, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "{}", string(decompressed))
	})

	t.Run("non-compressible content-type, no body -> gzipped {}", func(t *testing.T) {
		t.Parallel()
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "image/png")
			// no body written
		})
		mw := Gzip()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rr := httptest.NewRecorder()
		mw(handler).ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "gzip", rr.Header().Get("Content-Encoding"))
		// Content-Type remains whatever handler set
		assert.Equal(t, "image/png", rr.Header().Get("Content-Type"))
		// Body should be compressed {}
		body := rr.Body.Bytes()
		reader, err := gzip.NewReader(strings.NewReader(string(body)))
		require.NoError(t, err)
		defer reader.Close()
		decompressed, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "{}", string(decompressed))
	})

	t.Run("no content-type set, no body -> gzipped {} with application/json", func(t *testing.T) {
		t.Parallel()
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// no headers, no body
		})
		mw := Gzip()
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rr := httptest.NewRecorder()
		mw(handler).ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "gzip", rr.Header().Get("Content-Encoding"))
		assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
		body := rr.Body.Bytes()
		reader, err := gzip.NewReader(strings.NewReader(string(body)))
		require.NoError(t, err)
		defer reader.Close()
		decompressed, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "{}", string(decompressed))
	})
}

func TestGzipMiddleware_GzipWriterInitFailureFallback(t *testing.T) {
	t.Parallel()
	// Force invalid gzip level via custom option to trigger NewWriterLevel error
	mw := Gzip(func(c *GzipConfig) { c.Level = 99 })

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set compressible content type, but middleware will fail to init gzip writer
		w.Header().Set("Content-Type", "application/json")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rr := httptest.NewRecorder()
	mw(handler).ServeHTTP(rr, req)

	// Should fallback to gzipped JSON {}
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "gzip", rr.Header().Get("Content-Encoding"))
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	body := rr.Body.Bytes()
	reader, err := gzip.NewReader(strings.NewReader(string(body)))
	require.NoError(t, err)
	defer reader.Close()
	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "{}", string(decompressed))
}

func TestGzipResponseWriter(t *testing.T) {
	t.Parallel()
	// Create a test response writer.
	rr := httptest.NewRecorder()

	// Create gzip response writer.
	gzipWriter, err := NewGzipResponseWriter(rr, 6)
	require.NoError(t, err)

	// Write some data.
	testData := "This is test data that should be compressed"
	_, err = gzipWriter.Write([]byte(testData))
	require.NoError(t, err)

	// Write header.
	gzipWriter.WriteHeader(http.StatusOK)

	// Close the writer.
	err = gzipWriter.Close()
	require.NoError(t, err)

	// Check headers.
	assert.Equal(t, "gzip", rr.Header().Get("Content-Encoding"))
	assert.Empty(t, rr.Header().Get("Content-Length"))

	// Verify content is compressed.
	body := rr.Body.Bytes()
	assert.NotEqual(t, testData, string(body))

	// Decompress and verify.
	reader, err := gzip.NewReader(strings.NewReader(string(body)))
	require.NoError(t, err)
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, string(decompressed))
}

func TestGzipConfig(t *testing.T) {
	t.Parallel()
	t.Run("default config", func(t *testing.T) {
		t.Parallel()
		config := DefaultGzipConfig()
		assert.Equal(t, 6, config.Level)
		assert.Equal(t, 1024, config.MinSize)
		assert.Contains(t, config.CompressTypes, "application/json")
		assert.Contains(t, config.CompressTypes, "text/plain")
	})

	t.Run("custom level", func(t *testing.T) {
		t.Parallel()
		config := DefaultGzipConfig()
		WithGzipLevel(9)(&config)
		assert.Equal(t, 9, config.Level)
	})

	t.Run("invalid level", func(t *testing.T) {
		t.Parallel()
		config := DefaultGzipConfig()
		WithGzipLevel(0)(&config)
		assert.Equal(t, 6, config.Level) // Should remain default.

		WithGzipLevel(10)(&config)
		assert.Equal(t, 6, config.Level) // Should remain default.
	})

	t.Run("custom min size", func(t *testing.T) {
		t.Parallel()
		config := DefaultGzipConfig()
		WithGzipMinSize(2048)(&config)
		assert.Equal(t, 2048, config.MinSize)
	})

	t.Run("custom compress types", func(t *testing.T) {
		t.Parallel()
		config := DefaultGzipConfig()
		WithGzipCompressTypes("custom/type")(&config)
		assert.Equal(t, []string{"custom/type"}, config.CompressTypes)
	})
}

func TestShouldCompressByContentType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		contentType string
		config      GzipConfig
		expected    bool
	}{
		{
			name:        "should compress JSON",
			contentType: "application/json",
			config:      DefaultGzipConfig(),
			expected:    true,
		},
		{
			name:        "should compress text/plain",
			contentType: "text/plain",
			config:      DefaultGzipConfig(),
			expected:    true,
		},
		{
			name:        "should not compress image/png",
			contentType: "image/png",
			config:      DefaultGzipConfig(),
			expected:    false,
		},
		{
			name:        "should not compress empty content type",
			contentType: "",
			config:      DefaultGzipConfig(),
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := shouldCompressByContentType(tt.contentType, tt.config)
			assert.Equal(t, tt.expected, result)
		})
	}
}
