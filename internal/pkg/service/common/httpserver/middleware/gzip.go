package middleware

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	// Default gzip compression level (1-9, where 9 is maximum compression).
	defaultGzipLevel = 6
	// Minimum content length to compress (bytes).
	minCompressSize = 1024
)

// responseWriterCapture captures the response to determine if it should be compressed.
type responseWriterCapture struct {
	http.ResponseWriter
	contentType string
	statusCode  int
	body        *strings.Builder
	written     bool
}

func (r *responseWriterCapture) WriteHeader(statusCode int) {
	if !r.written {
		r.statusCode = statusCode
		r.written = true
		// Capture content type when headers are written.
		r.contentType = r.ResponseWriter.Header().Get("Content-Type")
	}
}

func (r *responseWriterCapture) Write(data []byte) (int, error) {
	if !r.written {
		r.WriteHeader(http.StatusOK)
	}
	if r.body == nil {
		r.body = &strings.Builder{}
	}
	r.body.Write(data)
	return len(data), nil
}

func (r *responseWriterCapture) Header() http.Header {
	return r.ResponseWriter.Header()
}

// GzipResponseWriter wraps http.ResponseWriter to provide gzip compression.
type GzipResponseWriter struct {
	http.ResponseWriter
	writer     io.Writer
	gzipWriter *gzip.Writer
	level      int
}

// NewGzipResponseWriter creates a new gzip response writer.
func NewGzipResponseWriter(w http.ResponseWriter, level int) (*GzipResponseWriter, error) {
	gzipWriter, err := gzip.NewWriterLevel(w, level)
	if err != nil {
		return nil, err
	}

	return &GzipResponseWriter{
		ResponseWriter: w,
		writer:         gzipWriter,
		gzipWriter:     gzipWriter,
		level:          level,
	}, nil
}

// Write implements io.Writer interface.
func (g *GzipResponseWriter) Write(data []byte) (int, error) {
	return g.writer.Write(data)
}

// WriteHeader sets the response headers.
func (g *GzipResponseWriter) WriteHeader(statusCode int) {
	// Set content encoding header.
	g.Header().Set("Content-Encoding", "gzip")
	// Remove content length as it will be different after compression.
	g.Header().Del("Content-Length")
	g.ResponseWriter.WriteHeader(statusCode)
}

// Close closes the gzip writer.
func (g *GzipResponseWriter) Close() error {
	if g.gzipWriter != nil {
		return g.gzipWriter.Close()
	}
	return nil
}

// GzipConfig holds configuration for gzip compression.
type GzipConfig struct {
	Level         int
	MinSize       int
	CompressTypes []string
}

// DefaultGzipConfig returns default gzip configuration.
func DefaultGzipConfig() GzipConfig {
	return GzipConfig{
		Level:   defaultGzipLevel,
		MinSize: minCompressSize,
		CompressTypes: []string{
			"text/plain",
			"text/html",
			"text/css",
			"text/javascript",
			"application/javascript",
			"application/json",
			"application/xml",
			"application/x-yaml",
			"application/yaml",
		},
	}
}

// WithGzipLevel sets the gzip compression level (1-9).
func WithGzipLevel(level int) func(*GzipConfig) {
	return func(c *GzipConfig) {
		if level >= 1 && level <= 9 {
			c.Level = level
		}
	}
}

// WithGzipMinSize sets the minimum content size to compress.
func WithGzipMinSize(size int) func(*GzipConfig) {
	return func(c *GzipConfig) {
		if size > 0 {
			c.MinSize = size
		}
	}
}

// WithGzipCompressTypes sets the content types to compress.
func WithGzipCompressTypes(types ...string) func(*GzipConfig) {
	return func(c *GzipConfig) {
		c.CompressTypes = types
	}
}

// shouldCompressByContentType checks if the content type should be compressed.
func shouldCompressByContentType(contentType string, config GzipConfig) bool {
	// Check content type.
	for _, compressType := range config.CompressTypes {
		if strings.Contains(contentType, compressType) {
			return true
		}
	}
	return false
}

// Gzip creates a middleware that compresses HTTP responses using gzip.
func Gzip(opts ...func(*GzipConfig)) Middleware {
	config := DefaultGzipConfig()
	for _, opt := range opts {
		opt(&config)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Check if client accepts gzip.
			acceptEncoding := r.Header.Get("Accept-Encoding")
			if !strings.Contains(acceptEncoding, "gzip") {
				next.ServeHTTP(w, r)
				return
			}

			// Create a response writer that can capture the content type.
			responseWriter := &responseWriterCapture{
				ResponseWriter: w,
				contentType:    "",
			}

			// Call next handler to get the response.
			next.ServeHTTP(responseWriter, r)

			// Get the actual content type from the response headers.
			contentType := responseWriter.ResponseWriter.Header().Get("Content-Type")
			if contentType == "" {
				contentType = responseWriter.contentType
			}

			// Normalize status code; if nothing was written, default to 200 OK.
			statusCode := responseWriter.statusCode
			if statusCode == 0 {
				statusCode = http.StatusOK
			}

			// Safely access body; if nil, we'll decide a fallback below.
			bodyNil := responseWriter.body == nil
			bodyStr := ""
			if !bodyNil {
				bodyStr = responseWriter.body.String()
			}

			// Check if we should compress based on content type.
			if !shouldCompressByContentType(contentType, config) {
				// If we shouldn't compress by content-type, do not apply gzip even for empty body.
				if bodyNil {
					// No body was written by handler. Return plain empty JSON and log the reason.
					setJSONContentTypeIfEmpty(w)
					if span, ok := RequestSpan(r.Context()); ok {
						span.AddEvent("gzip: empty body fallback (no gzip)", trace.WithAttributes(attribute.String("reason", "handler wrote no body")))
					}
					w.Header().Del("Content-Encoding")
					w.Header().Del("Content-Length")
					w.WriteHeader(statusCode)
					_, _ = w.Write([]byte("{}"))
					return
				}

				w.WriteHeader(statusCode)
				if _, err := w.Write([]byte(bodyStr)); err != nil {
					// Cannot do much more since headers may have been sent.
					return
				}
				return
			}

			// Compress and write the response.
			gzipWriter, err := gzip.NewWriterLevel(w, config.Level)
			if err != nil {
				// Fallback to default gzip writer and log the reason.
				if span, ok := RequestSpan(r.Context()); ok {
					span.AddEvent("gzip: writer init failed, using default", trace.WithAttributes(attribute.String("error", err.Error())))
				}
				w.Header().Set("Content-Encoding", "gzip")
				w.Header().Del("Content-Length")
				if bodyNil {
					setJSONContentTypeIfEmpty(w)
				}
				w.WriteHeader(statusCode)
				gw := gzip.NewWriter(w)
				defer gw.Close()
				if bodyNil {
					_, _ = gw.Write([]byte("{}"))
					return
				}
				_, _ = gw.Write([]byte(bodyStr))
				return
			}

			// Now that we have a gzip writer, set gzip headers and status.
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Del("Content-Length")
			if bodyNil {
				setJSONContentTypeIfEmpty(w)
			}
			w.WriteHeader(statusCode)

			defer gzipWriter.Close()
			if bodyNil {
				if span, ok := RequestSpan(r.Context()); ok {
					span.AddEvent("gzip: empty body compressed", trace.WithAttributes(attribute.String("reason", "handler wrote no body")))
				}
				_, _ = gzipWriter.Write([]byte("{}"))
				return
			}
			if _, err := gzipWriter.Write([]byte(bodyStr)); err != nil {
				// Cannot do much more since headers may have been sent.
				return
			}
		})
	}
}

// setJSONContentTypeIfEmpty ensures a sensible default content type when the handler
// did not explicitly set any. It keeps the header untouched if already present.
func setJSONContentTypeIfEmpty(w http.ResponseWriter) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
}
