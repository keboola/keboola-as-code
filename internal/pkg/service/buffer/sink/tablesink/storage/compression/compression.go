// Package compression provides configuration for compression and decompression for local and staging storage.
// The Config structure provides options for different types and algorithms of compression.
// Separate packages "writer" and "reader" provide compression and decompression, this package contains common code.
package compression

const (
	TypeNone = Type("none")
	TypeGZIP = Type("gzip")
	TypeZSTD = Type("zstd")

	DefaultGZIPImpl = GZIPImplParallel
	// GZIPImplStandard - https://pkg.go.dev/compress/gzip
	GZIPImplStandard = GZIPImplementation("standard")
	// GZIPImplFast - https://pkg.go.dev/github.com/klauspost/compress/gzip
	GZIPImplFast = GZIPImplementation("fast")
	// GZIPImplParallel - https://pkg.go.dev/github.com/klauspost/pgzip
	GZIPImplParallel = GZIPImplementation("parallel")
)

type GZIPImplementation string

type Type string
