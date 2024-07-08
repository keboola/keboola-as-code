package encoder

// Encoder writers record values as bytes to the underlying writer.
// It is used inside the Writer pipeline, at the beginning, before the compression.
type Encoder interface {
	WriteRecord(values []any) error
	Flush() error
	Close() error
}
