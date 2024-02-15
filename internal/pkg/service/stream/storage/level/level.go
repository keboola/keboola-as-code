package level

const (
	// Local - data is buffered on a local disk.
	Local = Level("local")
	// Staging - data is uploaded to the staging storage.
	Staging = Level("staging")
	// Target - data is imported to the target storage.
	Target = Level("target")
)

// Level on which the data is stored during processing.
type Level string

func AllLevels() []Level {
	return []Level{Local, Staging, Target}
}

func (l Level) String() string {
	return string(l)
}
