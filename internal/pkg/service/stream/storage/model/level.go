package model

const (
	// LevelLocal - data is buffered on a local disk.
	LevelLocal = Level("local")
	// LevelStaging - data is uploaded to the staging storage.
	LevelStaging = Level("staging")
	// LevelTarget - data is imported to the target storage.
	LevelTarget = Level("target")
)

// Level on which the data is stored during processing.
type Level string

func AllLevels() []Level {
	return []Level{LevelLocal, LevelStaging, LevelTarget}
}

func (l Level) String() string {
	return string(l)
}
