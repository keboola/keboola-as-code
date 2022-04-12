package model

type PathsGenerator interface {
	AddRenamed(path RenamedPath)
	RenameEnabled() bool // if true, existing paths will be renamed
}

// OnObjectPathUpdateEvent contains object with updated path.
type OnObjectPathUpdateEvent struct {
	PathsGenerator PathsGenerator
	Object         Object
	Renamed        bool
	OldPath        string
	NewPath        string
}
