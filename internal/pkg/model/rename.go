package model

type RenamedPath struct {
	Object     Object
	OldPath    string
	RenameFrom string // old path with renamed parents dirs
	NewPath    string
}

type RenameAction struct {
	Manifest    ObjectManifest
	OldPath     string
	RenameFrom  string // old path with renamed parents dirs
	NewPath     string
	Description string
}

func (a *RenameAction) String() string {
	return a.Description
}
