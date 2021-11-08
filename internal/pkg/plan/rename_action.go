package plan

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type RenameAction struct {
	OldPath     string
	RenameFrom  string // old path with renamed parents dirs
	NewPath     string
	Description string
	Record      model.Record
}

func (a *RenameAction) String() string {
	return a.Description
}
