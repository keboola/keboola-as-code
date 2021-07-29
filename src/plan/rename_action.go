package plan

import (
	"fmt"
	"keboola-as-code/src/model"
	"path/filepath"
)

type RenameAction struct {
	OldPath     string
	NewPath     string
	Description string
	Record      model.Record
}

func (a *RenameAction) String() string {
	return a.Description
}

func (a *RenameAction) Validate() error {
	if !filepath.IsAbs(a.OldPath) {
		return fmt.Errorf("old path must be absolute")
	}
	if !filepath.IsAbs(a.NewPath) {
		return fmt.Errorf("new path must be absolute")
	}
	return nil
}
