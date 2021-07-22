package model

import (
	"fmt"
	"path/filepath"
)

type RenamePlan struct {
	OldPath     string
	NewPath     string
	Description string
}

func (p *RenamePlan) Validate() error {
	if !filepath.IsAbs(p.OldPath) {
		return fmt.Errorf("old path must be absolute")
	}
	if !filepath.IsAbs(p.NewPath) {
		return fmt.Errorf("new path must be absolute")
	}
	return nil
}
