// Package branchmapping provides utilities for managing git-to-Keboola branch mappings.
package branchmapping

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	FileName = "branch-mapping.json"
	Version  = 1
)

// Path returns the path to the branch mapping file.
func Path() string {
	return filesystem.Join(filesystem.MetadataDir, FileName)
}

// BranchMapping represents a mapping from git branch to Keboola branch.
type BranchMapping struct {
	// ID is the Keboola branch ID. Nil means production (default branch).
	ID *string `json:"id"`
	// Name is the Keboola branch name for display purposes.
	Name string `json:"name" validate:"required"`
}

// File represents the branch-mapping.json file structure.
type File struct {
	Version  int                       `json:"version" validate:"required,min=1,max=1"`
	Mappings map[string]*BranchMapping `json:"mappings" validate:"required"`
}

// New creates a new empty branch mapping file.
func New() *File {
	return &File{
		Version:  Version,
		Mappings: make(map[string]*BranchMapping),
	}
}

// Load loads the branch mapping file from the filesystem.
func Load(ctx context.Context, fs filesystem.Fs) (*File, error) {
	path := Path()

	// Check if file exists
	if !fs.IsFile(ctx, path) {
		return nil, errors.Errorf("branch mapping file \"%s\" not found", path)
	}

	// Read JSON file
	content := New()
	if _, err := fs.FileLoader().ReadJSONFileTo(ctx, filesystem.NewFileDef(path).SetDescription("branch mapping"), content); err != nil {
		return nil, err
	}

	// Validate
	if err := content.Validate(ctx); err != nil {
		return content, err
	}

	return content, nil
}

// Save writes the branch mapping file to the filesystem.
func Save(ctx context.Context, fs filesystem.Fs, f *File) error {
	// Validate
	if err := f.Validate(ctx); err != nil {
		return err
	}

	// Write JSON file
	content, err := json.EncodeString(f, true)
	if err != nil {
		return errors.PrefixError(err, "cannot encode branch mapping")
	}
	file := filesystem.NewRawFile(Path(), content)
	if err := fs.WriteFile(ctx, file); err != nil {
		return err
	}
	return nil
}

// Exists checks if the branch mapping file exists.
func Exists(ctx context.Context, fs filesystem.Fs) bool {
	return fs.IsFile(ctx, Path())
}

// Validate validates the branch mapping file.
func (f *File) Validate(ctx context.Context) error {
	if err := validator.New().Validate(ctx, f); err != nil {
		return errors.PrefixError(err, "branch mapping is not valid")
	}
	return nil
}

// GetMapping returns the mapping for a git branch.
func (f *File) GetMapping(gitBranch string) (*BranchMapping, bool) {
	mapping, ok := f.Mappings[gitBranch]
	return mapping, ok
}

// SetMapping sets or updates the mapping for a git branch.
func (f *File) SetMapping(gitBranch string, mapping *BranchMapping) {
	f.Mappings[gitBranch] = mapping
}

// RemoveMapping removes the mapping for a git branch.
func (f *File) RemoveMapping(gitBranch string) bool {
	if _, ok := f.Mappings[gitBranch]; ok {
		delete(f.Mappings, gitBranch)
		return true
	}
	return false
}

// IsProduction checks if the mapping points to production (null ID).
func (m *BranchMapping) IsProduction() bool {
	return m.ID == nil
}

// GetID returns the branch ID as a string, or empty string for production.
func (m *BranchMapping) GetID() string {
	if m.ID == nil {
		return ""
	}
	return *m.ID
}
