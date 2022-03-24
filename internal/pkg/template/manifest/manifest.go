package manifest

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/object"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type records = manifest.Collection

// File contains content of the manifest. Jsonnet has not been executed yet.
type File struct {
	file *filesystem.RawFile
}

// Manifest is evaluated File.
type Manifest struct {
	naming naming.Template
	*records
}

func New(ctx context.Context) *Manifest {
	// Disable "required_in_project" validation tag
	ctx = context.WithValue(ctx, validator.DisableRequiredInProjectKey, true)

	namingRegistry := naming.NewRegistry()
	return &Manifest{
		naming:  naming.ForTemplate(),
		records: manifest.NewCollection(ctx, namingRegistry, object.NewPathSorter(namingRegistry)),
	}
}

// Load manifest File.
func Load(fs filesystem.Fs) (*File, error) {
	path := Path()
	if !fs.IsFile(path) {
		return nil, fmt.Errorf("manifest \"%s\" not found", path)
	}

	f, err := fs.ReadFile(filesystem.NewFileDef(path).SetDescription("manifest"))
	if err != nil {
		return nil, err
	}

	return &File{file: f}, nil
}

// Evaluate Jsonnet content.
func (f *File) Evaluate(ctx context.Context, jsonNetCtx *jsonnet.Context) (*Manifest, error) {
	// Evaluate Jsonnet
	content, err := evaluateFile(f.file, jsonNetCtx)
	if err != nil {
		return nil, err
	}

	// Create manifest
	m := New(ctx)

	// Set records
	if err := m.records.Set(content.records()); err != nil {
		return nil, fmt.Errorf(`cannot load manifest: %w`, err)
	}

	// Return
	return m, nil
}

func (m *Manifest) Save(fs filesystem.Fs) error {
	// Create file content
	content := newFile()
	content.setRecords(m.records.All())

	// Save file
	if err := saveFile(fs, content); err != nil {
		return err
	}

	m.records.ResetChanged()
	return nil
}

func (m *Manifest) Path() string {
	return Path()
}

func (m *Manifest) NamingTemplate() naming.Template {
	return m.naming
}

func (m *Manifest) IsObjectIgnored(_ model.Object) bool {
	return false
}
