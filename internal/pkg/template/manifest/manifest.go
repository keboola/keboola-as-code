package manifest

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type records = manifest.Records

// File contains content of the manifest. Jsonnet has not been executed yet.
type File struct {
	file *filesystem.RawFile
}

// Manifest is evaluated File.
type Manifest struct {
	naming     naming.Template
	mainConfig *model.ConfigKey
	*records
}

func New() *Manifest {
	return &Manifest{
		naming:  naming.ForTemplate(),
		records: manifest.NewRecords(model.SortByPath),
	}
}

// Load manifest File.
func Load(ctx context.Context, fs filesystem.Fs) (*File, error) {
	path := Path()
	if !fs.IsFile(ctx, path) {
		return nil, errors.Errorf("manifest \"%s\" not found", path)
	}

	f, err := fs.ReadFile(ctx, filesystem.NewFileDef(path).SetDescription("manifest"))
	if err != nil {
		return nil, err
	}

	return &File{file: f}, nil
}

// Evaluate Jsonnet content.
func (f *File) Evaluate(ctx context.Context, jsonnetCtx *jsonnet.Context) (*Manifest, error) {
	// Evaluate Jsonnet
	content, err := evaluateFile(ctx, f.file, jsonnetCtx)
	if err != nil {
		return nil, err
	}

	// Create manifest
	m := New()

	// Get records
	records, err := content.records()
	if err != nil {
		return nil, errors.Errorf(`cannot load configurations from manifest "%s": %w`, f.file.Path(), err)
	}

	// Set records
	if err := m.records.SetRecords(records); err != nil {
		return nil, errors.Errorf(`cannot load manifest: %w`, err)
	}

	// Set main config
	m.mainConfig = content.MainConfig

	// Return
	return m, nil
}

// EvaluateWithoutRecords is used for empty manifests to load context information into `Manifest` structure.
func (f *File) EvaluateAlwaysWithRecords(ctx context.Context, jsonnetCtx *jsonnet.Context) (m *Manifest, err error) {
	// Evaluate Jsonnet
	content, err := evaluateFile(ctx, f.file, jsonnetCtx)
	if err != nil {
		return nil, err
	}

	// Create manifest
	m = New()

	// Get records, skip error to proceed
	records, _ := content.records()

	// Set records
	if err := m.records.SetRecords(records); err != nil {
		return nil, errors.Errorf(`cannot load manifest: %w`, err)
	}

	// Set main config
	m.mainConfig = content.MainConfig

	// Return
	return m, nil
}

func (f *File) RawContent() string {
	return f.file.Content
}

func (m *Manifest) Save(ctx context.Context, fs filesystem.Fs) error {
	// Create file content
	content := newFile()
	content.setRecords(m.records.All())
	content.MainConfig = m.mainConfig

	// Save file
	if err := saveFile(ctx, fs, content); err != nil {
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

func (m *Manifest) MainConfig() *model.ConfigKey {
	return m.mainConfig
}
