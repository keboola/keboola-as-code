package input

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	FileName = "inputs.jsonnet"
)

func Path() string {
	return filesystem.Join("src", FileName)
}

type file struct {
	StepsGroups StepsGroups `json:"stepsGroups" validate:"dive"`
}

func newFile() *file {
	return &file{
		StepsGroups: make(StepsGroups, 0),
	}
}

func loadFile(ctx context.Context, fs filesystem.Fs, jsonnetCtx *jsonnet.Context) (*file, error) {
	// Check if file exists
	path := Path()
	if !fs.IsFile(ctx, path) {
		return nil, errors.Errorf("file \"%s\" not found", path)
	}

	// Read file
	fileDef := filesystem.NewFileDef(path).SetDescription("inputs")
	content := newFile()
	if _, err := fs.FileLoader().WithJsonnetContext(jsonnetCtx).ReadJsonnetFileTo(ctx, fileDef, content); err != nil {
		return nil, err
	}

	// Validate
	if err := content.validate(ctx); err != nil {
		return nil, err
	}

	return content, nil
}

func saveFile(ctx context.Context, fs filesystem.Fs, content *file) error {
	// Validate
	if err := content.validate(ctx); err != nil {
		return err
	}

	// Convert to Json
	jsonContent, err := json.EncodeString(content, true)
	if err != nil {
		return err
	}

	// Convert to Jsonnet
	jsonnetStr, err := jsonnet.Format(jsonContent)
	if err != nil {
		return err
	}

	// Write file
	f := filesystem.NewRawFile(Path(), jsonnetStr)
	if err := fs.WriteFile(ctx, f); err != nil {
		return err
	}

	return nil
}

func (f file) validate(ctx context.Context) error {
	return f.StepsGroups.ValidateDefinitions(ctx)
}
