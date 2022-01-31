package input

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	FileName = "inputs.jsonnet"
)

func Path() string {
	return filesystem.Join("src", FileName)
}

type file struct {
	Inputs []Input `json:"inputs" validate:"dive"`
}

func newFile() *file {
	return &file{
		Inputs: make([]Input, 0),
	}
}

func loadFile(fs filesystem.Fs) (*file, error) {
	// Check if file exists
	path := Path()
	if !fs.IsFile(path) {
		return nil, fmt.Errorf("file \"%s\" not found", path)
	}

	// Read file
	fileDef := filesystem.NewFileDef(path).SetDescription("inputs")
	content := newFile()
	if _, err := fs.FileLoader().ReadJsonNetFileTo(fileDef, content); err != nil {
		return nil, err
	}

	// Validate
	if err := content.validate(); err != nil {
		return nil, err
	}

	return content, nil
}

func saveFile(fs filesystem.Fs, content *file) error {
	// Validate
	if err := content.validate(); err != nil {
		return err
	}

	// Convert to Json
	jsonContent, err := json.EncodeString(content, true)
	if err != nil {
		return err
	}

	// Convert to JsonNet
	jsonNet, err := jsonnet.Format(jsonContent)
	if err != nil {
		return err
	}

	// Write file
	f := filesystem.NewRawFile(Path(), jsonNet)
	if err := fs.WriteFile(f); err != nil {
		return err
	}

	return nil
}

func (i file) validate() error {
	rules := []validator.Rule{
		{
			Tag:          "template-input-id",
			Func:         validateInputId,
			ErrorMessage: "{0} can only contain alphanumeric characters, dots and underscores",
		},
		{
			Tag:          "template-input-default",
			Func:         validateInputDefault,
			ErrorMessage: "{0} must be the same type as type or options",
		},
		{
			Tag:          "template-input-options",
			Func:         validateInputOptions,
			ErrorMessage: "{0} allowed only for select and multiselect",
		},
		{
			Tag:          "template-input-type",
			Func:         validateInputType,
			ErrorMessage: "{0} allowed only for input type",
		},
		{
			Tag:          "template-input-rules",
			Func:         validateInputRules,
			ErrorMessage: "{0} is not valid",
		},
		{
			Tag:          "template-input-if",
			Func:         validateInputIf,
			ErrorMessage: "{0} is not valid",
		},
	}
	return validator.Validate(context.Background(), i, rules...)
}
