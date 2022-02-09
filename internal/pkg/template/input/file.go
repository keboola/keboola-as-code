package input

import (
	"context"
	"fmt"
	"reflect"

	goValidator "github.com/go-playground/validator/v10"
	"github.com/umisama/go-regexpcache"

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
			Tag: "template-input-id",
			Func: func(fl goValidator.FieldLevel) bool {
				return regexpcache.MustCompile(`^[a-zA-Z0-9\.\-\_]+$`).MatchString(fl.Field().String())
			},
			ErrorMsg: "{0} can only contain alphanumeric characters, dots, underscores and dashes",
		},
		{
			Tag: "template-input-default-value",
			Func: func(fl goValidator.FieldLevel) bool {
				typeValue := fl.Parent().FieldByName("Type").Interface().(Type)
				// Invalid type is validated by other rule
				return !typeValue.IsValid() || typeValue.ValidateValue(fl.Field()) == nil
			},
			ErrorMsg: "{0} must match the specified type",
		},
		{
			Tag: "template-input-default-options",
			Func: func(fl goValidator.FieldLevel) bool {
				kind := fl.Parent().FieldByName("Kind").Interface().(Kind)
				options := fl.Parent().FieldByName("Options").Interface().(Options)
				return validateDefaultOptions(fl.Field().Interface(), kind, options)
			},
			ErrorMsg: "{0} can only contain values from the specified options",
		},
		{
			Tag: "template-input-options",
			Func: func(fl goValidator.FieldLevel) bool {
				kind := fl.Parent().FieldByName("Kind").Interface().(Kind)
				// XOR
				// for select/multiselect, options length != 0
				// for other, options length == 0
				return (kind == KindSelect || kind == KindMultiSelect) != (fl.Field().Len() == 0)
			},
			ErrorMsgFunc: func(fe goValidator.FieldError) string {
				if options, ok := fe.Value().(Options); ok && len(options) == 0 {
					return fmt.Sprintf("%s must contain at least one item", fe.Field())
				}
				return fmt.Sprintf("%s should only be set for select and multiselect kinds", fe.Field())
			},
		},
		{
			Tag: "template-input-kind",
			Func: func(fl goValidator.FieldLevel) bool {
				return fl.Parent().FieldByName("Kind").Interface().(Kind).IsValid()
			},
			ErrorMsgFunc: func(fe goValidator.FieldError) string {
				return fmt.Sprintf("%s %s is not allowed, allowed values: %s", fe.Field(), fe.Value(), allKinds().String())
			},
		},
		{
			Tag: "template-input-type",
			Func: func(fl goValidator.FieldLevel) bool {
				return fl.Parent().FieldByName("Type").Interface().(Type).IsValid()
			},
			ErrorMsgFunc: func(fe goValidator.FieldError) string {
				return fmt.Sprintf("%s %s is not allowed, allowed values: %s", fe.Field(), fe.Value(), allTypes().String())
			},
		},
		{
			Tag: "template-input-type-for-kind",
			Func: func(fl goValidator.FieldLevel) bool {
				typeField := fl.Field().Interface().(Type)
				kindField := fl.Parent().FieldByName("Kind").Interface().(Kind)
				if !kindField.IsValid() || !typeField.IsValid() {
					// invalid kind or type, skip this validation
					return true
				}
				return kindField.ValidateType(typeField) == nil
			},
			ErrorMsgFunc: func(fe goValidator.FieldError) string {
				return fmt.Sprintf("%s %s is not allowed for the specified kind", fe.Field(), fe.Value())
			},
		},
		{
			Tag: "template-input-rules",
			Func: func(fl goValidator.FieldLevel) (valid bool) {
				// Run with an empty value to validate rules
				err := fl.Field().Interface().(Rules).ValidateValue("", "")
				if _, ok := err.(InvalidRulesError); ok { // nolint: errorlint
					return false
				}
				return true
			},
			ErrorMsgFunc: func(fe goValidator.FieldError) string {
				err := fe.Value().(Rules).ValidateValue("", "")
				return fmt.Sprintf("%s is not valid: %s", fe.Field(), err.Error())
			},
		},
		{
			Tag: "template-input-if",
			Func: func(fl goValidator.FieldLevel) bool {
				if fl.Field().Kind() == reflect.String {
					_, err := fl.Field().Interface().(If).compile()
					return err == nil
				}
				return false
			},
			ErrorMsgFunc: func(fe goValidator.FieldError) string {
				_, err := fe.Value().(If).compile()
				return err.Error()
			},
		},
	}
	return validator.Validate(context.Background(), i, rules...)
}
