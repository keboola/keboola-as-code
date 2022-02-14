package input

import (
	"context"
	"fmt"
	"reflect"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Inputs []Input

func NewInputs() *Inputs {
	inputs := make(Inputs, 0)
	return &inputs
}

// Load inputs from the FileName.
func Load(fs filesystem.Fs) (*Inputs, error) {
	f, err := loadFile(fs)
	if err != nil {
		return nil, err
	}
	return &f.Inputs, nil
}

func (i Inputs) Validate() error {
	return validate(i)
}

func (i *Inputs) Add(input Input) {
	*i = append(*i, input)
}

// Save inputs to the FileName.
func (i *Inputs) Save(fs filesystem.Fs) error {
	if err := saveFile(fs, &file{Inputs: *i}); err != nil {
		return err
	}
	return nil
}

func (i *Inputs) All() []Input {
	out := make([]Input, len(*i))
	copy(out, *i)
	return out
}

func (i *Inputs) Set(inputs []Input) *Inputs {
	*i = inputs
	return i
}

func (i Inputs) Path() string {
	return Path()
}

type Values []Value

type Value struct {
	Id    string
	Value interface{}
}

type Input struct {
	Id          string      `json:"id" validate:"required,template-input-id"`
	Name        string      `json:"name" validate:"required"`
	Description string      `json:"description" validate:"required"`
	Type        Type        `json:"type" validate:"required,template-input-type,template-input-type-for-kind"`
	Kind        Kind        `json:"kind" validate:"required,template-input-kind"`
	Default     interface{} `json:"default,omitempty" validate:"omitempty,template-input-default-value,template-input-default-options"`
	Rules       Rules       `json:"rules,omitempty" validate:"omitempty,template-input-rules"`
	If          If          `json:"showIf,omitempty" validate:"omitempty,template-input-if"`
	Options     Options     `json:"options,omitempty" validate:"template-input-options"`
}

// ValidateUserInput validates input from the template user using Input.Rules.
func (i Input) ValidateUserInput(userInput interface{}, ctx context.Context) error {
	if err := i.Type.ValidateValue(reflect.ValueOf(userInput)); err != nil {
		return fmt.Errorf("%s %w", i.Name, err)
	}
	return i.Rules.ValidateValue(userInput, i.Id)
}

// Available decides if the input should be visible to user according to Input.If.
func (i Input) Available(params map[string]interface{}) (bool, error) {
	result, err := i.If.Evaluate(params)
	if err != nil {
		return false, utils.PrefixError(fmt.Sprintf(`invalid input "%s"`, i.Id), err)
	}
	return result, nil
}
