package input

import (
	"context"
	"fmt"
	"reflect"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Inputs []Input

func NewInputs() *Inputs {
	inputs := make(Inputs, 0)
	return &inputs
}

func (i Inputs) Validate() error {
	return validate(i)
}

func (i *Inputs) Add(input Input) {
	*i = append(*i, input)
}

func (i *Inputs) GetIndex(index int) Input {
	return (*i)[index]
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

type Values []Value

type Value struct {
	Id      string
	Value   interface{}
	Skipped bool // the input was skipped and the default value was used
}

type Input struct {
	Id          string      `json:"id" validate:"required,template-input-id"`
	Name        string      `json:"name" validate:"required,min=1,max=25"`
	Description string      `json:"description" validate:"max=60"`
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

func (i Input) DefaultOrEmpty() interface{} {
	if i.Default != nil {
		return i.Default
	}
	return i.Type.EmptyValue()
}
