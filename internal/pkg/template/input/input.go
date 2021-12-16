package input

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Inputs []*Input

// ValidateDefinitions validates template inputs definition.
func (i Inputs) ValidateDefinitions() error {
	validations := []validator.Validation{
		{
			Tag:  "template-input-id",
			Func: validateInputId,
		},
		{
			Tag:  "template-input-default",
			Func: validateInputDefault,
		},
		{
			Tag:  "template-input-options",
			Func: validateInputOptions,
		},
		{
			Tag:  "template-input-type",
			Func: validateInputType,
		},
		{
			Tag:  "template-input-rules",
			Func: validateInputRules,
		},
	}
	return validator.Validate(i, validations...)
}

type Input struct {
	Id          string      `json:"id" validate:"required,template-input-id"`
	Name        string      `json:"name" validate:"required"`
	Description string      `json:"description" validate:"required"`
	Default     interface{} `json:"default,omitempty" validate:"omitempty,template-input-default"`
	Kind        string      `json:"kind" validate:"required,oneof=input password textarea confirm select multiselect"`
	Type        string      `json:"type,omitempty" validate:"required_if=Kind input,omitempty,oneof=string int float64,template-input-type"`
	Options     []Option    `json:"options,omitempty" validate:"required_if=Type select Type multiselect,template-input-options"`
	Rules       string      `json:"rules,omitempty" validate:"template-input-rules"`
	If          string      `json:"if,omitempty"`
}

// ValidateUserInput validates input from the template user using Input.Rules.
func (i Input) ValidateUserInput(userInput interface{}, ctx context.Context) error {
	if err := validateUserInputTypeByKind(userInput, i.Kind); err != nil {
		return err
	}

	return validateUserInputWithRules(userInput, i.Rules, ctx)
}

type Option string
