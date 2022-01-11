package input

import (
	"context"

	goValuate "gopkg.in/Knetic/govaluate.v3"

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
		{
			Tag:  "template-input-if",
			Func: validateInputIf,
		},
	}
	return validator.Validate(i, validations...)
}

const (
	KindInput       = "input"
	KindPassword    = "password"
	KindTextarea    = "textarea"
	KindConfirm     = "confirm"
	KindSelect      = "select"
	KindMultiSelect = "multiselect"
)

type Input struct {
	Id          string      `json:"id" validate:"required,template-input-id"`
	Name        string      `json:"name" validate:"required"`
	Description string      `json:"description" validate:"required"`
	Default     interface{} `json:"default,omitempty" validate:"omitempty,template-input-default"`
	Kind        string      `json:"kind" validate:"required,oneof=input password textarea confirm select multiselect"`
	Type        string      `json:"type,omitempty" validate:"required_if=Kind input,omitempty,oneof=string int float64,template-input-type"`
	Options     Options     `json:"options,omitempty" validate:"required_if=Type select Type multiselect,template-input-options"`
	Rules       string      `json:"rules,omitempty" validate:"template-input-rules"`
	If          string      `json:"if,omitempty" validate:"template-input-if"`
}

// ValidateUserInput validates input from the template user using Input.Rules.
func (i Input) ValidateUserInput(userInput interface{}, ctx context.Context) error {
	if err := validateUserInputTypeByKind(userInput, i.Kind); err != nil {
		return err
	}

	if i.Kind == KindInput && i.Type != "" {
		err := validateUserInputByType(userInput, i.Type)
		if err != nil {
			return err
		}
	}

	return validateUserInputWithRules(userInput, i.Rules, ctx)
}

// Available decides if the input should be visible to user according to Input.If.
func (i Input) Available(params map[string]interface{}) bool {
	if i.If == "" {
		return true
	}
	expression, err := goValuate.NewEvaluableExpression(i.If)
	if err != nil {
		panic(err)
	}
	result, err := expression.Evaluate(params)
	if err != nil {
		panic(err)
	}
	return result.(bool)
}

type Option struct {
	Id   string `json:"id" validate:"required"`
	Name string `json:"name" validate:"required"`
}

type Options []Option

func (options Options) GetIndexByName(name string) int {
	for i, o := range options {
		if o.Name == name {
			return i
		}
	}
	return 0
}

func (options Options) Names() []string {
	optionsNames := make([]string, 0)
	for _, o := range options {
		optionsNames = append(optionsNames, o.Name)
	}
	return optionsNames
}
