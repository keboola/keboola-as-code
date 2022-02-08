package input

import (
	"context"

	goValuate "gopkg.in/Knetic/govaluate.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

type Inputs struct {
	inputs []Input
}

func NewInputs(inputs []Input) *Inputs {
	if inputs == nil {
		inputs = make([]Input, 0)
	}
	return &Inputs{
		inputs: inputs,
	}
}

// Load inputs from the FileName.
func Load(fs filesystem.Fs) (*Inputs, error) {
	f, err := loadFile(fs)
	if err != nil {
		return nil, err
	}
	return &Inputs{inputs: f.Inputs}, nil
}

// Save inputs to the FileName.
func (i *Inputs) Save(fs filesystem.Fs) error {
	if err := saveFile(fs, &file{Inputs: i.inputs}); err != nil {
		return err
	}
	return nil
}

func (i *Inputs) All() []Input {
	out := make([]Input, len(i.inputs))
	copy(out, i.inputs)
	return out
}

func (i *Inputs) Set(inputs []Input) {
	i.inputs = inputs
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
	Default     interface{} `json:"default,omitempty" validate:"omitempty,template-input-default"`
	Kind        string      `json:"kind" validate:"required,oneof=input password textarea confirm select multiselect"`
	Type        string      `json:"type,omitempty" validate:"required_if=Kind input,omitempty,oneof=string int float64,template-input-type"`
	Options     Options     `json:"options,omitempty" validate:"required_if=Type select Type multiselect,template-input-options"`
	Rules       string      `json:"rules,omitempty" validate:"template-input-rules"`
	If          string      `json:"if,omitempty" validate:"template-input-if"`
}

// ValidateUserInput validates input from the template user using Input.Rules.
func (i Input) ValidateUserInput(userInput interface{}, ctx context.Context) error {
	if err := validateUserInputTypeByKind(userInput, i.Kind, i.Name); err != nil {
		return err
	}

	if i.Kind == string(KindInput) && i.Type != "" {
		err := validateUserInputByType(userInput, i.Type, i.Name)
		if err != nil {
			return err
		}
	}

	if i.Rules == "" {
		return nil
	}

	return validateUserInputWithRules(ctx, userInput, i.Rules, i.Name)
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
