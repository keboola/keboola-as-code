package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
)

type AskVariableName struct{}

func (p *AskVariableName) Description() string {
	return "Variable Name"
}

func (p *AskVariableName) Question() string {
	return "Enter the name of the vault variable:"
}

func (p *AskVariableName) Default() string {
	return ""
}

func (p *AskVariableName) Validator() dialog.Validator {
	return dialog.ValueRequired
}

type AskVariableValue struct{}

func (p *AskVariableValue) Description() string {
	return "Variable Value"
}

func (p *AskVariableValue) Question() string {
	return "Enter the value of the vault variable:"
}

func (p *AskVariableValue) Default() string {
	return ""
}

func (p *AskVariableValue) Validator() dialog.Validator {
	return dialog.ValueRequired
}
