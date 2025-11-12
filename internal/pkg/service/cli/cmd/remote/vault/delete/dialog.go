package deletevault

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
)

type AskVariableName struct{}

func (p *AskVariableName) Description() string {
	return "Variable Name"
}

func (p *AskVariableName) Question() string {
	return "Enter the name of the vault variable to delete:"
}

func (p *AskVariableName) Default() string {
	return ""
}

func (p *AskVariableName) Validator() dialog.Validator {
	return dialog.ValueRequired
}
