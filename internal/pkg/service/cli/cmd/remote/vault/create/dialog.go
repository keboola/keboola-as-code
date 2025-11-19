package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
)

func AskVariableName() *prompt.Question {
	return &prompt.Question{
		Label:       "Variable name",
		Description: "Enter the vault variable name.",
		Validator:   prompt.ValueRequired,
	}
}

func AskVariableValue() *prompt.Question {
	return &prompt.Question{
		Label:       "Variable value",
		Description: "Enter the vault variable value.",
		Validator:   prompt.ValueRequired,
		Hidden:      true,
	}
}
