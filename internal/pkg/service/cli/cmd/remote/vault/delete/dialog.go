package deletevault

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
)

func AskVariableName() *prompt.Question {
	return &prompt.Question{
		Label:       "Variable name",
		Description: "Enter the vault variable name to delete.",
		Validator:   prompt.ValueRequired,
	}
}
