package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
)

// inputsDetailDialog to define name/description for each user input.
type inputsDetailDialog struct {
	prompt prompt.Prompt
	inputs inputsMap
}

func newInputsDetailsDialog(prompt prompt.Prompt, inputs inputsMap) *inputsDetailDialog {
	return &inputsDetailDialog{prompt: prompt, inputs: inputs}
}

func (d *inputsDetailDialog) ask() error {
	return nil
}
