package dialog

import "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"

type Dialogs struct {
	prompt.Prompt
}

func New(prompt prompt.Prompt) *Dialogs {
	return &Dialogs{prompt}
}
