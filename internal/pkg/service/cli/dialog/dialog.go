package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
)

type Dialogs struct {
	prompt.Prompt
	options *options.Options
}

func New(prompt prompt.Prompt, opts *options.Options) *Dialogs {
	return &Dialogs{Prompt: prompt, options: opts}
}
