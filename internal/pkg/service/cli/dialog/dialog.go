package dialog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
)

type Dialogs struct {
	prompt.Prompt
	options *options.Options
}

func New(prompt prompt.Prompt, opts *options.Options) *Dialogs {
	return &Dialogs{Prompt: prompt, options: opts}
}

func NewForTest(t *testing.T, interactive bool) (*Dialogs, *options.Options, terminal.Console) {
	t.Helper()

	opts := options.New()
	if interactive {
		// Create virtual console
		console, err := terminal.New(t)
		assert.NoError(t, err)

		// Create prompt
		p := cli.NewPrompt(console.Tty(), console.Tty(), console.Tty(), false)

		// Create dialogs
		return New(p, opts), opts, console
	} else {
		return New(nopPrompt.New(), opts), opts, nil
	}
}
