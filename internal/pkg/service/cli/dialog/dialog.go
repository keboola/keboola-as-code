package dialog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
)

type Dialogs struct {
	prompt.Prompt
}

func New(prompt prompt.Prompt) *Dialogs {
	return &Dialogs{Prompt: prompt}
}

func NewForTest(t *testing.T, interactive bool) (*Dialogs, terminal.Console) {
	t.Helper()

	if interactive {
		// Create virtual console
		console, err := terminal.New(t)
		require.NoError(t, err)

		// Create prompt
		p := cli.NewPrompt(console.Tty(), console.Tty(), console.Tty(), false)

		// Create dialogs
		return New(p), console
	} else {
		return New(nopPrompt.New()), nil
	}
}
