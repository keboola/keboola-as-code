package dialog_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
)

func createDialogs(t *testing.T, interactive bool) (*dialog.Dialogs, terminal.Console) {
	t.Helper()

	if interactive {
		// Create virtual console
		console, err := terminal.New(t)
		require.NoError(t, err)

		// Create prompt
		p := cli.NewPrompt(console.Tty(), console.Tty(), console.Tty(), false)

		// Create dialogs
		return dialog.New(p), console
	} else {
		return dialog.New(nopPrompt.New()), nil
	}
}
