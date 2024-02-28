package dialog_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
)

func createDialogs(t *testing.T, interactive bool) (*dialog.Dialogs, *options.Options, terminal.Console) {
	t.Helper()

	opts := options.New()
	if interactive {
		// Create virtual console
		console, err := terminal.New(t)
		assert.NoError(t, err)

		// Create prompt
		p := cli.NewPrompt(console.Tty(), console.Tty(), console.Tty(), false)

		// Create dialogs
		return dialog.New(p, opts), opts, console
	} else {
		return dialog.New(nopPrompt.New(), opts), opts, nil
	}
}
