package dialog_test

import (
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	interactivePrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
)

func registerMockedBranchesResponse(httpTransport *httpmock.MockTransport, branches []*storageapi.Branch) {
	httpTransport.RegisterResponder(
		"GET", `=~/storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, branches),
	)
}

func createDialogs(t *testing.T, interactive bool) (*dialog.Dialogs, terminal.Console) {
	t.Helper()

	if interactive {
		// Create virtual console
		console, err := terminal.New(t)
		assert.NoError(t, err)

		// Create prompt
		prompt := interactivePrompt.New(console.Tty(), console.Tty(), console.Tty())

		// Create dialogs
		return dialog.New(prompt), console
	} else {
		return dialog.New(nopPrompt.New()), nil
	}
}
