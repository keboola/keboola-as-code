package dialog_test

import (
	"testing"
	"time"

	"github.com/Netflix/go-expect"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	interactivePrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

func mockedStorageApi(branches []*model.Branch) *remote.StorageApi {
	api, httpTransport := testapi.NewMockedStorageApi(log.NewDebugLogger())
	httpTransport.RegisterResponder(
		"GET", `=~/storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, branches),
	)
	return api
}

func createDialogs(t *testing.T, interactive bool) (*dialog.Dialogs, *expect.Console) {
	t.Helper()

	if interactive {
		// Create virtual console
		stdout := testhelper.VerboseStdout()
		console, _, err := testhelper.NewVirtualTerminal(t, expect.WithStdout(stdout), expect.WithCloser(stdout), expect.WithDefaultTimeout(5*time.Second))
		assert.NoError(t, err)

		// Create prompt
		prompt := interactivePrompt.New(console.Tty(), console.Tty(), console.Tty())

		// Create dialogs
		return dialog.New(prompt), console
	} else {
		return dialog.New(nopPrompt.New()), nil
	}
}
