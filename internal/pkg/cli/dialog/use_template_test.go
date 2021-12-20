package dialog_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
)

func TestAskUseTemplateOptions(t *testing.T) {
	t.Parallel()

	// Dependencies
	var err error
	var httpTransport *httpmock.MockTransport
	dialog, console := createDialogs(t, true)
	d := testdeps.NewDependencies()
	d.LoggerValue = log.NewNopLogger()
	d.FsValue = testfs.NewMemoryFs()
	d.StorageApiValue, httpTransport, _ = testapi.NewMockedStorageApi()
	setupCreateTemplateApiResponses(httpTransport)

	// Set fake file editor
	dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Enter your Facebook username")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`username`)
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your Facebook password")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`password`)
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your age")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("text") // enter invalid string value
		assert.NoError(t, err)

		_, err = console.ExpectString(`Sorry, your reply was invalid: value "text" is not integer`)
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(strings.Repeat(Backspace, 4)) // remove "text"
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("25") // enter valid numeric value
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	inputs := input.Inputs{
		{
			Id:          "facebook.username",
			Name:        "Facebook username",
			Description: "Enter your Facebook username",
			Type:        "string",
			Kind:        "input",
		},
		{
			Id:          "facebook.password",
			Name:        "Facebook password",
			Description: "Enter your Facebook password",
			Type:        "string",
			Kind:        "password",
		},
		{
			Id:          "age",
			Name:        "Your age",
			Description: "Enter your age",
			Type:        "int",
			Kind:        "input",
		},
		{
			Id:          "restricted",
			Name:        "Restricted content",
			Description: "Do you want to see restricted content?",
			Kind:        "confirm",
			If:          "age>18",
		},
	}
	opts, err := dialog.AskUseTemplateOptions(inputs)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, map[string]interface{}{"facebook.username": "username", "facebook.password": "password", "age": "25"}, opts)
}
