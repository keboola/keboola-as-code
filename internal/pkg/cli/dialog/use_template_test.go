package dialog_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

// If condition for restricted input is met by setting the age above the limit.
func TestAskUseTemplateOptionsIfMet(t *testing.T) {
	t.Parallel()

	// test dependencies
	dialog, console := createDialogs(t, true)
	d := testdeps.New()
	_, httpTransport := d.UseMockedStorageApi()
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

		// username can contain alphanum only
		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("u-s")
		assert.NoError(t, err)

		_, err = console.ExpectString("username can only contain alphanumeric characters")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(strings.Repeat(Backspace, 3)) // remove "u-s"
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("username")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your Facebook password")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("password")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your age")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("text") // enter invalid string value
		assert.NoError(t, err)

		_, err = console.ExpectString(`Sorry, your reply was invalid: value "text" is not integer`)
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(strings.Repeat(Backspace, 4)) // remove "text"
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("25") // enter valid numeric value
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("yes")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.ExpectString("What do you like to drink?")
		assert.NoError(t, err)

		_, err = console.ExpectString("Beer")
		assert.NoError(t, err)

		_, err = console.ExpectString("Wine")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.DownArrow) // -> Wine
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> confirm
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.ExpectString("Anything stronger?")
		assert.NoError(t, err)

		_, err = console.ExpectString("Rum")
		assert.NoError(t, err)

		_, err = console.ExpectString("Vodka")
		assert.NoError(t, err)

		_, err = console.ExpectString("Whiskey")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.DownArrow) // -> Vodka
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.DownArrow) // -> Whiskey
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // -> confirm
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
			Rules:       "alphanum",
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
		{
			Id:          "drink",
			Name:        "Favorite drink",
			Description: "What do you like to drink?",
			Kind:        "select",
			If:          "age>18",
			Options:     input.Options{{Id: "beer", Name: "Beer"}, {Id: "wine", Name: "Wine"}},
		},
		{
			Id:          "drinks",
			Name:        "Stronger drinks",
			Description: "Anything stronger?",
			Kind:        "multiselect",
			If:          "age>18",
			Options:     input.Options{{Id: "rum", Name: "Rum"}, {Id: "vodka", Name: "Vodka"}, {Id: "whiskey", Name: "Whiskey"}},
		},
	}
	opts, err := dialog.AskUseTemplateOptions(inputs)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, map[string]interface{}{"facebook.username": "username", "facebook.password": "password", "age": 25, "restricted": true, "drink": "wine", "drinks": []string{"rum", "whiskey"}}, opts)
}

// If condition for restricted input is not met by setting the age below the limit and so that input is not shown to the user.
func TestAskUseTemplateOptionsIfNotMet(t *testing.T) {
	t.Parallel()

	// test ependencies
	dialog, console := createDialogs(t, true)
	d := testdeps.New()
	_, httpTransport := d.UseMockedStorageApi()
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
		_, err = console.SendLine("username")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your Facebook password")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("password")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter your age")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("15")
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
		{
			Id:          "drink",
			Name:        "Favorite drink",
			Description: "What do you like to drink?",
			Kind:        "select",
			If:          "age>18",
			Options:     input.Options{{Id: "beer", Name: "Beer"}, {Id: "wine", Name: "Wine"}},
		},
	}
	opts, err := dialog.AskUseTemplateOptions(inputs)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, map[string]interface{}{"facebook.username": "username", "facebook.password": "password", "age": 15}, opts)
}
