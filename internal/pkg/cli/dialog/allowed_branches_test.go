package dialog_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Netflix/go-expect"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	DownArrow = "\u001B[B"
	Space     = " "
	Enter     = "\n"
	Backspace = "\b"
)

// TestAllowedBranchesByFlag use flag value if present.
func TestAskAllowedBranchesByFlag(t *testing.T) {
	t.Parallel()
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	registerMockedBranchesResponse(
		d.MockedHttpTransport(),
		[]*storageapi.Branch{{BranchKey: storageapi.BranchKey{ID: 123}, Name: "Main", IsDefault: true}},
	)
	d.Options().SetDefault(`branches`, `*`)
	d.Options().Set(`branches`, `foo, bar`)

	// No interaction expected
	allowedBranches, err := dialog.AskAllowedBranches(context.Background(), d)
	assert.NoError(t, err)
	assert.Equal(t, model.AllowedBranches{"foo", "bar"}, allowedBranches)
	assert.NoError(t, console.Tty().Close())
	assert.NoError(t, console.Close())
}

// TestAllowedBranchesDefaultValue use default value if terminal is not interactive.
func TestAskAllowedBranchesDefaultValue(t *testing.T) {
	t.Parallel()
	dialog, _ := createDialogs(t, false)
	d := dependencies.NewMockedDeps()
	registerMockedBranchesResponse(
		d.MockedHttpTransport(),
		[]*storageapi.Branch{{BranchKey: storageapi.BranchKey{ID: 123}, Name: "Main", IsDefault: true}},
	)
	d.Options().SetDefault(`branches`, `*`)

	// No interaction expected
	allowedBranches, err := dialog.AskAllowedBranches(context.Background(), d)
	assert.NoError(t, err)
	assert.Equal(t, model.AllowedBranches{model.AllBranchesDef}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select first option from the interactive select box
// -> only main branch.
func TestAskAllowedBranchesOnlyMain(t *testing.T) {
	t.Parallel()
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	registerMockedBranchesResponse(
		d.MockedHttpTransport(),
		[]*storageapi.Branch{{BranchKey: storageapi.BranchKey{ID: 123}, Name: "Main", IsDefault: true}},
	)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		selectOption(t, 1, console) // only main branch
		_, err := console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	allowedBranches, err := dialog.AskAllowedBranches(context.Background(), d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, model.AllowedBranches{model.MainBranchDef}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select second option from the interactive select box
// -> all branches.
func TestAskAllowedBranchesAllBranches(t *testing.T) {
	t.Parallel()
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	registerMockedBranchesResponse(
		d.MockedHttpTransport(),
		[]*storageapi.Branch{{BranchKey: storageapi.BranchKey{ID: 123}, Name: "Main", IsDefault: true}},
	)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		selectOption(t, 2, console) // all branches
		_, err := console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	allowedBranches, err := dialog.AskAllowedBranches(context.Background(), d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, model.AllowedBranches{model.AllBranchesDef}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select third option from the interactive select box
// -> select branches, and select 2/4 of the listed brances.
func TestAskAllowedBranchesSelectedBranches(t *testing.T) {
	t.Parallel()
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	registerMockedBranchesResponse(
		d.MockedHttpTransport(),
		[]*storageapi.Branch{
			{BranchKey: storageapi.BranchKey{ID: 10}, Name: "Main", IsDefault: true},
			{BranchKey: storageapi.BranchKey{ID: 20}, Name: "foo", IsDefault: false},
			{BranchKey: storageapi.BranchKey{ID: 30}, Name: "bar", IsDefault: false},
			{BranchKey: storageapi.BranchKey{ID: 40}, Name: "baz", IsDefault: false},
		},
	)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		selectOption(t, 3, console) // selected branches
		_, err := console.ExpectString(`Main (10)`)
		assert.NoError(t, err)
		_, err = console.ExpectString(`foo (20)`)
		assert.NoError(t, err)
		_, err = console.ExpectString(`bar (30)`)
		assert.NoError(t, err)
		_, err = console.ExpectString(`baz (40)`)
		assert.NoError(t, err)
		time.Sleep(50 * time.Millisecond)

		// Skip Main
		_, err = console.Send(DownArrow)
		assert.NoError(t, err)
		// Select foo
		_, err = console.Send(Space)
		assert.NoError(t, err)
		_, err = console.Send(DownArrow)
		assert.NoError(t, err)
		// Skip bar
		_, err = console.Send(DownArrow)
		assert.NoError(t, err)
		// Select baz
		_, err = console.Send(Space)
		assert.NoError(t, err)
		_, err = console.Send(Enter)
		assert.NoError(t, err)
		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	allowedBranches, err := dialog.AskAllowedBranches(context.Background(), d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert, foo and baz IDs
	assert.Equal(t, model.AllowedBranches{"20", "40"}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select fourth option from the interactive select box
// -> type IDs or names and type two custom definitions.
func TestAskAllowedBranchesTypeList(t *testing.T) {
	t.Parallel()
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	registerMockedBranchesResponse(
		d.MockedHttpTransport(),
		[]*storageapi.Branch{
			{BranchKey: storageapi.BranchKey{ID: 10}, Name: "Main", IsDefault: true},
			{BranchKey: storageapi.BranchKey{ID: 20}, Name: "foo", IsDefault: false},
			{BranchKey: storageapi.BranchKey{ID: 30}, Name: "bar", IsDefault: false},
			{BranchKey: storageapi.BranchKey{ID: 40}, Name: "baz", IsDefault: false},
		},
	)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		selectOption(t, 4, console) // type custom definitions
		_, err := console.ExpectString("Please enter one branch definition per line.")
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = console.Send("f**\n")
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = console.Send("b*z\n")
		assert.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		_, err = console.Send("\n\n\n")
		assert.NoError(t, err)
		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	allowedBranches, err := dialog.AskAllowedBranches(context.Background(), d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert, foo and baz IDs
	assert.Equal(t, model.AllowedBranches{"f**", "b*z"}, allowedBranches)
}

// selectOption from interactive select box.
func selectOption(t *testing.T, option int, c *expect.Console) {
	t.Helper()

	var err error
	_, err = c.ExpectString("Allowed project's branches:")
	assert.NoError(t, err)
	_, err = c.ExpectString(ModeMainBranch)
	assert.NoError(t, err)
	_, err = c.ExpectString(ModeAllBranches)
	assert.NoError(t, err)
	_, err = c.ExpectString(ModeSelectSpecific)
	assert.NoError(t, err)
	_, err = c.ExpectString(ModeTypeList)
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	for i := 1; i < option; i++ {
		_, err = c.Send(DownArrow)
		assert.NoError(t, err)
	}
	_, err = c.Send(Enter) // enter
	assert.NoError(t, err)
}
