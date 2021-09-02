package interaction

import (
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

const (
	DownArrow = "\u001B[B"
	Space     = " "
	Enter     = "\n"
)

// TestAllowedBranchesByFlag use flag value if present.
func TestAllowedBranchesByFlag(t *testing.T) {
	prompt, console, _ := createVirtualPrompt(t)

	// No interaction expected
	allBranches := []*model.Branch{{BranchKey: model.BranchKey{Id: 123}, Name: "Main", IsDefault: true}}
	allowedBranches := prompt.GetAllowedBranches(allBranches, true, "foo, bar")
	assert.Equal(t, model.AllowedBranches{"foo", "bar"}, allowedBranches)
	assert.NoError(t, console.Tty().Close())
	assert.NoError(t, console.Close())
}

// TestAllowedBranchesDefaultValue use default value if terminal is not interactive.
func TestAllowedBranchesDefaultValue(t *testing.T) {
	prompt, console, _ := createVirtualPrompt(t)
	prompt.Interactive = false

	// No interaction expected
	allBranches := []*model.Branch{{BranchKey: model.BranchKey{Id: 123}, Name: "Main", IsDefault: true}}
	allowedBranches := prompt.GetAllowedBranches(allBranches, false, "*")
	assert.Equal(t, model.AllowedBranches{model.AllBranchesDef}, allowedBranches)
	assert.NoError(t, console.Tty().Close())
	assert.NoError(t, console.Close())
}

// TestAllowedBranchesOnlyMain - select first option from the interactive select box
// -> only main branch.
func TestAllowedBranchesOnlyMain(t *testing.T) {
	prompt, c, _ := createVirtualPrompt(t)
	allBranches := []*model.Branch{{BranchKey: model.BranchKey{Id: 123}, Name: "Main", IsDefault: true}}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		selectOption(t, 1, c) // only main branch
		_, err := c.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	allowedBranches := prompt.GetAllowedBranches(allBranches, false, "*")
	assert.NoError(t, c.Tty().Close())
	wg.Wait()
	assert.NoError(t, c.Close())

	// Assert
	assert.Equal(t, model.AllowedBranches{model.MainBranchDef}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select second option from the interactive select box
// -> all branches.
func TestAllowedBranchesAllBranches(t *testing.T) {
	prompt, c, _ := createVirtualPrompt(t)
	allBranches := []*model.Branch{{BranchKey: model.BranchKey{Id: 123}, Name: "Main", IsDefault: true}}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		selectOption(t, 2, c) // all branches
		_, err := c.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	allowedBranches := prompt.GetAllowedBranches(allBranches, false, "*")
	assert.NoError(t, c.Tty().Close())
	wg.Wait()
	assert.NoError(t, c.Close())

	// Assert
	assert.Equal(t, model.AllowedBranches{model.AllBranchesDef}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select third option from the interactive select box
// -> select branches, and select 2/4 of the listed brances.
func TestAllowedBranchesSelectedBranches(t *testing.T) {
	prompt, c, _ := createVirtualPrompt(t)
	allBranches := []*model.Branch{
		{BranchKey: model.BranchKey{Id: 10}, Name: "Main", IsDefault: true},
		{BranchKey: model.BranchKey{Id: 20}, Name: "foo", IsDefault: false},
		{BranchKey: model.BranchKey{Id: 30}, Name: "bar", IsDefault: false},
		{BranchKey: model.BranchKey{Id: 40}, Name: "baz", IsDefault: false},
	}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		selectOption(t, 3, c) // selected branches
		_, err := c.ExpectString(`[10] Main`)
		assert.NoError(t, err)
		_, err = c.ExpectString(`[20] foo`)
		assert.NoError(t, err)
		_, err = c.ExpectString(`[30] bar`)
		assert.NoError(t, err)
		_, err = c.ExpectString(`[40] baz`)
		assert.NoError(t, err)
		time.Sleep(50 * time.Millisecond)

		// Skip Main
		_, err = c.Send(DownArrow)
		assert.NoError(t, err)
		// Select foo
		_, err = c.Send(Space)
		assert.NoError(t, err)
		_, err = c.Send(DownArrow)
		assert.NoError(t, err)
		// Skip bar
		_, err = c.Send(DownArrow)
		assert.NoError(t, err)
		// Select baz
		_, err = c.Send(Space)
		assert.NoError(t, err)
		_, err = c.Send(Enter)
		assert.NoError(t, err)
		_, err = c.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	allowedBranches := prompt.GetAllowedBranches(allBranches, false, "*")
	assert.NoError(t, c.Tty().Close())
	wg.Wait()
	assert.NoError(t, c.Close())

	// Assert, foo and baz IDs
	assert.Equal(t, model.AllowedBranches{"20", "40"}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select fourth option from the interactive select box
// -> type IDs or names and type two custom definitions.
func TestAllowedBranchesTypeList(t *testing.T) {
	prompt, c, _ := createVirtualPrompt(t)
	allBranches := []*model.Branch{
		{BranchKey: model.BranchKey{Id: 10}, Name: "Main", IsDefault: true},
		{BranchKey: model.BranchKey{Id: 20}, Name: "foo", IsDefault: false},
		{BranchKey: model.BranchKey{Id: 30}, Name: "bar", IsDefault: false},
		{BranchKey: model.BranchKey{Id: 40}, Name: "baz", IsDefault: false},
	}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		selectOption(t, 4, c) // type custom definitions
		time.Sleep(50 * time.Millisecond)
		_, err := c.Send("f**\n")
		assert.NoError(t, err)
		_, err = c.Send("b*z\n")
		assert.NoError(t, err)
		_, err = c.Send("\n\n\n")
		assert.NoError(t, err)
		_, err = c.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	allowedBranches := prompt.GetAllowedBranches(allBranches, false, "*")
	assert.NoError(t, c.Tty().Close())
	wg.Wait()
	assert.NoError(t, c.Close())

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

func createVirtualPrompt(t *testing.T) (*Prompt, *expect.Console, *vt10x.State) {
	// Create virtual console
	var stdout io.Writer
	if utils.TestIsVerbose() {
		stdout = os.Stdout
	} else {
		stdout = io.Discard
	}
	console, state, err := vt10x.NewVT10XConsole(expect.WithStdout(stdout), expect.WithDefaultTimeout(5*time.Second))
	assert.NoError(t, err)
	prompt := NewPrompt(console.Tty(), console.Tty(), console.Tty())
	prompt.Interactive = true
	return prompt, console, state
}
