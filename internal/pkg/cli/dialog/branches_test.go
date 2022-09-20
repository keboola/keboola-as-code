package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestSelectBranchInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, console := createDialogs(t, true)
	o := options.New()

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{Id: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{Id: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{Id: 3}, Name: `Branch 3`}
	allBranches := []*model.Branch{branch1, branch2, branch3}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("LABEL:")
		assert.NoError(t, err)

		_, err = console.ExpectString("Branch 1 (1)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Branch 2 (2)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Branch 3 (3)")
		assert.NoError(t, err)

		_, err = console.SendLine(testhelper.DownArrow) // down arrow -> select Branch 2
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	out, err := dialog.SelectBranch(o, allBranches, `LABEL`)
	assert.Same(t, branch2, out)
	assert.NoError(t, err)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}

func TestSelectBranchByFlag(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)
	o := options.New()
	o.Set(`branch`, 2)

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{Id: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{Id: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{Id: 3}, Name: `Branch 3`}
	allBranches := []*model.Branch{branch1, branch2, branch3}

	// Run
	out, err := dialog.SelectBranch(o, allBranches, `LABEL`)
	assert.Same(t, branch2, out)
	assert.NoError(t, err)
}

func TestSelectBranchMissing(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)
	o := options.New()

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{Id: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{Id: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{Id: 3}, Name: `Branch 3`}
	allBranches := []*model.Branch{branch1, branch2, branch3}

	// Run
	out, err := dialog.SelectBranch(o, allBranches, `LABEL`)
	assert.Nil(t, out)
	assert.Error(t, err)
	assert.Equal(t, `please specify branch`, err.Error())
}

func TestSelectBranchesInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, console := createDialogs(t, true)
	o := options.New()

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{Id: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{Id: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{Id: 3}, Name: `Branch 3`}
	branch4 := &model.Branch{BranchKey: model.BranchKey{Id: 4}, Name: `Branch 4`}
	branch5 := &model.Branch{BranchKey: model.BranchKey{Id: 5}, Name: `Branch 5`}
	allBranches := []*model.Branch{branch1, branch2, branch3, branch4, branch5}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("LABEL:")
		assert.NoError(t, err)

		_, err = console.ExpectString("Branch 1 (1)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Branch 2 (2)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Branch 3 (3)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Branch 4 (4)")
		assert.NoError(t, err)

		_, err = console.ExpectString("Branch 5 (5)")
		assert.NoError(t, err)

		_, err = console.Send(testhelper.DownArrow) // -> Branch 2
		assert.NoError(t, err)

		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		_, err = console.Send(testhelper.DownArrow) // -> Branch 3
		assert.NoError(t, err)

		_, err = console.Send(testhelper.DownArrow) // -> Branch 4
		assert.NoError(t, err)

		_, err = console.Send(testhelper.Space) // -> select
		assert.NoError(t, err)

		_, err = console.Send(testhelper.Enter) // -> confirm
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	out, err := dialog.SelectBranches(o, allBranches, `LABEL`)
	assert.Equal(t, []*model.Branch{branch2, branch4}, out)
	assert.NoError(t, err)

	// Close terminal
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())
}

func TestSelectBranchesByFlag(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)
	o := options.New()
	o.Set(`branches`, `2,4`)

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{Id: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{Id: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{Id: 3}, Name: `Branch 3`}
	branch4 := &model.Branch{BranchKey: model.BranchKey{Id: 4}, Name: `Branch 4`}
	branch5 := &model.Branch{BranchKey: model.BranchKey{Id: 5}, Name: `Branch 5`}
	allBranches := []*model.Branch{branch1, branch2, branch3, branch4, branch5}

	// Run
	out, err := dialog.SelectBranches(o, allBranches, `LABEL`)
	assert.Equal(t, []*model.Branch{branch2, branch4}, out)
	assert.NoError(t, err)
}

func TestSelectBranchesMissing(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)
	o := options.New()

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{Id: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{Id: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{Id: 3}, Name: `Branch 3`}
	branch4 := &model.Branch{BranchKey: model.BranchKey{Id: 4}, Name: `Branch 4`}
	branch5 := &model.Branch{BranchKey: model.BranchKey{Id: 5}, Name: `Branch 5`}
	allBranches := []*model.Branch{branch1, branch2, branch3, branch4, branch5}

	// Run
	out, err := dialog.SelectBranches(o, allBranches, `LABEL`)
	assert.Nil(t, out)
	assert.Error(t, err)
	assert.Equal(t, `please specify at least one branch`, err.Error())
}
