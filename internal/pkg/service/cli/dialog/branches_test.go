package dialog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	syncInit "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/sync/init"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func TestSelectBranchInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, console := createDialogs(t, true)

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{ID: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{ID: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{ID: 3}, Name: `Branch 3`}
	allBranches := []*model.Branch{branch1, branch2, branch3}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("LABEL:"))

		assert.NoError(t, console.ExpectString("Branch 1 (1)"))

		assert.NoError(t, console.ExpectString("Branch 2 (2)"))

		assert.NoError(t, console.ExpectString("Branch 3 (3)"))

		// down arrow -> select Branch 2
		assert.NoError(t, console.SendDownArrow())
		assert.NoError(t, console.SendEnter())

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	out, err := dialog.SelectBranch(allBranches, `LABEL`, configmap.NewValue(branch2.String()))
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

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{ID: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{ID: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{ID: 3}, Name: `Branch 3`}
	allBranches := []*model.Branch{branch1, branch2, branch3}

	// Run
	out, err := dialog.SelectBranch(allBranches, `LABEL`, configmap.Value[string]{Value: branch2.Name, SetBy: configmap.SetByFlag})
	assert.Same(t, branch2, out)
	assert.NoError(t, err)
}

func TestSelectBranchNonInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{ID: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{ID: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{ID: 3}, Name: `Branch 3`}
	allBranches := []*model.Branch{branch1, branch2, branch3}

	// Run
	_, err := dialog.SelectBranch(allBranches, `LABEL`, configmap.Value[string]{Value: "", SetBy: configmap.SetByDefault})
	assert.ErrorContains(t, err, "please specify branch")
}

func TestSelectBranchMissing(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{ID: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{ID: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{ID: 3}, Name: `Branch 3`}
	allBranches := []*model.Branch{branch1, branch2, branch3}

	// Run
	out, err := dialog.SelectBranch(allBranches, `LABEL`, configmap.NewValue(""))
	assert.Nil(t, out)
	assert.Error(t, err)
	assert.Equal(t, `please specify branch`, err.Error())
}

func TestSelectBranchesInteractive(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, console := createDialogs(t, true)

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{ID: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{ID: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{ID: 3}, Name: `Branch 3`}
	branch4 := &model.Branch{BranchKey: model.BranchKey{ID: 4}, Name: `Branch 4`}
	branch5 := &model.Branch{BranchKey: model.BranchKey{ID: 5}, Name: `Branch 5`}
	allBranches := []*model.Branch{branch1, branch2, branch3, branch4, branch5}

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("LABEL:"))

		assert.NoError(t, console.ExpectString("Branch 1 (1)"))

		assert.NoError(t, console.ExpectString("Branch 2 (2)"))

		assert.NoError(t, console.ExpectString("Branch 3 (3)"))

		assert.NoError(t, console.ExpectString("Branch 4 (4)"))

		assert.NoError(t, console.ExpectString("Branch 5 (5)"))

		assert.NoError(t, console.SendDownArrow()) // -> Branch 2

		assert.NoError(t, console.SendSpace()) // -> select

		assert.NoError(t, console.SendDownArrow()) // -> Branch 3

		assert.NoError(t, console.SendDownArrow()) // -> Branch 4

		assert.NoError(t, console.SendSpace()) // -> select

		assert.NoError(t, console.SendEnter()) // -> confirm

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	out, err := syncInit.SelectBranches(allBranches, `LABEL`, dialog, syncInit.Flags{})
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

	f := syncInit.Flags{
		Branches: configmap.NewValueWithOrigin("2,4", configmap.SetByFlag),
	}

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{ID: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{ID: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{ID: 3}, Name: `Branch 3`}
	branch4 := &model.Branch{BranchKey: model.BranchKey{ID: 4}, Name: `Branch 4`}
	branch5 := &model.Branch{BranchKey: model.BranchKey{ID: 5}, Name: `Branch 5`}
	allBranches := []*model.Branch{branch1, branch2, branch3, branch4, branch5}

	// Run
	out, err := syncInit.SelectBranches(allBranches, `LABEL`, dialog, f)
	assert.Equal(t, []*model.Branch{branch2, branch4}, out)
	assert.NoError(t, err)
}

func TestSelectBranchesMissing(t *testing.T) {
	t.Parallel()

	// Dependencies
	dialog, _ := createDialogs(t, false)

	// All branches
	branch1 := &model.Branch{BranchKey: model.BranchKey{ID: 1}, Name: `Branch 1`}
	branch2 := &model.Branch{BranchKey: model.BranchKey{ID: 2}, Name: `Branch 2`}
	branch3 := &model.Branch{BranchKey: model.BranchKey{ID: 3}, Name: `Branch 3`}
	branch4 := &model.Branch{BranchKey: model.BranchKey{ID: 4}, Name: `Branch 4`}
	branch5 := &model.Branch{BranchKey: model.BranchKey{ID: 5}, Name: `Branch 5`}
	allBranches := []*model.Branch{branch1, branch2, branch3, branch4, branch5}

	// Run
	out, err := syncInit.SelectBranches(allBranches, `LABEL`, dialog, syncInit.Flags{})
	assert.Nil(t, out)
	assert.Error(t, err)
	assert.Equal(t, `please specify at least one branch`, err.Error())
}
