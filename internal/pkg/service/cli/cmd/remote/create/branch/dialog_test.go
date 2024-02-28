package branch

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
)

func TestAskCreateBranch(t *testing.T) {
	t.Parallel()

	d, _, console := dialog.NewForTest(t, true)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Enter a name for the new branch"))

		assert.NoError(t, console.SendLine(`Foo Bar`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := AskCreateBranch(d, configmap.NewValue("Foo Bar"))
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createBranch.Options{
		Name: `Foo Bar`,
		Pull: true,
	}, opts)
}
