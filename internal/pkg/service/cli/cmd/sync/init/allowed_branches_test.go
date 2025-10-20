package init

import (
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/terminal"
)

// TestAllowedBranchesByFlag use flag value if present.
func TestAskAllowedBranchesByFlag(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())
	registerMockedBranchesResponse(
		deps.MockedHTTPTransport(),
		[]*keboola.Branch{{BranchKey: keboola.BranchKey{ID: 123}, Name: "Main", IsDefault: true}},
	)
	// o.SetDefault(`branches`, `*`)
	// o.Set(`branches`, `foo, bar`)

	f := Flags{
		Branches: configmap.NewValueWithOrigin("foo, bar", configmap.SetByFlag),
	}

	// No interaction expected
	allowedBranches, err := AskAllowedBranches(t.Context(), deps, d, f)
	require.NoError(t, err)
	assert.Equal(t, model.AllowedBranches{"foo", "bar"}, allowedBranches)
	require.NoError(t, console.Tty().Close())
	require.NoError(t, console.Close())
}

// TestAllowedBranchesDefaultValue use default value if terminal is not interactive.
func TestAskAllowedBranchesDefaultValue(t *testing.T) {
	t.Parallel()

	d, _ := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())

	registerMockedBranchesResponse(
		deps.MockedHTTPTransport(),
		[]*keboola.Branch{{BranchKey: keboola.BranchKey{ID: 123}, Name: "Main", IsDefault: true}},
	)

	f := Flags{
		Branches: configmap.NewValueWithOrigin("*", configmap.SetByFlag),
	}

	// No interaction expected
	allowedBranches, err := AskAllowedBranches(t.Context(), deps, d, f)
	require.NoError(t, err)
	assert.Equal(t, model.AllowedBranches{model.AllBranchesDef}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select first option from the interactive select box
// -> only main branch.
func TestAskAllowedBranchesOnlyMain(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())
	registerMockedBranchesResponse(
		deps.MockedHTTPTransport(),
		[]*keboola.Branch{{BranchKey: keboola.BranchKey{ID: 123}, Name: "Main", IsDefault: true}},
	)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {
		selectOption(t, 1, console) // only main branch
		require.NoError(t, console.ExpectEOF())
	})

	// Run
	allowedBranches, err := AskAllowedBranches(t.Context(), deps, d, Flags{})
	require.NoError(t, err)
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert
	assert.Equal(t, model.AllowedBranches{model.MainBranchDef}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select second option from the interactive select box
// -> all branches.
func TestAskAllowedBranchesAllBranches(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())
	registerMockedBranchesResponse(
		deps.MockedHTTPTransport(),
		[]*keboola.Branch{{BranchKey: keboola.BranchKey{ID: 123}, Name: "Main", IsDefault: true}},
	)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {
		selectOption(t, 2, console) // all branches
		require.NoError(t, console.ExpectEOF())
	})

	// Run
	allowedBranches, err := AskAllowedBranches(t.Context(), deps, d, Flags{})
	require.NoError(t, err)
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert
	assert.Equal(t, model.AllowedBranches{model.AllBranchesDef}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select third option from the interactive select box
// -> select branches, and select 2/4 of the listed brances.
func TestAskAllowedBranchesSelectedBranches(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())
	registerMockedBranchesResponse(
		deps.MockedHTTPTransport(),
		[]*keboola.Branch{
			{BranchKey: keboola.BranchKey{ID: 10}, Name: "Main", IsDefault: true},
			{BranchKey: keboola.BranchKey{ID: 20}, Name: "foo", IsDefault: false},
			{BranchKey: keboola.BranchKey{ID: 30}, Name: "bar", IsDefault: false},
			{BranchKey: keboola.BranchKey{ID: 40}, Name: "baz", IsDefault: false},
		},
	)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {
		selectOption(t, 3, console) // selected branches
		require.NoError(t, console.ExpectString(`Main (10)`))
		require.NoError(t, console.ExpectString(`foo (20)`))
		require.NoError(t, console.ExpectString(`bar (30)`))
		require.NoError(t, console.ExpectString(`baz (40)`))
		time.Sleep(50 * time.Millisecond)

		// Skip Main
		require.NoError(t, console.SendDownArrow())
		// Select foo
		require.NoError(t, console.SendSpace())
		require.NoError(t, console.SendDownArrow())
		// Skip bar
		require.NoError(t, console.SendDownArrow())
		// Select baz
		require.NoError(t, console.SendSpace())
		require.NoError(t, console.SendEnter())
		require.NoError(t, console.ExpectEOF())
	})

	// Run
	allowedBranches, err := AskAllowedBranches(t.Context(), deps, d, Flags{})
	require.NoError(t, err)
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert, foo and baz IDs
	assert.Equal(t, model.AllowedBranches{"20", "40"}, allowedBranches)
}

// TestAllowedBranchesOnlyMain - select fourth option from the interactive select box
// -> type IDs or names and type two custom definitions.
func TestAskAllowedBranchesTypeList(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())
	registerMockedBranchesResponse(
		deps.MockedHTTPTransport(),
		[]*keboola.Branch{
			{BranchKey: keboola.BranchKey{ID: 10}, Name: "Main", IsDefault: true},
			{BranchKey: keboola.BranchKey{ID: 20}, Name: "foo", IsDefault: false},
			{BranchKey: keboola.BranchKey{ID: 30}, Name: "bar", IsDefault: false},
			{BranchKey: keboola.BranchKey{ID: 40}, Name: "baz", IsDefault: false},
		},
	)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {
		selectOption(t, 4, console) // type custom definitions
		require.NoError(t, console.ExpectString("Please enter one branch definition per line."))
		require.NoError(t, console.Send("f**\n"))
		require.NoError(t, console.Send("b*z\n"))
		require.NoError(t, console.Send("\n\n\n"))
		require.NoError(t, console.ExpectEOF())
	})

	// Run
	allowedBranches, err := AskAllowedBranches(t.Context(), deps, d, Flags{})
	require.NoError(t, err)
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert, foo and baz IDs
	assert.Equal(t, model.AllowedBranches{"f**", "b*z"}, allowedBranches)
}

// selectOption from interactive select box.
func selectOption(t *testing.T, option int, c terminal.Console) {
	t.Helper()

	require.NoError(t, c.ExpectString("Allowed project's branches:"))
	require.NoError(t, c.ExpectString(ModeMainBranch))
	require.NoError(t, c.ExpectString(ModeAllBranches))
	require.NoError(t, c.ExpectString(ModeSelectSpecific))
	require.NoError(t, c.ExpectString(ModeTypeList))
	for i := 1; i < option; i++ {
		require.NoError(t, c.SendDownArrow())
	}
	require.NoError(t, c.SendEnter())
}

func registerMockedBranchesResponse(httpTransport *httpmock.MockTransport, branches []*keboola.Branch) {
	httpTransport.RegisterResponder(
		"GET", `=~/storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, branches),
	)
}
