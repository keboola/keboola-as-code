package model_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/model"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/enttest"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/key"
)

func TestCreate_SetParent(t *testing.T) {
	t.Parallel()
	ctx, client, tx := txForTest(t)

	branch, err := branchBuilder(tx).
		SetBranchID(123).
		Save(ctx)
	require.NoError(t, err)

	config, err := configBuilder(tx).
		SetParent(branch).
		SetComponentID("keboola.my-component").
		SetConfigID("my-config").
		Save(ctx)
	require.NoError(t, err)

	row, err := rowBuilder(tx).
		SetParent(config).
		SetRowID("my-row").
		Save(ctx)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
	checkResults(t, ctx, client, branch, config, row)
}

func TestCreate_SetPartOfID(t *testing.T) {
	t.Parallel()
	ctx, client, tx := txForTest(t)

	branch, err := branchBuilder(tx).
		SetBranchID(123).
		Save(ctx)
	require.NoError(t, err)

	config, err := configBuilder(tx).
		SetBranchID(branch.BranchID).
		SetComponentID("keboola.my-component").
		SetConfigID("my-config").
		Save(ctx)
	require.NoError(t, err)

	row, err := rowBuilder(tx).
		SetBranchID(branch.BranchID).
		SetComponentID("keboola.my-component").
		SetConfigID("my-config").
		SetRowID("my-row").
		Save(ctx)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
	checkResults(t, ctx, client, branch, config, row)
}

func TestCreate_SetID(t *testing.T) {
	t.Parallel()
	ctx, client, tx := txForTest(t)

	branch, err := branchBuilder(tx).
		SetID(key.BranchKey{BranchID: 123}).
		Save(ctx)
	require.NoError(t, err)

	config, err := configBuilder(tx).
		SetID(key.ConfigurationKey{
			BranchID:    branch.BranchID,
			ComponentID: "keboola.my-component",
			ConfigID:    "my-config",
		}).
		Save(ctx)
	require.NoError(t, err)

	row, err := rowBuilder(tx).
		SetID(key.ConfigurationRowKey{
			BranchID:    branch.BranchID,
			ComponentID: "keboola.my-component",
			ConfigID:    "my-config",
			RowID:       "my-row",
		}).
		Save(ctx)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
	checkResults(t, ctx, client, branch, config, row)
}

func checkResults(t *testing.T, ctx context.Context, client *model.Client, b *model.Branch, c *model.Configuration, r *model.ConfigurationRow) {
	t.Helper()

	// Check created entities
	assertResults(t, b, c, r)

	// Check entities load from the DB
	b, err := client.Branch.Query().First(ctx)
	require.NoError(t, err)
	c, err = client.Configuration.Query().First(ctx)
	require.NoError(t, err)
	r, err = client.ConfigurationRow.Query().First(ctx)
	require.NoError(t, err)
	assertResults(t, b, c, r)
}

func assertResults(t *testing.T, b *model.Branch, c *model.Configuration, r *model.ConfigurationRow) {
	t.Helper()

	// Test created entities
	assert.Equal(t, keboola.BranchID(123), b.BranchID)
	assert.Equal(t, "main", b.Name)
	assert.True(t, b.IsDefault)
	assert.Equal(t, "my main branch", b.Description)

	assert.Equal(t, keboola.BranchID(123), c.BranchID)
	assert.Equal(t, keboola.ComponentID("keboola.my-component"), c.ComponentID)
	assert.Equal(t, keboola.ConfigID("my-config"), c.ConfigID)
	assert.Equal(t, "my-config", c.Name)
	assert.Equal(t, "My Config", c.Description)
	assert.False(t, c.IsDisabled)
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "foo1", Value: "bar1"},
	}), c.Content)

	assert.Equal(t, keboola.BranchID(123), r.BranchID)
	assert.Equal(t, keboola.ComponentID("keboola.my-component"), r.ComponentID)
	assert.Equal(t, keboola.ConfigID("my-config"), r.ConfigID)
	assert.Equal(t, keboola.RowID("my-row"), r.RowID)
	assert.Equal(t, "my-row", r.Name)
	assert.Equal(t, "My Row", r.Description)
	assert.True(t, r.IsDisabled)
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "foo2", Value: "bar2"},
	}), r.Content)
}

func txForTest(t *testing.T) (context.Context, *model.Client, *model.Tx) {
	t.Helper()

	ctx, client := clientForTest(t)
	tx, err := client.Tx(ctx)
	if err != nil {
		assert.Fail(t, err.Error())
	}
	return ctx, client, tx
}

func clientForTest(t *testing.T) (context.Context, *model.Client) {
	t.Helper()

	ctx := t.Context()
	client := enttest.Open(t, "sqlite3", "file:model_test?mode=memory&_fk=1")
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	return ctx, client
}

func createEntities(t *testing.T, ctx context.Context, client *model.Client) {
	t.Helper()

	tx, err := client.Tx(ctx)
	require.NoError(t, err)

	branch, err := branchBuilder(tx).
		SetBranchID(123).
		Save(ctx)
	require.NoError(t, err)

	config, err := configBuilder(tx).
		SetParent(branch).
		SetComponentID("keboola.my-component").
		SetConfigID("my-config").
		Save(ctx)
	require.NoError(t, err)

	_, err = rowBuilder(tx).
		SetParent(config).
		SetRowID("my-row").
		Save(ctx)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
}

func branchBuilder(tx *model.Tx) *model.BranchCreate {
	return tx.Branch.
		Create().
		SetName("main").
		SetIsDefault(true).
		SetDescription("my main branch")
}

func configBuilder(tx *model.Tx) *model.ConfigurationCreate {
	return tx.Configuration.
		Create().
		SetName("my-config").
		SetDescription("My Config").
		SetContent(orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "foo1", Value: "bar1"},
		}))
}

func rowBuilder(tx *model.Tx) *model.ConfigurationRowCreate {
	return tx.ConfigurationRow.
		Create().
		SetName("my-row").
		SetDescription("My Row").
		SetIsDisabled(true).
		SetContent(orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "foo2", Value: "bar2"},
		}))
}
