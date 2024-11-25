package model_test

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/configurationrow"
	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/key"
)

func TestUpdate(t *testing.T) {
	t.Parallel()
	ctx, client := clientForTest(t)

	// Create
	createEntities(t, ctx, client)

	// Query
	rowID := key.ConfigurationRowKey{
		BranchID:    123,
		ComponentID: "keboola.my-component",
		ConfigID:    "my-config",
		RowID:       "my-row",
	}
	row, err := client.ConfigurationRow.Query().Where(configurationrow.ID(rowID)).Only(ctx)
	require.NoError(t, err)
	assert.Equal(t, "my-row", row.Name)
	assert.Equal(t, "My Row", row.Description)
	assert.True(t, row.IsDisabled)

	// Update
	_, err = row.Update().SetName("new-name").SetDescription("New Description").SetIsDisabled(false).Save(ctx)
	require.NoError(t, err)

	// Query again
	row, err = client.ConfigurationRow.Query().Where(configurationrow.ID(rowID)).Only(ctx)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, "new-name", row.Name)
	assert.Equal(t, "New Description", row.Description)
	assert.False(t, row.IsDisabled)
}
