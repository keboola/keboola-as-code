package model_test

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T) {
	t.Parallel()
	ctx, client := clientForTest(t)

	// Create
	createEntities(t, ctx, client)
	assert.Equal(t, 1, client.Branch.Query().CountX(ctx))
	assert.Equal(t, 1, client.Configuration.Query().CountX(ctx))
	assert.Equal(t, 1, client.ConfigurationRow.Query().CountX(ctx))

	// Cascade delete
	count, err := client.Branch.Delete().Exec(ctx)
	assert.Equal(t, 1, count)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, 0, client.Branch.Query().CountX(ctx))
	assert.Equal(t, 0, client.Configuration.Query().CountX(ctx))
	assert.Equal(t, 0, client.ConfigurationRow.Query().CountX(ctx))
}
