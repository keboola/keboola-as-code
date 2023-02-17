package model_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	atlasSchema "ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlite"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/schema"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/platform/model/migrate"
)

// TestModelDump dumps the model in Ariga Atlas format.
func TestModelDump(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Create connection
	db, err := sql.Open("sqlite3", "file:model_test?mode=memory&_fk=1")
	assert.NoError(t, err)

	// Migrate
	tables, err := schema.CopyTables(migrate.Tables)
	assert.NoError(t, err)
	assert.NoError(t, migrate.Create(ctx, migrate.NewSchema(db), tables))

	// Inspect
	driver, err := sqlite.Open(db)
	assert.NoError(t, err)
	spec, err := driver.InspectRealm(ctx, &atlasSchema.InspectRealmOption{Mode: atlasSchema.InspectTables})
	assert.NoError(t, err)
	hclBytes, err := sqlite.MarshalHCL(spec)
	assert.NoError(t, err)
	hcl := string(hclBytes)

	// Dump
	assert.NoError(t, os.RemoveAll(".out"))
	assert.NoError(t, os.MkdirAll(".out", 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(".out", "model_actual.hcl"), hclBytes, 0644))

	// Compare
	expected, err := os.ReadFile("expected/model_expected.hcl")
	assert.NoError(t, err)
	assert.Equal(t, string(expected), hcl)
}
