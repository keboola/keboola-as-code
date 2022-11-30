package model_test

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

func TestTableID_String(t *testing.T) {
	t.Parallel()

	tableID := model.TableID{
		Stage:  model.TableStageIn,
		Bucket: "main",
		Table:  "table1",
	}
	s := tableID.String()
	assert.Equal(t, "in.c-main.table1", s)

	parsed, err := model.ParseTableID(s)
	assert.NoError(t, err)
	assert.Equal(t, tableID, parsed)
}

func TestImportConditions_ShouldImport_Defaults(t *testing.T) {
	t.Parallel()

	// Defaults
	ic := model.ImportConditions{
		Count: 0,
		Size:  0,
		Time:  0,
	}
	// Defaults not met
	res, desc := ic.ShouldImport(model.CurrentImportState{
		Count:        50,
		Size:         1 * datasize.KB,
		Now:          time.Now(),
		LastImportAt: time.Now().Add(-1 * time.Minute),
	})
	assert.False(t, res)
	assert.Equal(t, "conditions not met", desc)

	// Default count met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        2000,
		Size:         1 * datasize.MB,
		Now:          time.Now(),
		LastImportAt: time.Now().Add(-1 * time.Minute),
	})
	assert.True(t, res)
	assert.Equal(t, "import count limit met, received: 2000 rows, limit: 1000 rows", desc)

	// Default size met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        100,
		Size:         5 * datasize.MB,
		Now:          time.Now(),
		LastImportAt: time.Now().Add(-1 * time.Minute),
	})
	assert.True(t, res)
	assert.Equal(t, "import size limit met, received: 5MB, limit: 1MB", desc)

	// Default time met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        100,
		Size:         1 * datasize.KB,
		Now:          time.Now(),
		LastImportAt: time.Now().Add(-10 * time.Minute),
	})
	assert.True(t, res)
	assert.Contains(t, desc, "import time limit met, last import at")
	assert.Contains(t, desc, "limit: 5m0s")
}

func TestImportConditions_ShouldImport_Custom(t *testing.T) {
	t.Parallel()

	// Defaults
	ic := model.ImportConditions{
		Count: 100,
		Size:  5 * datasize.MB,
		Time:  10 * time.Minute,
	}
	// Not met
	res, desc := ic.ShouldImport(model.CurrentImportState{
		Count:        50,
		Size:         1 * datasize.MB,
		Now:          time.Now(),
		LastImportAt: time.Now().Add(-1 * time.Minute),
	})
	assert.False(t, res)
	assert.Equal(t, "conditions not met", desc)

	// Count met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        200,
		Size:         1 * datasize.MB,
		Now:          time.Now(),
		LastImportAt: time.Now().Add(-1 * time.Minute),
	})
	assert.True(t, res)
	assert.Equal(t, "import count limit met, received: 200 rows, limit: 100 rows", desc)

	// Size met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        50,
		Size:         10 * datasize.MB,
		Now:          time.Now(),
		LastImportAt: time.Now().Add(-1 * time.Minute),
	})
	assert.True(t, res)
	assert.Equal(t, "import size limit met, received: 10MB, limit: 5MB", desc)

	// Time met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        50,
		Size:         1 * datasize.MB,
		Now:          time.Now(),
		LastImportAt: time.Now().Add(-20 * time.Minute),
	})
	assert.True(t, res)
	assert.Contains(t, desc, "import time limit met, last import at")
	assert.Contains(t, desc, "limit: 10m0s")
}
