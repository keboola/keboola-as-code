package model_test

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

func TestImportConditions_ShouldImport_Defaults(t *testing.T) {
	t.Parallel()

	now, _ := time.Parse(time.RFC3339, "2010-01-01T01:01:01Z")
	nowUTC := model.UTCTime(now)
	before01MinUTC := model.UTCTime(now.Add(-1 * time.Minute))
	before20MinUTC := model.UTCTime(now.Add(-20 * time.Minute))

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
		Now:          nowUTC,
		LastImportAt: before01MinUTC,
	})
	assert.False(t, res)
	assert.Equal(t, "conditions not met", desc)

	// Default count met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        2000,
		Size:         1 * datasize.MB,
		Now:          nowUTC,
		LastImportAt: before01MinUTC,
	})
	assert.True(t, res)
	assert.Equal(t, "import count threshold met, received: 2000 rows, threshold: 1000 rows", desc)

	// Default size met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        100,
		Size:         5 * datasize.MB,
		Now:          nowUTC,
		LastImportAt: before01MinUTC,
	})
	assert.True(t, res)
	assert.Equal(t, "import size threshold met, received: 5MB, threshold: 1MB", desc)

	// Default time met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        100,
		Size:         1 * datasize.KB,
		Now:          nowUTC,
		LastImportAt: before20MinUTC,
	})
	assert.True(t, res)
	assert.Equal(t, "import time threshold met, last import at: 2010-01-01T00:41:01.000Z, passed: 20m0s threshold: 5m0s", desc)
}

func TestImportConditions_ShouldImport_Custom(t *testing.T) {
	t.Parallel()

	now, _ := time.Parse(time.RFC3339, "2010-01-01T01:01:01Z")
	nowUTC := model.UTCTime(now)
	before01MinUTC := model.UTCTime(now.Add(-1 * time.Minute))
	before20MinUTC := model.UTCTime(now.Add(-20 * time.Minute))

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
		Now:          nowUTC,
		LastImportAt: before01MinUTC,
	})
	assert.False(t, res)
	assert.Equal(t, "conditions not met", desc)

	// Count met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        200,
		Size:         1 * datasize.MB,
		Now:          nowUTC,
		LastImportAt: before01MinUTC,
	})
	assert.True(t, res)
	assert.Equal(t, "import count threshold met, received: 200 rows, threshold: 100 rows", desc)

	// Size met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        50,
		Size:         10 * datasize.MB,
		Now:          nowUTC,
		LastImportAt: before01MinUTC,
	})
	assert.True(t, res)
	assert.Equal(t, "import size threshold met, received: 10MB, threshold: 5MB", desc)

	// Time met
	res, desc = ic.ShouldImport(model.CurrentImportState{
		Count:        50,
		Size:         1 * datasize.MB,
		Now:          nowUTC,
		LastImportAt: before20MinUTC,
	})
	assert.True(t, res)
	assert.Equal(t, "import time threshold met, last import at: 2010-01-01T00:41:01.000Z, passed: 20m0s threshold: 10m0s", desc)
}
