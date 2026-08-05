package remote

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func rowWithID(id string) *model.ConfigRow {
	return &model.ConfigRow{ConfigRowKey: model.ConfigRowKey{ID: keboola.RowID(id)}}
}

func rowIDs(rows []*model.ConfigRow) []string {
	ids := make([]string, len(rows))
	for i, row := range rows {
		ids[i] = string(row.ID)
	}
	return ids
}

func TestApplyRowsSortOrder_Empty(t *testing.T) {
	t.Parallel()
	rows := []*model.ConfigRow{rowWithID("1"), rowWithID("2")}
	assert.Equal(t, rows, applyRowsSortOrder(rows, nil))
}

func TestApplyRowsSortOrder_Reorders(t *testing.T) {
	t.Parallel()
	rows := []*model.ConfigRow{rowWithID("1"), rowWithID("2"), rowWithID("3")}
	result := applyRowsSortOrder(rows, []string{"3", "1", "2"})
	assert.Equal(t, []string{"3", "1", "2"}, rowIDs(result))
}

func TestApplyRowsSortOrder_UnlistedRowsAppendedInOriginalOrder(t *testing.T) {
	t.Parallel()
	rows := []*model.ConfigRow{rowWithID("1"), rowWithID("2"), rowWithID("3")}
	// "2" isn't mentioned in sortOrder - stays at the end, keeping its relative position.
	result := applyRowsSortOrder(rows, []string{"3", "1"})
	assert.Equal(t, []string{"3", "1", "2"}, rowIDs(result))
}

func TestApplyRowsSortOrder_UnknownIDsInSortOrderAreIgnored(t *testing.T) {
	t.Parallel()
	rows := []*model.ConfigRow{rowWithID("1"), rowWithID("2")}
	// "does-not-exist" doesn't correspond to any row - skipped, not an error.
	result := applyRowsSortOrder(rows, []string{"does-not-exist", "2", "1"})
	assert.Equal(t, []string{"2", "1"}, rowIDs(result))
}
