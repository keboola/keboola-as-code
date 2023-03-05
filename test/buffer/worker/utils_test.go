package worker

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/umisama/go-regexpcache"
)

// tablePreview loads preview of the table, sorted by the "id" column.
func (ts *testSuite) tablePreview(tableID, sortBy string) *keboola.TablePreview {
	opts := []keboola.PreviewOption{keboola.WithLimitRows(20), keboola.WithOrderBy(sortBy, keboola.OrderAsc)}
	preview, err := ts.project.
		KeboolaProjectAPI().
		PreviewTableRequest(keboola.MustParseTableID(tableID), opts...).
		Send(ts.ctx)
	assert.NoError(ts.t, err)

	// Replace random dates
	for i, row := range preview.Rows {
		for j := range row {
			col := &preview.Rows[i][j]
			if regexpcache.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$`).MatchString(*col) {
				*col = "<date>"
			}
		}
	}

	return preview
}
