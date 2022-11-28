package sql

import (
	"strings"

	"github.com/umisama/go-regexpcache"
)

// Taken from UI: https://github.com/keboola/kbc-ui/blob/master/src/scripts/modules/transformations/utils/splitSqlQueriesWorker.js
// Taken and modified from: http://stackoverflow.com/questions/4747808/split-mysql-queries-in-array-each-queries-separated-by/5610067#5610067
const splitSQLRegexp = `\s*((?:'[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*"|\/\*[^*]*\*+(?:[^*/][^*]*\*+)*\/|#.*|--.*|[^"';#])+(?:;|$))`

func Split(sql string) []string {
	sql = strings.TrimSuffix(sql, "\n")
	rawResults := regexpcache.MustCompile(splitSQLRegexp).FindAllString(sql, -1)

	// Trim spaces
	results := make([]string, 0)
	for _, result := range rawResults {
		result := strings.TrimSpace(result)
		if len(result) > 0 {
			results = append(results, result)
		}
	}

	return results
}

func Join(statements []string) string {
	return strings.Join(statements, "\n\n")
}
