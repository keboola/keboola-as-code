package sql

import (
	"github.com/umisama/go-regexpcache"
	"strings"
)

// Taken from UI: https://github.com/keboola/kbc-ui/blob/master/src/scripts/modules/transformations/utils/splitSqlQueriesWorker.js
// Taken and modified from: http://stackoverflow.com/questions/4747808/split-mysql-queries-in-array-each-queries-separated-by/5610067#5610067
const splitSqlRegexp = `\s*((?:'[^'\\]*(?:\\.[^'\\]*)*'|"[^"\\]*(?:\\.[^"\\]*)*"|\/\*[^*]*\*+(?:[^*/][^*]*\*+)*\/|#.*|--.*|[^"';#])+(?:;|$))`

func Split(sql string) []string {
	results := regexpcache.MustCompile(splitSqlRegexp).FindAllString(sql, -1)
	if results == nil {
		return make([]string, 0)
	}
	return results
}

func Join(statements []string) string {
	return strings.Join(statements, "\n\n")
}
