package etcdlogger

import (
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/umisama/go-regexpcache"
)

type tHelper interface {
	Helper()
}

// Assert logs captured by the KVLogWrapper.
// Comments "// ..." and empty lines are ignored.
func Assert(t assert.TestingT, expected, actual string) bool {
	if h, ok := t.(tHelper); ok {
		h.Helper()
	}

	stripComments := regexpcache.MustCompile(`(?m)^\s*//.*$`)
	expected = stripComments.ReplaceAllString(expected, "")
	actual = stripComments.ReplaceAllString(actual, "")

	stripEmptyLines := regexpcache.MustCompile(`(^|\n)\s*\n`)
	expected = stripEmptyLines.ReplaceAllString(expected, "\n")
	actual = stripEmptyLines.ReplaceAllString(actual, "\n")

	return wildcards.Assert(t, expected, actual)
}
