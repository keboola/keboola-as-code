package etcdlogger

import (
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/stretchr/testify/assert"
	"github.com/umisama/go-regexpcache"
	"os"
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

// AssertFromFile logs captured by the KVLogWrapper.
// Comments "// ..." and empty lines are ignored.
func AssertFromFile(t assert.TestingT, path, actual string) bool {
	data, err := os.ReadFile(path)
	if assert.NoError(t, err) || errors.Is(err, os.ErrNotExist) {
		expected := string(data)
		return Assert(t, expected, actual)
	}
	return false
}
