package jsonnet

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNopImporter(t *testing.T) {
	t.Parallel()
	_, err := Evaluate(`import "foo/bar.jsonnet"`, nil)
	assert.Error(t, err)
	assert.Equal(t, "jsonnet error: RUNTIME ERROR: imports are not enabled", err.Error())
}
