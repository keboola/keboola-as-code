package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidatorProcessNamespace(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "", processNamespace(""))
	assert.Equal(t, "", processNamespace("Struct.field"))
	assert.Equal(t, "field1.field2", processNamespace("Struct.field1.__nested__.field2.field3"))
}
