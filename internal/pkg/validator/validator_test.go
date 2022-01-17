package validator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidatorProcessNamespace(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "", processNamespace(""))
	assert.Equal(t, "", processNamespace("Struct.field"))
	assert.Equal(t, "field1.field2", processNamespace("Struct.field1.__nested__.field2.field3"))
}

func TestValidatorRequiredInProject(t *testing.T) {
	err := ValidateCtx(`value`, context.Background(), `required_in_project`, `some_field`)
	assert.NoError(t, err)

	err = ValidateCtx(``, context.Background(), `required_in_project`, `some_field`)
	assert.Error(t, err)
	// assert.Equal(t, "...", err.Error())
}
