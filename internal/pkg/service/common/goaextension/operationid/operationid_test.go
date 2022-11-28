package operationid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapOperationId(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "FooBarBazXyzAbc", mapOperationID("templates#foo-bar_BazXyz-Abc"))
	assert.Equal(t, "FooBarBazXyzAbc", mapOperationID("foo-bar_BazXyz-Abc"))
	assert.Equal(t, "Openapi3Json", mapOperationID("templates#/v1/documentation/openapi3.json"))
	assert.Equal(t, "Openapi3Json", mapOperationID("/v1/documentation/openapi3.json"))
}
