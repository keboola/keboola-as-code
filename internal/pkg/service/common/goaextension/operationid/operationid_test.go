package operationid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapOperationId(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "FooBarBazXyzAbc", mapOperationId("templates#foo-bar_BazXyz-Abc"))
	assert.Equal(t, "FooBarBazXyzAbc", mapOperationId("foo-bar_BazXyz-Abc"))
	assert.Equal(t, "Openapi3Json", mapOperationId("templates#/v1/documentation/openapi3.json"))
	assert.Equal(t, "Openapi3Json", mapOperationId("/v1/documentation/openapi3.json"))
}
