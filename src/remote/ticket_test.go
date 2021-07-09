package remote

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateNewId(t *testing.T) {
	api, _ := TestStorageApiWithToken(t)
	ticket, err := api.GenerateNewId()
	assert.NoError(t, err)
	assert.NotNil(t, ticket)
	assert.NotEmpty(t, ticket.Id)
}
