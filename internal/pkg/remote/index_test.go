package remote

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListAllComponents(t *testing.T) {
	a, _ := TestStorageApiWithToken(t)
	components, err := a.ListAllComponents()
	assert.NoError(t, err)
	assert.Greater(t, len(components), 0)
}
