package remote

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetComponent(t *testing.T) {
	a, _ := TestStorageApiWithToken(t)
	component, err := a.GetComponent("ex-generic-v2")
	assert.False(t, component.IsDeprecated())
	assert.NoError(t, err)
	assert.NotNil(t, component)
	assert.Equal(t, "ex-generic-v2", component.Id)
	assert.Equal(t, "extractor", component.Type)
}

func TestGetComponentNotFound(t *testing.T) {
	a, _ := TestStorageApiWithToken(t)
	component, err := a.GetComponent("foo-bar")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Component foo-bar not found")
	assert.Nil(t, component)
}

func TestComponentIsDeprecated(t *testing.T) {
	a, _ := TestStorageApiWithToken(t)
	component, err := a.GetComponent("wr-dropbox")
	assert.NoError(t, err)
	assert.NotNil(t, component)
	assert.True(t, component.IsDeprecated())
}
