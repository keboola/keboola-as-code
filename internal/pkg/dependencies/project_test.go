package dependencies

import (
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"
)

func TestNewProjectDeps(t *testing.T) {
	t.Parallel()
	d := NewMockedDeps()
	token := storageapi.Token{IsMaster: false}
	_, err := newProjectDeps(d, d, token)
	assert.Error(t, err)
	assert.Equal(t, "a master token of a project administrator is required", err.Error())
}
