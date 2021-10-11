package remote_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/remote"
)

func TestListAllComponents(t *testing.T) {
	a, _ := TestStorageApiWithToken(t)
	components, err := a.ListAllComponents()
	assert.NoError(t, err)
	assert.Greater(t, len(components), 0)
}

func TestNewComponentList(t *testing.T) {
	a, _ := TestStorageApiWithToken(t)
	components, err := a.NewComponentList()
	assert.NoError(t, err)
	assert.Greater(t, len(components), 0)
	assert.True(t, strings.HasPrefix(components[0].Id, `keboola.`))
}
