package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestManifestInMemory(t *testing.T) {
	m := NewInMemory()
	assert.Len(t, m.All(), 0)

	key := fixtures.MockedKey{Id: "123"}
	m.MustAdd(&fixtures.MockedManifest{MockedKey: key, PathValue: model.NewAbsPath("foo", "bar")})
	assert.Len(t, m.All(), 1)

	m.Remove(key)
	assert.Len(t, m.All(), 0)
}
