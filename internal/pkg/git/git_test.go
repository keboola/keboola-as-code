package git

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGit_Available(t *testing.T) {
	t.Parallel()

	// should be always true as git is available in the container running the tests
	assert.True(t, Available())
}
