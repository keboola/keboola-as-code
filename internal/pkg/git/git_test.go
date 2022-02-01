package git

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestGit_CheckoutTemplateRepository(t *testing.T) {
	t.Parallel()

	// should be always true as git is available in the container running the tests
	assert.True(t, Available())

	fs, err := CheckoutTemplateRepository("https://github.com/keboola/keboola-as-code-templates.git", "main", log.NewDebugLogger())
	assert.NoError(t, err)
	assert.True(t, fs.Exists("manifest.json"))
}
