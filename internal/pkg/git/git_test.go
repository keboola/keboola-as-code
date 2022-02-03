package git

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestGit_Available(t *testing.T) {
	t.Parallel()

	// should be always true as git is available in the container running the tests
	assert.True(t, Available())
}

func TestGit_CheckoutTemplateRepository(t *testing.T) {
	t.Parallel()

	// checkout fail from a non-existing url
	_, err := CheckoutTemplateRepository("https://non-existing-url", "main", log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `git repository not found on url "https://non-existing-url"`, err.Error())

	// checkout fail from a non-existing github repository
	_, err = CheckoutTemplateRepository("https://github.com/keboola/non-existing-repo.git", "main", log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `git repository not found on url "https://github.com/keboola/non-existing-repo.git"`, err.Error())

	// checkout fail from a non-existing branch
	_, err = CheckoutTemplateRepository("https://github.com/keboola/keboola-as-code-templates.git", "non-existing-ref", log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `branch "non-existing-ref" not found in the repository`, err.Error())

	// checkout success
	fs, err := CheckoutTemplateRepository("https://github.com/keboola/keboola-as-code-templates.git", "main", log.NewDebugLogger())
	assert.NoError(t, err)
	assert.True(t, fs.Exists(".keboola/repository.json"))
}
