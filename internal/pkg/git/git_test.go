// nolint: forbidigo
package git

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func TestGit_Available(t *testing.T) {
	t.Parallel()

	// should be always true as git is available in the container running the tests
	assert.True(t, Available())
}

func TestGit_CheckoutTemplateRepository_Remote(t *testing.T) {
	t.Parallel()

	// checkout fail from a non-existing url
	repository := model.TemplateRepository{Type: "git", Name: "keboola", Url: "https://non-existing-url", Ref: "main"}
	template, err := model.NewTemplateRefFromString(repository, "tmpl1", "v1")
	assert.NoError(t, err)
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `templates git repository not found on url "https://non-existing-url"`, err.Error())

	// checkout fail from a non-existing github repository
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: "https://github.com/keboola/non-existing-repo.git", Ref: "main"}
	template, err = model.NewTemplateRefFromString(repository, "tmpl1", "v1")
	assert.NoError(t, err)
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `templates git repository not found on url "https://github.com/keboola/non-existing-repo.git"`, err.Error())

	// checkout fail from a non-existing branch
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: "https://github.com/keboola/keboola-as-code-templates.git", Ref: "non-existing-ref"}
	template, err = model.NewTemplateRefFromString(repository, "tmpl1", "v1")
	assert.NoError(t, err)
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `reference "non-existing-ref" not found in the templates git repository "https://github.com/keboola/keboola-as-code-templates.git"`, err.Error())

	// checkout fail due to non-existing template
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: "https://github.com/keboola/keboola-as-code-templates.git", Ref: "main"}
	template, err = model.NewTemplateRefFromString(repository, "non-existing-template", "v1")
	assert.NoError(t, err)
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `template "non-existing-template" not found:
  - searched in git repository "https://github.com/keboola/keboola-as-code-templates.git"
  - reference "main"`, err.Error())
}

func TestGit_CheckoutTemplateRepository_Local(t *testing.T) {
	t.Parallel()

	// Copy the git repository to temp
	tmpDir, err := ioutil.TempDir("", "keboola-as-code-templates-tests-")
	assert.NoError(t, err)
	assert.NoError(t, aferofs.CopyFs2Fs(nil, "test-repository", nil, tmpDir))
	assert.NoError(t, os.Rename(fmt.Sprintf("%s/.gittest", tmpDir), fmt.Sprintf("%s/.git", tmpDir))) // nolint: forbidigo
	defer func() {
		_ = os.RemoveAll(tmpDir) // nolint: forbidigo
	}()

	// checkout fail due to non-existing template in the branch
	repository := model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}
	template, err := model.NewTemplateRefFromString(repository, "template2", "1.0.0")
	assert.NoError(t, err)
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, fmt.Sprintf(`template "template2" not found:
  - searched in git repository "file://%s"
  - reference "main"`, tmpDir), err.Error())

	// checkout fail due to non-existing version of existing template
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template, err = model.NewTemplateRefFromString(repository, "template2", "1.0.8")
	assert.NoError(t, err)
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, fmt.Sprintf(`template "template2" found but version "1.0.8" is missing:
  - searched in git repository "file://%s"
  - reference "b1"`, tmpDir), err.Error())

	// checkout fail due to non-existing src folder of existing template
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template, err = model.NewTemplateRefFromString(repository, "template2", "1.0.0")
	assert.NoError(t, err)
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, fmt.Sprintf(`folder "template2/v1/src" not found:
  - searched in git repository "file://%s"
  - reference "b1"`, tmpDir), err.Error())

	// checkout success because template2 exists only in branch b1
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template, err = model.NewTemplateRefFromString(repository, "template2", "2.1.0")
	assert.NoError(t, err)
	fs, err := CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.NoError(t, err)
	assert.True(t, fs.Exists("template2/v2/src/manifest.jsonnet"))
	// another template folder should not exist
	assert.False(t, fs.Exists("template1"))
}

func TestGit_CheckoutWholeRepository(t *testing.T) {
	t.Parallel()

	fs, dir, err := CheckoutWholeRepository(repository.DefaultRepository(), log.NewDebugLogger())
	assert.NoError(t, err)
	_, err = os.Stat(dir)
	assert.NoError(t, err)
	assert.False(t, os.IsNotExist(err))
	assert.True(t, fs.Exists("/.keboola/repository.json"))
}
