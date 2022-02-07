package git

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
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
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `templates git repository not found on url "https://non-existing-url"`, err.Error())

	// checkout fail from a non-existing github repository
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: "https://github.com/keboola/non-existing-repo.git", Ref: "main"}
	template, err = model.NewTemplateRefFromString(repository, "tmpl1", "v1")
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `templates git repository not found on url "https://github.com/keboola/non-existing-repo.git"`, err.Error())

	// checkout fail from a non-existing branch
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: "https://github.com/keboola/keboola-as-code-templates.git", Ref: "non-existing-ref"}
	template, err = model.NewTemplateRefFromString(repository, "tmpl1", "v1")
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `reference "non-existing-ref" not found in the templates git repository "https://github.com/keboola/keboola-as-code-templates.git"`, err.Error())

	// checkout fail due to non-existing template
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: "https://github.com/keboola/keboola-as-code-templates.git", Ref: "main"}
	template, err = model.NewTemplateRefFromString(repository, "non-existing-template", "v1")
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, `template "non-existing-template" in version "v1" not found in the templates git repository "https://github.com/keboola/keboola-as-code-templates.git"`, err.Error())
}

func TestGit_CheckoutTemplateRepository_Local(t *testing.T) {
	t.Parallel()

	// Copy the git repository to temp
	tmpDir := copyToTemp(t, "test-repository")
	assert.NoError(t, os.Rename(fmt.Sprintf("%s/.gittest", tmpDir), fmt.Sprintf("%s/.git", tmpDir))) // nolint: forbidigo
	defer func(path string) {
		_ = os.RemoveAll(path) // nolint: forbidigo
	}(tmpDir)

	// checkout fail due to non-existing template in the branch
	repository := model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "main"}
	template, err := model.NewTemplateRefFromString(repository, "template2", "1.0.0")
	_, err = CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.Error(t, err)
	assert.Equal(t, fmt.Sprintf(`template "template2" in version "v1" not found in the templates git repository "file://%s"`, tmpDir), err.Error())

	// checkout success because template2 exists only in branch b1
	repository = model.TemplateRepository{Type: "git", Name: "keboola", Url: fmt.Sprintf("file://%s", tmpDir), Ref: "b1"}
	template, err = model.NewTemplateRefFromString(repository, "template2", "1.0.0")
	fs, err := CheckoutTemplateRepository(template, log.NewDebugLogger())
	assert.NoError(t, err)
	assert.True(t, fs.Exists("template2/v1/src/manifest.jsonnet"))
}

// nolint: forbidigo
func copyToTemp(t *testing.T, source string) string {
	t.Helper()

	destination, err := ioutil.TempDir("", "keboola-as-code-templates-tests-")
	assert.NoError(t, err)
	err = filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		relPath := strings.Replace(path, source, "", 1)
		if relPath == "" {
			return nil
		}
		if info.IsDir() {
			return os.Mkdir(filepath.Join(destination, relPath), 0o755)
		} else {
			data, err1 := ioutil.ReadFile(filepath.Join(source, relPath))
			if err1 != nil {
				return err1
			}
			return ioutil.WriteFile(filepath.Join(destination, relPath), data, 0o777)
		}
	})
	assert.NoError(t, err)

	return destination
}
