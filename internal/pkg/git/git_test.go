package git

import (
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

func TestGit_CheckoutTemplateRepository(t *testing.T) {
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
