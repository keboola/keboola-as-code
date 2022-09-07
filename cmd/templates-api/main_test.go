package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestParseRepositories(t *testing.T) {
	t.Parallel()

	result, err := parseRepositories("")
	assert.NoError(t, err)
	assert.Empty(t, result)

	result, err = parseRepositories("foo|file://bar")
	assert.NoError(t, err)
	assert.Equal(t, []model.TemplateRepository{
		{
			Type: model.RepositoryTypeDir,
			Name: "foo",
			Url:  "bar",
		},
	}, result)

	result, err = parseRepositories("foo|https://bar.com|baz")
	assert.NoError(t, err)
	assert.Equal(t, []model.TemplateRepository{
		{
			Type: model.RepositoryTypeGit,
			Name: "foo",
			Url:  "https://bar.com",
			Ref:  "baz",
		},
	}, result)

	result, err = parseRepositories("foo1|https://bar.com|baz;foo2|file://bar")
	assert.NoError(t, err)
	assert.Equal(t, []model.TemplateRepository{
		{
			Type: model.RepositoryTypeGit,
			Name: "foo1",
			Url:  "https://bar.com",
			Ref:  "baz",
		},
		{
			Type: model.RepositoryTypeDir,
			Name: "foo2",
			Url:  "bar",
		},
	}, result)

	_, err = parseRepositories("foo")
	assert.Error(t, err)
	assert.Equal(t, `invalid repository definition "foo": required format <name>|https://<repository>|<branch> or <name>|file://<repository>`, err.Error())

	_, err = parseRepositories("foo|ftp://bar.com")
	assert.Error(t, err)
	assert.Equal(t, `invalid repository path "ftp://bar.com": must start with "file://" or "https://"`, err.Error())

	_, err = parseRepositories("foo|file://bar|abc")
	assert.Error(t, err)
	assert.Equal(t, `invalid repository definition "foo|file://bar|abc": required format <name>|file://<repository>`, err.Error())

	_, err = parseRepositories("foo|https://bar|abc|def")
	assert.Error(t, err)
	assert.Equal(t, `invalid repository definition "foo|https://bar|abc|def": required format <name>:https://<repository>:<branch>`, err.Error())

	_, err = parseRepositories("foo|https://bar.com|baz;foo|file://bar|bar")
	assert.Error(t, err)
	assert.Equal(t, `duplicate repository name "foo" found when parsing default repositories`, err.Error())
}
