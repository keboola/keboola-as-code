package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRepositories_MarshalText(t *testing.T) {
	t.Parallel()

	result, err := (Repositories{}).MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "", string(result))

	result, err = (Repositories{
		{
			Type: model.RepositoryTypeGit,
			Name: "git",
			URL:  "https://bar.com",
			Ref:  "baz",
		},
		{
			Type: model.RepositoryTypeDir,
			Name: "dir",
			URL:  "bar",
		},
	}).MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "git|https://bar.com|baz;dir|file://bar", string(result))
}

func TestRepositories_UnmarshalText(t *testing.T) {
	t.Parallel()

	result := Repositories{}
	err := result.UnmarshalText([]byte(""))
	require.NoError(t, err)
	assert.Empty(t, result)

	err = result.UnmarshalText([]byte("foo|file://bar"))
	require.NoError(t, err)
	assert.Equal(t, Repositories{
		{
			Type: model.RepositoryTypeDir,
			Name: "foo",
			URL:  "bar",
		},
	}, result)

	err = result.UnmarshalText([]byte("foo|https://bar.com|baz"))
	require.NoError(t, err)
	assert.Equal(t, Repositories{
		{
			Type: model.RepositoryTypeGit,
			Name: "foo",
			URL:  "https://bar.com",
			Ref:  "baz",
		},
	}, result)

	err = result.UnmarshalText([]byte("foo1|https://bar.com|baz;foo2|file://bar"))
	require.NoError(t, err)
	assert.Equal(t, Repositories{
		{
			Type: model.RepositoryTypeGit,
			Name: "foo1",
			URL:  "https://bar.com",
			Ref:  "baz",
		},
		{
			Type: model.RepositoryTypeDir,
			Name: "foo2",
			URL:  "bar",
		},
	}, result)

	err = result.UnmarshalText([]byte("foo"))
	require.Error(t, err)
	assert.Equal(t, `invalid repository definition "foo": required format <name>|https://<repository>|<branch> or <name>|file://<repository>`, err.Error())

	err = result.UnmarshalText([]byte("foo|ftp://bar.com"))
	require.Error(t, err)
	assert.Equal(t, `invalid repository path "ftp://bar.com": must start with "file://" or "https://"`, err.Error())

	err = result.UnmarshalText([]byte("foo|file://bar|abc"))
	require.Error(t, err)
	assert.Equal(t, `invalid repository definition "foo|file://bar|abc": required format <name>|file://<repository>`, err.Error())

	err = result.UnmarshalText([]byte("foo|https://bar|abc|def"))
	require.Error(t, err)
	assert.Equal(t, `invalid repository definition "foo|https://bar|abc|def": required format <name>|https://<repository>|<branch>`, err.Error())

	err = result.UnmarshalText([]byte("foo|https://bar.com|baz;foo|file://bar|bar"))
	require.Error(t, err)
	assert.Equal(t, `duplicate repository name "foo" found when parsing default repositories`, err.Error())
}
