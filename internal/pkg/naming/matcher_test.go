package naming

import (
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestNamingMatchConfigPathNotMatched(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	componentID, err := m.MatchConfigPath(
		BranchKey{},
		NewAbsPath(
			"parent/path",
			"foo",
		))
	assert.NoError(t, err)
	assert.Empty(t, componentID)
}

func TestNamingMatchConfigPathOrdinary(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	componentID, err := m.MatchConfigPath(
		BranchKey{},
		NewAbsPath(
			"parent/path",
			"extractor/keboola.ex-db-mysql/with-rows",
		))
	assert.NoError(t, err)
	assert.Equal(t, storageapi.ComponentID(`keboola.ex-db-mysql`), componentID)
}

func TestNamingMatchConfigPathSharedCode(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	componentID, err := m.MatchConfigPath(
		BranchKey{},
		NewAbsPath(
			"parent/path",
			"_shared/keboola.python-transformation-v2",
		))
	assert.NoError(t, err)
	assert.Equal(t, storageapi.SharedCodeComponentID, componentID)
}

func TestNamingMatchConfigPathVariables(t *testing.T) {
	t.Parallel()
	n := NewPathMatcher(TemplateWithIds())
	componentID, err := n.MatchConfigPath(
		ConfigKey{},
		NewAbsPath(
			"parent/path",
			"variables",
		))
	assert.NoError(t, err)
	assert.Equal(t, storageapi.VariablesComponentID, componentID)
}

func TestNamingMatchSharedCodeVariables(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	componentID, err := m.MatchConfigPath(
		ConfigRowKey{ComponentID: storageapi.SharedCodeComponentID},
		NewAbsPath(
			"shared/code/path",
			"variables",
		))
	assert.NoError(t, err)
	assert.Equal(t, storageapi.VariablesComponentID, componentID)
}

func TestNamingMatchConfigRowPathNotMatched(t *testing.T) {
	t.Parallel()
	n := NewPathMatcher(TemplateWithIds())
	matched := n.MatchConfigRowPath(
		&storageapi.Component{
			ComponentKey: storageapi.ComponentKey{ID: "foo.bar"},
		},
		NewAbsPath(
			"parent/path",
			"foo",
		),
	)
	assert.False(t, matched)
}

func TestNamingMatchConfigRowPathOrdinary(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	matched := m.MatchConfigRowPath(
		&storageapi.Component{
			ComponentKey: storageapi.ComponentKey{ID: "foo.bar"},
		},
		NewAbsPath(
			"parent/path",
			"rows/foo",
		),
	)
	assert.True(t, matched)
}

func TestNamingMatchConfigRowPathSharedCode(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	matched := m.MatchConfigRowPath(
		&storageapi.Component{
			ComponentKey: storageapi.ComponentKey{ID: storageapi.SharedCodeComponentID},
		},
		NewAbsPath(
			"parent/path",
			"codes/foo",
		))
	assert.True(t, matched)
}

func TestNamingMatchConfigRowPathVariables(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	matched := m.MatchConfigRowPath(
		&storageapi.Component{
			ComponentKey: storageapi.ComponentKey{ID: storageapi.VariablesComponentID},
		},
		NewAbsPath(
			"parent/path",
			"values/foo",
		))
	assert.True(t, matched)
}
