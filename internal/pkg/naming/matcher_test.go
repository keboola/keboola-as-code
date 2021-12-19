package naming

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestNamingMatchConfigPathNotMatched(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	componentId, err := m.MatchConfigPath(
		BranchKey{},
		NewPathInProject(
			"parent/path",
			"foo",
		))
	assert.NoError(t, err)
	assert.Empty(t, componentId)
}

func TestNamingMatchConfigPathOrdinary(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	componentId, err := m.MatchConfigPath(
		BranchKey{},
		NewPathInProject(
			"parent/path",
			"extractor/keboola.ex-db-mysql/with-rows",
		))
	assert.NoError(t, err)
	assert.Equal(t, ComponentId(`keboola.ex-db-mysql`), componentId)
}

func TestNamingMatchConfigPathSharedCode(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	componentId, err := m.MatchConfigPath(
		BranchKey{},
		NewPathInProject(
			"parent/path",
			"_shared/keboola.python-transformation-v2",
		))
	assert.NoError(t, err)
	assert.Equal(t, SharedCodeComponentId, componentId)
}

func TestNamingMatchConfigPathVariables(t *testing.T) {
	t.Parallel()
	n := NewPathMatcher(TemplateWithIds())
	componentId, err := n.MatchConfigPath(
		ConfigKey{},
		NewPathInProject(
			"parent/path",
			"variables",
		))
	assert.NoError(t, err)
	assert.Equal(t, VariablesComponentId, componentId)
}

func TestNamingMatchSharedCodeVariables(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	componentId, err := m.MatchConfigPath(
		ConfigRowKey{ComponentId: SharedCodeComponentId},
		NewPathInProject(
			"shared/code/path",
			"variables",
		))
	assert.NoError(t, err)
	assert.Equal(t, VariablesComponentId, componentId)
}

func TestNamingMatchConfigRowPathNotMatched(t *testing.T) {
	t.Parallel()
	n := NewPathMatcher(TemplateWithIds())
	matched := n.MatchConfigRowPath(
		&Component{
			ComponentKey: ComponentKey{Id: "foo.bar"},
		},
		NewPathInProject(
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
		&Component{
			ComponentKey: ComponentKey{Id: "foo.bar"},
		},
		NewPathInProject(
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
		&Component{
			ComponentKey: ComponentKey{Id: SharedCodeComponentId},
		},
		NewPathInProject(
			"parent/path",
			"codes/foo",
		))
	assert.True(t, matched)
}

func TestNamingMatchConfigRowPathVariables(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	matched := m.MatchConfigRowPath(
		&Component{
			ComponentKey: ComponentKey{Id: VariablesComponentId},
		},
		NewPathInProject(
			"parent/path",
			"values/foo",
		))
	assert.True(t, matched)
}
