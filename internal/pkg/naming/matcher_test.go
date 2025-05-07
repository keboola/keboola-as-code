package naming

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, err)
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
	require.NoError(t, err)
	assert.Equal(t, keboola.ComponentID(`keboola.ex-db-mysql`), componentID)
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
	require.NoError(t, err)
	assert.Equal(t, keboola.SharedCodeComponentID, componentID)
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
	require.NoError(t, err)
	assert.Equal(t, keboola.VariablesComponentID, componentID)
}

func TestNamingMatchSharedCodeVariables(t *testing.T) {
	t.Parallel()
	m := NewPathMatcher(TemplateWithIds())
	componentID, err := m.MatchConfigPath(
		ConfigRowKey{ComponentID: keboola.SharedCodeComponentID},
		NewAbsPath(
			"shared/code/path",
			"variables",
		))
	require.NoError(t, err)
	assert.Equal(t, keboola.VariablesComponentID, componentID)
}

func TestNamingMatchConfigRowPathNotMatched(t *testing.T) {
	t.Parallel()
	n := NewPathMatcher(TemplateWithIds())
	matched := n.MatchConfigRowPath(
		&keboola.Component{
			ComponentKey: keboola.ComponentKey{ID: "foo.bar"},
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
		&keboola.Component{
			ComponentKey: keboola.ComponentKey{ID: "foo.bar"},
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
		&keboola.Component{
			ComponentKey: keboola.ComponentKey{ID: keboola.SharedCodeComponentID},
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
		&keboola.Component{
			ComponentKey: keboola.ComponentKey{ID: keboola.VariablesComponentID},
		},
		NewAbsPath(
			"parent/path",
			"values/foo",
		))
	assert.True(t, matched)
}
