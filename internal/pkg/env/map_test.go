package env

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvMap(t *testing.T) {
	t.Parallel()
	m := Empty()
	assert.Empty(t, m.Keys())

	// Set
	m.Set(`abc_def`, `123`)
	assert.Equal(t, []string{`ABC_DEF`}, m.Keys())
	assert.Equal(t, `123`, m.Get(`abc_def`))
	assert.Equal(t, `123`, m.MustGet(`abc_def`))
	assert.Equal(t, `123`, m.Get(`ABC_DEF`))
	v, found := m.Lookup(`ABC_def`)
	assert.Equal(t, `123`, v)
	assert.True(t, found)

	// Overwrite
	m.Set(`abc_DEF`, `456`)
	assert.Equal(t, []string{`ABC_DEF`}, m.Keys())
	assert.Equal(t, `456`, m.Get(`abc_DEF`))
	assert.Equal(t, `456`, m.MustGet(`abc_DEF`))
	assert.Equal(t, `456`, m.Get(`ABC_DEF`))
	v, found = m.Lookup(`abc_def`)
	assert.Equal(t, `456`, v)
	assert.True(t, found)

	// Missing key
	assert.Equal(t, []string{`ABC_DEF`}, m.Keys())
	assert.Equal(t, ``, m.Get(`foo`))
	assert.PanicsWithError(t, `missing ENV variable "FOO"`, func() {
		m.MustGet(`foo`)
	})
	m.Unset(`foo`)
	assert.Equal(t, []string{`ABC_DEF`}, m.Keys())

	// Unset
	m.Unset(`ABC_def`)
	assert.Empty(t, m.Keys())
	str, err := m.ToString()
	require.NoError(t, err)
	assert.Equal(t, ``, str)

	// ToString
	m.Set(`A`, `123`)
	m.Set(`X`, `Y`)
	str, err = m.ToString()
	require.NoError(t, err)
	assert.Equal(t, "A=123\nX=\"Y\"", str)
}

func TestEnvMapFromMap(t *testing.T) {
	t.Parallel()
	data := map[string]string{
		`A`: `123`,
		`B`: `456`,
	}

	m := FromMap(data)
	assert.Equal(t, `123`, m.Get(`A`))
	assert.Equal(t, `456`, m.Get(`B`))
	assert.Equal(t, []string{`A`, `B`}, m.Keys())

	assert.Equal(t, data, m.ToMap())
}

// nolint paralleltest
func TestEnvMapFromOs(t *testing.T) {
	require.NoError(t, os.Setenv(`Foo`, `bar`)) // nolint forbidigo
	m, err := FromOs()
	assert.NotNil(t, m)
	require.NoError(t, err)
	str, err := m.ToString()
	require.NoError(t, err)
	assert.Contains(t, str, `FOO="bar"`)
}

func TestEnvMapMerge(t *testing.T) {
	t.Parallel()
	m1 := Empty()
	m2 := Empty()

	m1.Set(`A`, `1`)
	m1.Set(`B`, `2`)

	m2.Set(`B`, `20`)
	m2.Set(`C`, `30`)

	m1.Merge(m2, false) // overwrite = false
	str, err := m1.ToString()

	require.NoError(t, err)
	assert.Equal(t, "A=1\nB=2\nC=30", str)
}

func TestEnvMapMergeOverwrite(t *testing.T) {
	t.Parallel()
	m1 := Empty()
	m2 := Empty()

	m1.Set(`A`, `1`)
	m1.Set(`B`, `2`)

	m2.Set(`B`, `20`)
	m2.Set(`C`, `30`)

	m1.Merge(m2, true) // overwrite = true
	str, err := m1.ToString()

	require.NoError(t, err)
	assert.Equal(t, "A=1\nB=20\nC=30", str)
}
