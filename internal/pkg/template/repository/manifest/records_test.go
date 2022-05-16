package manifest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTemplateRecord_DefaultVersion_NotFound(t *testing.T) {
	t.Parallel()
	template := TemplateRecord{}
	_, found := template.DefaultVersion()
	assert.False(t, found)
}

func TestTemplateRecord_DefaultVersion_Found(t *testing.T) {
	t.Parallel()
	template := TemplateRecord{
		Versions: []VersionRecord{
			{
				Version:     version(`0.0.1`),
				Stable:      false,
				Description: `Version 0`,
			},
			{
				Version:     version(`1.2.3`),
				Stable:      true,
				Description: `Version 1`,
			},
			{
				Version:     version(`0.1.0`),
				Stable:      false,
				Description: `Version 0.1`,
			},
		},
	}

	v, found := template.DefaultVersion()
	assert.True(t, found)
	assert.Equal(t, VersionRecord{
		Version:     version(`1.2.3`),
		Stable:      true,
		Description: `Version 1`,
	}, v)
}

func TestTemplateRecord_DefaultVersion_Found_Minimal(t *testing.T) {
	t.Parallel()
	template := TemplateRecord{
		Versions: []VersionRecord{
			{
				Version:     version(`0.0.1`),
				Stable:      false,
				Description: `Version 0`,
			},
		},
	}

	v, found := template.DefaultVersion()
	assert.True(t, found)
	assert.Equal(t, VersionRecord{
		Version:     version(`0.0.1`),
		Stable:      false,
		Description: `Version 0`,
	}, v)
}

func TestTemplateRecord_GetByPath_NotFound(t *testing.T) {
	t.Parallel()
	r := &TemplateRecord{}
	value, found := r.GetByPath(`v1`)
	assert.Empty(t, value)
	assert.False(t, found)
}

func TestTemplateRecord_GetByPath_Found(t *testing.T) {
	t.Parallel()
	r := &TemplateRecord{}
	version1 := version(`v1.2.3`)
	r.AddVersion(version1)
	version2 := version(`v2.0.0`)
	r.AddVersion(version2)
	value, found := r.GetByPath(`v1`)
	assert.Equal(t, version1, value.Version)
	assert.True(t, found)
}

func TestTemplateRecord_GetByVersion_Complex(t *testing.T) {
	t.Parallel()

	// Add some versions
	r := &TemplateRecord{}
	r.AddVersion(version(`v1.0.0`))
	r.AddVersion(version(`v1.2.2`))
	r.AddVersion(version(`v1.2.3`))
	r.AddVersion(version(`v2.4.5`))
	r.AddVersion(version(`v0.0.1`))
	r.AddVersion(version(`v0.0.2`))
	r.AddVersion(version(`v0.0.3`))
	r.AddVersion(version(`v0.1.4`))

	// Test cases
	cases := []struct {
		wanted string
		found  string
	}{
		{"v0.0.1", "0.0.1"},
		{"0.0.1", "0.0.1"},
		{"0.0.2", "0.0.2"},
		{"0.0.3", "0.0.3"},
		{"0.0.4", ""},
		{"v0.0", "0.0.3"},
		{"0.0", "0.0.3"},
		{"0.1.4", "0.1.4"},
		{"0.1.5", ""},
		{"v0", "0.1.4"},
		{"0", "0.1.4"},
		{"1.0", "1.0.0"},
		{"1.0.0", "1.0.0"},
		{"1", "1.2.3"},
		{"1.2", "1.2.3"},
		{"1.2.2", "1.2.2"},
		{"1.2.3", "1.2.3"},
		{"1.2.4", ""},
		{"1.3", ""},
		{"2", "2.4.5"},
		{"2.4", "2.4.5"},
		{"2.4.5", "2.4.5"},
		{"2.4.5", "2.4.5"},
		{"2.4.6", ""},
		{"2.5", ""},
	}

	for i, c := range cases {
		desc := fmt.Sprintf("case: %d, wanted: %s", i, c.wanted)
		value, found := r.GetVersion(version(c.wanted))
		if c.found == "" {
			assert.False(t, found, desc)
		} else {
			assert.True(t, found, desc)
			assert.Equal(t, c.found, value.Version.String(), desc)
		}
	}
}
