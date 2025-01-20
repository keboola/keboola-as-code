package manifest

import (
	"fmt"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
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
	r.AddVersion(version1, []string{})
	version2 := version(`v2.0.0`)
	r.AddVersion(version2, []string{})
	value, found := r.GetByPath(`v1`)
	assert.Equal(t, version1, value.Version)
	assert.True(t, found)
}

func TestTemplateRecord_GetByVersion_Complex(t *testing.T) {
	t.Parallel()

	// Add some versions
	r := &TemplateRecord{}
	r.AddVersion(version(`v1.0.0`), []string{})
	r.AddVersion(version(`v1.2.2`), []string{})
	r.AddVersion(version(`v1.2.3`), []string{})
	r.AddVersion(version(`v2.4.5`), []string{})
	r.AddVersion(version(`v0.0.1`), []string{})
	r.AddVersion(version(`v0.0.2`), []string{})
	r.AddVersion(version(`v0.0.3`), []string{})
	r.AddVersion(version(`v0.1.4`), []string{})

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

func TestTemplateRecord_HasBackend(t *testing.T) {
	t.Parallel()

	type fields struct {
		ID           string
		Name         string
		Description  string
		Categories   []string
		Deprecated   bool
		Path         string
		Requirements Requirements
		Versions     []VersionRecord
	}
	type args struct {
		projectBackends []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{name: "supported backend", fields: fields{
			ID:          "my-template-1",
			Name:        "My Template-1",
			Description: "Foo ....",
			Deprecated:  false,
			Categories:  []string{"Other"},
			Path:        "path1",
			Requirements: Requirements{
				Backends: []string{"snowflake"},
			},
			Versions: []VersionRecord{
				{Version: version("1.2.4"), Description: "", Stable: false},
			},
		}, args: args{projectBackends: []string{"snowflake"}}, want: true},

		{name: "supported backend (more project backend)", fields: fields{
			ID:          "my-template-1",
			Name:        "My Template-1",
			Description: "Foo ....",
			Deprecated:  false,
			Categories:  []string{"Other"},
			Path:        "path1",
			Requirements: Requirements{
				Backends: []string{"snowflake"},
			},
			Versions: []VersionRecord{
				{Version: version("1.2.4"), Description: "", Stable: false},
			},
		}, args: args{projectBackends: []string{"bigquery", "teradata", "snowflake", "mysql"}}, want: true},

		{name: "unsupported backend", fields: fields{
			ID:          "my-template-1",
			Name:        "My Template-1",
			Description: "Foo ....",
			Deprecated:  false,
			Categories:  []string{"Other"},
			Path:        "path1",
			Requirements: Requirements{
				Backends: []string{"bigquery"},
			},
			Versions: []VersionRecord{
				{Version: version("1.2.4"), Description: "", Stable: false},
			},
		}, args: args{projectBackends: []string{"teradata", "snowflake", "mysql"}}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := &TemplateRecord{
				ID:           tt.fields.ID,
				Name:         tt.fields.Name,
				Description:  tt.fields.Description,
				Categories:   tt.fields.Categories,
				Deprecated:   tt.fields.Deprecated,
				Path:         tt.fields.Path,
				Requirements: tt.fields.Requirements,
				Versions:     tt.fields.Versions,
			}
			assert.Equalf(t, tt.want, v.HasBackend(tt.args.projectBackends), "HasBackend(%v)", tt.args.projectBackends)
		})
	}
}

func TestTemplateRecord_HasFeature(t *testing.T) {
	t.Parallel()

	type fields struct {
		ID           string
		Name         string
		Description  string
		Categories   []string
		Deprecated   bool
		Path         string
		Requirements Requirements
		Versions     []VersionRecord
	}
	type args struct {
		features keboola.FeaturesMap
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{name: "supported features", fields: fields{
			ID:          "my-template-1",
			Name:        "My Template-1",
			Description: "Foo ....",
			Deprecated:  false,
			Categories:  []string{"Other"},
			Path:        "path1",
			Requirements: Requirements{
				Features: []string{"feature4", "feature2"},
			},
			Versions: []VersionRecord{
				{Version: version("1.2.4"), Description: "", Stable: false},
			},
		}, args: args{features: keboola.Features{"feature1", "feature2", "feature3", "feature4"}.ToMap()}, want: true},

		{name: "unsupported features", fields: fields{
			ID:          "my-template-1",
			Name:        "My Template-1",
			Description: "Foo ....",
			Deprecated:  false,
			Categories:  []string{"Other"},
			Path:        "path1",
			Requirements: Requirements{
				Features: []string{"feature3", "feature5"},
			},
			Versions: []VersionRecord{
				{Version: version("1.2.4"), Description: "", Stable: false},
			},
		}, args: args{features: keboola.Features{"feature1", "feature2", "feature3", "feature4"}.ToMap()}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := &TemplateRecord{
				ID:           tt.fields.ID,
				Name:         tt.fields.Name,
				Description:  tt.fields.Description,
				Categories:   tt.fields.Categories,
				Deprecated:   tt.fields.Deprecated,
				Path:         tt.fields.Path,
				Requirements: tt.fields.Requirements,
				Versions:     tt.fields.Versions,
			}
			assert.Equalf(t, tt.want, v.CheckProjectFeatures(tt.args.features), "CheckProjectFeatures(%v)", tt.args.features)
		})
	}
}

func TestTemplateRecord_HasComponent(t *testing.T) {
	t.Parallel()

	type fields struct {
		ID           string
		Name         string
		Description  string
		Categories   []string
		Deprecated   bool
		Path         string
		Requirements Requirements
		Versions     []VersionRecord
	}
	type args struct {
		components *model.ComponentsMap
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{name: "supported components", fields: fields{
			ID:          "my-template-1",
			Name:        "My Template-1",
			Description: "Foo ....",
			Deprecated:  false,
			Categories:  []string{"Other"},
			Path:        "path1",
			Requirements: Requirements{
				Components: []string{"keboola.python-transformation-v2", "foo.bar"},
			},
			Versions: []VersionRecord{
				{Version: version("1.2.4"), Description: "", Stable: false},
			},
		}, args: args{components: testapi.MockedComponentsMap()}, want: true},

		{name: "unsupported components", fields: fields{
			ID:          "my-template-1",
			Name:        "My Template-1",
			Description: "Foo ....",
			Deprecated:  false,
			Categories:  []string{"Other"},
			Path:        "path1",
			Requirements: Requirements{
				Components: []string{"wrong-component", "foo.bar"},
			},
			Versions: []VersionRecord{
				{Version: version("1.2.4"), Description: "", Stable: false},
			},
		}, args: args{components: testapi.MockedComponentsMap()}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := &TemplateRecord{
				ID:           tt.fields.ID,
				Name:         tt.fields.Name,
				Description:  tt.fields.Description,
				Categories:   tt.fields.Categories,
				Deprecated:   tt.fields.Deprecated,
				Path:         tt.fields.Path,
				Requirements: tt.fields.Requirements,
				Versions:     tt.fields.Versions,
			}
			assert.Equalf(t, tt.want, v.CheckProjectComponents(tt.args.components), "CheckProjectComponents(%v)", tt.args.components)
		})
	}
}
