package naming

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathTemplateRegexp(t *testing.T) {
	t.Parallel()
	var matched bool
	var matches map[string]string

	cases := []struct {
		comment  string
		template string
		path     string
		matched  bool
		matches  map[string]string
	}{
		{
			comment:  "row, not matched 1",
			template: `rows/{config_row_id}-{config_row_name}`,
			path:     `rows/`,
			matched:  false,
			matches:  nil,
		},
		{
			comment:  "row, not matched 2",
			template: `rows/{config_row_id}-{config_row_name}`,
			path:     `rows/123-name/foo`,
			matched:  false,
			matches:  nil,
		},
		{
			comment:  "all placeholders present - row",
			template: `rows/{config_row_id}-{config_row_name}`,
			path:     `rows/123-name`,
			matched:  true,
			matches: map[string]string{
				"config_row_id":   "123",
				"config_row_name": "name",
			},
		},
		{
			comment:  "config_row_id can be missing - if row has been created manually and it is not persisted",
			template: `rows/{config_row_id}-{config_row_name}`,
			path:     `rows/foo`,
			matched:  true,
			matches: map[string]string{
				"config_row_id":   "",
				"config_row_name": "foo",
			},
		},
		{
			comment:  "config, not matched 1",
			template: `{component_type}/{component_id}/{config_id}-{config_name}`,
			path:     `extractor/keboola.foo-bar`,
			matched:  false,
			matches:  nil,
		},
		{
			comment:  "config, not matched 2",
			template: `{component_type}/{component_id}/{config_id}-{config_name}`,
			path:     `extractor/keboola.foo-bar/123-name/foo`,
			matched:  false,
			matches:  nil,
		},
		{
			comment:  "all placeholders present - config",
			template: `{component_type}/{component_id}/{config_id}-{config_name}`,
			path:     `extractor/keboola.foo-bar/123-name`,
			matched:  true,
			matches: map[string]string{
				"component_type": "extractor",
				"component_id":   "keboola.foo-bar",
				"config_id":      "123",
				"config_name":    "name",
			},
		},
		{
			comment:  "config_id can be missing - if config has been created manually and it is not persisted",
			template: `{component_type}/{component_id}/{config_id}-{config_name}`,
			path:     `extractor/keboola.foo-bar/foo`,
			matched:  true,
			matches: map[string]string{
				"component_type": "extractor",
				"component_id":   "keboola.foo-bar",
				"config_id":      "",
				"config_name":    "foo",
			},
		},
	}

	// Assert
	for _, data := range cases {
		path := PathTemplate(data.template)
		matched, matches = path.MatchPath(data.path)
		assert.Equalf(t, data.matched, matched, data.comment)
		assert.Equalf(t, data.matches, matches, data.comment)
	}
}
