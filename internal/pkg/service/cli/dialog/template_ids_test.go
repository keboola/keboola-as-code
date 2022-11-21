package dialog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
)

func TestTemplateIdsDialog_DefaultValue(t *testing.T) {
	t.Parallel()

	branch := &model.Branch{
		BranchKey: model.BranchKey{Id: 1},
		Name:      "Branch",
	}
	configs := []*model.ConfigWithRows{
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "123"},
				Name:      "My Config 1",
			},
		},
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "456"},
				Name:      "My Config 2",
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "456", Id: "1"},
					Name:         "My Row",
				},
				{
					ConfigRowKey: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "456", Id: "2"},
					Name:         "My Row",
				},
				{
					ConfigRowKey: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "456", Id: "3"},
					Name:         "#$%^_",
				},
			},
		},
	}

	// Expected default value
	expected := `
<!--
Please enter a human readable ID for each configuration. For example "L0-raw-data-ex".
Allowed characters: a-z, A-Z, 0-9, "-".
These IDs will be used in the template.

Please edit each line below "## Config ..." and "### Row ...".
Do not edit lines starting with "#"!
-->


## Config "My Config 1" foo.bar:123
my-config-1

## Config "My Config 2" foo.bar:456
my-config-2

### Row "My Row" foo.bar:456:1
my-row

### Row "My Row" foo.bar:456:2
my-row-001

### Row "#$%^_" foo.bar:456:3
config-row

`

	// Check default value
	d := templateIdsDialog{prompt: nopPrompt.New(), branch: branch, configs: configs}
	assert.Equal(t, expected, d.defaultValue())
}
