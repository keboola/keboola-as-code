package manifest

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/model"
	"testing"
)

func TestDefaultNaming(t *testing.T) {
	n := DefaultNaming()

	// Branch
	assert.Equal(
		t,
		"1234-my-super-branch",
		n.BranchPath(
			&model.Branch{
				BranchKey: model.BranchKey{
					Id: 1234,
				},
				Name: "my Super-BRANCH",
			},
		))

	// Config
	assert.Equal(
		t,
		"extractor/keboola.ex-foo-bar/456-my-production-config",
		n.ConfigPath(
			&model.Component{
				ComponentKey: model.ComponentKey{
					Id: "keboola.ex-foo-bar",
				},
				Type: "extractor",
			},
			&model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    1234,
					ComponentId: "keboola.ex-foo-bar",
					Id:          "456",
				},
				Name: "MyProductionConfig",
			},
		))

	// Config Row
	assert.Equal(
		t,
		"rows/789-row-ab-c",
		n.ConfigRowPath(
			&model.ConfigRow{
				ConfigRowKey: model.ConfigRowKey{
					BranchId:    1234,
					ComponentId: "keboola.ex-foo-bar",
					ConfigId:    "456",
					Id:          "789",
				},
				Name: "---  row AbC---",
			},
		))
}
