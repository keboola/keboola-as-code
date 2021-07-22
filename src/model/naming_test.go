package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDefaultNaming(t *testing.T) {
	n := DefaultNaming()

	// Branch
	assert.Equal(
		t,
		"1234-my-super-branch",
		n.BranchPath(
			&Branch{
				BranchKey: BranchKey{
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
			&Component{
				ComponentKey: ComponentKey{
					Id: "keboola.ex-foo-bar",
				},
				Type: "extractor",
			},
			&Config{
				ConfigKey: ConfigKey{
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
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1234,
					ComponentId: "keboola.ex-foo-bar",
					ConfigId:    "456",
					Id:          "789",
				},
				Name: "---  row AbC---",
			},
		))
}
