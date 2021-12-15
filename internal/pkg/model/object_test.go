package model

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/testassert"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestBranch_Clone(t *testing.T) {
	t.Parallel()
	value := &Branch{
		BranchKey:   BranchKey{Id: 123},
		Name:        "foo",
		Description: "bar",
		IsDefault:   true,
	}
	testassert.DeepEqualNotSame(t, value, value.Clone(), "")
}

func TestConfig_Clone(t *testing.T) {
	t.Parallel()
	value := &Config{
		ConfigKey:         ConfigKey{BranchId: 123, ComponentId: `foo.bar`, Id: `456`},
		Name:              "foo",
		Description:       "bar",
		ChangeDescription: `my change`,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "key", Value: "value"},
		}),
		SharedCode: &SharedCodeConfig{
			Target: ComponentId(`foo.bar`),
		},
		Transformation: &Transformation{
			Blocks: []*Block{
				{
					BlockKey: BlockKey{
						BranchId:    123,
						ComponentId: `foo.bar`,
						ConfigId:    `456`,
						Index:       1,
					},
					Name: "my block",
					Codes: Codes{
						{
							CodeKey: CodeKey{
								BranchId:    123,
								ComponentId: `foo.bar`,
								ConfigId:    `456`,
								BlockIndex:  1,
								Index:       1,
							},
							Name:    "my code",
							Scripts: Scripts{StaticScript{"foo"}, StaticScript{"bar"}},
						},
					},
				},
			},
			LinkToSharedCode: &LinkToSharedCode{
				Config: ConfigKey{
					BranchId:    123,
					ComponentId: SharedCodeComponentId,
					Id:          `456`,
				},
				Rows: UsedSharedCodeRows{
					ConfigRowKey{
						BranchId:    123,
						ComponentId: SharedCodeComponentId,
						ConfigId:    `456`,
						Id:          `789`,
					},
				},
			},
		},
		Orchestration: &Orchestration{
			Phases: []*Phase{
				{
					Name:    `foo`,
					Content: orderedmap.New(),
					Tasks: []*Task{
						{
							Name:    `bar`,
							Content: orderedmap.New(),
						},
					},
				},
			},
		},
		Relations: Relations{
			&VariablesForRelation{
				ComponentId: `foo.bar`,
				ConfigId:    `789`,
			},
		},
	}
	testassert.DeepEqualNotSame(t, value, value.Clone(), "")
}

func TestConfigRow_Clone(t *testing.T) {
	t.Parallel()
	value := &ConfigRow{
		ConfigRowKey:      ConfigRowKey{BranchId: 123, ComponentId: `foo.bar`, ConfigId: `456`, Id: `789`},
		Name:              "foo",
		Description:       "bar",
		ChangeDescription: `my change`,
		IsDisabled:        true,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "key", Value: "value"},
		}),
		SharedCode: &SharedCodeRow{
			Target: "keboola.snowflake-transformation",
			Scripts: Scripts{
				StaticScript{`SELECT 1;`},
			},
		},
	}
	testassert.DeepEqualNotSame(t, value, value.Clone(), "")
}
