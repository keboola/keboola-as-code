package diff_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	. "github.com/keboola/keboola-as-code/internal/pkg/state/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff/format"
	transformationDiff "github.com/keboola/keboola-as-code/internal/pkg/state/mapper/transformation/diff"
)

func TestDiff_Transformation(t *testing.T) {
	t.Parallel()

	A, B, d := state.NewCollection(), state.NewCollection(), NewDiffer(transformationDiff.Option())

	// A and B contains a transformation with differences
	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.snowflake-transformation`, ConfigId: `456`}
	transformationKey := model.TransformationKey{ConfigKey: configKey}
	block1Key := model.BlockKey{TransformationKey: transformationKey, BlockIndex: 0}
	block2Key := model.BlockKey{TransformationKey: transformationKey, BlockIndex: 1}
	code1Key := model.CodeKey{BlockKey: block1Key, CodeIndex: 0}
	code2Key := model.CodeKey{BlockKey: block2Key, CodeIndex: 0}
	A.MustAdd(&model.Branch{BranchKey: branchKey})
	A.MustAdd(&model.Config{ConfigKey: configKey})
	A.MustAdd(&model.Transformation{TransformationKey: transformationKey})
	A.MustAdd(&model.Block{BlockKey: block1Key, Name: "My block"})
	A.MustAdd(&model.Code{
		CodeKey: code1Key,
		Name:    "Code 1",
		Scripts: model.Scripts{
			model.StaticScript{Value: "SELECT 1;"},
			model.StaticScript{Value: "SELECT 2;"},
			model.StaticScript{Value: "SELECT 3;"},
		},
	})

	B.MustAdd(&model.Branch{BranchKey: branchKey})
	B.MustAdd(&model.Config{ConfigKey: configKey})
	B.MustAdd(&model.Transformation{TransformationKey: transformationKey})
	B.MustAdd(&model.Block{
		BlockKey: block1Key,
		Name:     "Block 1",
	})
	B.MustAdd(&model.Block{
		BlockKey: block2Key,
		Name:     "Block 2",
	})
	B.MustAdd(&model.Code{
		CodeKey: code1Key,
		Name:    "Code 1",
		Scripts: model.Scripts{
			model.StaticScript{Value: "SELECT 1;"},
		},
	})
	B.MustAdd(&model.Code{
		CodeKey: code2Key,
		Name:    "Code 2",
		Scripts: model.Scripts{
			model.StaticScript{Value: "SELECT 2;"},
		},
	})

	// Do diff
	results, err := d.Diff(A, B)
	assert.NoError(t, err)

	// Config result state is ResultNotEqual
	result, found := results.Get(configKey)
	assert.Equal(t, ResultNotEqual, result.State)
	assert.True(t, found)
	//assert.Equal(t, `description, name`, result2.ChangedFields.String())

	// Setup naming
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(configKey, model.NewAbsPath(`my-branch`, `my-config`))
	namingReg.MustAttach(transformationKey, model.NewAbsPath(`my-branch/my-config`, `blocks`))
	namingReg.MustAttach(block1Key, model.NewAbsPath(`my-branch/my-config/blocks`, `001-my-block-1`))
	namingReg.MustAttach(block2Key, model.NewAbsPath(`my-branch/my-config/blocks`, `002-my-block-2`))
	namingReg.MustAttach(code1Key, model.NewAbsPath(`my-branch/my-config/blocks/001-block-1`, `001-code-1`))
	namingReg.MustAttach(code2Key, model.NewAbsPath(`my-branch/my-config/blocks/001-block-1`, `001-code-2`))

	// Formatted result without details
	assert.Equal(t, "* C branch:123/component:keboola.snowflake-transformation/config:456 | changes: transformation\n", format.Format(results))

	// Formatted result with details
	assert.Equal(t, strings.TrimLeft(`
* C branch:123/component:keboola.snowflake-transformation/config:456
    transformation
    - # My block
    + # Block 1
      ## Code 1
      SELECT 1;
    + # Block 2
    + ## Code 2
      SELECT 2;
    - SELECT 3;
`, "\n"), format.Format(results, format.WithDetails()))

	// Formatted result without details + path is known
	assert.Equal(t, "* C my-branch/my-config | changes: transformation\n", format.Format(results, format.WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known
	assert.Equal(t, strings.TrimLeft(`
* C my-branch/my-config
    transformation
    - # My block
    + # Block 1
      ## Code 1
      SELECT 1;
    + # Block 2
    + ## Code 2
      SELECT 2;
    - SELECT 3;
`, "\n"), format.Format(results, format.WithNamingRegistry(namingReg), format.WithDetails()))
}
