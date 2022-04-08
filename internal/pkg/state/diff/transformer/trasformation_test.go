package transformer_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff"
)

func TestTransformer_Transformation(t *testing.T) {
	t.Parallel()

	A, B, naming, d := newDiffer()

	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.python-transformation-v2`, ConfigId: `456`}
	transformationKey := model.TransformationKey{ConfigKey: configKey}
	block1Key := model.BlockKey{TransformationKey: transformationKey, BlockIndex: 0}
	block2Key := model.BlockKey{TransformationKey: transformationKey, BlockIndex: 1}
	code1Key := model.CodeKey{BlockKey: block1Key, CodeIndex: 0}
	code2Key := model.CodeKey{BlockKey: block2Key, CodeIndex: 0}

	assert.NoError(t, naming.Attach(configKey, model.NewAbsPath(`branch`, `config`)))
	assert.NoError(t, naming.Attach(transformationKey, model.NewAbsPath(`branch/config`, `blocks`)))
	assert.NoError(t, naming.Attach(block1Key, model.NewAbsPath(`branch/config/blocks`, `001-my-block-1`)))
	assert.NoError(t, naming.Attach(block2Key, model.NewAbsPath(`branch/config/blocks`, `002-my-block-2`)))
	assert.NoError(t, naming.Attach(code1Key, model.NewAbsPath(`branch/config/blocks/001-block-1`, `001-code-1`)))
	assert.NoError(t, naming.Attach(code2Key, model.NewAbsPath(`branch/config/blocks/001-block-1`, `001-code-2`)))

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

	result, err := d.Diff(A, B)
	assert.NoError(t, err)
	fmt.Println(result.String(diff.FormatOptions{Details: true}))
	assert.Fail(t, "aaa")

	//	expectedShort := `* C branch/config | changed: transformation`
	//	expectedLong := `
	//
	//`
	//assert.Equal(t, "", strings.Join(results.Format(false), "\n"))
	//assert.Equal(t, strings.Trim(expectedShort, "\n"), strings.Join(results.Format(false), "\n"))
	//assert.Equal(t, strings.Trim(expectedLong, "\n"), strings.Join(results.Format(true), "\n"))
}
