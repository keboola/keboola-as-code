package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestPath_String_Empty(t *testing.T) {
	p := Path{}
	assert.Equal(t, "", p.String())
}

func TestPath_String_Complex(t *testing.T) {
	p := Path{
		StepKind{Kind: model.TransformationKind},
		StepKind{Kind: model.BlockKind},
		StepObject{Key: model.BlockKey{BlockIndex: 0}},
		StepKind{Kind: model.CodeKind},
		StepObject{Key: model.CodeKey{CodeIndex: 0}},
		StepStructField{Field: "script"},
		StepSliceIndex{Index: 123},
		StepMapIndex{Index: "foo"},
		StepMapIndex{Index: "bar"},
	}
	assert.Equal(t, "transformation.block[001].code[001].script[123].foo.bar", p.String())
}
