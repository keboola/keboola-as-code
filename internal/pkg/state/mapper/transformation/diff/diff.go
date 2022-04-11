package diff

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff"
)

func Option() diff.Option {
	return diff.WithCmpOption(diff.OnlyOnceTransformer("transformation", func(v *model.ObjectNode) interface{} {
		if transformation, ok := v.Object.(*model.Transformation); ok {
			return newFormatter().format(transformation, v.Children)
		}
		return v
	}))
}

type formatter struct {
	builder strings.Builder
}

func newFormatter() *formatter {
	return &formatter{}
}

func (f *formatter) format(_ *model.Transformation, children model.ObjectChildren) string {
	f.builder.Reset()
	for _, blockRaw := range children.Get(model.BlockKind) {
		f.formatBlock(blockRaw.Object.(*model.Block), blockRaw.Children)
	}
	return strings.TrimRight(f.builder.String(), "\n")
}

func (f *formatter) formatBlock(block *model.Block, children model.ObjectChildren) {
	// Write name
	f.builder.WriteString(fmt.Sprintf("# %s\n", block.Name))

	// Codes
	for _, codeRaw := range children.Get(model.CodeKind) {
		f.formatCode(codeRaw.Object.(*model.Code))
	}
}

func (f *formatter) formatCode(code *model.Code) {
	// Write name
	f.builder.WriteString(fmt.Sprintf("## %s\n", code.Name))

	// Map scripts to string
	var scripts []string
	for _, script := range code.Scripts {
		scripts = append(scripts, script.Content())
	}

	// Write scripts
	f.builder.WriteString(strings.Join(scripts, "\n") + "\n")
}
