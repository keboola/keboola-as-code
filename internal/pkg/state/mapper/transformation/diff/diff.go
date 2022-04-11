package diff

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff/format"
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
	builder *format.Builder
}

func newFormatter() *formatter {
	return &formatter{builder: format.NewBuilder()}
}

func (f *formatter) format(_ *model.Transformation, children model.ObjectChildren) *format.Builder {
	f.builder.Reset()
	for _, blockRaw := range children.Get(model.BlockKind) {
		f.formatBlock(blockRaw.Object.(*model.Block), blockRaw.Children)
	}

	f.builder.FinalizeFn(func(str string) string {
		return strings.TrimRight(str, "\n")
	})

	return f.builder
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
