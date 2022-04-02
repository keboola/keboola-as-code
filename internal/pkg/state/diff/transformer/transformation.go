package transformer

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (t *Transformer) transformationToString(transformation *model.ObjectLeaf) string {
	var builder strings.Builder

	// Blocks
	for _, blockRaw := range transformation.Get(model.BlockKind) {
		block := blockRaw.Object.(*model.Block)
		builder.WriteString(fmt.Sprintf("# %s\n", block.Name))

		// Codes
		for _, codeRaw := range blockRaw.Get(model.CodeKind) {
			code := codeRaw.Object.(*model.Code)
			builder.WriteString(fmt.Sprintf("## %s\n", code.Name))

			// Scripts
			builder.WriteString(code.Scripts.String(code.ComponentId))
		}
	}

	return strings.Trim(builder.String(), "\n")
}
