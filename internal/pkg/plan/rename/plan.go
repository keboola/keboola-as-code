package rename

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Plan struct {
	actions []model.RenameAction
}

func (p *Plan) Empty() bool {
	return len(p.actions) == 0
}

func (p *Plan) Name() string {
	return "rename"
}

func (p *Plan) Log(log log.Logger) {
	writer := log.InfoWriter()
	writer.WriteString(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	actions := p.actions

	if len(actions) == 0 {
		writer.WriteStringIndent(1, "no paths to rename")
	} else {
		for _, action := range actions {
			writer.WriteStringIndent(1, "- "+action.String())
		}
	}
}

func (p *Plan) Invoke(ctx context.Context, localManager *local.Manager) error {
	return newRenameExecutor(ctx, localManager, p).invoke()
}
