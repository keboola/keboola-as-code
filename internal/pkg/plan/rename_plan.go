package plan

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type RenamePlan struct {
	actions []model.RenameAction
}

func (p *RenamePlan) Empty() bool {
	return len(p.actions) == 0
}

func (p *RenamePlan) Name() string {
	return "rename"
}

func (p *RenamePlan) Log(writer *log.WriteCloser) {
	writer.WriteStringNoErr(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	actions := p.actions

	if len(actions) == 0 {
		writer.WriteStringNoErrIndent1("no paths to rename")
	} else {
		for _, action := range actions {
			writer.WriteStringNoErrIndent1("- " + action.String())
		}
	}
}

func (p *RenamePlan) Invoke(ctx context.Context, localManager *local.Manager) error {
	return newRenameExecutor(ctx, localManager, p).invoke()
}
