package delete_template

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
)

type DeleteAction struct {
	State    model.ObjectState
	Manifest model.ObjectManifest
}

type Plan struct {
	actions []DeleteAction
}

func (p *Plan) Empty() bool {
	return len(p.actions) == 0
}

func (p *Plan) Name() string {
	return "delete-template"
}

func (p *Plan) Log(log log.Logger) {
	writer := log.InfoWriter()
	writer.WriteString(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	actions := p.actions

	if len(actions) == 0 {
		writer.WriteStringIndent(1, "nothing to delete")
	} else {
		for _, action := range actions {
			writer.WriteStringIndent(1, fmt.Sprintf("%s %s %s", diff.DeleteMark, model.ConfigAbbr, action.State.Path()))
		}
	}
}

func (p *Plan) Invoke(ctx context.Context, localManager *local.Manager) error {
	return newExecutor(ctx, localManager, p).invoke()
}
