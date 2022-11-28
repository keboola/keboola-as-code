package delete_template

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type DeleteAction struct {
	State    model.ObjectState
	Manifest model.ObjectManifest
}

type Plan struct {
	actions      []DeleteAction
	projectState *state.State
	branchKey    model.BranchKey
	instanceID   string
}

func (p *Plan) Empty() bool {
	return len(p.actions) == 0
}

func (p *Plan) Name() string {
	return "delete template instance"
}

func (p *Plan) Log(log log.Logger) {
	writer := log.InfoWriter()
	writer.WriteString(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	if len(p.actions) == 0 {
		writer.WriteStringIndent(1, "nothing to delete")
	} else {
		for _, action := range p.actions {
			writer.WriteStringIndent(1, fmt.Sprintf("%s %s %s", diff.DeleteMark, model.ConfigAbbr, action.State.Path()))
		}
	}
}

func (p *Plan) Invoke(ctx context.Context) error {
	return newExecutor(ctx, p).invoke()
}
