package delete_template

import (
	"context"
	"fmt"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
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

func (p *Plan) Log(w io.Writer) {
	fmt.Fprintf(w, `Plan for "%s" operation:`, p.Name())
	fmt.Fprintln(w)
	if len(p.actions) == 0 {
		fmt.Fprintln(w, "  nothing to delete")
	} else {
		for _, action := range p.actions {
			fmt.Fprintf(w, "  %s %s %s", diff.DeleteMark, model.ConfigAbbr, action.State.Path())
			fmt.Fprintln(w)
		}
	}
}

func (p *Plan) Invoke(ctx context.Context) error {
	return newExecutor(ctx, p).invoke()
}
