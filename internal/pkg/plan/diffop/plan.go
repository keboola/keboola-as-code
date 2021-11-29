package diffop

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// Plan is based on the diff results.
type Plan struct {
	*state.State
	name                string
	actions             []*action
	allowedRemoteDelete bool
}

func NewPlan(name string, state *state.State) *Plan {
	return &Plan{name: name, State: state}
}

func (p *Plan) Empty() bool {
	return len(p.actions) == 0
}

func (p *Plan) Name() string {
	return p.name
}

func (p *Plan) AllowRemoteDelete() {
	p.allowedRemoteDelete = true
}

func (p *Plan) Invoke(logger *zap.SugaredLogger, ctx context.Context, changeDescription string) error {
	executor := newExecutor(p, logger, ctx, changeDescription)
	return executor.invoke()
}

func (p *Plan) Log(writer *log.WriteCloser) {
	writer.WriteStringNoErr(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	actions := p.actions
	sort.SliceStable(actions, func(i, j int) bool {
		return actions[i].Path() < actions[j].Path()
	})

	if len(actions) == 0 {
		writer.WriteStringNoErrIndent1("no difference")
	} else {
		skippedDeleteCount := 0
		for _, action := range actions {
			msg := action.String()
			if !p.allowedRemoteDelete &&
				(action.action == ActionDeleteRemote) {
				msg += " - SKIPPED"
				skippedDeleteCount++
			}
			writer.WriteStringNoErrIndent1(msg)
		}

		if skippedDeleteCount > 0 {
			writer.WriteStringNoErr("Skipped remote objects deletion, use \"--force\" to delete them.")
		}
	}
}

func (p *Plan) Validate() error {
	errors := utils.NewMultiError()
	for _, action := range p.actions {
		if err := action.validate(); err != nil {
			errors.Append(err)
		}
	}

	if errors.Len() > 0 {
		return utils.PrefixError(fmt.Sprintf(`cannot perform the "%s" operation`, p.Name()), errors)
	}

	return nil
}

func (p *Plan) Add(result *diff.Result, actionType ActionType) {
	p.actions = append(p.actions, &action{Result: result, action: actionType})
}
