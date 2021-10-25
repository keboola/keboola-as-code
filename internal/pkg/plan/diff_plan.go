package plan

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"sort"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// DiffPlan - plan based on the diff results.
type DiffPlan struct {
	*state.State
	name                string
	actions             []*DiffAction
	allowedRemoteDelete bool
	changeDescription   string
}

func (p *DiffPlan) Name() string {
	return p.name
}

func (p *DiffPlan) AllowRemoteDelete() {
	p.allowedRemoteDelete = true
}

func (p *DiffPlan) Invoke(
	logger *zap.SugaredLogger,
	api *remote.StorageApi,
	schedulerApi *scheduler.Api,
	ctx context.Context,
) error {
	executor := newDiffExecutor(p, logger, api, schedulerApi, ctx)
	return executor.invoke()
}

func (p *DiffPlan) Log(writer *log.WriteCloser) {
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

func (p *DiffPlan) Validate() error {
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

func (p *DiffPlan) add(result *diff.Result, action DiffActionType) {
	p.actions = append(p.actions, &DiffAction{Result: result, action: action})
}
