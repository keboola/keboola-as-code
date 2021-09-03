package plan

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"keboola-as-code/src/diff"
	"keboola-as-code/src/log"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
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

func (p *DiffPlan) Invoke(logger *zap.SugaredLogger, api *remote.StorageApi, ctx context.Context) error {
	executor := newDiffExecutor(p, logger, api, ctx)
	return executor.invoke()
}

func (p *DiffPlan) Log(writer *log.WriteCloser) {
	writer.WriteStringNoErr(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	actions := p.actions
	sort.SliceStable(actions, func(i, j int) bool {
		return actions[i].RelativePath() < actions[j].RelativePath()
	})

	if len(actions) == 0 {
		writer.WriteStringNoErr("\tno difference")
	} else {
		skippedDeleteCount := 0
		for _, action := range actions {
			msg := action.String()
			if !p.allowedRemoteDelete &&
				(action.action == ActionDeleteRemote || action.action == ActionMarkDeletedRemote) {
				msg += " - SKIPPED"
				skippedDeleteCount++
			}
			writer.WriteStringNoErr("\t" + msg)
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
