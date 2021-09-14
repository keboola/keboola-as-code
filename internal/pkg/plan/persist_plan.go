package plan

import (
	"fmt"

	"go.uber.org/zap"

	"keboola-as-code/src/log"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/state"
)

type PersistPlan struct {
	actions []PersistAction
}

func (p *PersistPlan) Empty() bool {
	return len(p.actions) == 0
}

func (p *PersistPlan) Name() string {
	return "persist"
}

func (p *PersistPlan) Log(writer *log.WriteCloser) {
	writer.WriteStringNoErr(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	actions := p.actions

	if len(actions) == 0 {
		writer.WriteStringNoErr("\tno new or deleted objects found")
	} else {
		for _, action := range actions {
			writer.WriteStringNoErr("\t" + action.String())
		}
	}
}

func (p *PersistPlan) Invoke(logger *zap.SugaredLogger, api *remote.StorageApi, projectState *state.State) error {
	return newPersistExecutor(logger, api, projectState, p).invoke()
}
