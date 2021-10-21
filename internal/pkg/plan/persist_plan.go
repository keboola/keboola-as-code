package plan

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
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
		writer.WriteStringNoErrIndent1("no new or deleted objects found")
	} else {
		for _, action := range actions {
			writer.WriteStringNoErrIndent1(action.String())
		}
	}
}

func (p *PersistPlan) Invoke(logger *zap.SugaredLogger, api *remote.StorageApi, projectState *state.State) error {
	return newPersistExecutor(logger, api, projectState, p).invoke()
}
