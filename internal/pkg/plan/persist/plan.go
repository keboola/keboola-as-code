package persist

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type Plan struct {
	actions []action
}

func (p *Plan) Empty() bool {
	return len(p.actions) == 0
}

func (p *Plan) Name() string {
	return "persist"
}

func (p *Plan) Log(writer *log.WriteCloser) {
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

func (p *Plan) Invoke(logger *zap.SugaredLogger, api *remote.StorageApi, projectState *state.State) error {
	return newExecutor(logger, api, projectState, p).invoke()
}
