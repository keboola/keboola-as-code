package persist

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
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

func (p *Plan) Log(logger log.Logger) {
	writer := logger.InfoWriter()
	writer.WriteString(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	actions := p.actions

	if len(actions) == 0 {
		writer.WriteStringIndent(1, "no new or deleted objects found")
	} else {
		for _, action := range actions {
			writer.WriteStringIndent(1, action.String())
		}
	}
}

func (p *Plan) Invoke(logger log.Logger, api *storageapi.Api, projectState *state.State) error {
	return newExecutor(logger, api, projectState, p).invoke()
}
