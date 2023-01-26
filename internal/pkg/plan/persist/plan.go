package persist

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

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
	if len(p.actions) == 0 {
		writer.WriteStringIndent(1, "no new or deleted objects found")
	} else {
		for _, action := range p.actions {
			writer.WriteStringIndent(1, action.String())
		}
	}
}

func (p *Plan) Invoke(ctx context.Context, logger log.Logger, apiClient *keboola.API, projectState *state.State) error {
	return newExecutor(ctx, logger, apiClient, projectState, p).invoke()
}
