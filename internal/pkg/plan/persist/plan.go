package persist

import (
	"context"
	"fmt"
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

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

func (p *Plan) Log(w io.Writer) {
	fmt.Fprintf(w, `Plan for "%s" operation:`, p.Name())
	fmt.Fprintln(w)
	if len(p.actions) == 0 {
		fmt.Fprintln(w, "  no new or deleted objects found")
	} else {
		for _, action := range p.actions {
			fmt.Fprintln(w, "  "+action.String())
		}
	}
}

func (p *Plan) Invoke(ctx context.Context, logger log.Logger, keboolaProjectAPI *keboola.AuthorizedAPI, projectState *state.State) error {
	return newExecutor(ctx, logger, keboolaProjectAPI, projectState, p).invoke()
}
