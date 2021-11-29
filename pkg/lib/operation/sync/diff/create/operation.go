package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type Options struct {
	State *state.State
}

func Run(o Options) (*diff.Results, error) {
	differ := diff.NewDiffer(o.State)
	results, err := differ.Diff()
	if err != nil {
		return nil, err
	}
	return results, nil
}
