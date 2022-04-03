package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff"
)

type Options struct {
	Objects model.ObjectStates
}

func Run(o Options) (*diff.Result, error) {
	differ := diff.NewDiffer(o.Objects)
	results, err := differ.Diff()
	if err != nil {
		return nil, err
	}
	return results, nil
}
