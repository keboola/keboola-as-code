package create

import (
	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Options struct {
	Objects model.ObjectStates
}

func Run(o Options) (*diff.Results, error) {
	differ := diff.NewDiffer(o.Objects)
	results, err := differ.Diff()
	if err != nil {
		return nil, err
	}
	return results, nil
}
