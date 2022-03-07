package branch

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Options struct {
	Name string
	Pull bool
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
}

func Run(o Options, d dependencies) (branch *model.Branch, err error) {
	logger := d.Logger()

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}

	// Create branch by API
	branch = &model.Branch{Name: o.Name}
	if _, err := storageApi.CreateBranch(branch); err != nil {
		return nil, fmt.Errorf(`cannot create branch: %w`, err)
	}

	logger.Info(fmt.Sprintf(`Created new %s "%s".`, branch.Kind().Name, branch.Name))
	return branch, nil
}
