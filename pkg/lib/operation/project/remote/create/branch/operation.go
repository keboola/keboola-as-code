package branch

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type Options struct {
	Name string
	Pull bool
}

type dependencies interface {
	Logger() log.Logger
	StorageApiClient() client.Sender
}

func Run(ctx context.Context, o Options, d dependencies) (branch *storageapi.Branch, err error) {
	logger := d.Logger()

	// Create branch by API
	branch = &storageapi.Branch{Name: o.Name}
	if _, err := storageapi.CreateBranchRequest(branch).Send(ctx, d.StorageApiClient()); err != nil {
		return nil, fmt.Errorf(`cannot create branch: %w`, err)
	}

	logger.Info(fmt.Sprintf(`Created new branch "%s".`, branch.Name))
	return branch, nil
}
