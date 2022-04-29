package delete_template

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
)

type Options struct {
	Branch   model.BranchKey
	Instance string
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
}

func Run(projectState *project.State, branch model.BranchKey, instance string, d dependencies) error {
	return nil
}
