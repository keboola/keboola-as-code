package upgrade

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type Options struct {
	Branch   model.BranchKey
	Instance string
	Inputs   template.InputsValues
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	EncryptionApi() (*encryptionapi.Api, error)
}

func Run(projectState *project.State, tmpl *template.Template, o Options, d dependencies) (string, error) {
	return "", fmt.Errorf("not implemented")
}
