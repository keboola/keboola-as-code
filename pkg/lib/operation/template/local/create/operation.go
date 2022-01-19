package create

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type ConfigDef struct {
	Key        model.ConfigKey
	TemplateId string
	Rows       []ConfigRowDef
}

type ConfigRowDef struct {
	Key        model.ConfigRowKey
	TemplateId string
}

type Options struct {
	Id          string
	Name        string
	Description string
	Configs     []ConfigDef
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
}

func Run(o Options, d dependencies) (err error) {
	return nil
}
