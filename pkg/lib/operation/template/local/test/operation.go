package test

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type Options struct {
	Path string
}

type dependencies interface {
	Logger() log.Logger
}

func Run(_ *template.Template, _ dependencies) (err error) {
	return fmt.Errorf(`not implemented`)
}
