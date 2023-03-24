package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/jsonnet"
)

type Mapper struct {
	bufferAPIHost     string
	templateValidator *jsonnet.Validator
}

type dependencies interface {
	APIConfig() config.Config
}

func NewMapper(d dependencies) *Mapper {
	return &Mapper{
		bufferAPIHost:     d.APIConfig().PublicAddress.String(),
		templateValidator: jsonnet.NewValidator(),
	}
}
