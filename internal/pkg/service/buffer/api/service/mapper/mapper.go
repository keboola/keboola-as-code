package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Mapper struct {
	bufferAPIHost     string
	validator         validator.Validator
	templateValidator *jsonnet.Validator
}

type dependencies interface {
	APIConfig() config.APIConfig
	Validator() validator.Validator
}

func NewMapper(d dependencies) *Mapper {
	return &Mapper{
		bufferAPIHost:     d.APIConfig().PublicAddress.String(),
		validator:         d.Validator(),
		templateValidator: jsonnet.NewValidator(),
	}
}
