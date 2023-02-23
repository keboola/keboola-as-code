package mapper

import "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive/jsonnet"

type Mapper struct {
	bufferAPIHost     string
	templateValidator *jsonnet.Validator
}

type dependencies interface {
	BufferAPIHost() string
}

func NewMapper(d dependencies) *Mapper {
	return &Mapper{
		bufferAPIHost:     d.BufferAPIHost(),
		templateValidator: jsonnet.NewValidator(),
	}
}
