package jsonnetfiles

import (
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
)

type jsonNetMapper struct {
	jsonNetCtx *jsonnet.Context
}

func NewMapper(jsonNetCtx *jsonnet.Context) *jsonNetMapper {
	return &jsonNetMapper{jsonNetCtx: jsonNetCtx}
}
