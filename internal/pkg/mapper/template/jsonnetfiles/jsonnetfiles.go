package jsonnetfiles

import (
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
)

type jsonnetMapper struct {
	jsonnetCtx *jsonnet.Context
}

func NewMapper(jsonnetCtx *jsonnet.Context) *jsonnetMapper {
	return &jsonnetMapper{jsonnetCtx: jsonnetCtx}
}
