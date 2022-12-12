package model

import (
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type Token = storageapi.Token

type TokenForExport struct {
	key.ExportKey
	Token `validate:"dive"`
}
