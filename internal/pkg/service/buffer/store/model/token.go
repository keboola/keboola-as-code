package model

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type StorageToken = keboola.Token

type Token struct {
	key.ExportKey
	StorageToken `validate:"dive"`
}
