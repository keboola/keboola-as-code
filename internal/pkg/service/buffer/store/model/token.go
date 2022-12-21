package model

import (
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type StorageToken = storageapi.Token

type Token struct {
	key.ExportKey
	StorageToken `validate:"dive"`
}
