package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type ReceiverBase struct {
	key.ReceiverKey
	Name   string `json:"name" validate:"required,min=1,max=40"`
	Secret string `json:"secret" validate:"required,len=48"`
}

type Receiver struct {
	ReceiverBase
	Exports []Export `validate:"dive"`
}
