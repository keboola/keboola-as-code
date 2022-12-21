package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type ExportBase struct {
	key.ExportKey
	Name             string           `json:"name" validate:"required,min=1,max=40"`
	ImportConditions ImportConditions `json:"importConditions" validate:"required"`
}

type Export struct {
	ExportBase
	Mapping    Mapping `validate:"dive"`
	Token      Token   `validate:"dive"`
	OpenedFile File    `validate:"dive"`
}
