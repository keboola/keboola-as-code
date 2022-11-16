package model

import (
	"time"

	"github.com/c2h5oh/datasize"
)

type ImportCondition struct {
	Count int               `json:"count" validate:"min=1,max=10000000"`
	Size  datasize.ByteSize `json:"size" validate:"min=100B,max=50MB"`
	Time  time.Duration     `json:"time" validate:"min=30s,max=24h"`
}

type Export struct {
	ID               string            `json:"exportId" validate:"required,min=1,max=48"`
	Name             string            `json:"name" validate:"required,min=1,max=40"`
	ImportConditions []ImportCondition `json:"importConditions" validate:"required"`
}

type Receiver struct {
	ID        string `json:"receiverId" validate:"required,min=1,max=48"`
	ProjectID int    `json:"projectId" validate:"required"`
	Name      string `json:"name" validate:"required,min=1,max=40"`
	Secret    string `json:"secret" validate:"required,len=48"`
}
