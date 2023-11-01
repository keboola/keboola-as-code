package definition

import "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"

type SourceType string

type Source struct {
	key.SourceKey
	Versioned
	Switchable
	SoftDeletable
	Type        SourceType  `json:"type" validate:"required,oneof=http"`
	Name        string      `json:"name" validate:"required,min=1,max=40"`
	Description string      `json:"description,omitempty" validate:"max=4096"`
	HTTP        *HTTPSource `json:"http" validate:"required_if=Type http"`
}
