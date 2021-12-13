package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type Inputs []*Input

type Input struct {
	Id          string                 `json:"id" validate:"required,template-input-id"`
	Name        string                 `json:"name" validate:"required"`
	Description string                 `json:"description" validate:"required"`
	Type        string                 `json:"type" validate:"required"`
	Default     interface{}            `json:"default,omitempty" validate:"omitempty,template-input-default"`
	Kind        string                 `json:"kind" validate:"required,oneof=input password textarea confirm select multiselect"`
	Options     []Option               `json:"options,omitempty" validate:"required_if=Type select Type multiselect,template-input-options,dive,template-input-option"`
	Rules       *orderedmap.OrderedMap `json:"rules,omitempty"`
	If          string                 `json:"if,omitempty"`
}

type Option interface{}
