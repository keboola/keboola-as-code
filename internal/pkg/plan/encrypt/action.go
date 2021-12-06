package encrypt

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type action struct {
	model.ObjectState
	object model.ObjectWithContent
	values []*UnencryptedValue
}

type UnencryptedValue struct {
	path  orderedmap.Key
	value string
}
