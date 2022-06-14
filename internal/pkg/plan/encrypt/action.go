package encrypt

import (
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type action struct {
	model.ObjectState
	object model.ObjectWithContent
	values []*UnencryptedValue
}

type UnencryptedValue struct {
	path  orderedmap.Path
	value string
}
