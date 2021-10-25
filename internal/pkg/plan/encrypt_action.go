package plan

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type EncryptAction struct {
	model.ObjectState
	object model.ObjectWithContent
	values []*UnencryptedValue
}

type UnencryptedValue struct {
	path  utils.KeyPath
	value string
}
