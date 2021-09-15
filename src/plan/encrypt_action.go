package plan

import (
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

type EncryptAction struct {
	object   model.ObjectWithContent
	manifest model.Record
	values   []*UnencryptedValue
}

type UnencryptedValue struct {
	path  utils.KeyPath
	value string
}
