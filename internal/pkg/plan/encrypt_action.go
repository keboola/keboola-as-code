package plan

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
