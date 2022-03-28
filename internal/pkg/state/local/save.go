package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type saveCtx struct {
	*uow
	object        model.Object
	recipe        *model.RemoteSaveRecipe
	changedFields model.ChangedFields
	objectExists  bool
	onSuccess     func()
}

func (c *saveCtx) save() {

}
