package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type loadCtx struct {
	*uow
	onLoad func(object model.Object) bool
}

func (c loadCtx) loadAll() {

}
