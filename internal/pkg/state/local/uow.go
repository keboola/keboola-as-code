package local

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type uow struct {
	ctx     context.Context
	objects model.Objects
	changes *model.Changes
	errors  *utils.MultiError
}
