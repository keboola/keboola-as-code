package variables

import (
	mapperPkg "github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/helper"
)

// mapper embeds variables config according "variables_id".
type mapper struct {
	mapperPkg.Context
	*helper.SharedCodeHelper
}

func NewMapper(context mapperPkg.Context) *mapper {
	return &mapper{Context: context, SharedCodeHelper: helper.New(context.State, context.NamingRegistry)}
}
