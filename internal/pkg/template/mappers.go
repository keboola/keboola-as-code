package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
)

func MappersFor(s *local.State, d dependencies, ctx Context) (local.Mappers, error) {
	//jsonNetCtx := ctx.JsonNetContext()
	//replacements, err := ctx.Replacements()
	//if err != nil {
	//	return nil, err
	//}

	mappers := local.Mappers{}

	// Add metadata on "template use" operation
	//if c, ok := ctx.(*UseContext); ok {
	//	mappers = append(mappers, metadata.NewMapper(s, c.TemplateRef(), c.InstanceId()))
	//}

	return mappers, nil
}
