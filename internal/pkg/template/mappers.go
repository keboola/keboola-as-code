package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper"
)

func MappersFor(s *local.State, d dependencies, ctx Context) (mapper.Mappers, error) {
	//jsonNetCtx := ctx.JsonNetContext()
	//replacements, err := ctx.Replacements()
	//if err != nil {
	//	return nil, err
	//}

	mappers := mapper.Mappers{}

	// Add metadata on "template use" operation
	//if c, ok := ctx.(*UseContext); ok {
	//	mappers = append(mappers, metadata.NewMapper(s, c.TemplateRef(), c.InstanceId()))
	//}

	return mappers, nil
}
