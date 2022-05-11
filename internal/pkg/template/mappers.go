package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/branchmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/configmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/defaultbucket"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/description"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/ignore"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/jsonnetfiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func MappersFor(s *state.State, d dependencies, ctx Context) (mapper.Mappers, error) {
	jsonNetCtx := ctx.JsonNetContext()
	replacements, err := ctx.Replacements()
	if err != nil {
		return nil, err
	}

	mappers := mapper.Mappers{
		// Template
		jsonnetfiles.NewMapper(jsonNetCtx),
		// Core files
		corefiles.NewMapper(s),
		description.NewMapper(),
		// Storage
		defaultbucket.NewMapper(s),
		// Variables
		variables.NewMapper(s),
		sharedcode.NewVariablesMapper(s),
		// Special components
		scheduler.NewMapper(s, d),
		orchestrator.NewMapper(s),
		// Native codes
		transformation.NewMapper(s),
		sharedcode.NewCodesMapper(s),
		// Shared code links
		sharedcode.NewLinksMapper(s),
		// Relations between objects
		relations.NewMapper(s),
		// Skip variables configurations that are not used in any configuration
		ignore.NewMapper(s),
		// Branch metadata
		branchmetadata.NewMapper(s, d),
		// Configurations metadata
		configmetadata.NewMapper(s, d),
		// Template
		replacevalues.NewMapper(s, replacements),
	}

	// Add metadata on "template use" operation
	if c, ok := ctx.(*UseContext); ok {
		mappers = append(mappers, metadata.NewMapper(s, c.TemplateRef(), c.InstanceId(), c.ObjectIds()))
	}

	return mappers, nil
}
