package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/defaultbucket"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/description"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/jsonnetfiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacekeys"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func MappersFor(s *state.State, d dependencies, replacements replacekeys.Keys) mapper.Mappers {
	return mapper.Mappers{
		// Template
		jsonnetfiles.NewMapper(),
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
		// Template
		replacekeys.NewMapper(s, replacements),
	}
}
