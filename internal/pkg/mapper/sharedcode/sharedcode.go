package sharedcode

import (
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/links"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/variables"
)

// NewCodesMapper saves shared codes (config rows) to "codes" local dir.
func NewCodesMapper(context mapper.Context) interface{} {
	return codes.NewMapper(context)
}

// NewVariablesMapper embeds variables config according "variables_id".
func NewVariablesMapper(context mapper.Context) interface{} {
	return variables.NewMapper(context)
}

// NewLinksMapper replaces "shared_code_id" with "shared_code_path" in local fs.
func NewLinksMapper(localManager *local.Manager, context mapper.Context) interface{} {
	return links.NewMapper(localManager, context)
}
