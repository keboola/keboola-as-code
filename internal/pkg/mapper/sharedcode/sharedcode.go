package sharedcode

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/links"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// NewCodesMapper saves shared codes (config rows) to "codes" local dir.
func NewCodesMapper(context model.MapperContext) interface{} {
	return codes.NewMapper(context)
}

// NewVariablesMapper embeds variables config according "variables_id".
func NewVariablesMapper(context model.MapperContext) interface{} {
	return variables.NewMapper(context)
}

// NewLinksMapper replaces "shared_code_id" with "shared_code_path" in local fs.
func NewLinksMapper(context model.MapperContext) interface{} {
	return links.NewMapper(context)
}
