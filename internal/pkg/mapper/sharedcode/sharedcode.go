package sharedcode

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/links"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// NewCodesMapper saves shared codes (config rows) to "codes" local dir.
func NewCodesMapper(s *state.State) any {
	return codes.NewMapper(s)
}

// NewVariablesMapper embeds variables config according "variables_id".
func NewVariablesMapper(s *state.State) any {
	return variables.NewMapper(s)
}

// NewLinksMapper replaces "shared_code_id" with "shared_code_path" in local fs.
func NewLinksMapper(s *state.State) any {
	return links.NewMapper(s)
}
