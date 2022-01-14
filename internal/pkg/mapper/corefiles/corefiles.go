package corefiles

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// coreFilesMapper performs local loading / saving of files: config.json, meta.json, description.md.
type coreFilesMapper struct {
	state *state.State
}

func NewMapper(state *state.State) *coreFilesMapper {
	return &coreFilesMapper{
		state: state,
	}
}
