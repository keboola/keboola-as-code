package corefiles

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
)

// coreFilesMapper performs local loading / saving of files: config.json, meta.json, description.md.
type coreFilesMapper struct {
	state *local.State
}

func NewLocalMapper(state *local.State) *coreFilesMapper {
	return &coreFilesMapper{
		state: state,
	}
}
