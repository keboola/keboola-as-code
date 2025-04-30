package branchmetadata

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// branchMetadataMapper add metadata to configurations loaded from API.
type branchMetadataMapper struct {
	dependencies
	logger log.Logger
	state  *state.State
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

func NewMapper(s *state.State, d dependencies) *branchMetadataMapper {
	return &branchMetadataMapper{dependencies: d, logger: s.Logger(), state: s}
}
