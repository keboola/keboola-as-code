package branchmetadata

import (
	"github.com/keboola/go-client/pkg/client"

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
	StorageAPIClient() client.Sender
}

func NewMapper(s *state.State, d dependencies) *branchMetadataMapper {
	return &branchMetadataMapper{dependencies: d, logger: s.Logger(), state: s}
}
