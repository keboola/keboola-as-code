package configmetadata

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// configMetadataMapper add metadata to configurations loaded from API.
type configMetadataMapper struct {
	dependencies
	logger log.Logger
	state  *state.State
}

type dependencies interface {
	KeboolaAPIClient() *keboola.API
}

func NewMapper(s *state.State, d dependencies) *configMetadataMapper {
	return &configMetadataMapper{dependencies: d, logger: s.Logger(), state: s}
}
