package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// Job contains all Keboola-specific data we need for polling jobs.
// At the end throttle the sink, so it does not overloads the service.
type Job struct {
	key.JobKey
	Deleted bool   `json:"-"` // internal field to mark the entity for deletion, there is no soft delete
	Token   string `json:"token"`
}
