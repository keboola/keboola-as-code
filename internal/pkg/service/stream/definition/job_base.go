package definition

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// Job contains all Keboola-specific data we need for polling jobs.
type Job struct {
	key.JobKey
	SoftDeletable
	StorageJobID keboola.StorageJobID
}
