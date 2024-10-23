package keboola

import (
	"github.com/keboola/go-client/pkg/keboola"
)

// Job contains all Keboola-specific data we need for polling jobs.
type Job struct {
	StorageJobID keboola.StorageJobID
}
