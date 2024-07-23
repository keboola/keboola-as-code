package model

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"

type Target struct {
	// Provider of the target data destination.
	Provider Provider `json:"provider" validate:"required"`
	// Import - file import configuration.
	Import config.ImportConfig `json:"import"`
}

type Provider string
