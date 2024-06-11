package model

type Target struct {
	// Provider of the target data destination.
	Provider Provider `json:"provider" validate:"required"`
}

type Provider string
