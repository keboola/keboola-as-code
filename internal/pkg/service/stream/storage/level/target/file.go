package target

type Target struct {
	// Provider of the target data destination.
	Provider Provider `json:"provider" validate:"required"`
}

type Provider string

func New() Target {
	return Target{}
}
