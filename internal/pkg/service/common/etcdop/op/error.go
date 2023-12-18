package op

// EmptyResultError signals an empty result of the etcd operation.
// See also WithResult.WithEmptyResultAsError method.
type EmptyResultError struct {
	error
}

func (v EmptyResultError) Unwrap() error {
	return v.error
}

func NewEmptyResultError(err error) EmptyResultError {
	return EmptyResultError{error: err}
}
