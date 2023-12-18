package op

type EmptyResultError struct {
	error
}

func (v EmptyResultError) Unwrap() error {
	return v.error
}

func NewEmptyResultError(err error) EmptyResultError {
	return EmptyResultError{error: err}
}
