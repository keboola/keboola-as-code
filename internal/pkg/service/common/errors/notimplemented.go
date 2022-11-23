package errors

type NotImplementedError struct{}

func NewNotImplementedError() NotImplementedError {
	return NotImplementedError{}
}

func (NotImplementedError) ErrorName() string {
	return "notImplemented"
}

func (NotImplementedError) Error() string {
	return "operation not implemented"
}

func (NotImplementedError) ErrorUserMessage() string {
	return "Operation not implemented."
}
