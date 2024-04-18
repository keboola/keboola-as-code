package errors

import "github.com/keboola/keboola-as-code/internal/pkg/idgenerator"

type ErrorWithExceptionID struct {
	error
	exceptionID string
}

func GenerateExceptionID() string {
	return idgenerator.RequestID()
}

func WrapWithExceptionID(exceptionID string, err error) error {
	// Skip, if there is already an exceptionID
	if _, ok := err.(WithExceptionID); ok {
		return err
	}

	// Generate exceptionID if it is empty
	if exceptionID == "" {
		exceptionID = GenerateExceptionID()
	}

	return ErrorWithExceptionID{error: err, exceptionID: exceptionID}
}

func (e ErrorWithExceptionID) Unwrap() error {
	return e.error
}

func (e ErrorWithExceptionID) ErrorExceptionID() string {
	return e.exceptionID
}
