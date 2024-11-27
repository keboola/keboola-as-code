package errors

import (
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type WithExceptionIDError struct {
	error
	exceptionID string
}

func GenerateExceptionID() string {
	return idgenerator.RequestID()
}

func WrapWithExceptionID(exceptionID string, err error) error {
	// Skip, if there is already an exceptionID
	var withExceptionID WithExceptionIDError
	if errors.As(err, &withExceptionID) {
		return err
	}

	// Generate exceptionID if it is empty
	if exceptionID == "" {
		exceptionID = GenerateExceptionID()
	}

	return WithExceptionIDError{error: err, exceptionID: exceptionID}
}

func (e WithExceptionIDError) Unwrap() error {
	return e.error
}

func (e WithExceptionIDError) ExceptionID() string {
	return e.exceptionID
}
