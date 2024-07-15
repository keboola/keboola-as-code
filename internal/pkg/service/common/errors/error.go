// Package errors provides common errors for all services.
package errors

type WithStatusCode interface {
	error
	StatusCode() int
}

type WithName interface {
	ErrorName() string
}

type WithUserMessage interface {
	ErrorUserMessage() string
}

type WithExceptionID interface {
	ErrorExceptionID() string
}

type WithLogMessage interface {
	ErrorLogMessage() string
}

type WithErrorLogEnabled interface {
	ErrorLogEnabled() bool
}
