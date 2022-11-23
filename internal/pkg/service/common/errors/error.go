// Package error provides common errors for all services.
package errors

type WithStatusCode interface {
	StatusCode() int
}

type WithName interface {
	ErrorName() string
}

type WithUserMessage interface {
	ErrorUserMessage() string
}

type WithExceptionId interface {
	ErrorExceptionId() string
}

type WithLogMessage interface {
	ErrorLogMessage() string
}
