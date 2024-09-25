package dispatcher

import (
	"net/http"
)

type ShutdownError struct{}

type NoSourceFoundError struct{}

type SourceDisabledError struct{}

func (e ShutdownError) Error() string {
	return "node is shutting down"
}

func (ShutdownError) ErrorName() string {
	return "shutdown"
}

func (ShutdownError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (e NoSourceFoundError) Error() string {
	return "the specified combination of projectID, sourceID and secret was not found"
}

func (e NoSourceFoundError) ErrorName() string {
	return "noSourceFound"
}

func (e NoSourceFoundError) StatusCode() int {
	return http.StatusNotFound
}

func (e SourceDisabledError) Error() string {
	return "the specified source is disabled in all branches"
}

func (e SourceDisabledError) ErrorName() string {
	return "disabledSource"
}

func (e SourceDisabledError) StatusCode() int {
	return http.StatusNotFound
}
