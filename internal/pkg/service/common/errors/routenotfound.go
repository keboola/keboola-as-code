package errors

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type RouteNotFoundError struct {
	err error
}

func NewRouteNotFound(err error) RouteNotFoundError {
	return RouteNotFoundError{err: err}
}

func (RouteNotFoundError) StatusCode() int {
	return http.StatusNotFound
}

func (e RouteNotFoundError) Error() string {
	return e.err.Error()
}

func (RouteNotFoundError) ErrorName() string {
	return "routeNotFound"
}

func (e RouteNotFoundError) ErrorUserMessage() string {
	return errors.Format(e, errors.FormatAsSentences())
}
