package router

import (
	"fmt"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

type ShutdownError struct{}

type SinkNotFoundError struct {
	sinkKey key.SinkKey
}

type SinkDisabledError struct {
	sinkKey key.SinkKey
}

func (e ShutdownError) Error() string {
	return "node is shutting down"
}

func (ShutdownError) ErrorName() string {
	return "shutdown"
}

func (ShutdownError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (e SinkNotFoundError) SinkKey() key.SinkKey {
	return e.sinkKey
}

func (SinkNotFoundError) ErrorName() string {
	return "sinkNotFound"
}

func (e SinkNotFoundError) Error() string {
	return fmt.Sprintf("sink %q not found", e.sinkKey)
}

func (SinkNotFoundError) StatusCode() int {
	return http.StatusNotFound
}

func (e SinkDisabledError) SinkKey() key.SinkKey {
	return e.sinkKey
}

func (SinkDisabledError) ErrorName() string {
	return "sinkDisabled"
}

func (e SinkDisabledError) Error() string {
	return fmt.Sprintf("sink %q is disabled", e.sinkKey)
}

func (SinkDisabledError) StatusCode() int {
	return http.StatusBadRequest
}
