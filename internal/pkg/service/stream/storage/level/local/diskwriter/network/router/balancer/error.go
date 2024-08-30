package balancer

import "net/http"

type NoPipelineError struct{}

type PipelineNotReadyError struct{}

type NoPipelineReadyError struct{}

func (e NoPipelineError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (e NoPipelineError) ErrorName() string {
	return "noPipeline"
}

func (e NoPipelineError) Error() string {
	return "no pipeline"
}

func (e PipelineNotReadyError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (e PipelineNotReadyError) ErrorName() string {
	return "pipelineNotReady"
}

func (e PipelineNotReadyError) Error() string {
	return "pipeline is not ready"
}

func (e NoPipelineReadyError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (e NoPipelineReadyError) ErrorName() string {
	return "noPipelineReady"
}

func (e NoPipelineReadyError) Error() string {
	return "no pipeline is ready"
}
