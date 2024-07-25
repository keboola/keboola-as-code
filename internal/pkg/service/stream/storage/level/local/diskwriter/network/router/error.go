package router

import "net/http"

type NoPipelineError struct{}

type NoPipelineReadyError struct{}

type NoOpenedSliceFoundError struct{}

func (e NoPipelineError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (e NoPipelineError) ErrorName() string {
	return "noPipeline"
}

func (e NoPipelineError) Error() string {
	return "no pipeline"
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

func (e NoOpenedSliceFoundError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (e NoOpenedSliceFoundError) ErrorName() string {
	return "noOpenedSliceFound"
}

func (e NoOpenedSliceFoundError) Error() string {
	return "no opened slice found"
}
