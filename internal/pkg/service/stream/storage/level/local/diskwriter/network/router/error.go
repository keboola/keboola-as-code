package router

import "net/http"

type NoOpenedSliceFoundError struct{}

func (e NoOpenedSliceFoundError) StatusCode() int {
	return http.StatusServiceUnavailable
}

func (e NoOpenedSliceFoundError) ErrorName() string {
	return "noOpenedSliceFound"
}

func (e NoOpenedSliceFoundError) Error() string {
	return "no opened slice found"
}
