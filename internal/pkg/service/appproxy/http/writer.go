package http

import (
	"net/http"
)

type StatusCodeCallback func(writer http.ResponseWriter, statusCode int)

// callbackResponseWriter implements http.ResponseWriter and allows adjusting the response right before the status code is written.
type callbackResponseWriter struct {
	http.ResponseWriter
	callback *StatusCodeCallback
}

func NewCallbackResponseWriter(writer http.ResponseWriter, callback StatusCodeCallback) http.ResponseWriter {
	return &callbackResponseWriter{
		ResponseWriter: writer,
		callback:       &callback,
	}
}

func (w *callbackResponseWriter) WriteHeader(statusCode int) {
	if w.callback != nil {
		callback := *w.callback
		w.callback = nil
		callback(w, statusCode)
	}
	w.ResponseWriter.WriteHeader(statusCode)
}
