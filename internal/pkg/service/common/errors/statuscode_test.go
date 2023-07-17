package errors

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestHTTPCodeFrom(t *testing.T) {
	t.Parallel()
	assert.Equal(t, StatusClientClosedRequest, HTTPCodeFrom(context.Canceled))
	assert.Equal(t, http.StatusRequestTimeout, HTTPCodeFrom(context.DeadlineExceeded))
	assert.Equal(t, http.StatusInternalServerError, HTTPCodeFrom(errors.New("some error")))
	assert.Equal(t, http.StatusConflict, HTTPCodeFrom(NewResourceAlreadyExistsError("<what>", "<key>", "<in>")))
	assert.Equal(t, http.StatusBadRequest, HTTPCodeFrom(NewBadRequestError(errors.New("message"))))
	assert.Equal(t, http.StatusNotFound, HTTPCodeFrom(NewEndpointNotFoundError(&url.URL{Host: "host.local"})))
	assert.Equal(t, http.StatusInsufficientStorage, HTTPCodeFrom(NewInsufficientStorageError(errors.New("message"))))
	assert.Equal(t, http.StatusInternalServerError, HTTPCodeFrom(NewNotImplementedError()))
	assert.Equal(t, http.StatusRequestEntityTooLarge, HTTPCodeFrom(NewPayloadTooLargeError(errors.New("message"))))
	assert.Equal(t, http.StatusNotFound, HTTPCodeFrom(NewResourceNotFoundError("<what>", "<key>", "<in>")))
	assert.Equal(t, http.StatusUnsupportedMediaType, HTTPCodeFrom(NewUnsupportedMediaTypeError(errors.New("message"))))
}
