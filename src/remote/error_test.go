package remote

import (
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func TestErrorMsg1(t *testing.T) {
	e := &Error{Message: "msg", response: newResponseWithStatusCode(404)}
	assert.Equal(t, `msg, method: "GET", url: "https://example.com", httpCode: "404"`, e.Error())
}

func TestErrorMsg2(t *testing.T) {
	e := &Error{Message: "msg", ErrCode: "errCode", ExceptionId: "exceptionId", response: newResponseWithStatusCode(404)}
	assert.Equal(t, `msg, method: "GET", url: "https://example.com", httpCode: "404", errCode: "errCode", exceptionId: "exceptionId"`, e.Error())
}

func TestErrorHttpStatus(t *testing.T) {
	e := &Error{}
	e.SetResponse(newResponseWithStatusCode(123))
	assert.Equal(t, 123, e.HttpStatus())
}

func TestErrorIsBadRequest(t *testing.T) {
	e := &Error{}
	e.SetResponse(newResponseWithStatusCode(123))
	assert.False(t, e.IsBadRequest())
	e.SetResponse(newResponseWithStatusCode(400))
	assert.True(t, e.IsBadRequest())
}

func TestErrorIsUnauthorized(t *testing.T) {
	e := &Error{}
	e.SetResponse(newResponseWithStatusCode(123))
	assert.False(t, e.IsUnauthorized())
	e.SetResponse(newResponseWithStatusCode(401))
	assert.True(t, e.IsUnauthorized())
}

func TestErrorIsForbidden(t *testing.T) {
	e := &Error{}
	e.SetResponse(newResponseWithStatusCode(123))
	assert.False(t, e.IsForbidden())
	e.SetResponse(newResponseWithStatusCode(403))
	assert.True(t, e.IsForbidden())
}

func TestErrorIsNotFound(t *testing.T) {
	e := &Error{}
	e.SetResponse(newResponseWithStatusCode(123))
	assert.False(t, e.IsNotFound())
	e.SetResponse(newResponseWithStatusCode(404))
	assert.True(t, e.IsNotFound())
}

func newResponseWithStatusCode(code int) *resty.Response {
	return &resty.Response{
		Request:     &resty.Request{Method: resty.MethodGet, URL: "https://example.com"},
		RawResponse: &http.Response{StatusCode: code},
	}
}
